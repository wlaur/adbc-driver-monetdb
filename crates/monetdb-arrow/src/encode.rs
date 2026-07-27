//! Arrow to COPY BINARY conversion.
//!
//! Layouts follow MonetDB's `sql_bincopyconvert.c`, `copybinary.h`, and
//! `bincopy-backref.rst`.

use std::{
    collections::HashMap,
    fmt,
    net::{Ipv4Addr, Ipv6Addr},
    num::NonZeroUsize,
};

use arrow_array::{
    Array, BinaryArray, BinaryViewArray, BooleanArray, Date32Array, Date64Array, Decimal128Array,
    DictionaryArray, DurationMicrosecondArray, DurationMillisecondArray, DurationNanosecondArray,
    DurationSecondArray, FixedSizeBinaryArray, Float16Array, Float32Array, Float64Array, Int8Array,
    Int16Array, Int32Array, Int64Array, LargeBinaryArray, LargeStringArray, NullArray,
    PrimitiveArray, StringArray, StringViewArray, Time32MillisecondArray, Time32SecondArray,
    Time64MicrosecondArray, Time64NanosecondArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt8Array,
    UInt16Array, UInt32Array, UInt64Array,
    types::{
        ArrowDictionaryKeyType, ArrowPrimitiveType, Int8Type, Int16Type, Int32Type, Int64Type,
        UInt8Type, UInt16Type, UInt32Type, UInt64Type,
    },
};
#[cfg(target_endian = "little")]
use arrow_buffer::ToByteSlice;
use arrow_schema::{DataType, Field, TimeUnit};
use chrono::{Datelike, NaiveDate};
use monetdb::MonetType;

#[derive(Debug)]
pub enum EncodeError {
    Unsupported(DataType),
    UnsupportedMonetType(MonetType),
    InvalidValue {
        row: usize,
        message: &'static str,
    },
    TypeMismatch {
        expected: DataType,
        actual: DataType,
    },
}

impl fmt::Display for EncodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unsupported(data_type) => write!(f, "Arrow type {data_type} is not supported"),
            Self::UnsupportedMonetType(data_type) => {
                write!(
                    f,
                    "MonetDB type {data_type} is not supported for COPY BINARY encoding"
                )
            }
            Self::InvalidValue { row, message } => {
                write!(f, "invalid value at row {row}: {message}")
            }
            Self::TypeMismatch { expected, actual } => {
                write!(f, "expected Arrow type {expected}, found {actual}")
            }
        }
    }
}

impl std::error::Error for EncodeError {}

#[derive(Debug)]
pub enum ChunkedEncodeError<E> {
    Encode(EncodeError),
    Sink(E),
}

/// Estimate a column's encoded size before string backreferences.
pub fn estimated_encoded_size(field: &Field, array: &dyn Array) -> Result<usize, EncodeError> {
    if let Some(width) = fixed_wire_width(field, array.data_type())? {
        return checked_encoded_size(array.len(), width);
    }
    let size = match array.data_type() {
        DataType::Utf8 => estimated_strings(
            downcast::<StringArray>(array, DataType::Utf8)?.iter(),
            array.len(),
        ),
        DataType::LargeUtf8 => estimated_strings(
            downcast::<LargeStringArray>(array, DataType::LargeUtf8)?.iter(),
            array.len(),
        ),
        DataType::Utf8View => estimated_strings(
            downcast::<StringViewArray>(array, DataType::Utf8View)?.iter(),
            array.len(),
        ),
        DataType::Dictionary(key, _) => estimated_dictionary_strings(array, key),
        DataType::Binary => estimated_blobs(
            downcast::<BinaryArray>(array, DataType::Binary)?.iter(),
            array.len(),
        ),
        DataType::LargeBinary => estimated_blobs(
            downcast::<LargeBinaryArray>(array, DataType::LargeBinary)?.iter(),
            array.len(),
        ),
        DataType::BinaryView => estimated_blobs(
            downcast::<BinaryViewArray>(array, DataType::BinaryView)?.iter(),
            array.len(),
        ),
        DataType::FixedSizeBinary(_) => {
            let values = downcast::<FixedSizeBinaryArray>(array, array.data_type().clone())?;
            estimated_blobs(
                (0..values.len()).map(|row| (!values.is_null(row)).then(|| values.value(row))),
                values.len(),
            )
        }
        data_type => Err(EncodeError::Unsupported(data_type.clone())),
    }?;
    Ok(size)
}

pub fn fixed_encoded_width(field: &Field) -> Result<Option<usize>, EncodeError> {
    if *field.data_type() == DataType::Null {
        return Ok(Some(2));
    }
    Ok(crate::wire::fixed_wire_width(monet_type_for_field(field)?))
}

/// Encode one logical column from any number of Arrow chunks and emit bounded
/// wire-format pieces without concatenating them.
pub fn encode_column_chunks<E>(
    field: &Field,
    arrays: &[&dyn Array],
    target_bytes: NonZeroUsize,
    mut emit: impl FnMut(&[u8]) -> Result<(), E>,
) -> Result<usize, ChunkedEncodeError<E>> {
    let mut total = 0usize;
    for array in arrays {
        let estimated =
            estimated_encoded_size(field, *array).map_err(ChunkedEncodeError::Encode)?;
        let rows_per_chunk = if estimated == 0 {
            array.len().max(1)
        } else {
            target_bytes
                .get()
                .saturating_mul(array.len())
                .checked_div(estimated)
                .unwrap_or(0)
                .clamp(1, array.len().max(1))
        };
        for start in (0..array.len()).step_by(rows_per_chunk) {
            let len = (array.len() - start).min(rows_per_chunk);
            let slice = array.slice(start, len);
            if let Some(bytes) =
                borrowed_wire_bytes(field, slice.as_ref()).map_err(ChunkedEncodeError::Encode)?
            {
                total = total.checked_add(bytes.len()).ok_or_else(|| {
                    ChunkedEncodeError::Encode(EncodeError::InvalidValue {
                        row: 0,
                        message: "encoded column length overflows",
                    })
                })?;
                emit(bytes).map_err(ChunkedEncodeError::Sink)?;
            } else {
                let bytes =
                    encode_column(field, slice.as_ref()).map_err(ChunkedEncodeError::Encode)?;
                total = total.checked_add(bytes.len()).ok_or_else(|| {
                    ChunkedEncodeError::Encode(EncodeError::InvalidValue {
                        row: 0,
                        message: "encoded column length overflows",
                    })
                })?;
                emit(&bytes).map_err(ChunkedEncodeError::Sink)?;
            }
        }
    }
    Ok(total)
}

pub fn monet_type_for_field(field: &Field) -> Result<MonetType, EncodeError> {
    let extension = field
        .metadata()
        .get("ARROW:extension:name")
        .map(String::as_str);
    Ok(match field.data_type() {
        DataType::Null => MonetType::Varchar(1),
        DataType::Boolean => MonetType::Bool,
        DataType::Int8 => MonetType::TinyInt,
        DataType::Int16 => MonetType::SmallInt,
        DataType::Int32 if extension == Some("monetdb.interval_month") => MonetType::MonthInterval,
        DataType::Int32 => MonetType::Int,
        DataType::Int64 => MonetType::BigInt,
        DataType::UInt8 => MonetType::SmallInt,
        DataType::UInt16 => MonetType::Int,
        DataType::UInt32 => MonetType::BigInt,
        DataType::UInt64 if extension == Some("monetdb.oid") => MonetType::Oid,
        DataType::UInt64 => MonetType::HugeInt,
        DataType::Float16 | DataType::Float32 => MonetType::Real,
        DataType::Float64 => MonetType::Double,
        DataType::Decimal128(38, 0) if extension == Some("monetdb.hugeint") => MonetType::HugeInt,
        DataType::Decimal128(precision, scale) if *scale >= 0 => {
            MonetType::Decimal(*precision, *scale as u8)
        }
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => match extension {
            Some("arrow.json") => MonetType::Json,
            Some("monetdb.inet") => MonetType::Inet,
            Some("monetdb.inet4") => MonetType::Inet4,
            Some("monetdb.inet6") => MonetType::Inet6,
            Some("monetdb.url") => MonetType::Url,
            _ => MonetType::Varchar(0),
        },
        DataType::Dictionary(_, value)
            if matches!(
                value.as_ref(),
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
            ) =>
        {
            MonetType::Varchar(0)
        }
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => MonetType::Blob,
        DataType::FixedSizeBinary(16) if extension == Some("arrow.uuid") => MonetType::Uuid,
        DataType::FixedSizeBinary(_) => MonetType::Blob,
        DataType::Date32 | DataType::Date64 => MonetType::Date,
        DataType::Time64(_) if extension == Some("monetdb.timetz") => MonetType::TimeTz,
        DataType::Time32(_) | DataType::Time64(_) => MonetType::Time,
        DataType::Timestamp(_, timezone) if timezone.is_some() => MonetType::TimestampTz,
        DataType::Timestamp(_, _) => MonetType::Timestamp,
        DataType::Duration(_) if extension == Some("monetdb.interval_day") => {
            MonetType::DayInterval
        }
        DataType::Duration(_) => MonetType::SecInterval,
        data_type => return Err(EncodeError::Unsupported(data_type.clone())),
    })
}

pub fn sql_type_for_field(field: &Field) -> Result<String, EncodeError> {
    Ok(match monet_type_for_field(field)? {
        MonetType::Bool => "BOOLEAN".into(),
        MonetType::TinyInt => "TINYINT".into(),
        MonetType::SmallInt => "SMALLINT".into(),
        MonetType::Int => "INT".into(),
        MonetType::BigInt => "BIGINT".into(),
        MonetType::HugeInt => "HUGEINT".into(),
        MonetType::Oid => "OID".into(),
        MonetType::Decimal(precision, scale) => format!("DECIMAL({precision}, {scale})"),
        MonetType::Varchar(width) if width > 0 => format!("VARCHAR({width})"),
        MonetType::Varchar(_) => "STRING".into(),
        MonetType::Real => "REAL".into(),
        MonetType::Double => "DOUBLE".into(),
        MonetType::SecInterval => "INTERVAL SECOND".into(),
        MonetType::MonthInterval => "INTERVAL MONTH".into(),
        MonetType::DayInterval => "INTERVAL DAY".into(),
        MonetType::Time => "TIME(6)".into(),
        MonetType::TimeTz => "TIME(6) WITH TIME ZONE".into(),
        MonetType::Date => "DATE".into(),
        MonetType::Timestamp => "TIMESTAMP(6)".into(),
        MonetType::TimestampTz => "TIMESTAMP(6) WITH TIME ZONE".into(),
        MonetType::Blob => "BLOB".into(),
        MonetType::Url => "URL".into(),
        MonetType::Inet => "INET".into(),
        MonetType::Inet4 => "INET4".into(),
        MonetType::Inet6 => "INET6".into(),
        MonetType::Json => "JSON".into(),
        MonetType::Uuid => "UUID".into(),
        MonetType::Geometry => "GEOMETRY".into(),
        MonetType::Xml => "XML".into(),
    })
}

/// Encode an Arrow column for `COPY BINARY`.
///
/// `field.data_type()` and `array.data_type()` must describe the same logical
/// type; extension metadata on `field` selects MonetDB-specific wire types.
pub fn encode_column(field: &Field, array: &dyn Array) -> Result<Vec<u8>, EncodeError> {
    let monet_type = monet_type_for_field(field)?;
    let mut out = Vec::new();
    if let Some(width) = fixed_wire_width(field, array.data_type())? {
        let bytes = array
            .len()
            .checked_mul(width)
            .ok_or(EncodeError::InvalidValue {
                row: 0,
                message: "encoded column length overflows",
            })?;
        out.try_reserve_exact(bytes)
            .map_err(|_| EncodeError::InvalidValue {
                row: 0,
                message: "encoded column is too large",
            })?;
    }
    match (array.data_type(), monet_type) {
        (DataType::Utf8, MonetType::Inet4) => {
            encode_addresses(
                downcast::<StringArray>(array, DataType::Utf8)?.iter(),
                &mut out,
                |value| value.parse::<Ipv4Addr>().map(|address| address.octets()),
                "value is not a valid IPv4 address",
            )?;
            return Ok(out);
        }
        (DataType::LargeUtf8, MonetType::Inet4) => {
            encode_addresses(
                downcast::<LargeStringArray>(array, DataType::LargeUtf8)?.iter(),
                &mut out,
                |value| value.parse::<Ipv4Addr>().map(|address| address.octets()),
                "value is not a valid IPv4 address",
            )?;
            return Ok(out);
        }
        (DataType::Utf8View, MonetType::Inet4) => {
            encode_addresses(
                downcast::<StringViewArray>(array, DataType::Utf8View)?.iter(),
                &mut out,
                |value| value.parse::<Ipv4Addr>().map(|address| address.octets()),
                "value is not a valid IPv4 address",
            )?;
            return Ok(out);
        }
        (DataType::Utf8, MonetType::Inet6) => {
            encode_addresses(
                downcast::<StringArray>(array, DataType::Utf8)?.iter(),
                &mut out,
                |value| value.parse::<Ipv6Addr>().map(|address| address.octets()),
                "value is not a valid IPv6 address",
            )?;
            return Ok(out);
        }
        (DataType::LargeUtf8, MonetType::Inet6) => {
            encode_addresses(
                downcast::<LargeStringArray>(array, DataType::LargeUtf8)?.iter(),
                &mut out,
                |value| value.parse::<Ipv6Addr>().map(|address| address.octets()),
                "value is not a valid IPv6 address",
            )?;
            return Ok(out);
        }
        (DataType::Utf8View, MonetType::Inet6) => {
            encode_addresses(
                downcast::<StringViewArray>(array, DataType::Utf8View)?.iter(),
                &mut out,
                |value| value.parse::<Ipv6Addr>().map(|address| address.octets()),
                "value is not a valid IPv6 address",
            )?;
            return Ok(out);
        }
        (DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View, MonetType::Inet) => {
            return Err(EncodeError::UnsupportedMonetType(MonetType::Inet));
        }
        _ => {}
    }
    macro_rules! signed {
        ($array:ty, $type:ty, $variant:expr) => {{
            let values = downcast::<$array>(array, $variant)?;
            encode_primitive(values, <$type>::MIN, &mut out)?;
        }};
    }
    macro_rules! unsigned {
        ($array:ty, $wire:ty, $variant:expr) => {{
            let values = downcast::<$array>(array, $variant)?;
            for row in 0..values.len() {
                let is_null = values.is_null(row);
                let value: $wire = if is_null {
                    <$wire>::MIN
                } else {
                    <$wire>::from(values.value(row))
                };
                out.extend_from_slice(&value.to_le_bytes());
            }
        }};
    }
    match array.data_type() {
        DataType::Null => {
            let values = downcast::<NullArray>(array, DataType::Null)?;
            let encoded_len = values
                .len()
                .checked_mul(2)
                .ok_or(EncodeError::InvalidValue {
                    row: 0,
                    message: "NULL column is too large to encode",
                })?;
            out.resize(encoded_len, 0);
            for pair in out.chunks_exact_mut(2) {
                pair[0] = 0x80;
            }
        }
        DataType::Boolean => encode_bool(downcast(array, DataType::Boolean)?, &mut out),
        DataType::Int8 => signed!(Int8Array, i8, DataType::Int8),
        DataType::Int16 => signed!(Int16Array, i16, DataType::Int16),
        DataType::Int32 => signed!(Int32Array, i32, DataType::Int32),
        DataType::Int64 => signed!(Int64Array, i64, DataType::Int64),
        DataType::UInt8 => unsigned!(UInt8Array, i16, DataType::UInt8),
        DataType::UInt16 => unsigned!(UInt16Array, i32, DataType::UInt16),
        DataType::UInt32 => unsigned!(UInt32Array, i64, DataType::UInt32),
        DataType::UInt64 if monet_type_for_field(field)? == MonetType::Oid => {
            encode_oid(downcast(array, DataType::UInt64)?, &mut out)?
        }
        DataType::UInt64 => unsigned!(UInt64Array, i128, DataType::UInt64),
        DataType::Float16 => encode_f16(downcast(array, DataType::Float16)?, &mut out)?,
        DataType::Float32 => encode_f32(downcast(array, DataType::Float32)?, &mut out)?,
        DataType::Float64 => encode_f64(downcast(array, DataType::Float64)?, &mut out)?,
        DataType::Decimal128(precision, _) => {
            let enforce_precision = monet_type_for_field(field)? != MonetType::HugeInt;
            encode_decimal(
                downcast(array, array.data_type().clone())?,
                *precision,
                enforce_precision,
                &mut out,
            )?
        }
        DataType::Utf8 => {
            let values = downcast::<StringArray>(array, DataType::Utf8)?;
            reserve_strings(values.value_offsets(), values.len(), &mut out)?;
            encode_strings(values.iter(), &mut out)?;
        }
        DataType::LargeUtf8 => {
            let values = downcast::<LargeStringArray>(array, DataType::LargeUtf8)?;
            reserve_strings(values.value_offsets(), values.len(), &mut out)?;
            encode_strings(values.iter(), &mut out)?;
        }
        DataType::Utf8View => encode_strings(
            downcast::<StringViewArray>(array, DataType::Utf8View)?.iter(),
            &mut out,
        )?,
        DataType::Dictionary(key, _) => encode_dictionary(array, key, &mut out)?,
        DataType::Binary => encode_blobs(
            downcast::<BinaryArray>(array, DataType::Binary)?.iter(),
            &mut out,
        ),
        DataType::LargeBinary => encode_blobs(
            downcast::<LargeBinaryArray>(array, DataType::LargeBinary)?.iter(),
            &mut out,
        ),
        DataType::BinaryView => encode_blobs(
            downcast::<BinaryViewArray>(array, DataType::BinaryView)?.iter(),
            &mut out,
        ),
        DataType::FixedSizeBinary(16) if monet_type_for_field(field)? == MonetType::Uuid => {
            encode_uuid(downcast(array, DataType::FixedSizeBinary(16))?, &mut out)?
        }
        DataType::FixedSizeBinary(_) => {
            let values = downcast::<FixedSizeBinaryArray>(array, array.data_type().clone())?;
            encode_blobs(
                (0..values.len()).map(|row| (!values.is_null(row)).then(|| values.value(row))),
                &mut out,
            );
        }
        DataType::Date32 => encode_date32(downcast(array, DataType::Date32)?, &mut out)?,
        DataType::Date64 => encode_date64(downcast(array, DataType::Date64)?, &mut out)?,
        DataType::Time32(unit) => encode_time32(array, *unit, &mut out)?,
        DataType::Time64(unit) => encode_time64(array, *unit, &mut out)?,
        DataType::Timestamp(unit, _) => encode_timestamp(array, *unit, &mut out)?,
        DataType::Duration(unit) => encode_duration(
            array,
            *unit,
            monet_type_for_field(field)? == MonetType::DayInterval,
            &mut out,
        )?,
        data_type => return Err(EncodeError::Unsupported(data_type.clone())),
    }
    Ok(out)
}

fn encode_addresses<'a, const WIDTH: usize, E>(
    values: impl Iterator<Item = Option<&'a str>>,
    out: &mut Vec<u8>,
    parse: impl Fn(&str) -> Result<[u8; WIDTH], E>,
    invalid_message: &'static str,
) -> Result<(), EncodeError> {
    for (row, value) in values.enumerate() {
        let octets = match value {
            None => [0; WIDTH],
            Some(value) => parse(value).map_err(|_| EncodeError::InvalidValue {
                row,
                message: invalid_message,
            })?,
        };
        if value.is_some() && octets.iter().all(|&byte| byte == 0) {
            return Err(EncodeError::InvalidValue {
                row,
                message: "address collides with MonetDB's NULL sentinel",
            });
        }
        out.extend_from_slice(&octets);
    }
    Ok(())
}

fn fixed_wire_width(field: &Field, data_type: &DataType) -> Result<Option<usize>, EncodeError> {
    if *data_type == DataType::Null {
        return Ok(Some(2));
    }
    fixed_encoded_width(field)
}

fn checked_encoded_size(rows: usize, width: usize) -> Result<usize, EncodeError> {
    rows.checked_mul(width).ok_or(EncodeError::InvalidValue {
        row: 0,
        message: "encoded column length overflows",
    })
}

fn estimated_strings<'a>(
    mut values: impl Iterator<Item = Option<&'a str>>,
    rows: usize,
) -> Result<usize, EncodeError> {
    values.try_fold(0usize, |total, value| {
        let bytes = value.map_or(2, |value| value.len().saturating_add(1));
        total.checked_add(bytes).ok_or(EncodeError::InvalidValue {
            row: rows,
            message: "encoded string column length overflows",
        })
    })
}

fn estimated_dictionary_strings(array: &dyn Array, key: &DataType) -> Result<usize, EncodeError> {
    macro_rules! keys {
        ($type:ty) => {{
            let values = downcast::<DictionaryArray<$type>>(array, array.data_type().clone())?;
            estimated_dictionary_values(values)
        }};
    }
    match key {
        DataType::Int8 => keys!(Int8Type),
        DataType::Int16 => keys!(Int16Type),
        DataType::Int32 => keys!(Int32Type),
        DataType::Int64 => keys!(Int64Type),
        DataType::UInt8 => keys!(UInt8Type),
        DataType::UInt16 => keys!(UInt16Type),
        DataType::UInt32 => keys!(UInt32Type),
        DataType::UInt64 => keys!(UInt64Type),
        _ => Err(EncodeError::Unsupported(array.data_type().clone())),
    }
}

fn estimated_dictionary_values<K: ArrowDictionaryKeyType>(
    array: &DictionaryArray<K>,
) -> Result<usize, EncodeError> {
    macro_rules! values {
        ($array:ty) => {{
            let values = array
                .values()
                .as_any()
                .downcast_ref::<$array>()
                .ok_or_else(|| EncodeError::Unsupported(array.data_type().clone()))?;
            for row in 0..array.len() {
                if array.key(row).is_some_and(|key| key >= values.len()) {
                    return Err(EncodeError::InvalidValue {
                        row,
                        message: "dictionary key is outside the dictionary values",
                    });
                }
            }
            estimated_strings(
                (0..array.len()).map(|row| {
                    array
                        .key(row)
                        .filter(|&key| !values.is_null(key))
                        .map(|key| values.value(key))
                }),
                array.len(),
            )
        }};
    }
    match array.values().data_type() {
        DataType::Utf8 => values!(StringArray),
        DataType::LargeUtf8 => values!(LargeStringArray),
        DataType::Utf8View => values!(StringViewArray),
        _ => Err(EncodeError::Unsupported(array.data_type().clone())),
    }
}

fn estimated_blobs<'a>(
    mut values: impl Iterator<Item = Option<&'a [u8]>>,
    rows: usize,
) -> Result<usize, EncodeError> {
    values.try_fold(0usize, |total, value| {
        let bytes = 8usize.saturating_add(value.map_or(0, <[u8]>::len));
        total.checked_add(bytes).ok_or(EncodeError::InvalidValue {
            row: rows,
            message: "encoded BLOB column length overflows",
        })
    })
}

#[cfg(target_endian = "little")]
fn borrowed_wire_bytes<'a>(
    field: &Field,
    array: &'a dyn Array,
) -> Result<Option<&'a [u8]>, EncodeError> {
    macro_rules! signed {
        ($array:ty, $type:ty, $variant:expr) => {{
            let values = downcast::<$array>(array, $variant)?;
            for row in 0..values.len() {
                if values.value(row) == <$type>::MIN {
                    return if values.is_null(row) {
                        Ok(None)
                    } else {
                        Err(EncodeError::InvalidValue {
                            row,
                            message: "value collides with MonetDB's NULL sentinel",
                        })
                    };
                }
            }
            if values.null_count() == 0 {
                Some(values.values().inner().as_slice())
            } else {
                None
            }
        }};
    }
    let bytes = match array.data_type() {
        DataType::Int8 => signed!(Int8Array, i8, DataType::Int8),
        DataType::Int16 => signed!(Int16Array, i16, DataType::Int16),
        DataType::Int32 => signed!(Int32Array, i32, DataType::Int32),
        DataType::Int64 => signed!(Int64Array, i64, DataType::Int64),
        DataType::Float32 => {
            let values = downcast::<Float32Array>(array, DataType::Float32)?;
            for row in 0..values.len() {
                if !values.is_null(row) && !values.value(row).is_finite() {
                    return Err(EncodeError::InvalidValue {
                        row,
                        message: "non-finite floats are outside MonetDB's supported value domain",
                    });
                }
            }
            (values.null_count() == 0).then(|| values.values().inner().as_slice())
        }
        DataType::Float64 => {
            let values = downcast::<Float64Array>(array, DataType::Float64)?;
            for row in 0..values.len() {
                if !values.is_null(row) && !values.value(row).is_finite() {
                    return Err(EncodeError::InvalidValue {
                        row,
                        message: "non-finite floats are outside MonetDB's supported value domain",
                    });
                }
            }
            (values.null_count() == 0).then(|| values.values().inner().as_slice())
        }
        _ => None,
    };
    let _ = field;
    Ok(bytes)
}

#[cfg(target_endian = "big")]
fn borrowed_wire_bytes<'a>(
    _field: &Field,
    _array: &'a dyn Array,
) -> Result<Option<&'a [u8]>, EncodeError> {
    Ok(None)
}

fn downcast<T: 'static>(array: &dyn Array, expected: DataType) -> Result<&T, EncodeError> {
    array
        .as_any()
        .downcast_ref()
        .ok_or_else(|| EncodeError::TypeMismatch {
            expected,
            actual: array.data_type().clone(),
        })
}

fn encode_primitive<T: ArrowPrimitiveType>(
    array: &PrimitiveArray<T>,
    null: T::Native,
    out: &mut Vec<u8>,
) -> Result<(), EncodeError>
where
    T::Native: PartialEq + WireInteger,
{
    for row in 0..array.len() {
        if !array.is_null(row) && array.value(row) == null {
            return Err(EncodeError::InvalidValue {
                row,
                message: "value collides with MonetDB's NULL sentinel",
            });
        }
    }
    #[cfg(target_endian = "little")]
    copy_native_with_nulls(array, null.to_byte_slice(), out);
    #[cfg(target_endian = "big")]
    for row in 0..array.len() {
        let value = if array.is_null(row) {
            null
        } else {
            array.value(row)
        };
        value.append_le(out);
    }
    Ok(())
}

trait WireInteger {
    #[cfg(target_endian = "big")]
    fn append_le(self, out: &mut Vec<u8>);
}

macro_rules! wire_integer {
    ($($type:ty),+ $(,)?) => {
        $(
            impl WireInteger for $type {
                #[cfg(target_endian = "big")]
                fn append_le(self, out: &mut Vec<u8>) {
                    out.extend_from_slice(&self.to_le_bytes());
                }
            }
        )+
    };
}

wire_integer!(i8, i16, i32, i64);

#[cfg(target_endian = "little")]
fn copy_native_with_nulls<T: ArrowPrimitiveType>(
    array: &PrimitiveArray<T>,
    null: &[u8],
    out: &mut Vec<u8>,
) {
    let start = out.len();
    out.extend_from_slice(array.values().inner().as_slice());
    if array.null_count() == 0 {
        return;
    }
    let width = std::mem::size_of::<T::Native>();
    for row in 0..array.len() {
        if array.is_null(row) {
            let offset = start + row * width;
            out[offset..offset + width].copy_from_slice(null);
        }
    }
}

fn encode_bool(array: &BooleanArray, out: &mut Vec<u8>) {
    out.extend((0..array.len()).map(|row| {
        if array.is_null(row) {
            0x80
        } else {
            u8::from(array.value(row))
        }
    }));
}

fn encode_oid(array: &UInt64Array, out: &mut Vec<u8>) -> Result<(), EncodeError> {
    const NULL: u64 = 1 << 63;
    // `gdk_atoms.h` reserves exactly 2^63 as nil. The driver conservatively
    // rejects the whole upper half so values cannot cross signed MAPI paths.
    for row in 0..array.len() {
        let value = if array.is_null(row) {
            NULL
        } else {
            let value = array.value(row);
            if value >= NULL {
                return Err(EncodeError::InvalidValue {
                    row,
                    message: "OID is outside MonetDB's non-NULL range",
                });
            }
            value
        };
        out.extend_from_slice(&value.to_le_bytes());
    }
    Ok(())
}

fn encode_f32(array: &Float32Array, out: &mut Vec<u8>) -> Result<(), EncodeError> {
    for row in 0..array.len() {
        if !array.is_null(row) {
            let value = array.value(row);
            if !value.is_finite() {
                return Err(EncodeError::InvalidValue {
                    row,
                    message: "non-finite floats are outside MonetDB's supported value domain",
                });
            }
        }
    }
    #[cfg(target_endian = "little")]
    copy_native_with_nulls(array, &f32::NAN.to_le_bytes(), out);
    #[cfg(target_endian = "big")]
    for row in 0..array.len() {
        let value = if array.is_null(row) {
            f32::NAN
        } else {
            array.value(row)
        };
        out.extend_from_slice(&value.to_le_bytes());
    }
    Ok(())
}

fn encode_f16(array: &Float16Array, out: &mut Vec<u8>) -> Result<(), EncodeError> {
    for row in 0..array.len() {
        let value = if array.is_null(row) {
            f32::NAN
        } else {
            let value = f32::from(array.value(row));
            if !value.is_finite() {
                return Err(EncodeError::InvalidValue {
                    row,
                    message: "non-finite floats are outside MonetDB's supported value domain",
                });
            }
            value
        };
        out.extend_from_slice(&value.to_le_bytes());
    }
    Ok(())
}

fn encode_f64(array: &Float64Array, out: &mut Vec<u8>) -> Result<(), EncodeError> {
    for row in 0..array.len() {
        if !array.is_null(row) {
            let value = array.value(row);
            if !value.is_finite() {
                return Err(EncodeError::InvalidValue {
                    row,
                    message: "non-finite floats are outside MonetDB's supported value domain",
                });
            }
        }
    }
    #[cfg(target_endian = "little")]
    copy_native_with_nulls(array, &f64::NAN.to_le_bytes(), out);
    #[cfg(target_endian = "big")]
    for row in 0..array.len() {
        let value = if array.is_null(row) {
            f64::NAN
        } else {
            array.value(row)
        };
        out.extend_from_slice(&value.to_le_bytes());
    }
    Ok(())
}

fn encode_decimal(
    array: &Decimal128Array,
    precision: u8,
    enforce_precision: bool,
    out: &mut Vec<u8>,
) -> Result<(), EncodeError> {
    if !(1..=38).contains(&precision) {
        return Err(EncodeError::InvalidValue {
            row: 0,
            message: "decimal precision must be 1..=38",
        });
    }
    let limit = enforce_precision.then(|| 10i128.pow(u32::from(precision)));
    for row in 0..array.len() {
        let value = if array.is_null(row) {
            None
        } else {
            Some(array.value(row))
        };
        if value.is_some_and(|value| limit.is_some_and(|limit| value <= -limit || value >= limit)) {
            return Err(EncodeError::InvalidValue {
                row,
                message: "decimal value exceeds its declared precision",
            });
        }
        macro_rules! narrow {
            ($type:ty) => {{
                let wire = match value {
                    None => <$type>::MIN,
                    Some(value) => {
                        <$type>::try_from(value).map_err(|_| EncodeError::InvalidValue {
                            row,
                            message: "decimal does not fit its MonetDB backing integer",
                        })?
                    }
                };
                if value.is_some() && wire == <$type>::MIN {
                    return Err(EncodeError::InvalidValue {
                        row,
                        message: "decimal collides with MonetDB's NULL sentinel",
                    });
                }
                out.extend_from_slice(&wire.to_le_bytes());
            }};
        }
        match precision {
            1..=2 => narrow!(i8),
            3..=4 => narrow!(i16),
            5..=9 => narrow!(i32),
            10..=18 => narrow!(i64),
            19..=38 => narrow!(i128),
            _ => unreachable!("precision was validated before encoding"),
        }
    }
    Ok(())
}

fn encode_strings<'a>(
    values: impl Iterator<Item = Option<&'a str>>,
    out: &mut Vec<u8>,
) -> Result<(), EncodeError> {
    const CARDINALITY_SAMPLE_ROWS: usize = 4096;
    let (minimum_rows, maximum_rows) = values.size_hint();
    let total_rows = maximum_rows.unwrap_or(minimum_rows);
    let sample_rows = total_rows.min(CARDINALITY_SAMPLE_ROWS);
    let mut seen = HashMap::<&str, usize>::with_capacity(sample_rows);
    let mut null = None;
    for (row, value) in values.enumerate() {
        match value {
            Some(value) if value.as_bytes().contains(&0) => {
                return Err(EncodeError::InvalidValue {
                    row,
                    message: "string contains a NUL byte",
                });
            }
            Some(value) => match seen.insert(value, row) {
                Some(previous) => append_backref(row - previous, out),
                None => {
                    out.extend_from_slice(value.as_bytes());
                    out.push(0);
                }
            },
            None => {
                match null {
                    Some(previous) if row - previous < 64 => append_backref(row - previous, out),
                    _ => out.extend_from_slice(&[0x80, 0]),
                }
                null = Some(row);
            }
        }
        if total_rows > CARDINALITY_SAMPLE_ROWS
            && row + 1 == sample_rows
            && seen.len() >= sample_rows * 3 / 4
        {
            seen.reserve(total_rows.saturating_sub(seen.len()));
        }
    }
    Ok(())
}

fn reserve_strings<T: Copy + Into<i64>>(
    offsets: &[T],
    rows: usize,
    out: &mut Vec<u8>,
) -> Result<(), EncodeError> {
    let bytes = match (offsets.first(), offsets.last()) {
        (Some(first), Some(last)) => (*last).into().checked_sub((*first).into()),
        _ => Some(0),
    }
    .and_then(|bytes| usize::try_from(bytes).ok())
    .and_then(|bytes| bytes.checked_add(rows))
    .ok_or(EncodeError::InvalidValue {
        row: 0,
        message: "encoded string column length overflows",
    })?;
    out.try_reserve_exact(bytes)
        .map_err(|_| EncodeError::InvalidValue {
            row: 0,
            message: "encoded string column is too large",
        })
}

fn append_backref(mut distance: usize, out: &mut Vec<u8>) {
    if distance <= 63 {
        out.push(0x80 + distance as u8);
        return;
    }
    out.push(0x80);
    loop {
        let chunk = (distance & 0x7f) as u8;
        distance >>= 7;
        out.push(chunk | if distance == 0 { 0 } else { 0x80 });
        if distance == 0 {
            break;
        }
    }
}

fn encode_dictionary(
    array: &dyn Array,
    key: &DataType,
    out: &mut Vec<u8>,
) -> Result<(), EncodeError> {
    macro_rules! keys {
        ($type:ty) => {{
            let values = downcast::<DictionaryArray<$type>>(array, array.data_type().clone())?;
            encode_dictionary_strings(values, out)
        }};
    }
    match key {
        DataType::Int8 => keys!(Int8Type),
        DataType::Int16 => keys!(Int16Type),
        DataType::Int32 => keys!(Int32Type),
        DataType::Int64 => keys!(Int64Type),
        DataType::UInt8 => keys!(UInt8Type),
        DataType::UInt16 => keys!(UInt16Type),
        DataType::UInt32 => keys!(UInt32Type),
        DataType::UInt64 => keys!(UInt64Type),
        _ => Err(EncodeError::Unsupported(array.data_type().clone())),
    }
}

fn encode_dictionary_strings<K: ArrowDictionaryKeyType>(
    array: &DictionaryArray<K>,
    out: &mut Vec<u8>,
) -> Result<(), EncodeError> {
    macro_rules! values {
        ($array:ty) => {{
            let values = array
                .values()
                .as_any()
                .downcast_ref::<$array>()
                .ok_or_else(|| EncodeError::Unsupported(array.data_type().clone()))?;
            for row in 0..array.len() {
                if array.key(row).is_some_and(|key| key >= values.len()) {
                    return Err(EncodeError::InvalidValue {
                        row,
                        message: "dictionary key is outside the dictionary values",
                    });
                }
            }
            encode_strings(
                (0..array.len()).map(|row| {
                    array
                        .key(row)
                        .filter(|&key| !values.is_null(key))
                        .map(|key| values.value(key))
                }),
                out,
            )
        }};
    }
    match array.values().data_type() {
        DataType::Utf8 => values!(StringArray),
        DataType::LargeUtf8 => values!(LargeStringArray),
        DataType::Utf8View => values!(StringViewArray),
        _ => Err(EncodeError::Unsupported(array.data_type().clone())),
    }
}

fn encode_blobs<'a>(values: impl Iterator<Item = Option<&'a [u8]>>, out: &mut Vec<u8>) {
    for value in values {
        match value {
            None => out.extend_from_slice(&(-1i64).to_le_bytes()),
            Some(value) => {
                out.extend_from_slice(&(value.len() as i64).to_le_bytes());
                out.extend_from_slice(value);
            }
        }
    }
}

fn encode_uuid(array: &FixedSizeBinaryArray, out: &mut Vec<u8>) -> Result<(), EncodeError> {
    for row in 0..array.len() {
        if array.is_null(row) {
            out.extend_from_slice(&[0; 16]);
        } else if array.value(row).iter().all(|&byte| byte == 0) {
            return Err(EncodeError::InvalidValue {
                row,
                message: "all-zero UUID is MonetDB's NULL sentinel",
            });
        } else {
            out.extend_from_slice(array.value(row));
        }
    }
    Ok(())
}

fn encode_date32(array: &Date32Array, out: &mut Vec<u8>) -> Result<(), EncodeError> {
    for row in 0..array.len() {
        if array.is_null(row) {
            out.extend_from_slice(&[0xff; 4]);
        } else {
            append_date(i64::from(array.value(row)), row, out)?;
        }
    }
    Ok(())
}

fn encode_date64(array: &Date64Array, out: &mut Vec<u8>) -> Result<(), EncodeError> {
    for row in 0..array.len() {
        if array.is_null(row) {
            out.extend_from_slice(&[0xff; 4]);
        } else if array.value(row) % 86_400_000 != 0 {
            return Err(EncodeError::InvalidValue {
                row,
                message: "Date64 is not a whole day",
            });
        } else {
            append_date(array.value(row) / 86_400_000, row, out)?;
        }
    }
    Ok(())
}

fn append_date(days: i64, row: usize, out: &mut Vec<u8>) -> Result<(), EncodeError> {
    let date = date_from_days(days, row)?;
    // MonetDB's `gdk_time.c` defines YEAR_MIN as -4712; its binary importer
    // silently converts earlier dates to nil.
    if date.year() < -4712 {
        return Err(EncodeError::InvalidValue {
            row,
            message: "date is earlier than MonetDB's minimum year -4712",
        });
    }
    let year = i16::try_from(date.year()).map_err(|_| EncodeError::InvalidValue {
        row,
        message: "date year is outside MonetDB's range",
    })?;
    out.extend_from_slice(&[date.day() as u8, date.month() as u8]);
    out.extend_from_slice(&year.to_le_bytes());
    Ok(())
}

fn encode_time32(array: &dyn Array, unit: TimeUnit, out: &mut Vec<u8>) -> Result<(), EncodeError> {
    match unit {
        TimeUnit::Second => encode_time_values(
            downcast::<Time32SecondArray>(array, DataType::Time32(unit))?,
            1_000_000,
            out,
        ),
        TimeUnit::Millisecond => encode_time_values(
            downcast::<Time32MillisecondArray>(array, DataType::Time32(unit))?,
            1_000,
            out,
        ),
        _ => Err(EncodeError::Unsupported(DataType::Time32(unit))),
    }
}

fn encode_time64(array: &dyn Array, unit: TimeUnit, out: &mut Vec<u8>) -> Result<(), EncodeError> {
    match unit {
        TimeUnit::Microsecond => encode_time_values(
            downcast::<Time64MicrosecondArray>(array, DataType::Time64(unit))?,
            1,
            out,
        ),
        TimeUnit::Nanosecond => {
            let values = downcast::<Time64NanosecondArray>(array, DataType::Time64(unit))?;
            for row in 0..values.len() {
                if values.is_null(row) {
                    out.extend_from_slice(&[0xff; 8]);
                } else if values.value(row) % 1_000 != 0 {
                    return Err(EncodeError::InvalidValue {
                        row,
                        message: "time is not microsecond-aligned",
                    });
                } else {
                    append_time(values.value(row) / 1_000, row, out)?;
                }
            }
            Ok(())
        }
        _ => Err(EncodeError::Unsupported(DataType::Time64(unit))),
    }
}

fn encode_time_values<T>(
    array: &PrimitiveArray<T>,
    factor: i64,
    out: &mut Vec<u8>,
) -> Result<(), EncodeError>
where
    T: ArrowPrimitiveType,
    T::Native: Into<i64>,
{
    for row in 0..array.len() {
        if array.is_null(row) {
            out.extend_from_slice(&[0xff; 8]);
        } else {
            append_time(array.value(row).into() * factor, row, out)?;
        }
    }
    Ok(())
}

fn append_time(micros: i64, row: usize, out: &mut Vec<u8>) -> Result<(), EncodeError> {
    if !(0..86_400_000_000).contains(&micros) {
        return Err(EncodeError::InvalidValue {
            row,
            message: "time is outside one day",
        });
    }
    let seconds = micros / 1_000_000;
    out.extend_from_slice(&((micros % 1_000_000) as u32).to_le_bytes());
    out.extend_from_slice(&[
        (seconds % 60) as u8,
        ((seconds / 60) % 60) as u8,
        (seconds / 3_600) as u8,
        0,
    ]);
    Ok(())
}

fn encode_timestamp(
    array: &dyn Array,
    unit: TimeUnit,
    out: &mut Vec<u8>,
) -> Result<(), EncodeError> {
    macro_rules! timestamps {
        ($array:ty, $factor:expr) => {{
            encode_timestamp_values(
                downcast::<$array>(array, array.data_type().clone())?,
                $factor,
                out,
            )
        }};
    }
    match unit {
        TimeUnit::Second => timestamps!(TimestampSecondArray, 1_000_000),
        TimeUnit::Millisecond => timestamps!(TimestampMillisecondArray, 1_000),
        TimeUnit::Microsecond => timestamps!(TimestampMicrosecondArray, 1),
        TimeUnit::Nanosecond => {
            let values = downcast::<TimestampNanosecondArray>(array, array.data_type().clone())?;
            for row in 0..values.len() {
                if values.is_null(row) {
                    out.extend_from_slice(&[0xff; 12]);
                } else if values.value(row) % 1_000 != 0 {
                    return Err(EncodeError::InvalidValue {
                        row,
                        message: "timestamp is not microsecond-aligned",
                    });
                } else {
                    append_timestamp(values.value(row) / 1_000, row, out)?;
                }
            }
            Ok(())
        }
    }
}

fn encode_timestamp_values<T>(
    array: &PrimitiveArray<T>,
    factor: i64,
    out: &mut Vec<u8>,
) -> Result<(), EncodeError>
where
    T: ArrowPrimitiveType<Native = i64>,
{
    for row in 0..array.len() {
        if array.is_null(row) {
            out.extend_from_slice(&[0xff; 12]);
        } else {
            append_timestamp(
                array
                    .value(row)
                    .checked_mul(factor)
                    .ok_or(EncodeError::InvalidValue {
                        row,
                        message: "timestamp overflows microseconds",
                    })?,
                row,
                out,
            )?;
        }
    }
    Ok(())
}

fn append_timestamp(micros: i64, row: usize, out: &mut Vec<u8>) -> Result<(), EncodeError> {
    append_time(micros.rem_euclid(86_400_000_000), row, out)?;
    append_date(micros.div_euclid(86_400_000_000), row, out)
}

fn encode_duration(
    array: &dyn Array,
    unit: TimeUnit,
    require_whole_days: bool,
    out: &mut Vec<u8>,
) -> Result<(), EncodeError> {
    macro_rules! durations {
        ($array:ty, $mul:expr, $div:expr) => {{
            encode_duration_values(
                downcast::<$array>(array, DataType::Duration(unit))?,
                $mul,
                $div,
                require_whole_days,
                out,
            )
        }};
    }
    match unit {
        TimeUnit::Second => durations!(DurationSecondArray, 1_000, 1),
        TimeUnit::Millisecond => durations!(DurationMillisecondArray, 1, 1),
        TimeUnit::Microsecond => durations!(DurationMicrosecondArray, 1, 1_000),
        TimeUnit::Nanosecond => durations!(DurationNanosecondArray, 1, 1_000_000),
    }
}

fn encode_duration_values<T>(
    array: &PrimitiveArray<T>,
    multiplier: i64,
    divisor: i64,
    require_whole_days: bool,
    out: &mut Vec<u8>,
) -> Result<(), EncodeError>
where
    T: ArrowPrimitiveType<Native = i64>,
{
    for row in 0..array.len() {
        let value = if array.is_null(row) {
            i64::MIN
        } else if array.value(row) % divisor != 0 {
            return Err(EncodeError::InvalidValue {
                row,
                message: "duration is not millisecond-aligned",
            });
        } else {
            let value = (array.value(row) / divisor).checked_mul(multiplier).ok_or(
                EncodeError::InvalidValue {
                    row,
                    message: "duration overflows milliseconds",
                },
            )?;
            if value == i64::MIN {
                return Err(EncodeError::InvalidValue {
                    row,
                    message: "duration collides with MonetDB's NULL sentinel",
                });
            }
            if require_whole_days && value % 86_400_000 != 0 {
                return Err(EncodeError::InvalidValue {
                    row,
                    message: "INTERVAL DAY value is not a whole day",
                });
            }
            value
        };
        out.extend_from_slice(&value.to_le_bytes());
    }
    Ok(())
}

fn date_from_days(days: i64, row: usize) -> Result<NaiveDate, EncodeError> {
    crate::wire::date_from_unix_days(days).ok_or(EncodeError::InvalidValue {
        row,
        message: "date is outside chrono's range",
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::ArrayRef;
    use proptest::prelude::*;

    use crate::decode::decode_strings;

    use super::*;

    fn string_values() -> impl Strategy<Value = Vec<Option<String>>> {
        let arbitrary = proptest::string::string_regex("[^\\x00]{0,80}")
            .expect("the property-test regex is valid")
            .prop_map(Some);
        prop::collection::vec(
            prop_oneof![
                2 => Just(None),
                3 => Just(Some(String::new())),
                3 => Just(Some("repeated".to_owned())),
                3 => Just(Some("数据库".to_owned())),
                9 => arbitrary,
            ],
            0..300,
        )
    }

    proptest! {
        #[test]
        fn string_codec_round_trips(values in string_values()) {
            let mut encoded = Vec::new();
            encode_strings(values.iter().map(Option::as_deref), &mut encoded).unwrap();
            let decoded = decode_strings(&encoded, values.len()).unwrap();
            prop_assert_eq!(decoded.iter().collect::<Vec<_>>(), values.iter().map(Option::as_deref).collect::<Vec<_>>());
        }
    }

    #[test]
    fn encodes_signed_values_and_nulls() {
        let field = Field::new("i", DataType::Int32, true);
        let array = Int32Array::from(vec![Some(1), None, Some(-2)]);
        let expected = [
            1i32.to_le_bytes(),
            i32::MIN.to_le_bytes(),
            (-2i32).to_le_bytes(),
        ]
        .concat();
        assert_eq!(encode_column(&field, &array).unwrap(), expected);
    }

    #[test]
    fn reports_fixed_encoded_widths_without_array_data() {
        assert_eq!(
            fixed_encoded_width(&Field::new("i", DataType::Int32, true)).unwrap(),
            Some(4)
        );
        assert_eq!(
            fixed_encoded_width(&Field::new("s", DataType::Utf8, true)).unwrap(),
            None
        );
        assert_eq!(
            fixed_encoded_width(&Field::new("n", DataType::Null, true)).unwrap(),
            Some(2)
        );
    }

    #[test]
    fn chunked_encoding_spans_arrays_without_concatenating_them() {
        let field = Field::new("i", DataType::Int32, false);
        let first: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let second: ArrayRef = Arc::new(Int32Array::from(vec![4, 5]));
        let arrays = [first.as_ref(), second.as_ref()];
        let mut emitted = Vec::new();
        let bytes = encode_column_chunks(&field, &arrays, NonZeroUsize::new(8).unwrap(), |chunk| {
            emitted.extend_from_slice(chunk);
            Ok::<_, ()>(())
        })
        .unwrap();

        let expected = [1i32, 2, 3, 4, 5]
            .into_iter()
            .flat_map(i32::to_le_bytes)
            .collect::<Vec<_>>();
        assert_eq!(bytes, expected.len());
        assert_eq!(emitted, expected);
    }

    #[test]
    fn estimated_variable_width_size_bounds_chunked_output() {
        let field = Field::new("s", DataType::Utf8, true);
        let array = StringArray::from(vec![Some("repeated"), None, Some("repeated"), Some("")]);
        let estimate = estimated_encoded_size(&field, &array).unwrap();
        let mut emitted = Vec::new();
        encode_column_chunks(&field, &[&array], NonZeroUsize::new(10).unwrap(), |chunk| {
            emitted.extend_from_slice(chunk);
            Ok::<_, ()>(())
        })
        .unwrap();
        assert!(estimate >= emitted.len());
        assert_eq!(decode_strings(&emitted, array.len()).unwrap(), array);
    }

    #[test]
    fn rejects_integer_null_sentinel_collision() {
        let field = Field::new("i", DataType::Int64, true);
        let array = Int64Array::from(vec![Some(i64::MIN)]);
        assert!(matches!(
            encode_column(&field, &array),
            Err(EncodeError::InvalidValue {
                row: 0,
                message: "value collides with MonetDB's NULL sentinel"
            })
        ));
    }

    #[test]
    fn encodes_unsigned_values_using_wider_wire_types() {
        let field = Field::new("u", DataType::UInt8, true);
        let array = UInt8Array::from(vec![Some(255), None]);
        assert_eq!(
            encode_column(&field, &array).unwrap(),
            [255i16.to_le_bytes(), i16::MIN.to_le_bytes()].concat()
        );
    }

    #[test]
    fn encodes_decimal_backing_widths() {
        for (precision, expected) in [
            (2, [7i8.to_le_bytes(), i8::MIN.to_le_bytes()].concat()),
            (4, [7i16.to_le_bytes(), i16::MIN.to_le_bytes()].concat()),
            (9, [7i32.to_le_bytes(), i32::MIN.to_le_bytes()].concat()),
            (18, [7i64.to_le_bytes(), i64::MIN.to_le_bytes()].concat()),
            (38, [7i128.to_le_bytes(), i128::MIN.to_le_bytes()].concat()),
        ] {
            let data_type = DataType::Decimal128(precision, 0);
            let field = Field::new("d", data_type.clone(), true);
            let array = Decimal128Array::from(vec![Some(7), None])
                .with_precision_and_scale(precision, 0)
                .unwrap();
            assert_eq!(encode_column(&field, &array).unwrap(), expected);
        }
    }

    #[test]
    fn rejects_decimal_values_outside_declared_precision() {
        let field = Field::new("d", DataType::Decimal128(9, 0), true);
        let array = Decimal128Array::from(vec![Some(1_000_000_000)])
            .with_precision_and_scale(9, 0)
            .unwrap();
        assert!(matches!(
            encode_column(&field, &array),
            Err(EncodeError::InvalidValue { row: 0, .. })
        ));
    }

    #[test]
    fn encodes_strings_with_backrefs() {
        let field = Field::new("s", DataType::Utf8, true);
        let array = StringArray::from(vec![Some("foo"), None, None, Some("foo")]);
        assert_eq!(
            encode_column(&field, &array).unwrap(),
            b"foo\0\x80\0\x81\x83"
        );
    }

    #[test]
    fn dictionary_null_values_encode_as_nulls() {
        let keys = Int8Array::from(vec![0, 1]);
        let values: ArrayRef = Arc::new(StringArray::from(vec![Some("x"), None]));
        let dictionary = DictionaryArray::<Int8Type>::try_new(keys, values).unwrap();
        let field = Field::new("d", dictionary.data_type().clone(), true);
        assert_eq!(encode_column(&field, &dictionary).unwrap(), b"x\0\x80\0");
    }

    #[test]
    fn emits_canonical_nulls_when_a_backref_would_be_longer() {
        let field = Field::new("s", DataType::Utf8, true);
        let values = (0..=16_384)
            .map(|row| (row != 0 && row != 16_384).then_some("x"))
            .collect::<Vec<_>>();
        let array = StringArray::from(values);
        let encoded = encode_column(&field, &array).unwrap();
        assert_eq!(&encoded[..2], &[0x80, 0]);
        assert_eq!(&encoded[encoded.len() - 2..], &[0x80, 0]);

        let values = (0..=64)
            .map(|row| (row != 0 && row != 64).then_some("x"))
            .collect::<Vec<_>>();
        let array = StringArray::from(values);
        let encoded = encode_column(&field, &array).unwrap();
        assert_eq!(&encoded[encoded.len() - 2..], &[0x80, 0]);
    }

    #[test]
    fn preserves_interval_extensions_on_write() {
        let month = Field::new("m", DataType::Int32, true).with_metadata(
            [(
                "ARROW:extension:name".to_owned(),
                "monetdb.interval_month".to_owned(),
            )]
            .into(),
        );
        assert_eq!(
            monet_type_for_field(&month).unwrap(),
            MonetType::MonthInterval
        );

        let day = Field::new("d", DataType::Duration(TimeUnit::Millisecond), true).with_metadata(
            [(
                "ARROW:extension:name".to_owned(),
                "monetdb.interval_day".to_owned(),
            )]
            .into(),
        );
        assert_eq!(monet_type_for_field(&day).unwrap(), MonetType::DayInterval);
    }

    #[test]
    fn preserves_oid_and_url_extensions_on_write() {
        let oid = Field::new("oid", DataType::UInt64, true)
            .with_metadata([("ARROW:extension:name".to_owned(), "monetdb.oid".to_owned())].into());
        assert_eq!(monet_type_for_field(&oid).unwrap(), MonetType::Oid);
        let values = UInt64Array::from(vec![Some(7), None]);
        assert_eq!(
            encode_column(&oid, &values).unwrap(),
            [7u64.to_le_bytes(), (1u64 << 63).to_le_bytes()].concat()
        );

        let url = Field::new("url", DataType::Utf8, true)
            .with_metadata([("ARROW:extension:name".to_owned(), "monetdb.url".to_owned())].into());
        assert_eq!(monet_type_for_field(&url).unwrap(), MonetType::Url);

        let inet = Field::new("inet", DataType::Utf8, true)
            .with_metadata([("ARROW:extension:name".to_owned(), "monetdb.inet".to_owned())].into());
        assert_eq!(monet_type_for_field(&inet).unwrap(), MonetType::Inet);
        assert!(matches!(
            encode_column(&inet, &StringArray::from(vec!["127.0.0.1"])),
            Err(EncodeError::UnsupportedMonetType(MonetType::Inet))
        ));

        let inet4 = Field::new("inet4", DataType::LargeUtf8, true).with_metadata(
            [(
                "ARROW:extension:name".to_owned(),
                "monetdb.inet4".to_owned(),
            )]
            .into(),
        );
        assert_eq!(monet_type_for_field(&inet4).unwrap(), MonetType::Inet4);
        assert_eq!(
            encode_column(
                &inet4,
                &LargeStringArray::from(vec![Some("127.0.0.1"), None])
            )
            .unwrap(),
            [127, 0, 0, 1, 0, 0, 0, 0]
        );

        let inet6 = Field::new("inet6", DataType::Utf8View, true).with_metadata(
            [(
                "ARROW:extension:name".to_owned(),
                "monetdb.inet6".to_owned(),
            )]
            .into(),
        );
        assert_eq!(monet_type_for_field(&inet6).unwrap(), MonetType::Inet6);
        assert_eq!(
            encode_column(&inet6, &StringViewArray::from(vec![Some("::1"), None])).unwrap(),
            [
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0,
            ]
        );
        for (field, values) in [
            (
                &inet4,
                Arc::new(LargeStringArray::from(vec!["0.0.0.0"])) as Arc<dyn Array>,
            ),
            (
                &inet6,
                Arc::new(StringViewArray::from(vec!["::"])) as Arc<dyn Array>,
            ),
        ] {
            assert!(matches!(
                encode_column(field, values.as_ref()),
                Err(EncodeError::InvalidValue {
                    message: "address collides with MonetDB's NULL sentinel",
                    ..
                })
            ));
        }
    }

    #[test]
    fn preserves_hugeint_and_timetz_extensions_on_write() {
        let hugeint = Field::new("h", DataType::Decimal128(38, 0), true).with_metadata(
            [(
                "ARROW:extension:name".to_owned(),
                "monetdb.hugeint".to_owned(),
            )]
            .into(),
        );
        assert_eq!(monet_type_for_field(&hugeint).unwrap(), MonetType::HugeInt);

        let timetz = Field::new("t", DataType::Time64(TimeUnit::Microsecond), true).with_metadata(
            [(
                "ARROW:extension:name".to_owned(),
                "monetdb.timetz".to_owned(),
            )]
            .into(),
        );
        assert_eq!(monet_type_for_field(&timetz).unwrap(), MonetType::TimeTz);
    }

    #[test]
    fn rejects_duration_null_sentinel_collision() {
        let field = Field::new("duration", DataType::Duration(TimeUnit::Millisecond), true);
        let values = DurationMillisecondArray::from(vec![Some(i64::MIN)]);
        assert!(matches!(
            encode_column(&field, &values),
            Err(EncodeError::InvalidValue { row: 0, .. })
        ));
    }

    #[test]
    fn interval_day_requires_whole_days() {
        let field = Field::new("d", DataType::Duration(TimeUnit::Millisecond), true).with_metadata(
            [(
                "ARROW:extension:name".to_owned(),
                "monetdb.interval_day".to_owned(),
            )]
            .into(),
        );
        let invalid = DurationMillisecondArray::from(vec![Some(1_234)]);
        assert!(matches!(
            encode_column(&field, &invalid),
            Err(EncodeError::InvalidValue { row: 0, .. })
        ));
        let valid = DurationMillisecondArray::from(vec![Some(86_400_000)]);
        assert!(encode_column(&field, &valid).is_ok());
    }

    #[test]
    fn rejects_non_finite_values() {
        let f32_field = Field::new("f32", DataType::Float32, true);
        for value in [f32::NAN, f32::INFINITY, f32::NEG_INFINITY] {
            let values = Float32Array::from(vec![value]);
            assert!(matches!(
                encode_column(&f32_field, &values),
                Err(EncodeError::InvalidValue { row: 0, .. })
            ));
        }

        let f64_field = Field::new("f64", DataType::Float64, true);
        for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            let values = Float64Array::from(vec![value]);
            assert!(matches!(
                encode_column(&f64_field, &values),
                Err(EncodeError::InvalidValue { row: 0, .. })
            ));
        }
    }

    #[test]
    fn encodes_sliced_arrays_from_their_logical_offset() {
        let field = Field::new("i", DataType::Int32, false);
        let array = Int32Array::from(vec![10, 20, 30, 40]).slice(1, 2);
        assert_eq!(
            encode_column(&field, &array).unwrap(),
            [20i32.to_le_bytes(), 30i32.to_le_bytes()].concat()
        );
    }

    #[test]
    fn rejects_nul_inside_strings() {
        let field = Field::new("s", DataType::Utf8, true);
        let array = StringArray::from(vec!["not\0representable"]);
        assert!(matches!(
            encode_column(&field, &array),
            Err(EncodeError::InvalidValue { row: 0, .. })
        ));
    }

    #[test]
    fn encodes_blobs_with_lengths_and_nulls() {
        let field = Field::new("b", DataType::Binary, true);
        let array = BinaryArray::from(vec![Some(b"ab".as_slice()), None, Some(b"".as_slice())]);
        assert_eq!(
            encode_column(&field, &array).unwrap(),
            [
                2i64.to_le_bytes().as_slice(),
                b"ab",
                (-1i64).to_le_bytes().as_slice(),
                0i64.to_le_bytes().as_slice(),
            ]
            .concat()
        );
    }

    #[test]
    fn maps_unsigned_types_by_widening() {
        assert_eq!(
            monet_type_for_field(&Field::new("u", DataType::UInt64, true)).unwrap(),
            MonetType::HugeInt
        );
    }

    #[test]
    fn temporal_layout_preserves_microseconds() {
        let field = Field::new("ts", DataType::Timestamp(TimeUnit::Microsecond, None), true);
        let array = TimestampMicrosecondArray::from(vec![Some(3_723_123_456), None]);
        let encoded = encode_column(&field, &array).unwrap();
        assert_eq!(&encoded[..12], &[64, 226, 1, 0, 3, 2, 1, 0, 1, 1, 178, 7]);
        assert_eq!(&encoded[12..], &[0xff; 12]);
    }

    #[test]
    fn chrono_date_conversion_round_trips_bce_and_boundary_years() {
        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        let dates = [-4712, -1, 0, 1, 1582, 1900, 2000, 32_767]
            .map(|year| NaiveDate::from_ymd_opt(year, 3, 1).unwrap());
        let days =
            dates.map(|date| i32::try_from(date.signed_duration_since(epoch).num_days()).unwrap());
        let array = Date32Array::from(days.to_vec());
        let field = Field::new("date", DataType::Date32, false);
        let bytes = encode_column(&field, &array).unwrap();
        let decoded = crate::decode::decode_column(&MonetType::Date, &bytes, dates.len()).unwrap();
        let decoded = decoded.as_any().downcast_ref::<Date32Array>().unwrap();
        assert_eq!(decoded.values(), array.values());

        let too_early = NaiveDate::from_ymd_opt(-4713, 12, 31).unwrap();
        let days = i32::try_from(too_early.signed_duration_since(epoch).num_days()).unwrap();
        let array = Date32Array::from(vec![days]);
        assert!(matches!(
            encode_column(&field, &array),
            Err(EncodeError::InvalidValue { row: 0, .. })
        ));
    }

    #[test]
    fn rejects_submicrosecond_time_values() {
        let field = Field::new("t", DataType::Time64(TimeUnit::Nanosecond), false);
        let array = Time64NanosecondArray::from(vec![1]);
        assert!(matches!(
            encode_column(&field, &array),
            Err(EncodeError::InvalidValue { row: 0, .. })
        ));
    }

    #[test]
    fn rejects_nested_types() {
        let field = Field::new_list("items", Field::new_list_field(DataType::Int32, true), true);
        assert!(matches!(
            monet_type_for_field(&field),
            Err(EncodeError::Unsupported(_))
        ));
    }
}
