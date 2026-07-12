//! Decoding MonetDB COPY BINARY column buffers into Arrow arrays.
//!
//! Wire layouts and null sentinels follow MonetDB's
//! `sql/backends/monet5/sql_bincopyconvert.c`, `common/utils/copybinary.h`,
//! and `documentation/source/bincopy-backref.rst`.

use std::{fmt, net::IpAddr, sync::Arc};

use arrow_array::{
    ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, DurationMillisecondArray,
    FixedSizeBinaryArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array,
    Int64Array, RecordBatch, StringArray, Time64MicrosecondArray, TimestampMicrosecondArray,
    UInt64Array,
    builder::{BinaryBuilder, BooleanBuilder, FixedSizeBinaryBuilder, StringBuilder},
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use monetdb::{MonetType, ResultColumn};

use crate::exportbin::{FrameError, parse_frame};

#[derive(Debug)]
pub enum DecodeError {
    Frame(FrameError),
    ColumnCount { expected: usize, actual: usize },
    Length { expected: usize, actual: usize },
    InvalidValue { row: usize, message: &'static str },
    InvalidUtf8 { row: usize },
    InvalidBackref { row: usize },
    Unsupported(MonetType),
    Arrow(arrow_schema::ArrowError),
}

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Frame(error) => error.fmt(f),
            Self::ColumnCount { expected, actual } => {
                write!(
                    f,
                    "result metadata has {expected} columns but frame has {actual}"
                )
            }
            Self::Length { expected, actual } => {
                write!(f, "column has {actual} bytes; expected {expected}")
            }
            Self::InvalidValue { row, message } => {
                write!(f, "invalid value at row {row}: {message}")
            }
            Self::InvalidUtf8 { row } => write!(f, "invalid UTF-8 at row {row}"),
            Self::InvalidBackref { row } => write!(f, "invalid string back-reference at row {row}"),
            Self::Unsupported(data_type) => {
                if matches!(
                    data_type,
                    MonetType::Geometry | MonetType::GeometryA | MonetType::Xml
                ) {
                    write!(
                        f,
                        "MonetDB type {data_type} is not available through Xexportbin; cast the column to VARCHAR in SQL"
                    )
                } else {
                    write!(
                        f,
                        "MonetDB type {data_type} is not supported by the binary protocol"
                    )
                }
            }
            Self::Arrow(error) => error.fmt(f),
        }
    }
}

impl std::error::Error for DecodeError {}

impl From<FrameError> for DecodeError {
    fn from(value: FrameError) -> Self {
        Self::Frame(value)
    }
}

impl From<arrow_schema::ArrowError> for DecodeError {
    fn from(value: arrow_schema::ArrowError) -> Self {
        Self::Arrow(value)
    }
}

pub fn decode_frame(frame: &[u8], columns: &[ResultColumn]) -> Result<RecordBatch, DecodeError> {
    let frame = parse_frame(frame)?;
    if columns.len() != frame.columns.len() {
        return Err(DecodeError::ColumnCount {
            expected: columns.len(),
            actual: frame.columns.len(),
        });
    }
    let row_count = usize::try_from(frame.row_count).map_err(|_| DecodeError::InvalidValue {
        row: 0,
        message: "row count does not fit in memory",
    })?;
    let fields = columns
        .iter()
        .map(field_for_column)
        .collect::<Result<Vec<_>, _>>()?;
    let arrays = columns
        .iter()
        .zip(frame.columns)
        .map(|(column, bytes)| decode_column(column.sql_type(), bytes, row_count))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)?)
}

pub fn field_for_column(column: &ResultColumn) -> Result<Field, DecodeError> {
    let mut field = Field::new(column.name(), data_type(column.sql_type())?, true);
    let extension = match column.sql_type() {
        MonetType::Json => Some("arrow.json"),
        MonetType::Uuid => Some("arrow.uuid"),
        MonetType::MonthInterval => Some("monetdb.interval_month"),
        _ => None,
    };
    if let Some(name) = extension {
        field = field.with_metadata([("ARROW:extension:name".to_owned(), name.to_owned())].into());
    }
    Ok(field)
}

fn data_type(data_type: &MonetType) -> Result<DataType, DecodeError> {
    Ok(match *data_type {
        MonetType::Bool => DataType::Boolean,
        MonetType::TinyInt => DataType::Int8,
        MonetType::SmallInt => DataType::Int16,
        MonetType::Int => DataType::Int32,
        MonetType::BigInt => DataType::Int64,
        MonetType::HugeInt => DataType::Decimal128(38, 0),
        MonetType::Oid => DataType::UInt64,
        MonetType::Decimal(precision, scale) => {
            DataType::Decimal128(precision, i8::try_from(scale).expect("u8 scale fits in i8"))
        }
        MonetType::Varchar(_) | MonetType::Url | MonetType::Json | MonetType::Inet => {
            DataType::Utf8
        }
        MonetType::Real => DataType::Float32,
        MonetType::Double => DataType::Float64,
        MonetType::MonthInterval => DataType::Int32,
        MonetType::DayInterval | MonetType::SecInterval => {
            DataType::Duration(TimeUnit::Millisecond)
        }
        MonetType::Time | MonetType::TimeTz => DataType::Time64(TimeUnit::Microsecond),
        MonetType::Date => DataType::Date32,
        MonetType::Timestamp => DataType::Timestamp(TimeUnit::Microsecond, None),
        MonetType::TimestampTz => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        MonetType::Blob => DataType::Binary,
        MonetType::Uuid => DataType::FixedSizeBinary(16),
        MonetType::Geometry | MonetType::GeometryA | MonetType::Xml => {
            return Err(DecodeError::Unsupported(*data_type));
        }
    })
}

pub fn decode_column(
    data_type: &MonetType,
    bytes: &[u8],
    row_count: usize,
) -> Result<ArrayRef, DecodeError> {
    Ok(match *data_type {
        MonetType::Bool => Arc::new(decode_bool(bytes, row_count)?),
        MonetType::TinyInt => Arc::new(Int8Array::from(decode_signed::<1, i8>(
            bytes,
            row_count,
            i8::MIN,
            i8::from_le_bytes,
        )?)),
        MonetType::SmallInt => Arc::new(Int16Array::from(decode_signed::<2, i16>(
            bytes,
            row_count,
            i16::MIN,
            i16::from_le_bytes,
        )?)),
        MonetType::Int => Arc::new(Int32Array::from(decode_signed::<4, i32>(
            bytes,
            row_count,
            i32::MIN,
            i32::from_le_bytes,
        )?)),
        MonetType::BigInt => Arc::new(Int64Array::from(decode_signed::<8, i64>(
            bytes,
            row_count,
            i64::MIN,
            i64::from_le_bytes,
        )?)),
        MonetType::HugeInt => Arc::new(decimal_array(
            decode_signed::<16, i128>(bytes, row_count, i128::MIN, i128::from_le_bytes)?,
            38,
            0,
        )?),
        MonetType::Oid => Arc::new(UInt64Array::from(decode_signed::<8, u64>(
            bytes,
            row_count,
            u64::MAX,
            u64::from_le_bytes,
        )?)),
        MonetType::Decimal(precision, scale) => Arc::new(decode_decimal(
            bytes,
            row_count,
            precision,
            i8::try_from(scale).expect("u8 scale fits in i8"),
        )?),
        MonetType::Varchar(_) | MonetType::Url | MonetType::Json => {
            Arc::new(decode_strings(bytes, row_count)?)
        }
        MonetType::Real => Arc::new(Float32Array::from(decode_float::<4, f32>(
            bytes,
            row_count,
            f32::from_le_bytes,
            f32::is_nan,
        )?)),
        MonetType::Double => Arc::new(Float64Array::from(decode_float::<8, f64>(
            bytes,
            row_count,
            f64::from_le_bytes,
            f64::is_nan,
        )?)),
        MonetType::MonthInterval => Arc::new(Int32Array::from(decode_signed::<4, i32>(
            bytes,
            row_count,
            i32::MIN,
            i32::from_le_bytes,
        )?)),
        MonetType::DayInterval | MonetType::SecInterval => {
            Arc::new(DurationMillisecondArray::from(decode_signed::<8, i64>(
                bytes,
                row_count,
                i64::MIN,
                i64::from_le_bytes,
            )?))
        }
        MonetType::Time | MonetType::TimeTz => Arc::new(decode_time(bytes, row_count)?),
        MonetType::Date => Arc::new(decode_date(bytes, row_count)?),
        MonetType::Timestamp => Arc::new(decode_timestamp(bytes, row_count, false)?),
        MonetType::TimestampTz => Arc::new(decode_timestamp(bytes, row_count, true)?),
        MonetType::Blob => Arc::new(decode_blob(bytes, row_count)?),
        MonetType::Uuid => Arc::new(decode_uuid(bytes, row_count)?),
        MonetType::Inet => Arc::new(decode_inet(bytes, row_count)?),
        MonetType::Geometry | MonetType::GeometryA | MonetType::Xml => {
            return Err(DecodeError::Unsupported(*data_type));
        }
    })
}

fn expect_fixed(bytes: &[u8], rows: usize, width: usize) -> Result<(), DecodeError> {
    let expected = rows.checked_mul(width).ok_or(DecodeError::InvalidValue {
        row: 0,
        message: "column length overflows",
    })?;
    if bytes.len() != expected {
        return Err(DecodeError::Length {
            expected,
            actual: bytes.len(),
        });
    }
    Ok(())
}

fn decode_signed<const N: usize, T: Copy + PartialEq>(
    bytes: &[u8],
    rows: usize,
    null: T,
    decode: impl Fn([u8; N]) -> T,
) -> Result<Vec<Option<T>>, DecodeError> {
    expect_fixed(bytes, rows, N)?;
    Ok(bytes
        .chunks_exact(N)
        .map(|chunk| {
            let value = decode(chunk.try_into().expect("chunk has fixed width"));
            (value != null).then_some(value)
        })
        .collect())
}

fn decode_float<const N: usize, T: Copy>(
    bytes: &[u8],
    rows: usize,
    decode: impl Fn([u8; N]) -> T,
    is_null: impl Fn(T) -> bool,
) -> Result<Vec<Option<T>>, DecodeError> {
    expect_fixed(bytes, rows, N)?;
    Ok(bytes
        .chunks_exact(N)
        .map(|chunk| {
            let value = decode(chunk.try_into().expect("chunk has fixed width"));
            (!is_null(value)).then_some(value)
        })
        .collect())
}

fn decode_bool(bytes: &[u8], rows: usize) -> Result<BooleanArray, DecodeError> {
    expect_fixed(bytes, rows, 1)?;
    let mut builder = BooleanBuilder::with_capacity(rows);
    for (row, value) in bytes.iter().copied().enumerate() {
        match value {
            0 => builder.append_value(false),
            1 => builder.append_value(true),
            0x80 => builder.append_null(),
            _ => {
                return Err(DecodeError::InvalidValue {
                    row,
                    message: "boolean is not 0, 1, or the NULL sentinel 0x80",
                });
            }
        }
    }
    Ok(builder.finish())
}

fn decode_decimal(
    bytes: &[u8],
    rows: usize,
    precision: u8,
    scale: i8,
) -> Result<Decimal128Array, DecodeError> {
    let values = match precision {
        0..=2 => decode_signed::<1, i8>(bytes, rows, i8::MIN, i8::from_le_bytes)?
            .into_iter()
            .map(|value| value.map(i128::from))
            .collect(),
        3..=4 => decode_signed::<2, i16>(bytes, rows, i16::MIN, i16::from_le_bytes)?
            .into_iter()
            .map(|value| value.map(i128::from))
            .collect(),
        5..=9 => decode_signed::<4, i32>(bytes, rows, i32::MIN, i32::from_le_bytes)?
            .into_iter()
            .map(|value| value.map(i128::from))
            .collect(),
        10..=18 => decode_signed::<8, i64>(bytes, rows, i64::MIN, i64::from_le_bytes)?
            .into_iter()
            .map(|value| value.map(i128::from))
            .collect(),
        19..=38 => decode_signed::<16, i128>(bytes, rows, i128::MIN, i128::from_le_bytes)?,
        _ => {
            return Err(DecodeError::InvalidValue {
                row: 0,
                message: "decimal precision must be between 1 and 38",
            });
        }
    };
    decimal_array(values, precision, scale)
}

fn decimal_array(
    values: Vec<Option<i128>>,
    precision: u8,
    scale: i8,
) -> Result<Decimal128Array, DecodeError> {
    Ok(Decimal128Array::from(values).with_precision_and_scale(precision, scale)?)
}

pub(crate) fn decode_strings(bytes: &[u8], rows: usize) -> Result<StringArray, DecodeError> {
    let mut builder = StringBuilder::with_capacity(rows, bytes.len());
    let mut history: Vec<Option<&str>> = Vec::with_capacity(rows);
    let mut pos = 0;
    for row in 0..rows {
        let Some(&first) = bytes.get(pos) else {
            return Err(DecodeError::Length {
                expected: pos + 1,
                actual: bytes.len(),
            });
        };
        let value = if first == 0x80 {
            let (distance, consumed) = long_backref(&bytes[pos + 1..], row)?;
            pos += consumed + 1;
            if distance == 0 {
                None
            } else {
                history
                    .get(
                        row.checked_sub(distance)
                            .ok_or(DecodeError::InvalidBackref { row })?,
                    )
                    .copied()
                    .ok_or(DecodeError::InvalidBackref { row })?
            }
        } else if (0x81..=0xbf).contains(&first) {
            pos += 1;
            let distance = usize::from(first - 0x80);
            history
                .get(
                    row.checked_sub(distance)
                        .ok_or(DecodeError::InvalidBackref { row })?,
                )
                .copied()
                .ok_or(DecodeError::InvalidBackref { row })?
        } else {
            let tail = &bytes[pos..];
            let end = tail
                .iter()
                .position(|&byte| byte == 0)
                .ok_or(DecodeError::Length {
                    expected: bytes.len() + 1,
                    actual: bytes.len(),
                })?;
            let string =
                std::str::from_utf8(&tail[..end]).map_err(|_| DecodeError::InvalidUtf8 { row })?;
            pos += end + 1;
            Some(string)
        };
        match value {
            Some(value) => builder.append_value(value),
            None => builder.append_null(),
        }
        history.push(value);
    }
    if pos != bytes.len() {
        return Err(DecodeError::Length {
            expected: pos,
            actual: bytes.len(),
        });
    }
    Ok(builder.finish())
}

fn long_backref(bytes: &[u8], row: usize) -> Result<(usize, usize), DecodeError> {
    let mut distance = 0usize;
    let mut shift = 0u32;
    for (index, byte) in bytes.iter().copied().enumerate() {
        let part = usize::from(byte & 0x7f)
            .checked_shl(shift)
            .ok_or(DecodeError::InvalidBackref { row })?;
        distance = distance
            .checked_add(part)
            .ok_or(DecodeError::InvalidBackref { row })?;
        if byte < 0x80 {
            return Ok((distance, index + 1));
        }
        shift = shift
            .checked_add(7)
            .filter(|shift| *shift < usize::BITS)
            .ok_or(DecodeError::InvalidBackref { row })?;
    }
    Err(DecodeError::InvalidBackref { row })
}

fn decode_blob(bytes: &[u8], rows: usize) -> Result<BinaryArray, DecodeError> {
    let mut builder = BinaryBuilder::with_capacity(rows, bytes.len());
    let mut pos = 0;
    for row in 0..rows {
        let header = bytes.get(pos..pos + 8).ok_or(DecodeError::Length {
            expected: pos + 8,
            actual: bytes.len(),
        })?;
        let length = i64::from_le_bytes(header.try_into().expect("blob header is 8 bytes"));
        pos += 8;
        if length < 0 {
            builder.append_null();
            continue;
        }
        let length = usize::try_from(length).map_err(|_| DecodeError::InvalidValue {
            row,
            message: "blob length does not fit in memory",
        })?;
        let value = bytes.get(pos..pos + length).ok_or(DecodeError::Length {
            expected: pos + length,
            actual: bytes.len(),
        })?;
        builder.append_value(value);
        pos += length;
    }
    if pos != bytes.len() {
        return Err(DecodeError::Length {
            expected: pos,
            actual: bytes.len(),
        });
    }
    Ok(builder.finish())
}

fn decode_uuid(bytes: &[u8], rows: usize) -> Result<FixedSizeBinaryArray, DecodeError> {
    expect_fixed(bytes, rows, 16)?;
    let mut builder = FixedSizeBinaryBuilder::with_capacity(rows, 16);
    for value in bytes.chunks_exact(16) {
        if value.iter().all(|&byte| byte == 0) {
            builder.append_null();
        } else {
            builder.append_value(value)?;
        }
    }
    Ok(builder.finish())
}

fn decode_date(bytes: &[u8], rows: usize) -> Result<Date32Array, DecodeError> {
    expect_fixed(bytes, rows, 4)?;
    let values = bytes
        .chunks_exact(4)
        .enumerate()
        .map(|(row, value)| date_value(value, row))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Date32Array::from(values))
}

fn date_value(value: &[u8], row: usize) -> Result<Option<i32>, DecodeError> {
    let day = value[0];
    let month = value[1];
    if month == u8::MAX {
        return Ok(None);
    }
    let year = i16::from_le_bytes(value[2..4].try_into().expect("date year is 2 bytes"));
    days_from_civil(i32::from(year), month, day)
        .map(Some)
        .ok_or(DecodeError::InvalidValue {
            row,
            message: "invalid Gregorian date",
        })
}

fn decode_time(bytes: &[u8], rows: usize) -> Result<Time64MicrosecondArray, DecodeError> {
    expect_fixed(bytes, rows, 8)?;
    let values = bytes
        .chunks_exact(8)
        .enumerate()
        .map(|(row, value)| time_value(value, row))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Time64MicrosecondArray::from(values))
}

fn time_value(value: &[u8], row: usize) -> Result<Option<i64>, DecodeError> {
    // The C field is named `ms`, but contains microseconds.
    let micros = u32::from_le_bytes(value[..4].try_into().expect("time fraction is 4 bytes"));
    if micros == u32::MAX {
        return Ok(None);
    }
    let second = value[4];
    let minute = value[5];
    let hour = value[6];
    if micros >= 1_000_000 || second >= 60 || minute >= 60 || hour >= 24 {
        return Err(DecodeError::InvalidValue {
            row,
            message: "invalid time of day",
        });
    }
    Ok(Some(
        ((i64::from(hour) * 60 + i64::from(minute)) * 60 + i64::from(second)) * 1_000_000
            + i64::from(micros),
    ))
}

fn decode_timestamp(
    bytes: &[u8],
    rows: usize,
    utc: bool,
) -> Result<TimestampMicrosecondArray, DecodeError> {
    expect_fixed(bytes, rows, 12)?;
    let values = bytes
        .chunks_exact(12)
        .enumerate()
        .map(|(row, value)| {
            let time = time_value(&value[..8], row)?;
            let date = date_value(&value[8..], row)?;
            match (date, time) {
                (None, None) => Ok(None),
                (Some(date), Some(time)) => Ok(Some(i64::from(date) * 86_400_000_000 + time)),
                _ => Err(DecodeError::InvalidValue {
                    row,
                    message: "timestamp has mismatched date/time NULL sentinels",
                }),
            }
        })
        .collect::<Result<Vec<_>, _>>()?;
    let array = TimestampMicrosecondArray::from(values);
    Ok(if utc {
        array.with_timezone("UTC")
    } else {
        array
    })
}

fn decode_inet(bytes: &[u8], rows: usize) -> Result<StringArray, DecodeError> {
    let width = if bytes.len() == rows.saturating_mul(4) {
        4
    } else if bytes.len() == rows.saturating_mul(16) {
        16
    } else {
        return Err(DecodeError::Unsupported(MonetType::Inet));
    };
    let mut builder = StringBuilder::with_capacity(rows, rows.saturating_mul(39));
    for value in bytes.chunks_exact(width) {
        if value.iter().all(|&byte| byte == 0) {
            builder.append_null();
        } else {
            let address = if width == 4 {
                IpAddr::from(<[u8; 4]>::try_from(value).expect("IPv4 is 4 bytes"))
            } else {
                IpAddr::from(<[u8; 16]>::try_from(value).expect("IPv6 is 16 bytes"))
            };
            builder.append_value(address.to_string());
        }
    }
    Ok(builder.finish())
}

fn days_from_civil(year: i32, month: u8, day: u8) -> Option<i32> {
    let days_in_month = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap_year(year) => 29,
        2 => 28,
        _ => return None,
    };
    if day == 0 || day > days_in_month {
        return None;
    }
    let year = year - i32::from(month <= 2);
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let year_of_era = year - era * 400;
    let month = i32::from(month);
    let day_of_year = (153 * (month + if month > 2 { -3 } else { 9 }) + 2) / 5 + i32::from(day) - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    Some(era * 146_097 + day_of_era - 719_468)
}

fn is_leap_year(year: i32) -> bool {
    year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)
}

#[cfg(test)]
mod tests {
    use arrow_array::Array;

    use super::*;

    fn golden_columns() -> Vec<Vec<u8>> {
        include_str!("../tests/fixtures/dec2025-sp3-columns.txt")
            .lines()
            .filter(|line| !line.starts_with('#'))
            .map(|line| {
                let hex = line.rsplit('|').next().expect("fixture line has hex bytes");
                assert_eq!(hex.len() % 2, 0);
                hex.as_bytes()
                    .chunks_exact(2)
                    .map(|digits| {
                        u8::from_str_radix(
                            std::str::from_utf8(digits).expect("hex digits are ASCII"),
                            16,
                        )
                        .expect("fixture contains valid hex")
                    })
                    .collect()
            })
            .collect()
    }

    #[test]
    fn decodes_real_server_golden_columns() {
        let types = [
            MonetType::Bool,
            MonetType::TinyInt,
            MonetType::SmallInt,
            MonetType::Int,
            MonetType::BigInt,
            MonetType::HugeInt,
            MonetType::Decimal(2, 0),
            MonetType::Decimal(4, 2),
            MonetType::Decimal(9, 0),
            MonetType::Decimal(18, 0),
            MonetType::Decimal(38, 0),
            MonetType::Real,
            MonetType::Double,
            MonetType::Varchar(0),
            MonetType::Blob,
            MonetType::Date,
            MonetType::Time,
            MonetType::TimeTz,
            MonetType::Timestamp,
            MonetType::TimestampTz,
            MonetType::MonthInterval,
            MonetType::DayInterval,
            MonetType::SecInterval,
            MonetType::Uuid,
            MonetType::Inet,
            MonetType::Inet,
            MonetType::Json,
            MonetType::Url,
        ];
        let columns = golden_columns();
        assert_eq!(columns.len(), types.len());
        let arrays = types
            .iter()
            .zip(&columns)
            .map(|(data_type, bytes)| decode_column(data_type, bytes, 2).unwrap())
            .collect::<Vec<_>>();
        for array in &arrays {
            assert_eq!(array.len(), 2);
            assert!(!array.is_null(0));
            assert!(array.is_null(1));
        }

        assert!(
            arrays[0]
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .value(0)
        );
        assert_eq!(
            arrays[1]
                .as_any()
                .downcast_ref::<Int8Array>()
                .unwrap()
                .value(0),
            7
        );
        assert_eq!(
            arrays[2]
                .as_any()
                .downcast_ref::<Int16Array>()
                .unwrap()
                .value(0),
            300
        );
        assert_eq!(
            arrays[3]
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0),
            70_000
        );
        assert_eq!(
            arrays[4]
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            5_000_000_000
        );
        assert_eq!(
            arrays[5]
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .unwrap()
                .value(0),
            123_456_789_012_345_678_901_234_567_890
        );
        for (index, expected) in [
            (6, 12),
            (7, 1_234),
            (8, 123_456_789),
            (9, 123_456_789_012_345_678),
            (10, 12_345_678_901_234_567_890_123_456_789_012_345_678),
        ] {
            assert_eq!(
                arrays[index]
                    .as_any()
                    .downcast_ref::<Decimal128Array>()
                    .unwrap()
                    .value(0),
                expected
            );
        }
        assert_eq!(
            arrays[11]
                .as_any()
                .downcast_ref::<Float32Array>()
                .unwrap()
                .value(0),
            1.5
        );
        assert_eq!(
            arrays[12]
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0),
            2.5
        );
        assert_eq!(
            arrays[13]
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "hello"
        );
        assert_eq!(
            arrays[14]
                .as_any()
                .downcast_ref::<BinaryArray>()
                .unwrap()
                .value(0),
            [0, 255]
        );
        assert_eq!(
            arrays[15]
                .as_any()
                .downcast_ref::<Date32Array>()
                .unwrap()
                .value(0),
            20_453
        );
        for index in [16, 17] {
            assert_eq!(
                arrays[index]
                    .as_any()
                    .downcast_ref::<Time64MicrosecondArray>()
                    .unwrap()
                    .value(0),
                3_723_123_456
            );
        }
        for index in [18, 19] {
            assert_eq!(
                arrays[index]
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .unwrap()
                    .value(0),
                1_767_142_923_123_456
            );
        }
        assert_eq!(
            arrays[20]
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0),
            14
        );
        for (index, expected) in [(21, 172_800_000), (22, 1_234)] {
            assert_eq!(
                arrays[index]
                    .as_any()
                    .downcast_ref::<DurationMillisecondArray>()
                    .unwrap()
                    .value(0),
                expected
            );
        }
        assert_eq!(
            arrays[23]
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .unwrap()
                .value(0),
            [
                0x12, 0x34, 0x56, 0x78, 0x12, 0x34, 0x56, 0x78, 0x12, 0x34, 0x56, 0x78, 0x12, 0x34,
                0x56, 0x78,
            ]
        );
        for (index, expected) in [
            (24, "127.0.0.1"),
            (25, "2001:db8::1"),
            (26, "{\"x\":1}"),
            (27, "https://example.com/x"),
        ] {
            assert_eq!(
                arrays[index]
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .value(0),
                expected
            );
        }
    }

    #[test]
    fn decodes_fixed_width_sentinels() {
        let ints = [
            1i32.to_le_bytes(),
            i32::MIN.to_le_bytes(),
            (-3i32).to_le_bytes(),
        ]
        .concat();
        let array = decode_column(&MonetType::Int, &ints, 3).unwrap();
        let array = array.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(
            array.iter().collect::<Vec<_>>(),
            vec![Some(1), None, Some(-3)]
        );

        let floats = [1.5f64.to_le_bytes(), f64::NAN.to_le_bytes()].concat();
        let array = decode_column(&MonetType::Double, &floats, 2).unwrap();
        let array = array.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(array.value(0), 1.5);
        assert!(array.is_null(1));
    }

    #[test]
    fn decodes_strings_nulls_and_backrefs() {
        let data = b"foo\0\x80\0\x81\x83bar\0";
        let array = decode_strings(data, 5).unwrap();
        assert_eq!(
            array.iter().collect::<Vec<_>>(),
            vec![Some("foo"), None, None, Some("foo"), Some("bar")]
        );
    }

    #[test]
    fn decodes_long_backrefs() {
        let mut data = b"first\0".to_vec();
        for _ in 0..64 {
            data.extend_from_slice(b"x\0");
        }
        data.extend_from_slice(&[0x80, 0xc1, 0x00]);
        let array = decode_strings(&data, 66).unwrap();
        assert_eq!(array.value(65), "first");
    }

    #[test]
    fn rejects_invalid_string_data() {
        assert!(matches!(
            decode_strings(b"unterminated", 1),
            Err(DecodeError::Length { .. })
        ));
        assert!(matches!(
            decode_strings(&[0x81], 1),
            Err(DecodeError::InvalidBackref { .. })
        ));
        assert!(matches!(
            decode_strings(&[0xff, 0], 1),
            Err(DecodeError::InvalidUtf8 { .. })
        ));
    }

    #[test]
    fn decodes_decimals_across_backing_widths() {
        for (precision, bytes, expected) in [
            (2, 12i8.to_le_bytes().to_vec(), 12i128),
            (4, 1234i16.to_le_bytes().to_vec(), 1234),
            (9, 123_456_789i32.to_le_bytes().to_vec(), 123_456_789),
            (18, 123_456_789i64.to_le_bytes().to_vec(), 123_456_789),
            (38, 123_456_789i128.to_le_bytes().to_vec(), 123_456_789),
        ] {
            let array = decode_decimal(&bytes, 1, precision, 2).unwrap();
            assert_eq!(array.value(0), expected);
            assert_eq!(array.precision(), precision);
            assert_eq!(array.scale(), 2);
        }
    }

    #[test]
    fn decodes_temporal_values_at_microsecond_precision() {
        let date = [1, 1, 0xb2, 0x07];
        assert_eq!(date_value(&date, 0).unwrap(), Some(0));

        let time = [64, 226, 1, 0, 3, 2, 1, 0];
        assert_eq!(time_value(&time, 0).unwrap(), Some(3_723_123_456));

        let timestamp = [time.as_slice(), date.as_slice()].concat();
        let array = decode_timestamp(&timestamp, 1, false).unwrap();
        assert_eq!(array.value(0), 3_723_123_456);
    }

    #[test]
    fn decodes_blobs_and_uuids() {
        let mut blobs = 3i64.to_le_bytes().to_vec();
        blobs.extend_from_slice(b"abc");
        blobs.extend_from_slice(&(-1i64).to_le_bytes());
        let array = decode_blob(&blobs, 2).unwrap();
        assert_eq!(array.value(0), b"abc");
        assert!(array.is_null(1));

        let mut uuids = vec![1; 16];
        uuids.extend_from_slice(&[0; 16]);
        let array = decode_uuid(&uuids, 2).unwrap();
        assert_eq!(array.value(0), &[1; 16]);
        assert!(array.is_null(1));
    }

    #[test]
    fn validates_fixed_width_lengths_and_booleans() {
        assert!(matches!(
            decode_column(&MonetType::Int, &[0; 3], 1),
            Err(DecodeError::Length { .. })
        ));
        assert!(matches!(
            decode_column(&MonetType::Bool, &[2], 1),
            Err(DecodeError::InvalidValue { .. })
        ));
        assert_eq!(
            data_type(&MonetType::Geometry).unwrap_err().to_string(),
            "MonetDB type GEOMETRY is not available through Xexportbin; cast the column to VARCHAR in SQL"
        );
    }
}
