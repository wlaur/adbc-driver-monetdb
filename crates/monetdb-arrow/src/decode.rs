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
use chrono::{Datelike, FixedOffset, NaiveDate, NaiveTime, TimeZone, Timelike};
use monetdb::{
    Cursor, MonetType, ResultColumn,
    convert::{
        raw_decimal::RawDecimal,
        raw_temporal::{RawDate, RawTime, RawTimeTz, RawTimestamp, RawTimestampTz},
    },
};
use rayon::prelude::*;

use crate::exportbin::{FrameError, parse_frame};

#[derive(Debug)]
pub enum DecodeError {
    Frame(FrameError),
    ResultId { expected: u64, actual: i64 },
    StartRow { expected: u64, actual: u64 },
    ColumnCount { expected: usize, actual: usize },
    Length { expected: usize, actual: usize },
    InvalidValue { row: usize, message: &'static str },
    InvalidUtf8 { row: usize },
    InvalidBackref { row: usize },
    Cursor(monetdb::CursorError),
    Unsupported(MonetType),
    Arrow(arrow_schema::ArrowError),
}

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Frame(error) => error.fmt(f),
            Self::ResultId { expected, actual } => {
                write!(f, "frame has result id {actual}; expected {expected}")
            }
            Self::StartRow { expected, actual } => {
                write!(f, "frame starts at row {actual}; expected {expected}")
            }
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
            Self::Cursor(error) => error.fmt(f),
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

impl From<monetdb::CursorError> for DecodeError {
    fn from(value: monetdb::CursorError) -> Self {
        Self::Cursor(value)
    }
}

fn decimal_scale(scale: u8) -> Result<i8, DecodeError> {
    i8::try_from(scale).map_err(|_| DecodeError::InvalidValue {
        row: 0,
        message: "decimal scale exceeds Arrow's maximum of 127",
    })
}

fn decimal_data_type(precision: u8, scale: u8) -> Result<DataType, DecodeError> {
    if !(1..=38).contains(&precision) || scale > precision {
        return Err(DecodeError::InvalidValue {
            row: 0,
            message: "decimal precision/scale must satisfy 1 <= precision <= 38 and scale <= precision",
        });
    }
    Ok(DataType::Decimal128(precision, decimal_scale(scale)?))
}

pub fn decode_frame(
    frame: &[u8],
    columns: &[ResultColumn],
    expected_result_id: u64,
    expected_start_row: u64,
) -> Result<RecordBatch, DecodeError> {
    let frame = parse_frame(frame)?;
    if u64::try_from(frame.result_id) != Ok(expected_result_id) {
        return Err(DecodeError::ResultId {
            expected: expected_result_id,
            actual: frame.result_id,
        });
    }
    if frame.start_row != expected_start_row {
        return Err(DecodeError::StartRow {
            expected: expected_start_row,
            actual: frame.start_row,
        });
    }
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
    let frame_bytes = frame
        .columns
        .iter()
        .fold(0usize, |total, column| total.saturating_add(column.len()));
    let arrays = if columns.len() > 1 && frame_bytes >= 1024 * 1024 {
        columns
            .par_iter()
            .zip(frame.columns.par_iter())
            .map(|(column, bytes)| decode_column(column.sql_type(), bytes, row_count))
            .collect::<Result<Vec<_>, _>>()?
    } else {
        columns
            .iter()
            .zip(frame.columns)
            .map(|(column, bytes)| decode_column(column.sql_type(), bytes, row_count))
            .collect::<Result<Vec<_>, _>>()?
    };
    Ok(RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)?)
}

/// Decode the current inline text row of a MAPI result through the same
/// validated wire-to-Arrow path used by `Xexportbin`.
pub fn decode_inline_row(
    cursor: &mut Cursor,
    columns: &[ResultColumn],
) -> Result<RecordBatch, DecodeError> {
    if !cursor.next_row()? {
        return Err(DecodeError::InvalidValue {
            row: 0,
            message: "inline result did not contain a row",
        });
    }
    let fields = columns
        .iter()
        .map(field_for_column)
        .collect::<Result<Vec<_>, _>>()?;
    let arrays = columns
        .iter()
        .enumerate()
        .map(|(index, column)| {
            let bytes = inline_wire_value(cursor, index, column.sql_type())?;
            decode_column(column.sql_type(), &bytes, 1)
        })
        .collect::<Result<Vec<_>, _>>()?;
    if cursor.next_row()? {
        return Err(DecodeError::InvalidValue {
            row: 1,
            message: "inline scalar result contained more than one row",
        });
    }
    Ok(RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)?)
}

fn inline_wire_value(
    cursor: &Cursor,
    index: usize,
    data_type: &MonetType,
) -> Result<Vec<u8>, DecodeError> {
    if cursor.get_str(index)?.is_none() {
        return null_wire_value(data_type);
    }
    Ok(match *data_type {
        MonetType::Bool => vec![u8::from(required(cursor.get_bool(index)?)?)],
        MonetType::TinyInt => required(cursor.get_i8(index)?)?.to_le_bytes().to_vec(),
        MonetType::SmallInt => required(cursor.get_i16(index)?)?.to_le_bytes().to_vec(),
        MonetType::Int => required(cursor.get_i32(index)?)?.to_le_bytes().to_vec(),
        MonetType::BigInt => required(cursor.get_i64(index)?)?.to_le_bytes().to_vec(),
        MonetType::HugeInt => required(cursor.get_i128(index)?)?.to_le_bytes().to_vec(),
        MonetType::Oid => {
            let rendered = required(cursor.get_str(index)?)?;
            parse_inline_oid(rendered)?.to_le_bytes().to_vec()
        }
        MonetType::Decimal(precision, scale) => {
            let decimal = required(cursor.get::<RawDecimal<i128>>(index)?)?;
            let value = decimal.at_scale(scale).ok_or(DecodeError::InvalidValue {
                row: 0,
                message: "decimal text has more fractional digits than its declared scale",
            })?;
            decimal_wire_value(value, precision)?
        }
        MonetType::Varchar(_) | MonetType::Url | MonetType::Json => {
            let value = required(cursor.get_str(index)?)?;
            let mut bytes = value.as_bytes().to_vec();
            bytes.push(0);
            bytes
        }
        MonetType::Real => required(cursor.get_f32(index)?)?.to_le_bytes().to_vec(),
        MonetType::Double => required(cursor.get_f64(index)?)?.to_le_bytes().to_vec(),
        MonetType::MonthInterval => required(cursor.get_i32(index)?)?.to_le_bytes().to_vec(),
        MonetType::DayInterval | MonetType::SecInterval => {
            let decimal = required(cursor.get::<RawDecimal<i64>>(index)?)?;
            decimal
                .at_scale(3)
                .ok_or(DecodeError::InvalidValue {
                    row: 0,
                    message: "interval text is not representable in milliseconds",
                })?
                .to_le_bytes()
                .to_vec()
        }
        MonetType::Time => time_wire(required(cursor.get::<RawTime>(index)?)?),
        MonetType::TimeTz => {
            let value = required(cursor.get::<RawTimeTz>(index)?)?;
            time_wire(normalize_time(value.time, value.tz.seconds_east)?)
        }
        MonetType::Date => date_wire(required(cursor.get::<RawDate>(index)?)?),
        MonetType::Timestamp => {
            let value = required(cursor.get::<RawTimestamp>(index)?)?;
            timestamp_wire(value.date, value.time)
        }
        MonetType::TimestampTz => {
            let value = required(cursor.get::<RawTimestampTz>(index)?)?;
            let (date, time) = normalize_timestamp(value.date, value.time, value.tz.seconds_east)?;
            timestamp_wire(date, time)
        }
        MonetType::Blob => {
            let value = required(cursor.get::<Vec<u8>>(index)?)?;
            let length = i64::try_from(value.len()).map_err(|_| DecodeError::InvalidValue {
                row: 0,
                message: "BLOB length does not fit on the wire",
            })?;
            let mut bytes = length.to_le_bytes().to_vec();
            bytes.extend_from_slice(&value);
            bytes
        }
        MonetType::Uuid => uuid::Uuid::parse_str(required(cursor.get_str(index)?)?)
            .map_err(|_| DecodeError::InvalidValue {
                row: 0,
                message: "invalid UUID text",
            })?
            .into_bytes()
            .to_vec(),
        MonetType::Inet => match required(cursor.get_str(index)?)?
            .parse::<IpAddr>()
            .map_err(|_| DecodeError::InvalidValue {
                row: 0,
                message: "invalid INET address",
            })? {
            IpAddr::V4(value) => value.octets().to_vec(),
            IpAddr::V6(value) => value.octets().to_vec(),
        },
        MonetType::Geometry | MonetType::GeometryA | MonetType::Xml => {
            return Err(DecodeError::Unsupported(*data_type));
        }
    })
}

fn required<T>(value: Option<T>) -> Result<T, DecodeError> {
    value.ok_or(DecodeError::InvalidValue {
        row: 0,
        message: "non-NULL inline value decoded as NULL",
    })
}

fn parse_inline_oid(rendered: &str) -> Result<u64, DecodeError> {
    rendered
        .strip_suffix("@0")
        .unwrap_or(rendered)
        .parse::<u64>()
        .map_err(|_| DecodeError::InvalidValue {
            row: 0,
            message: "invalid inline OID",
        })
}

fn null_wire_value(data_type: &MonetType) -> Result<Vec<u8>, DecodeError> {
    Ok(match *data_type {
        MonetType::Bool => vec![0x80],
        MonetType::TinyInt => i8::MIN.to_le_bytes().to_vec(),
        MonetType::SmallInt => i16::MIN.to_le_bytes().to_vec(),
        MonetType::Int | MonetType::MonthInterval => i32::MIN.to_le_bytes().to_vec(),
        MonetType::BigInt | MonetType::DayInterval | MonetType::SecInterval => {
            i64::MIN.to_le_bytes().to_vec()
        }
        MonetType::HugeInt => i128::MIN.to_le_bytes().to_vec(),
        MonetType::Oid => (1u64 << 63).to_le_bytes().to_vec(),
        MonetType::Decimal(precision, _) => match precision {
            0..=2 => i8::MIN.to_le_bytes().to_vec(),
            3..=4 => i16::MIN.to_le_bytes().to_vec(),
            5..=9 => i32::MIN.to_le_bytes().to_vec(),
            10..=18 => i64::MIN.to_le_bytes().to_vec(),
            19..=38 => i128::MIN.to_le_bytes().to_vec(),
            _ => {
                return Err(DecodeError::InvalidValue {
                    row: 0,
                    message: "decimal precision must be between 1 and 38",
                });
            }
        },
        MonetType::Varchar(_) | MonetType::Url | MonetType::Json => vec![0x80, 0],
        MonetType::Real => f32::NAN.to_le_bytes().to_vec(),
        MonetType::Double => f64::NAN.to_le_bytes().to_vec(),
        MonetType::Time | MonetType::TimeTz => vec![0xff; 8],
        MonetType::Date => vec![0xff; 4],
        MonetType::Timestamp | MonetType::TimestampTz => vec![0xff; 12],
        MonetType::Blob => (-1i64).to_le_bytes().to_vec(),
        MonetType::Uuid => vec![0; 16],
        MonetType::Inet => vec![0; 4],
        MonetType::Geometry | MonetType::GeometryA | MonetType::Xml => {
            return Err(DecodeError::Unsupported(*data_type));
        }
    })
}

fn decimal_wire_value(value: i128, precision: u8) -> Result<Vec<u8>, DecodeError> {
    macro_rules! narrow {
        ($type:ty) => {
            <$type>::try_from(value)
                .map_err(|_| DecodeError::InvalidValue {
                    row: 0,
                    message: "decimal value does not fit its backing integer",
                })?
                .to_le_bytes()
                .to_vec()
        };
    }
    Ok(match precision {
        0..=2 => narrow!(i8),
        3..=4 => narrow!(i16),
        5..=9 => narrow!(i32),
        10..=18 => narrow!(i64),
        19..=38 => value.to_le_bytes().to_vec(),
        _ => {
            return Err(DecodeError::InvalidValue {
                row: 0,
                message: "decimal precision must be between 1 and 38",
            });
        }
    })
}

fn time_wire(value: RawTime) -> Vec<u8> {
    let mut bytes = value.microseconds.to_le_bytes().to_vec();
    bytes.extend_from_slice(&[value.seconds, value.minutes, value.hours, 0]);
    bytes
}

fn date_wire(value: RawDate) -> Vec<u8> {
    let mut bytes = vec![value.day, value.month];
    bytes.extend_from_slice(&value.year.to_le_bytes());
    bytes
}

fn timestamp_wire(date: RawDate, time: RawTime) -> Vec<u8> {
    let mut bytes = time_wire(time);
    bytes.extend_from_slice(&date_wire(date));
    bytes
}

fn normalize_time(value: RawTime, seconds_east: i32) -> Result<RawTime, DecodeError> {
    let date = NaiveDate::from_ymd_opt(2000, 1, 2).ok_or(DecodeError::InvalidValue {
        row: 0,
        message: "could not construct reference date",
    })?;
    let time = naive_time(value, 0)?;
    let utc = fixed_offset(seconds_east)?
        .from_local_datetime(&date.and_time(time))
        .single()
        .ok_or(DecodeError::InvalidValue {
            row: 0,
            message: "invalid time zone conversion",
        })?
        .naive_utc();
    Ok(raw_time_from_naive(utc.time()))
}

fn normalize_timestamp(
    date: RawDate,
    time: RawTime,
    seconds_east: i32,
) -> Result<(RawDate, RawTime), DecodeError> {
    let date = naive_date(date, 0)?;
    let time = naive_time(time, 0)?;
    let utc = fixed_offset(seconds_east)?
        .from_local_datetime(&date.and_time(time))
        .single()
        .ok_or(DecodeError::InvalidValue {
            row: 0,
            message: "invalid timestamp time zone conversion",
        })?
        .naive_utc();
    Ok((
        raw_date_from_naive(utc.date())?,
        raw_time_from_naive(utc.time()),
    ))
}

fn raw_time_from_naive(value: NaiveTime) -> RawTime {
    RawTime {
        microseconds: value.nanosecond() / 1_000,
        seconds: value.second() as u8,
        minutes: value.minute() as u8,
        hours: value.hour() as u8,
    }
}

fn naive_date(value: RawDate, row: usize) -> Result<NaiveDate, DecodeError> {
    NaiveDate::from_ymd_opt(
        i32::from(value.year),
        u32::from(value.month),
        u32::from(value.day),
    )
    .ok_or(DecodeError::InvalidValue {
        row,
        message: "invalid Gregorian date",
    })
}

fn naive_time(value: RawTime, row: usize) -> Result<NaiveTime, DecodeError> {
    NaiveTime::from_hms_micro_opt(
        u32::from(value.hours),
        u32::from(value.minutes),
        u32::from(value.seconds),
        value.microseconds,
    )
    .ok_or(DecodeError::InvalidValue {
        row,
        message: "invalid time of day",
    })
}

fn fixed_offset(seconds_east: i32) -> Result<FixedOffset, DecodeError> {
    FixedOffset::east_opt(seconds_east).ok_or(DecodeError::InvalidValue {
        row: 0,
        message: "invalid UTC offset",
    })
}

fn raw_date_from_naive(value: NaiveDate) -> Result<RawDate, DecodeError> {
    Ok(RawDate {
        day: value.day() as u8,
        month: value.month() as u8,
        year: i16::try_from(value.year()).map_err(|_| DecodeError::InvalidValue {
            row: 0,
            message: "timestamp year does not fit MonetDB's wire format",
        })?,
    })
}

pub fn field_for_column(column: &ResultColumn) -> Result<Field, DecodeError> {
    field_for_monet_type(column.name(), column.sql_type())
}

pub fn field_for_monet_type(
    name: impl Into<String>,
    data_type: &MonetType,
) -> Result<Field, DecodeError> {
    let mut field = Field::new(name, data_type_for_monet_type(data_type)?, true);
    let extension = match data_type {
        MonetType::Json => Some("arrow.json"),
        MonetType::Uuid => Some("arrow.uuid"),
        MonetType::Oid => Some("monetdb.oid"),
        MonetType::Url => Some("monetdb.url"),
        MonetType::MonthInterval => Some("monetdb.interval_month"),
        MonetType::DayInterval => Some("monetdb.interval_day"),
        _ => None,
    };
    if let Some(name) = extension {
        field = field.with_metadata([("ARROW:extension:name".to_owned(), name.to_owned())].into());
    }
    Ok(field)
}

pub fn data_type_for_monet_type(data_type: &MonetType) -> Result<DataType, DecodeError> {
    Ok(match *data_type {
        MonetType::Bool => DataType::Boolean,
        MonetType::TinyInt => DataType::Int8,
        MonetType::SmallInt => DataType::Int16,
        MonetType::Int => DataType::Int32,
        MonetType::BigInt => DataType::Int64,
        MonetType::HugeInt => DataType::Decimal128(38, 0),
        MonetType::Oid => DataType::UInt64,
        MonetType::Decimal(precision, scale) => decimal_data_type(precision, scale)?,
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
            1u64 << 63,
            u64::from_le_bytes,
        )?)),
        MonetType::Decimal(precision, scale) => Arc::new(decode_decimal(
            bytes,
            row_count,
            precision,
            decimal_scale(scale)?,
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
    if !(1..=38).contains(&precision) || scale < 0 || scale > precision as i8 {
        return Err(DecodeError::InvalidValue {
            row: 0,
            message: "decimal precision/scale must satisfy 1 <= precision <= 38 and 0 <= scale <= precision",
        });
    }
    let limit = 10i128.pow(u32::from(precision));
    if let Some((row, _)) = values
        .iter()
        .enumerate()
        .find(|(_, value)| value.is_some_and(|value| value <= -limit || value >= limit))
    {
        return Err(DecodeError::InvalidValue {
            row,
            message: "decimal value exceeds its declared precision",
        });
    }
    Ok(Decimal128Array::from(values).with_precision_and_scale(precision, scale)?)
}

pub(crate) fn decode_strings(bytes: &[u8], rows: usize) -> Result<StringArray, DecodeError> {
    if rows > bytes.len() {
        return Err(DecodeError::Length {
            expected: rows,
            actual: bytes.len(),
        });
    }
    if bytes.len() > i32::MAX as usize {
        return Err(DecodeError::InvalidValue {
            row: 0,
            message: "UTF-8 column exceeds Arrow's 32-bit offset limit; lower adbc.monetdb.batch_rows",
        });
    }
    if let Some(array) = decode_strings_without_backrefs(bytes, rows)? {
        return Ok(array);
    }
    decode_strings_with_backrefs(bytes, rows)
}

fn decode_strings_without_backrefs(
    bytes: &[u8],
    rows: usize,
) -> Result<Option<StringArray>, DecodeError> {
    let mut builder = StringBuilder::with_capacity(rows, bytes.len());
    let mut pos = 0;
    let mut value_bytes = 0usize;
    for row in 0..rows {
        let Some(&first) = bytes.get(pos) else {
            return Err(DecodeError::Length {
                expected: pos + 1,
                actual: bytes.len(),
            });
        };
        if first == 0x80 {
            let (distance, consumed) = long_backref(&bytes[pos + 1..], row)?;
            if distance != 0 {
                return Ok(None);
            }
            pos += consumed + 1;
            builder.append_null();
            continue;
        }
        if (0x81..=0xbf).contains(&first) {
            return Ok(None);
        }
        let tail = &bytes[pos..];
        let end = memchr::memchr(0, tail).ok_or(DecodeError::Length {
            expected: bytes.len() + 1,
            actual: bytes.len(),
        })?;
        let value =
            std::str::from_utf8(&tail[..end]).map_err(|_| DecodeError::InvalidUtf8 { row })?;
        value_bytes = value_bytes
            .checked_add(value.len())
            .ok_or(DecodeError::InvalidValue {
                row,
                message: "UTF-8 column length overflows",
            })?;
        if value_bytes > i32::MAX as usize {
            return Err(DecodeError::InvalidValue {
                row,
                message: "UTF-8 column exceeds Arrow's 32-bit offset limit; lower adbc.monetdb.batch_rows",
            });
        }
        builder.append_value(value);
        pos += end + 1;
    }
    if pos != bytes.len() {
        return Err(DecodeError::Length {
            expected: pos,
            actual: bytes.len(),
        });
    }
    Ok(Some(builder.finish()))
}

fn decode_strings_with_backrefs(bytes: &[u8], rows: usize) -> Result<StringArray, DecodeError> {
    let mut builder = StringBuilder::with_capacity(rows, bytes.len());
    let mut history: Vec<Option<&str>> = Vec::with_capacity(rows);
    let mut pos = 0;
    let mut value_bytes = 0usize;
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
            let end = memchr::memchr(0, tail).ok_or(DecodeError::Length {
                expected: bytes.len() + 1,
                actual: bytes.len(),
            })?;
            let string =
                std::str::from_utf8(&tail[..end]).map_err(|_| DecodeError::InvalidUtf8 { row })?;
            pos += end + 1;
            Some(string)
        };
        match value {
            Some(value) => {
                value_bytes =
                    value_bytes
                        .checked_add(value.len())
                        .ok_or(DecodeError::InvalidValue {
                            row,
                            message: "UTF-8 column length overflows",
                        })?;
                if value_bytes > i32::MAX as usize {
                    return Err(DecodeError::InvalidValue {
                        row,
                        message: "UTF-8 column exceeds Arrow's 32-bit offset limit; lower adbc.monetdb.batch_rows",
                    });
                }
                builder.append_value(value);
            }
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
        let low = usize::from(byte & 0x7f);
        if low > (usize::MAX >> shift) {
            return Err(DecodeError::InvalidBackref { row });
        }
        let part = low << shift;
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
    let minimum = rows.checked_mul(8).ok_or(DecodeError::InvalidValue {
        row: 0,
        message: "BLOB header length overflows",
    })?;
    if minimum > bytes.len() {
        return Err(DecodeError::Length {
            expected: minimum,
            actual: bytes.len(),
        });
    }
    if bytes.len().saturating_sub(minimum) > i32::MAX as usize {
        return Err(DecodeError::InvalidValue {
            row: 0,
            message: "BLOB column exceeds Arrow's 32-bit offset limit",
        });
    }
    let mut builder = BinaryBuilder::with_capacity(rows, bytes.len());
    let mut pos = 0;
    let mut value_bytes = 0usize;
    for row in 0..rows {
        let header = bytes.get(pos..pos + 8).ok_or(DecodeError::Length {
            expected: pos + 8,
            actual: bytes.len(),
        })?;
        let length = i64::from_le_bytes(header.try_into().expect("blob header is 8 bytes"));
        pos += 8;
        if length == -1 {
            builder.append_null();
            continue;
        }
        if length < -1 {
            return Err(DecodeError::InvalidValue {
                row,
                message: "BLOB length is negative but is not the -1 NULL sentinel",
            });
        }
        let length = usize::try_from(length).map_err(|_| DecodeError::InvalidValue {
            row,
            message: "blob length does not fit in memory",
        })?;
        let value = bytes.get(pos..pos + length).ok_or(DecodeError::Length {
            expected: pos + length,
            actual: bytes.len(),
        })?;
        value_bytes = value_bytes
            .checked_add(length)
            .ok_or(DecodeError::InvalidValue {
                row,
                message: "BLOB column length overflows",
            })?;
        if value_bytes > i32::MAX as usize {
            return Err(DecodeError::InvalidValue {
                row,
                message: "BLOB column exceeds Arrow's 32-bit offset limit",
            });
        }
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
    let date = naive_date(RawDate { day, month, year }, row)?;
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).ok_or(DecodeError::InvalidValue {
        row,
        message: "could not construct Unix epoch date",
    })?;
    let days = date.signed_duration_since(epoch).num_days();
    Ok(Some(i32::try_from(days).map_err(|_| {
        DecodeError::InvalidValue {
            row,
            message: "date is outside Arrow's Date32 range",
        }
    })?))
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
        return Err(DecodeError::InvalidValue {
            row: 0,
            message: "INET column length is neither 4 nor 16 bytes per row",
        });
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
    fn validates_frame_identity_and_offset() {
        let mut frame = b"&6 7 0 0 5\n".to_vec();
        frame.extend_from_slice(&(frame.len() as i64).to_le_bytes());
        assert!(matches!(
            decode_frame(&frame, &[], 8, 5),
            Err(DecodeError::ResultId { .. })
        ));
        assert!(matches!(
            decode_frame(&frame, &[], 7, 4),
            Err(DecodeError::StartRow { .. })
        ));
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
    fn rejects_hostile_variable_width_metadata_without_allocating() {
        assert!(matches!(
            decode_strings(b"\0", usize::MAX),
            Err(DecodeError::Length { .. })
        ));
        assert!(matches!(
            decode_blob(&[0; 8], usize::MAX),
            Err(DecodeError::InvalidValue { .. })
        ));
    }

    #[test]
    fn rejects_decimal_scale_outside_arrow_range() {
        assert!(matches!(
            data_type_for_monet_type(&MonetType::Decimal(38, 128)),
            Err(DecodeError::InvalidValue { .. })
        ));
        assert!(matches!(
            decode_column(&MonetType::Decimal(38, 128), &[0; 16], 1),
            Err(DecodeError::InvalidValue { .. })
        ));
        for data_type in [MonetType::Decimal(0, 0), MonetType::Decimal(38, 39)] {
            assert!(matches!(
                data_type_for_monet_type(&data_type),
                Err(DecodeError::InvalidValue { .. })
            ));
        }
    }

    #[test]
    fn rejects_values_outside_decimal128_precision() {
        let value = 10i128.pow(38).to_le_bytes();
        assert!(matches!(
            decode_column(&MonetType::HugeInt, &value, 1),
            Err(DecodeError::InvalidValue { row: 0, .. })
        ));
    }

    #[test]
    fn rejects_overlong_backref_varint() {
        if usize::BITS == 64 {
            let mut encoded = vec![0x80; 10];
            encoded[9] = 0x02;
            assert!(matches!(
                long_backref(&encoded, 1),
                Err(DecodeError::InvalidBackref { row: 1 })
            ));
        }
    }

    #[test]
    fn decodes_oid_null_sentinel() {
        let array = decode_column(&MonetType::Oid, &(1u64 << 63).to_le_bytes(), 1).unwrap();
        let array = array.as_any().downcast_ref::<UInt64Array>().unwrap();
        assert!(array.is_null(0));
        assert_eq!(parse_inline_oid("42@0").unwrap(), 42);
        let field = field_for_monet_type("oid", &MonetType::Oid).unwrap();
        assert_eq!(
            field
                .metadata()
                .get("ARROW:extension:name")
                .map(String::as_str),
            Some("monetdb.oid")
        );
        let field = field_for_monet_type("url", &MonetType::Url).unwrap();
        assert_eq!(
            field
                .metadata()
                .get("ARROW:extension:name")
                .map(String::as_str),
            Some("monetdb.url")
        );
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
    fn chrono_handles_proleptic_dates_and_timezone_rollover() {
        for (year, day, valid) in [
            (-1, 28, true),
            (0, 29, true),
            (1900, 29, false),
            (2000, 29, true),
        ] {
            let bytes = [day, 2, year as u8, (year >> 8) as u8];
            assert_eq!(date_value(&bytes, 0).is_ok(), valid, "year {year}");
        }

        let time = RawTime {
            microseconds: 123_456,
            seconds: 0,
            minutes: 30,
            hours: 0,
        };
        assert_eq!(
            normalize_time(time, 3_600).unwrap(),
            RawTime {
                microseconds: 123_456,
                seconds: 0,
                minutes: 30,
                hours: 23,
            }
        );

        let (date, time) = normalize_timestamp(
            RawDate {
                day: 1,
                month: 1,
                year: 0,
            },
            time,
            3_600,
        )
        .unwrap();
        assert_eq!(
            date,
            RawDate {
                day: 31,
                month: 12,
                year: -1,
            }
        );
        assert_eq!(time.hours, 23);
        assert_eq!(time.minutes, 30);

        assert!(
            normalize_timestamp(
                RawDate {
                    day: 1,
                    month: 1,
                    year: i16::MIN,
                },
                RawTime {
                    microseconds: 0,
                    seconds: 0,
                    minutes: 0,
                    hours: 0,
                },
                3_600,
            )
            .is_err()
        );
    }

    #[test]
    fn decodes_blobs_and_uuids() {
        let mut blobs = 3i64.to_le_bytes().to_vec();
        blobs.extend_from_slice(b"abc");
        blobs.extend_from_slice(&(-1i64).to_le_bytes());
        let array = decode_blob(&blobs, 2).unwrap();
        assert_eq!(array.value(0), b"abc");
        assert!(array.is_null(1));

        assert!(matches!(
            decode_blob(&(-2i64).to_le_bytes(), 1),
            Err(DecodeError::InvalidValue { row: 0, .. })
        ));

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
            data_type_for_monet_type(&MonetType::Geometry)
                .unwrap_err()
                .to_string(),
            "MonetDB type GEOMETRY is not available through Xexportbin; cast the column to VARCHAR in SQL"
        );
    }
}
