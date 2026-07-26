//! Decoding MonetDB COPY BINARY column buffers into Arrow arrays.
//!
//! Wire layouts and null sentinels follow MonetDB's
//! `sql/backends/monet5/sql_bincopyconvert.c`, `common/utils/copybinary.h`,
//! and `documentation/source/bincopy-backref.rst`.

use std::{
    fmt::{self, Write},
    net::Ipv6Addr,
    sync::Arc,
};

use arrow_array::{
    ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, FixedSizeBinaryArray,
    PrimitiveArray, RecordBatch, RecordBatchOptions, StringArray, Time64MicrosecondArray,
    TimestampMicrosecondArray,
    builder::{BinaryBuilder, FixedSizeBinaryBuilder, StringBuilder},
    types::{
        ArrowPrimitiveType, DurationMillisecondType, Float32Type, Float64Type, Int8Type, Int16Type,
        Int32Type, Int64Type, UInt64Type,
    },
};
use arrow_buffer::{BooleanBuffer, Buffer, NullBuffer, ScalarBuffer};
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

use crate::exportbin::{ExportbinFrame, FrameError, parse_frame};

#[derive(Debug)]
pub enum DecodeError {
    Frame(FrameError),
    ResultId { expected: u64, actual: i64 },
    StartRow { expected: u64, actual: u64 },
    RowCount { requested: usize, actual: usize },
    ColumnCount { expected: usize, actual: usize },
    Length { expected: usize, actual: usize },
    InvalidColumn { message: &'static str },
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
            Self::RowCount { requested, actual } => {
                write!(
                    f,
                    "frame contains {actual} rows; at most {requested} were requested"
                )
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
            Self::InvalidColumn { message } => write!(f, "invalid column: {message}"),
            Self::InvalidValue { row, message } => {
                write!(f, "invalid value at row {row}: {message}")
            }
            Self::InvalidUtf8 { row } => write!(f, "invalid UTF-8 at row {row}"),
            Self::InvalidBackref { row } => write!(f, "invalid string back-reference at row {row}"),
            Self::Cursor(error) => error.fmt(f),
            Self::Unsupported(data_type) => {
                if matches!(
                    data_type,
                    MonetType::Geometry | MonetType::Inet | MonetType::Xml
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
    i8::try_from(scale).map_err(|_| DecodeError::InvalidColumn {
        message: "decimal scale exceeds Arrow's maximum of 127",
    })
}

fn decimal_data_type(precision: u8, scale: u8) -> Result<DataType, DecodeError> {
    if !(1..=38).contains(&precision) || scale > precision {
        return Err(DecodeError::InvalidColumn {
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
    requested_rows: usize,
) -> Result<RecordBatch, DecodeError> {
    let schema = schema_for_columns(columns)?;
    decode_frame_with_schema(
        frame,
        columns,
        expected_result_id,
        expected_start_row,
        requested_rows,
        schema,
    )
}

pub fn decode_frame_with_schema(
    frame: &[u8],
    columns: &[ResultColumn],
    expected_result_id: u64,
    expected_start_row: u64,
    requested_rows: usize,
    schema: Arc<Schema>,
) -> Result<RecordBatch, DecodeError> {
    let frame = parse_frame(frame)?;
    let row_count = validate_frame(
        &frame,
        columns,
        expected_result_id,
        expected_start_row,
        requested_rows,
    )?;
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
    record_batch(schema, arrays, row_count)
}

/// Return whether the result's fixed-width layout is dominated by columns
/// that can share an owned wire frame. Variable-width columns use the reusable
/// copy path so multi-window reads do not retain or repeatedly allocate frames.
pub fn prefers_owned_frame(columns: &[ResultColumn]) -> bool {
    prefers_owned_types(columns.iter().map(|column| *column.sql_type()))
}

/// Return an upper bound for one fixed-width `Xexportbin` response allocation.
pub fn owned_frame_capacity(columns: &[ResultColumn], rows: usize) -> Option<usize> {
    let mut capacity = 128usize;
    for column in columns {
        let width = crate::wire::fixed_wire_width(*column.sql_type())?;
        capacity = capacity
            .checked_add(31)?
            .checked_add(rows.checked_mul(width)?)?;
    }
    capacity
        .checked_add(31)?
        .checked_add(columns.len().checked_mul(16)?)?
        .checked_add(8)
}

fn prefers_owned_types(types: impl IntoIterator<Item = MonetType>) -> bool {
    let mut adoptable_bytes = 0usize;
    let mut frame_bytes = 0usize;
    for data_type in types {
        let width = match crate::wire::fixed_wire_width(data_type) {
            Some(width) => width,
            None => return false,
        };
        let adoptable = match data_type {
            MonetType::TinyInt
            | MonetType::SmallInt
            | MonetType::Int
            | MonetType::Real
            | MonetType::MonthInterval
            | MonetType::BigInt
            | MonetType::Oid
            | MonetType::Double
            | MonetType::DayInterval
            | MonetType::SecInterval
            | MonetType::HugeInt
            | MonetType::Decimal(19..=38, _) => true,
            MonetType::Bool
            | MonetType::Uuid
            | MonetType::Decimal(_, _)
            | MonetType::Time
            | MonetType::TimeTz
            | MonetType::Date
            | MonetType::Timestamp
            | MonetType::TimestampTz => false,
            MonetType::Varchar(_)
            | MonetType::Blob
            | MonetType::Url
            | MonetType::Inet
            | MonetType::Inet4
            | MonetType::Inet6
            | MonetType::Json
            | MonetType::Geometry
            | MonetType::Xml => unreachable!("variable-width types were handled above"),
        };
        frame_bytes = frame_bytes.saturating_add(width);
        if adoptable {
            adoptable_bytes = adoptable_bytes.saturating_add(width);
        }
    }
    adoption_is_worthwhile(adoptable_bytes, frame_bytes)
}

/// Decode a frame while allowing fixed-width Arrow arrays to share its owned
/// allocation. Columns that are not layout-compatible or aligned use the copy
/// decoder.
pub fn decode_frame_owned(
    frame: Vec<u8>,
    columns: &[ResultColumn],
    expected_result_id: u64,
    expected_start_row: u64,
    requested_rows: usize,
) -> Result<RecordBatch, DecodeError> {
    let schema = schema_for_columns(columns)?;
    decode_frame_owned_with_schema(
        frame,
        columns,
        expected_result_id,
        expected_start_row,
        requested_rows,
        schema,
    )
}

pub fn decode_frame_owned_with_schema(
    frame: Vec<u8>,
    columns: &[ResultColumn],
    expected_result_id: u64,
    expected_start_row: u64,
    requested_rows: usize,
    schema: Arc<Schema>,
) -> Result<RecordBatch, DecodeError> {
    let base = frame.as_ptr() as usize;
    let (row_count, ranges, frame_bytes) = {
        let parsed = parse_frame(&frame)?;
        let row_count = validate_frame(
            &parsed,
            columns,
            expected_result_id,
            expected_start_row,
            requested_rows,
        )?;
        let mut frame_bytes = 0usize;
        let ranges = parsed
            .columns
            .iter()
            .map(|bytes| {
                frame_bytes = frame_bytes.saturating_add(bytes.len());
                let start = (bytes.as_ptr() as usize).checked_sub(base).ok_or(
                    DecodeError::InvalidColumn {
                        message: "column pointer precedes its frame allocation",
                    },
                )?;
                let end = start
                    .checked_add(bytes.len())
                    .filter(|end| *end <= frame.len())
                    .ok_or(DecodeError::InvalidColumn {
                        message: "column range exceeds its frame allocation",
                    })?;
                Ok(start..end)
            })
            .collect::<Result<Vec<_>, DecodeError>>()?;
        if !should_adopt_frame(&frame, columns, &ranges, frame_bytes) {
            let arrays = decode_columns(columns, &parsed.columns, row_count, frame_bytes)?;
            return record_batch(schema, arrays, row_count);
        }
        (row_count, ranges, frame_bytes)
    };
    let buffer = Buffer::from_vec(frame);
    let decode = |(column, range): (&ResultColumn, &std::ops::Range<usize>)| {
        decode_owned_column(&buffer, range, column.sql_type(), row_count)
    };
    let arrays = if columns.len() > 1 && frame_bytes >= 1024 * 1024 {
        columns
            .par_iter()
            .zip(ranges.par_iter())
            .map(decode)
            .collect::<Result<Vec<_>, _>>()?
    } else {
        columns
            .iter()
            .zip(ranges.iter())
            .map(decode)
            .collect::<Result<Vec<_>, _>>()?
    };
    record_batch(schema, arrays, row_count)
}

fn schema_for_columns(columns: &[ResultColumn]) -> Result<Arc<Schema>, DecodeError> {
    let fields = columns
        .iter()
        .map(field_for_column)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Arc::new(Schema::new(fields)))
}

fn decode_columns(
    columns: &[ResultColumn],
    bytes: &[&[u8]],
    row_count: usize,
    frame_bytes: usize,
) -> Result<Vec<ArrayRef>, DecodeError> {
    if columns.len() > 1 && frame_bytes >= 1024 * 1024 {
        columns
            .par_iter()
            .zip(bytes.par_iter())
            .map(|(column, bytes)| decode_column(column.sql_type(), bytes, row_count))
            .collect()
    } else {
        columns
            .iter()
            .zip(bytes)
            .map(|(column, bytes)| decode_column(column.sql_type(), bytes, row_count))
            .collect()
    }
}

fn should_adopt_frame(
    frame: &[u8],
    columns: &[ResultColumn],
    ranges: &[std::ops::Range<usize>],
    frame_bytes: usize,
) -> bool {
    if cfg!(target_endian = "big") || frame_bytes == 0 {
        return false;
    }
    let adoptable_bytes = columns
        .iter()
        .zip(ranges)
        .filter_map(|(column, range)| {
            let alignment = match column.sql_type() {
                MonetType::TinyInt => std::mem::align_of::<i8>(),
                MonetType::SmallInt => std::mem::align_of::<i16>(),
                MonetType::Int | MonetType::Real | MonetType::MonthInterval => {
                    std::mem::align_of::<i32>()
                }
                MonetType::BigInt
                | MonetType::Oid
                | MonetType::Double
                | MonetType::DayInterval
                | MonetType::SecInterval => std::mem::align_of::<i64>(),
                MonetType::HugeInt | MonetType::Decimal(19..=38, _) => std::mem::align_of::<i128>(),
                _ => return None,
            };
            let pointer = frame.as_ptr().wrapping_add(range.start) as usize;
            pointer.is_multiple_of(alignment).then_some(range.len())
        })
        .fold(0usize, usize::saturating_add);
    adoption_is_worthwhile(adoptable_bytes, frame_bytes)
}

fn adoption_is_worthwhile(adoptable_bytes: usize, frame_bytes: usize) -> bool {
    frame_bytes > 0 && adoptable_bytes >= frame_bytes - frame_bytes / 4
}

fn validate_frame(
    frame: &ExportbinFrame<'_>,
    columns: &[ResultColumn],
    expected_result_id: u64,
    expected_start_row: u64,
    requested_rows: usize,
) -> Result<usize, DecodeError> {
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
    if row_count > requested_rows {
        return Err(DecodeError::RowCount {
            requested: requested_rows,
            actual: row_count,
        });
    }
    Ok(row_count)
}

fn record_batch(
    schema: Arc<Schema>,
    arrays: Vec<ArrayRef>,
    row_count: usize,
) -> Result<RecordBatch, DecodeError> {
    let options = RecordBatchOptions::new().with_row_count(Some(row_count));
    Ok(RecordBatch::try_new_with_options(schema, arrays, &options)?)
}

fn decode_owned_column(
    buffer: &Buffer,
    range: &std::ops::Range<usize>,
    data_type: &MonetType,
    rows: usize,
) -> Result<ArrayRef, DecodeError> {
    let adopted = match data_type {
        MonetType::TinyInt => {
            adopt_primitive::<Int8Type, _>(buffer, range, rows, |value| value == i8::MIN)?
        }
        MonetType::SmallInt => {
            adopt_primitive::<Int16Type, _>(buffer, range, rows, |value| value == i16::MIN)?
        }
        MonetType::Int => {
            adopt_primitive::<Int32Type, _>(buffer, range, rows, |value| value == i32::MIN)?
        }
        MonetType::BigInt => {
            adopt_primitive::<Int64Type, _>(buffer, range, rows, |value| value == i64::MIN)?
        }
        MonetType::Oid => {
            adopt_primitive::<UInt64Type, _>(buffer, range, rows, |value| value == 1 << 63)?
        }
        MonetType::Real => adopt_primitive::<Float32Type, _>(buffer, range, rows, f32::is_nan)?,
        MonetType::Double => adopt_primitive::<Float64Type, _>(buffer, range, rows, f64::is_nan)?,
        MonetType::MonthInterval => {
            adopt_primitive::<Int32Type, _>(buffer, range, rows, |value| value == i32::MIN)?
        }
        MonetType::DayInterval | MonetType::SecInterval => {
            adopt_primitive::<DurationMillisecondType, _>(buffer, range, rows, |value| {
                value == i64::MIN
            })?
        }
        MonetType::HugeInt => adopt_decimal(buffer, range, rows, 38, 0, true)?,
        MonetType::Decimal(precision @ 19..=38, scale) => adopt_decimal(
            buffer,
            range,
            rows,
            *precision,
            decimal_scale(*scale)?,
            false,
        )?,
        _ => None,
    };
    match adopted {
        Some(array) => Ok(array),
        None => decode_column(data_type, &buffer.as_slice()[range.clone()], rows),
    }
}

fn adopt_decimal(
    buffer: &Buffer,
    range: &std::ops::Range<usize>,
    rows: usize,
    precision: u8,
    scale: i8,
    hugeint: bool,
) -> Result<Option<ArrayRef>, DecodeError> {
    let bytes = &buffer.as_slice()[range.clone()];
    expect_fixed(bytes, rows, 16)?;
    if cfg!(target_endian = "big") || bytes.as_ptr().align_offset(std::mem::align_of::<i128>()) != 0
    {
        return Ok(None);
    }
    let values =
        ScalarBuffer::<i128>::new(buffer.slice_with_length(range.start, range.len()), 0, rows);
    let nulls = validate_decimal_values(&values, precision, hugeint)?;
    let array = Decimal128Array::new(values, nulls).with_precision_and_scale(precision, scale)?;
    Ok(Some(Arc::new(array)))
}

fn adopt_primitive<T, F>(
    buffer: &Buffer,
    range: &std::ops::Range<usize>,
    rows: usize,
    is_null: F,
) -> Result<Option<ArrayRef>, DecodeError>
where
    T: ArrowPrimitiveType,
    T::Native: Copy,
    F: Fn(T::Native) -> bool + Copy,
{
    let bytes = &buffer.as_slice()[range.clone()];
    expect_fixed(bytes, rows, std::mem::size_of::<T::Native>())?;
    if cfg!(target_endian = "big")
        || bytes
            .as_ptr()
            .align_offset(std::mem::align_of::<T::Native>())
            != 0
    {
        return Ok(None);
    }
    let values =
        ScalarBuffer::<T::Native>::new(buffer.slice_with_length(range.start, range.len()), 0, rows);
    let has_nulls = values.iter().copied().any(is_null);
    let nulls = has_nulls.then(|| {
        NullBuffer::new(BooleanBuffer::collect_bool(rows, |row| {
            !is_null(values[row])
        }))
    });
    let array = arrow_array::PrimitiveArray::<T>::try_new(values, nulls)?;
    Ok(Some(Arc::new(array)))
}

/// Decode the current inline text rows of a MAPI result through the same
/// validated wire-to-Arrow path used by `Xexportbin`.
pub fn decode_inline_rows(
    cursor: &mut Cursor,
    columns: &[ResultColumn],
    expected_rows: usize,
) -> Result<RecordBatch, DecodeError> {
    let fields = columns
        .iter()
        .map(field_for_column)
        .collect::<Result<Vec<_>, _>>()?;
    let mut buffers = columns.iter().map(|_| Vec::new()).collect::<Vec<_>>();
    let mut rows = 0;
    while cursor.next_row()? {
        if rows >= expected_rows {
            return Err(DecodeError::InvalidValue {
                row: rows,
                message: "inline result contained more rows than reported",
            });
        }
        for (index, column) in columns.iter().enumerate() {
            buffers[index].extend(inline_wire_value(cursor, index, column.sql_type())?);
        }
        rows += 1;
    }
    if rows != expected_rows {
        return Err(DecodeError::RowCount {
            requested: expected_rows,
            actual: rows,
        });
    }
    let arrays = columns
        .iter()
        .enumerate()
        .map(|(index, column)| decode_column(column.sql_type(), &buffers[index], rows))
        .collect::<Result<Vec<_>, _>>()?;
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
        MonetType::Inet => return Err(DecodeError::Unsupported(*data_type)),
        MonetType::Inet4 => required(cursor.get_str(index)?)?
            .parse::<std::net::Ipv4Addr>()
            .map_err(|_| DecodeError::InvalidValue {
                row: 0,
                message: "invalid INET4 address",
            })?
            .octets()
            .to_vec(),
        MonetType::Inet6 => required(cursor.get_str(index)?)?
            .parse::<std::net::Ipv6Addr>()
            .map_err(|_| DecodeError::InvalidValue {
                row: 0,
                message: "invalid INET6 address",
            })?
            .octets()
            .to_vec(),
        MonetType::Geometry | MonetType::Xml => {
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
            1..=2 => i8::MIN.to_le_bytes().to_vec(),
            3..=4 => i16::MIN.to_le_bytes().to_vec(),
            5..=9 => i32::MIN.to_le_bytes().to_vec(),
            10..=18 => i64::MIN.to_le_bytes().to_vec(),
            19..=38 => i128::MIN.to_le_bytes().to_vec(),
            _ => {
                return Err(DecodeError::InvalidColumn {
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
        MonetType::Inet4 => vec![0; 4],
        MonetType::Inet6 => vec![0; 16],
        MonetType::Geometry | MonetType::Inet | MonetType::Xml => {
            return Err(DecodeError::Unsupported(*data_type));
        }
    })
}

fn decimal_wire_value(value: i128, precision: u8) -> Result<Vec<u8>, DecodeError> {
    macro_rules! narrow {
        ($type:ty) => {{
            let narrowed = <$type>::try_from(value).map_err(|_| DecodeError::InvalidValue {
                row: 0,
                message: "decimal value does not fit its backing integer",
            })?;
            if narrowed == <$type>::MIN {
                return Err(DecodeError::InvalidValue {
                    row: 0,
                    message: "decimal value collides with the wire NULL sentinel",
                });
            }
            narrowed.to_le_bytes().to_vec()
        }};
    }
    Ok(match precision {
        1..=2 => narrow!(i8),
        3..=4 => narrow!(i16),
        5..=9 => narrow!(i32),
        10..=18 => narrow!(i64),
        19..=38 => {
            if value == i128::MIN {
                return Err(DecodeError::InvalidValue {
                    row: 0,
                    message: "decimal value collides with the wire NULL sentinel",
                });
            }
            value.to_le_bytes().to_vec()
        }
        _ => {
            return Err(DecodeError::InvalidColumn {
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
        MonetType::HugeInt => Some("monetdb.hugeint"),
        MonetType::Oid => Some("monetdb.oid"),
        MonetType::Inet4 => Some("monetdb.inet4"),
        MonetType::Inet6 => Some("monetdb.inet6"),
        MonetType::Url => Some("monetdb.url"),
        MonetType::MonthInterval => Some("monetdb.interval_month"),
        MonetType::DayInterval => Some("monetdb.interval_day"),
        MonetType::TimeTz => Some("monetdb.timetz"),
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
        MonetType::Varchar(_)
        | MonetType::Url
        | MonetType::Json
        | MonetType::Inet4
        | MonetType::Inet6 => DataType::Utf8,
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
        MonetType::Geometry | MonetType::Inet | MonetType::Xml => {
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
        MonetType::TinyInt => Arc::new(decode_primitive::<1, Int8Type>(
            bytes,
            row_count,
            i8::from_le_bytes,
            |value| value == i8::MIN,
        )?),
        MonetType::SmallInt => Arc::new(decode_primitive::<2, Int16Type>(
            bytes,
            row_count,
            i16::from_le_bytes,
            |value| value == i16::MIN,
        )?),
        MonetType::Int => Arc::new(decode_primitive::<4, Int32Type>(
            bytes,
            row_count,
            i32::from_le_bytes,
            |value| value == i32::MIN,
        )?),
        MonetType::BigInt => Arc::new(decode_primitive::<8, Int64Type>(
            bytes,
            row_count,
            i64::from_le_bytes,
            |value| value == i64::MIN,
        )?),
        MonetType::HugeInt => Arc::new(decode_decimal(bytes, row_count, 38, 0, true)?),
        MonetType::Oid => Arc::new(decode_primitive::<8, UInt64Type>(
            bytes,
            row_count,
            u64::from_le_bytes,
            |value| value == 1u64 << 63,
        )?),
        MonetType::Decimal(precision, scale) => Arc::new(decode_decimal(
            bytes,
            row_count,
            precision,
            decimal_scale(scale)?,
            false,
        )?),
        MonetType::Varchar(_) | MonetType::Url | MonetType::Json => {
            Arc::new(decode_strings(bytes, row_count)?)
        }
        MonetType::Real => Arc::new(decode_primitive::<4, Float32Type>(
            bytes,
            row_count,
            f32::from_le_bytes,
            f32::is_nan,
        )?),
        MonetType::Double => Arc::new(decode_primitive::<8, Float64Type>(
            bytes,
            row_count,
            f64::from_le_bytes,
            f64::is_nan,
        )?),
        MonetType::MonthInterval => Arc::new(decode_primitive::<4, Int32Type>(
            bytes,
            row_count,
            i32::from_le_bytes,
            |value| value == i32::MIN,
        )?),
        MonetType::DayInterval | MonetType::SecInterval => {
            Arc::new(decode_primitive::<8, DurationMillisecondType>(
                bytes,
                row_count,
                i64::from_le_bytes,
                |value| value == i64::MIN,
            )?)
        }
        MonetType::Time | MonetType::TimeTz => Arc::new(decode_time(bytes, row_count)?),
        MonetType::Date => Arc::new(decode_date(bytes, row_count)?),
        MonetType::Timestamp => Arc::new(decode_timestamp(bytes, row_count, false)?),
        MonetType::TimestampTz => Arc::new(decode_timestamp(bytes, row_count, true)?),
        MonetType::Blob => Arc::new(decode_blob(bytes, row_count)?),
        MonetType::Uuid => Arc::new(decode_uuid(bytes, row_count)?),
        MonetType::Inet4 | MonetType::Inet6 => Arc::new(decode_inet(bytes, row_count)?),
        MonetType::Geometry | MonetType::Inet | MonetType::Xml => {
            return Err(DecodeError::Unsupported(*data_type));
        }
    })
}

fn expect_fixed(bytes: &[u8], rows: usize, width: usize) -> Result<(), DecodeError> {
    let expected = rows.checked_mul(width).ok_or(DecodeError::InvalidColumn {
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

fn decode_primitive<const N: usize, T: ArrowPrimitiveType>(
    bytes: &[u8],
    rows: usize,
    _decode: impl Fn([u8; N]) -> T::Native,
    is_null: impl Fn(T::Native) -> bool,
) -> Result<PrimitiveArray<T>, DecodeError>
where
    T::Native: Copy,
{
    const {
        assert!(N == std::mem::size_of::<T::Native>());
    }
    expect_fixed(bytes, rows, N)?;
    #[cfg(target_endian = "little")]
    {
        let buffer = Buffer::from_slice_ref(bytes);
        let values = ScalarBuffer::<T::Native>::new(buffer, 0, rows);
        let has_nulls = values.iter().copied().any(&is_null);
        let nulls = has_nulls.then(|| {
            NullBuffer::new(BooleanBuffer::collect_bool(rows, |row| {
                !is_null(values[row])
            }))
        });
        Ok(PrimitiveArray::<T>::new(values, nulls))
    }
    #[cfg(target_endian = "big")]
    {
        let mut values = Vec::with_capacity(rows);
        let mut validity = Vec::with_capacity(rows);
        let mut has_nulls = false;
        for chunk in bytes.chunks_exact(N) {
            let value = _decode(chunk.try_into().expect("chunk has fixed width"));
            let valid = !is_null(value);
            values.push(value);
            validity.push(valid);
            has_nulls |= !valid;
        }
        let nulls = has_nulls.then(|| NullBuffer::from(validity));
        Ok(PrimitiveArray::<T>::new(values.into(), nulls))
    }
}

fn decode_bool(bytes: &[u8], rows: usize) -> Result<BooleanArray, DecodeError> {
    expect_fixed(bytes, rows, 1)?;
    let invalid = bytes.iter().copied().fold(false, |invalid, value| {
        invalid | (value > 1 && value != 0x80)
    });
    if invalid {
        let row = bytes
            .iter()
            .position(|&value| value > 1 && value != 0x80)
            .expect("the validation pass found an invalid boolean");
        return Err(DecodeError::InvalidValue {
            row,
            message: "boolean is not 0, 1, or the NULL sentinel 0x80",
        });
    }
    let values = BooleanBuffer::collect_bool(rows, |row| bytes[row] == 1);
    let nulls = memchr::memchr(0x80, bytes)
        .map(|_| NullBuffer::new(BooleanBuffer::collect_bool(rows, |row| bytes[row] != 0x80)));
    Ok(BooleanArray::new(values, nulls))
}

fn decode_decimal(
    bytes: &[u8],
    rows: usize,
    precision: u8,
    scale: i8,
    hugeint: bool,
) -> Result<Decimal128Array, DecodeError> {
    if !(1..=38).contains(&precision) || scale < 0 || scale > precision as i8 {
        return Err(DecodeError::InvalidColumn {
            message: "decimal precision/scale must satisfy 1 <= precision <= 38 and 0 <= scale <= precision",
        });
    }
    let limit = 10i128.pow(u32::from(precision));
    let width = if hugeint {
        16
    } else {
        match precision {
            1..=2 => 1,
            3..=4 => 2,
            5..=9 => 4,
            10..=18 => 8,
            19..=38 => 16,
            _ => unreachable!("precision was validated before decoding"),
        }
    };
    expect_fixed(bytes, rows, width)?;
    #[cfg(target_endian = "little")]
    if width == 16 {
        let values = ScalarBuffer::<i128>::new(Buffer::from_slice_ref(bytes), 0, rows);
        let nulls = validate_decimal_values(&values, precision, hugeint)?;
        return Ok(Decimal128Array::new(values, nulls).with_precision_and_scale(precision, scale)?);
    }
    let mut values = Vec::with_capacity(rows);
    let mut validity = Vec::with_capacity(rows);
    let mut has_nulls = false;
    macro_rules! decode_width {
        ($wire:ty, $width:literal) => {
            for (row, chunk) in bytes.chunks_exact($width).enumerate() {
                let value = i128::from(<$wire>::from_le_bytes(
                    chunk
                        .try_into()
                        .expect(concat!(stringify!($width), "-byte decimal")),
                ));
                let null = value == i128::from(<$wire>::MIN);
                if !null && (value <= -limit || value >= limit) {
                    return Err(DecodeError::InvalidValue {
                        row,
                        message: if hugeint {
                            "HUGEINT exceeds Arrow Decimal128's supported 38-digit range"
                        } else {
                            "decimal value exceeds its declared precision"
                        },
                    });
                }
                values.push(value);
                validity.push(!null);
                has_nulls |= null;
            }
        };
    }
    match width {
        1 => decode_width!(i8, 1),
        2 => decode_width!(i16, 2),
        4 => decode_width!(i32, 4),
        8 => decode_width!(i64, 8),
        16 => decode_width!(i128, 16),
        _ => unreachable!("MonetDB decimal backing widths are fixed"),
    }
    let nulls = has_nulls.then(|| NullBuffer::from(validity));
    Ok(Decimal128Array::new(values.into(), nulls).with_precision_and_scale(precision, scale)?)
}

fn validate_decimal_values(
    values: &ScalarBuffer<i128>,
    precision: u8,
    hugeint: bool,
) -> Result<Option<NullBuffer>, DecodeError> {
    let limit = 10i128.pow(u32::from(precision));
    let mut has_nulls = false;
    for (row, value) in values.iter().copied().enumerate() {
        let null = value == i128::MIN;
        if !null && (value <= -limit || value >= limit) {
            return Err(DecodeError::InvalidValue {
                row,
                message: if hugeint {
                    "HUGEINT exceeds Arrow Decimal128's supported 38-digit range"
                } else {
                    "decimal value exceeds its declared precision"
                },
            });
        }
        has_nulls |= null;
    }
    Ok(has_nulls.then(|| {
        NullBuffer::new(BooleanBuffer::collect_bool(values.len(), |row| {
            values[row] != i128::MIN
        }))
    }))
}

pub(crate) fn decode_strings(bytes: &[u8], rows: usize) -> Result<StringArray, DecodeError> {
    if rows > bytes.len() {
        return Err(DecodeError::InvalidColumn {
            message: "string column contains fewer encoded bytes than declared rows",
        });
    }
    if bytes.len() > i32::MAX as usize {
        return Err(DecodeError::InvalidColumn {
            message: "UTF-8 column exceeds Arrow's 32-bit offset limit; lower adbc.monetdb.read_batch_rows",
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
    let mut pos = 0usize;
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
        let end = memchr::memchr(0, tail).ok_or(DecodeError::InvalidValue {
            row,
            message: "string is not NUL-terminated",
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
                message: "UTF-8 column exceeds Arrow's 32-bit offset limit; lower adbc.monetdb.read_batch_rows",
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
    // Dec2025's `sql_bincopyconvert.c` emits literal strings only. Keep the
    // bounded backreference path for future exporters without letting a
    // hostile response expand without limit.
    let expansion_limit = bytes.len().saturating_mul(16).max(1024 * 1024);
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
            let end = memchr::memchr(0, tail).ok_or(DecodeError::InvalidValue {
                row,
                message: "string is not NUL-terminated",
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
                if value_bytes > i32::MAX as usize || value_bytes > expansion_limit {
                    return Err(DecodeError::InvalidValue {
                        row,
                        message: "UTF-8 back-references expand beyond the allowed wire-size multiple",
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
    // `bincopy-backref.rst` defines the long form and the server loader accepts
    // overlong representations. Bound the shift below; canonicality is not
    // required because this decoder never re-emits the representation.
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
    let minimum = rows.checked_mul(8).ok_or(DecodeError::InvalidColumn {
        message: "BLOB header length overflows",
    })?;
    if minimum > bytes.len() {
        return Err(DecodeError::Length {
            expected: minimum,
            actual: bytes.len(),
        });
    }
    if bytes.len().saturating_sub(minimum) > i32::MAX as usize {
        return Err(DecodeError::InvalidColumn {
            message: "BLOB column exceeds Arrow's 32-bit offset limit",
        });
    }
    let mut builder = BinaryBuilder::with_capacity(rows, bytes.len());
    let mut pos = 0usize;
    let mut value_bytes = 0usize;
    for row in 0..rows {
        let header_end = pos.checked_add(8).ok_or(DecodeError::InvalidValue {
            row,
            message: "BLOB header offset overflows",
        })?;
        let header = bytes.get(pos..header_end).ok_or(DecodeError::Length {
            expected: header_end,
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
        let value_end = pos.checked_add(length).ok_or(DecodeError::InvalidValue {
            row,
            message: "BLOB value offset overflows",
        })?;
        let value = bytes.get(pos..value_end).ok_or(DecodeError::Length {
            expected: value_end,
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
    // `gdk_atoms.h:is_uuid_nil` defines UUID nil as all-zero bytes.
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
    let mut values = Vec::with_capacity(rows);
    let mut validity = Vec::with_capacity(rows);
    let mut has_nulls = false;
    for (row, value) in bytes.chunks_exact(4).enumerate() {
        match date_value(value, row)? {
            Some(value) => {
                values.push(value);
                validity.push(true);
            }
            None => {
                values.push(0);
                validity.push(false);
                has_nulls = true;
            }
        }
    }
    let nulls = has_nulls.then(|| NullBuffer::from(validity));
    Ok(Date32Array::new(values.into(), nulls))
}

fn date_value(value: &[u8], row: usize) -> Result<Option<i32>, DecodeError> {
    let day = value[0];
    let month = value[1];
    if month == u8::MAX {
        return if value.iter().all(|byte| *byte == u8::MAX) {
            Ok(None)
        } else {
            Err(DecodeError::InvalidValue {
                row,
                message: "date has a partial NULL sentinel",
            })
        };
    }
    let year = i16::from_le_bytes(value[2..4].try_into().expect("date year is 2 bytes"));
    let (year, month, day) = (i32::from(year), u32::from(month), u32::from(day));
    if !valid_ymd(year, month, day) {
        return Err(DecodeError::InvalidValue {
            row,
            message: "invalid Gregorian date",
        });
    }
    Ok(Some(
        i32::try_from(days_from_civil(year, month, day)).map_err(|_| {
            DecodeError::InvalidValue {
                row,
                message: "date is outside Arrow's Date32 range",
            }
        })?,
    ))
}

#[inline]
fn valid_ymd(year: i32, month: u32, day: u32) -> bool {
    const DAYS_PER_MONTH: [u32; 12] = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    if !(1..=12).contains(&month) || day == 0 {
        return false;
    }
    let leap = (year % 4 == 0 && year % 100 != 0) || year % 400 == 0;
    let last_day = if month == 2 && leap {
        29
    } else {
        DAYS_PER_MONTH[(month - 1) as usize]
    };
    day <= last_day
}

#[inline]
fn days_from_civil(year: i32, month: u32, day: u32) -> i64 {
    // Howard Hinnant's proleptic-Gregorian `days_from_civil` algorithm:
    // https://howardhinnant.github.io/date_algorithms.html#days_from_civil
    let year = if month <= 2 { year - 1 } else { year };
    // Rust integer division truncates toward zero, so negative years need this
    // adjustment to obtain the floor-division era used by the algorithm.
    let era = (if year >= 0 { year } else { year - 399 }) / 400;
    let year_of_era = year - era * 400;
    let shifted_month = if month > 2 { month - 3 } else { month + 9 };
    let day_of_year = (153 * shifted_month + 2) / 5 + day - 1;
    let day_of_era = i64::from(year_of_era) * 365 + i64::from(year_of_era / 4)
        - i64::from(year_of_era / 100)
        + i64::from(day_of_year);
    i64::from(era) * 146_097 + day_of_era - 719_468
}

fn decode_time(bytes: &[u8], rows: usize) -> Result<Time64MicrosecondArray, DecodeError> {
    expect_fixed(bytes, rows, 8)?;
    let mut values = Vec::with_capacity(rows);
    let mut validity = Vec::with_capacity(rows);
    let mut has_nulls = false;
    for (row, value) in bytes.chunks_exact(8).enumerate() {
        match time_value(value, row)? {
            Some(value) => {
                values.push(value);
                validity.push(true);
            }
            None => {
                values.push(0);
                validity.push(false);
                has_nulls = true;
            }
        }
    }
    let nulls = has_nulls.then(|| NullBuffer::from(validity));
    Ok(Time64MicrosecondArray::new(values.into(), nulls))
}

fn time_value(value: &[u8], row: usize) -> Result<Option<i64>, DecodeError> {
    // The C field is named `ms`, but contains microseconds.
    let micros = u32::from_le_bytes(value[..4].try_into().expect("time fraction is 4 bytes"));
    if micros == u32::MAX {
        return if value.iter().all(|byte| *byte == u8::MAX) {
            Ok(None)
        } else {
            Err(DecodeError::InvalidValue {
                row,
                message: "time has a partial NULL sentinel",
            })
        };
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
    let mut values = Vec::with_capacity(rows);
    let mut validity = Vec::with_capacity(rows);
    let mut has_nulls = false;
    for (row, value) in bytes.chunks_exact(12).enumerate() {
        let time = time_value(&value[..8], row)?;
        let date = date_value(&value[8..], row)?;
        match (date, time) {
            (None, None) => {
                values.push(0);
                validity.push(false);
                has_nulls = true;
            }
            (Some(date), Some(time)) => {
                values.push(i64::from(date) * 86_400_000_000 + time);
                validity.push(true);
            }
            _ => {
                return Err(DecodeError::InvalidValue {
                    row,
                    message: "timestamp has mismatched date/time NULL sentinels",
                });
            }
        }
    }
    let nulls = has_nulls.then(|| NullBuffer::from(validity));
    let array = TimestampMicrosecondArray::new(values.into(), nulls);
    Ok(if utc {
        array.with_timezone("UTC")
    } else {
        array
    })
}

fn decode_inet(bytes: &[u8], rows: usize) -> Result<StringArray, DecodeError> {
    // `gdk_atoms.h:is_inet4_nil` defines the nil address; the 4/16-byte
    // layouts are the widths selected in `sql_bincopyconvert.c`.
    let width = if bytes.len() == rows.saturating_mul(4) {
        4
    } else if bytes.len() == rows.saturating_mul(16) {
        16
    } else {
        return Err(DecodeError::InvalidColumn {
            message: "INET column length is neither 4 nor 16 bytes per row",
        });
    };
    let mut builder = StringBuilder::with_capacity(rows, rows.saturating_mul(39));
    let mut scratch = String::with_capacity(39);
    for value in bytes.chunks_exact(width) {
        if value.iter().all(|&byte| byte == 0) {
            builder.append_null();
        } else {
            scratch.clear();
            if width == 4 {
                push_u8_decimal(&mut scratch, value[0]);
                scratch.push('.');
                push_u8_decimal(&mut scratch, value[1]);
                scratch.push('.');
                push_u8_decimal(&mut scratch, value[2]);
                scratch.push('.');
                push_u8_decimal(&mut scratch, value[3]);
            } else {
                let address =
                    Ipv6Addr::from(<[u8; 16]>::try_from(value).expect("IPv6 is 16 bytes"));
                write!(scratch, "{address}").expect("formatting an IP address cannot fail");
            }
            builder.append_value(&scratch);
        }
    }
    Ok(builder.finish())
}

fn push_u8_decimal(out: &mut String, value: u8) {
    if value >= 100 {
        out.push(char::from(b'0' + value / 100));
    }
    if value >= 10 {
        out.push(char::from(b'0' + (value / 10) % 10));
    }
    out.push(char::from(b'0' + value % 10));
}

#[cfg(test)]
mod tests {
    use arrow_array::{
        Array, DurationMillisecondArray, Float32Array, Float64Array, Int8Array, Int16Array,
        Int32Array, Int64Array, UInt64Array,
    };
    use proptest::prelude::*;

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
            MonetType::Inet4,
            MonetType::Inet6,
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

    proptest! {
        #[test]
        fn arbitrary_column_bytes_return_errors_or_row_sized_arrays(
            bytes in prop::collection::vec(any::<u8>(), 0..512),
            rows in 0usize..32,
        ) {
            let types = [
                MonetType::Bool,
                MonetType::TinyInt,
                MonetType::SmallInt,
                MonetType::Int,
                MonetType::BigInt,
                MonetType::HugeInt,
                MonetType::Oid,
                MonetType::Decimal(2, 0),
                MonetType::Decimal(38, 10),
                MonetType::Varchar(0),
                MonetType::Url,
                MonetType::Json,
                MonetType::Real,
                MonetType::Double,
                MonetType::MonthInterval,
                MonetType::DayInterval,
                MonetType::SecInterval,
                MonetType::Time,
                MonetType::TimeTz,
                MonetType::Date,
                MonetType::Timestamp,
                MonetType::TimestampTz,
                MonetType::Blob,
                MonetType::Uuid,
                MonetType::Inet4,
                MonetType::Inet6,
                MonetType::Geometry,
                MonetType::Inet,
                MonetType::Xml,
            ];
            for data_type in types {
                if let Ok(array) = decode_column(&data_type, &bytes, rows) {
                    prop_assert_eq!(array.len(), rows);
                }
            }
        }
    }

    #[test]
    fn validates_frame_identity_and_offset() {
        let mut frame = b"&6 7 0 0 5\n".to_vec();
        frame.extend_from_slice(&(frame.len() as i64).to_le_bytes());
        assert!(matches!(
            decode_frame(&frame, &[], 8, 5, 3),
            Err(DecodeError::ResultId { .. })
        ));
        assert!(matches!(
            decode_frame(&frame, &[], 7, 4, 3),
            Err(DecodeError::StartRow { .. })
        ));

        let mut frame = b"&6 7 0 2 0\n".to_vec();
        frame.extend_from_slice(&(frame.len() as i64).to_le_bytes());
        assert!(matches!(
            decode_frame(&frame, &[], 7, 0, 1),
            Err(DecodeError::RowCount {
                requested: 1,
                actual: 2
            })
        ));
        assert_eq!(decode_frame(&frame, &[], 7, 0, 2).unwrap().num_rows(), 2);
    }

    #[test]
    fn adopts_aligned_fixed_width_buffers_and_copies_misaligned_ones() {
        let mut bytes = vec![0u8; 32];
        bytes.extend_from_slice(&7i32.to_le_bytes());
        bytes.extend_from_slice(&i32::MIN.to_le_bytes());
        let expected_ptr = bytes.as_ptr().wrapping_add(32) as usize;
        let buffer = Buffer::from_vec(bytes);
        let array = decode_owned_column(&buffer, &(32..40), &MonetType::Int, 2).unwrap();
        let array = array.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(array.values().as_ptr() as usize, expected_ptr);
        assert_eq!(array.iter().collect::<Vec<_>>(), vec![Some(7), None]);

        let mut bytes = vec![0];
        bytes.extend_from_slice(&7i32.to_le_bytes());
        bytes.extend_from_slice(&8i32.to_le_bytes());
        let buffer = Buffer::from_vec(bytes);
        assert!(
            adopt_primitive::<Int32Type, _>(&buffer, &(1..9), 2, |value| value == i32::MIN)
                .unwrap()
                .is_none()
        );
        let array = decode_owned_column(&buffer, &(1..9), &MonetType::Int, 2).unwrap();
        let array = array.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(array.values(), &[7, 8]);

        let mut bytes = vec![0u8; 64];
        bytes.extend_from_slice(&7i128.to_le_bytes());
        bytes.extend_from_slice(&i128::MIN.to_le_bytes());
        let expected_ptr = bytes.as_ptr().wrapping_add(64) as usize;
        let buffer = Buffer::from_vec(bytes);
        let array = decode_owned_column(&buffer, &(64..96), &MonetType::Decimal(38, 2), 2).unwrap();
        let array = array.as_any().downcast_ref::<Decimal128Array>().unwrap();
        assert_eq!(array.values().as_ptr() as usize, expected_ptr);
        assert_eq!(array.iter().collect::<Vec<_>>(), vec![Some(7), None]);
    }

    #[test]
    fn adopts_frames_only_when_fixed_width_columns_dominate() {
        assert!(adoption_is_worthwhile(75, 100));
        assert!(!adoption_is_worthwhile(74, 100));
        assert!(!adoption_is_worthwhile(0, 0));
        assert!(prefers_owned_types([MonetType::BigInt, MonetType::Real]));
        assert!(prefers_owned_types([MonetType::HugeInt]));
        assert!(prefers_owned_types([MonetType::Decimal(19, 2)]));
        assert!(!prefers_owned_types([MonetType::Decimal(18, 2)]));
        assert!(prefers_owned_types(
            std::iter::repeat_n(MonetType::Real, 9).chain([MonetType::Timestamp])
        ));
        assert!(!prefers_owned_types([
            MonetType::BigInt,
            MonetType::Double,
            MonetType::Varchar(0),
            MonetType::Timestamp,
        ]));
        assert!(!prefers_owned_types([MonetType::Timestamp]));
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

        let infinities = [f64::INFINITY.to_le_bytes(), f64::NEG_INFINITY.to_le_bytes()].concat();
        let array = decode_column(&MonetType::Double, &infinities, 2).unwrap();
        let array = array.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(array.values(), &[f64::INFINITY, f64::NEG_INFINITY]);
    }

    #[test]
    fn inline_decimal_rejects_null_sentinel_collisions() {
        for (value, precision) in [
            (i128::from(i8::MIN), 2),
            (i128::from(i16::MIN), 4),
            (i128::from(i32::MIN), 9),
            (i128::from(i64::MIN), 18),
            (i128::MIN, 38),
        ] {
            let error = decimal_wire_value(value, precision).unwrap_err();
            assert!(error.to_string().contains("NULL sentinel"));
        }
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
            Err(DecodeError::InvalidColumn { .. })
        ));
        assert!(matches!(
            decode_blob(&[0; 8], usize::MAX),
            Err(DecodeError::InvalidColumn { .. })
        ));
    }

    #[test]
    fn limits_string_backref_expansion_relative_to_wire_size() {
        let mut encoded = vec![b'x'; 65_536];
        encoded.push(0);
        encoded.extend(std::iter::repeat_n(0x81, 17));
        assert!(matches!(
            decode_strings(&encoded, 18),
            Err(DecodeError::InvalidValue { row: 16, .. })
                | Err(DecodeError::InvalidValue { row: 17, .. })
        ));
    }

    #[test]
    fn rejects_decimal_scale_outside_arrow_range() {
        assert!(matches!(
            data_type_for_monet_type(&MonetType::Decimal(38, 128)),
            Err(DecodeError::InvalidColumn { .. })
        ));
        assert!(matches!(
            decode_column(&MonetType::Decimal(38, 128), &[0; 16], 1),
            Err(DecodeError::InvalidColumn { .. })
        ));
        for data_type in [MonetType::Decimal(0, 0), MonetType::Decimal(38, 39)] {
            assert!(matches!(
                data_type_for_monet_type(&data_type),
                Err(DecodeError::InvalidColumn { .. })
            ));
        }
    }

    #[test]
    fn rejects_values_outside_decimal128_precision() {
        let value = 10i128.pow(38).to_le_bytes();
        let error = decode_column(&MonetType::HugeInt, &value, 1).unwrap_err();
        assert!(error.to_string().contains("HUGEINT"));
        assert!(error.to_string().contains("38-digit"));
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
        for (data_type, extension) in [
            (MonetType::HugeInt, "monetdb.hugeint"),
            (MonetType::TimeTz, "monetdb.timetz"),
        ] {
            let field = field_for_monet_type("value", &data_type).unwrap();
            assert_eq!(
                field
                    .metadata()
                    .get("ARROW:extension:name")
                    .map(String::as_str),
                Some(extension)
            );
        }
    }

    #[test]
    fn rejects_invalid_string_data() {
        assert!(matches!(
            decode_strings(b"unterminated", 1),
            Err(DecodeError::InvalidValue {
                row: 0,
                message: "string is not NUL-terminated"
            })
        ));
        assert!(matches!(
            decode_strings(&[0x81], 1),
            Err(DecodeError::InvalidBackref { .. })
        ));
        assert!(matches!(
            decode_strings(&[0xff, 0], 1),
            Err(DecodeError::InvalidUtf8 { .. })
        ));
        assert!(matches!(
            decode_strings(b"value\0trailing", 1),
            Err(DecodeError::Length { .. })
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
            let array = decode_decimal(&bytes, 1, precision, 2, precision == 38).unwrap();
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
    fn civil_date_conversion_matches_chrono_at_era_boundaries() {
        for year in [-32768, -401, -400, -1, 0, 400, 1900, 2000, 32767] {
            for month in 0..=13 {
                for day in 0..=32 {
                    let chrono = NaiveDate::from_ymd_opt(year, month, day)
                        .map(|date| date.signed_duration_since(unix_epoch_for_test()).num_days());
                    let direct =
                        valid_ymd(year, month, day).then(|| days_from_civil(year, month, day));
                    assert_eq!(direct, chrono, "{year}-{month}-{day}");
                }
            }
        }
    }

    #[test]
    #[ignore = "exhaustively checks roughly 30 million wire-date combinations"]
    fn civil_date_conversion_matches_chrono_exhaustively() {
        for year in i32::from(i16::MIN)..=i32::from(i16::MAX) {
            for month in 0..=13 {
                for day in 0..=32 {
                    let chrono = NaiveDate::from_ymd_opt(year, month, day)
                        .map(|date| date.signed_duration_since(unix_epoch_for_test()).num_days());
                    let direct =
                        valid_ymd(year, month, day).then(|| days_from_civil(year, month, day));
                    assert_eq!(direct, chrono, "{year}-{month}-{day}");
                }
            }
        }
    }

    fn unix_epoch_for_test() -> NaiveDate {
        NaiveDate::from_ymd_opt(1970, 1, 1).expect("the Unix epoch is a valid date")
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
    fn rejects_mismatched_timestamp_sentinels_and_invalid_inet_widths() {
        let mut timestamp = vec![0xff; 8];
        timestamp.extend_from_slice(&[1, 1, 178, 7]);
        assert!(matches!(
            decode_timestamp(&timestamp, 1, false),
            Err(DecodeError::InvalidValue { row: 0, .. })
        ));
        assert!(matches!(
            decode_inet(&[1, 2, 3, 4, 5], 1),
            Err(DecodeError::InvalidColumn { .. })
        ));
        let ipv6_nil = decode_inet(&[0; 16], 1).unwrap();
        assert!(ipv6_nil.is_null(0));
        assert!(matches!(
            date_value(&[1, u8::MAX, u8::MAX, u8::MAX], 0),
            Err(DecodeError::InvalidValue { row: 0, .. })
        ));
        assert!(matches!(
            time_value(&[u8::MAX, u8::MAX, u8::MAX, u8::MAX, 0, 0, 0, 0], 0),
            Err(DecodeError::InvalidValue { row: 0, .. })
        ));
    }

    #[test]
    fn inet_formatting_matches_std() {
        let octets = [0, 1, 9, 10, 99, 100, 255];
        let mut ipv4 = Vec::new();
        let mut expected = Vec::new();
        for a in octets {
            for b in octets {
                for c in octets {
                    for d in octets {
                        let address = [a, b, c, d];
                        ipv4.extend_from_slice(&address);
                        expected.push(
                            (address != [0; 4])
                                .then(|| std::net::Ipv4Addr::from(address).to_string()),
                        );
                    }
                }
            }
        }
        let decoded = decode_inet(&ipv4, expected.len()).unwrap();
        assert_eq!(
            decoded.iter().collect::<Vec<_>>(),
            expected
                .iter()
                .map(|value| value.as_deref())
                .collect::<Vec<_>>()
        );

        let ipv6 = [
            [0; 16],
            Ipv6Addr::LOCALHOST.octets(),
            "2001:db8::1".parse::<Ipv6Addr>().unwrap().octets(),
            "::ffff:192.0.2.128".parse::<Ipv6Addr>().unwrap().octets(),
            "ffff:0:0:1::".parse::<Ipv6Addr>().unwrap().octets(),
        ];
        let bytes = ipv6.concat();
        let decoded = decode_inet(&bytes, ipv6.len()).unwrap();
        let expected =
            ipv6.map(|address| (address != [0; 16]).then(|| Ipv6Addr::from(address).to_string()));
        assert_eq!(
            decoded.iter().collect::<Vec<_>>(),
            expected
                .iter()
                .map(|value| value.as_deref())
                .collect::<Vec<_>>()
        );
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
        assert_eq!(
            data_type_for_monet_type(&MonetType::Inet)
                .unwrap_err()
                .to_string(),
            "MonetDB type INET is not available through Xexportbin; cast the column to VARCHAR in SQL"
        );
    }
}
