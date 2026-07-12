use adbc_core::error::{Result, Status};
use arrow_array::{
    Array, BinaryArray, BinaryViewArray, BooleanArray, Date32Array, Date64Array, Decimal128Array,
    DictionaryArray, DurationMicrosecondArray, DurationMillisecondArray, DurationNanosecondArray,
    DurationSecondArray, FixedSizeBinaryArray, Float16Array, Float32Array, Float64Array, Int8Array,
    Int16Array, Int32Array, Int64Array, LargeBinaryArray, LargeStringArray, RecordBatch,
    StringArray, StringViewArray, Time32MillisecondArray, Time32SecondArray,
    Time64MicrosecondArray, Time64NanosecondArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt8Array,
    UInt16Array, UInt32Array, UInt64Array,
    types::{
        ArrowDictionaryKeyType, Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type, UInt16Type,
        UInt32Type, UInt64Type,
    },
};
use arrow_schema::{DataType, TimeUnit};

use super::error;

pub(super) fn parameter_count(query: &str) -> Result<usize> {
    render_query(query, &[]).map(|(_, count)| count)
}

pub(super) fn render_row(query: &str, batch: &RecordBatch, row: usize) -> Result<String> {
    if row >= batch.num_rows() {
        return Err(error(
            "parameter row is out of bounds",
            Status::InvalidArguments,
        ));
    }
    let values = batch
        .columns()
        .iter()
        .map(|array| literal(array.as_ref(), row))
        .collect::<Result<Vec<_>>>()?;
    let (rendered, count) = render_query(query, &values)?;
    if count != values.len() {
        return Err(error(
            format!(
                "query has {count} positional parameters but {} values were bound",
                values.len()
            ),
            Status::InvalidArguments,
        ));
    }
    Ok(rendered)
}

fn render_query(query: &str, values: &[String]) -> Result<(String, usize)> {
    #[derive(Clone, Copy)]
    enum State {
        Normal,
        SingleQuote,
        DoubleQuote,
        LineComment,
        BlockComment,
    }

    let bytes = query.as_bytes();
    let mut output =
        String::with_capacity(query.len() + values.iter().map(String::len).sum::<usize>());
    let mut state = State::Normal;
    let mut index = 0;
    let mut parameter = 0;
    while index < bytes.len() {
        let current = bytes[index];
        let next = bytes.get(index + 1).copied();
        match state {
            State::Normal => match (current, next) {
                (b'?', _) => {
                    if let Some(value) = values.get(parameter) {
                        output.push_str(value);
                    }
                    parameter += 1;
                    index += 1;
                }
                (b'\'', _) => {
                    output.push('\'');
                    state = State::SingleQuote;
                    index += 1;
                }
                (b'"', _) => {
                    output.push('"');
                    state = State::DoubleQuote;
                    index += 1;
                }
                (b'-', Some(b'-')) => {
                    output.push_str("--");
                    state = State::LineComment;
                    index += 2;
                }
                (b'/', Some(b'*')) => {
                    output.push_str("/*");
                    state = State::BlockComment;
                    index += 2;
                }
                _ => {
                    output.push(current as char);
                    index += 1;
                }
            },
            State::SingleQuote => {
                output.push(current as char);
                if current == b'\\'
                    && let Some(next) = next
                {
                    output.push(next as char);
                    index += 2;
                } else if current == b'\'' {
                    if next == Some(b'\'') {
                        output.push('\'');
                        index += 2;
                    } else {
                        state = State::Normal;
                        index += 1;
                    }
                } else {
                    index += 1;
                }
            }
            State::DoubleQuote => {
                output.push(current as char);
                if current == b'"' {
                    if next == Some(b'"') {
                        output.push('"');
                        index += 2;
                    } else {
                        state = State::Normal;
                        index += 1;
                    }
                } else {
                    index += 1;
                }
            }
            State::LineComment => {
                output.push(current as char);
                index += 1;
                if current == b'\n' {
                    state = State::Normal;
                }
            }
            State::BlockComment => {
                output.push(current as char);
                if current == b'*' && next == Some(b'/') {
                    output.push('/');
                    index += 2;
                    state = State::Normal;
                } else {
                    index += 1;
                }
            }
        }
    }
    if !matches!(state, State::Normal | State::LineComment) {
        return Err(error(
            "query contains an unterminated quoted string, identifier, or comment",
            Status::InvalidArguments,
        ));
    }
    Ok((output, parameter))
}

fn literal(array: &dyn Array, row: usize) -> Result<String> {
    if array.is_null(row) {
        return Ok("NULL".into());
    }
    macro_rules! primitive {
        ($array:ty, $type:expr) => {{ downcast::<$array>(array, $type)?.value(row).to_string() }};
    }
    Ok(match array.data_type() {
        DataType::Null => "NULL".into(),
        DataType::Boolean => {
            if downcast::<BooleanArray>(array, DataType::Boolean)?.value(row) {
                "TRUE".into()
            } else {
                "FALSE".into()
            }
        }
        DataType::Int8 => primitive!(Int8Array, DataType::Int8),
        DataType::Int16 => primitive!(Int16Array, DataType::Int16),
        DataType::Int32 => primitive!(Int32Array, DataType::Int32),
        DataType::Int64 => primitive!(Int64Array, DataType::Int64),
        DataType::UInt8 => primitive!(UInt8Array, DataType::UInt8),
        DataType::UInt16 => primitive!(UInt16Array, DataType::UInt16),
        DataType::UInt32 => primitive!(UInt32Array, DataType::UInt32),
        DataType::UInt64 => primitive!(UInt64Array, DataType::UInt64),
        DataType::Float16 => float_literal(f64::from(
            downcast::<Float16Array>(array, DataType::Float16)?.value(row),
        ))?,
        DataType::Float32 => float_literal(f64::from(
            downcast::<Float32Array>(array, DataType::Float32)?.value(row),
        ))?,
        DataType::Float64 => {
            float_literal(downcast::<Float64Array>(array, DataType::Float64)?.value(row))?
        }
        DataType::Decimal128(_, scale) => decimal_literal(
            downcast::<Decimal128Array>(array, array.data_type().clone())?.value(row),
            *scale,
        )?,
        DataType::Utf8 => quote_string(downcast::<StringArray>(array, DataType::Utf8)?.value(row))?,
        DataType::LargeUtf8 => {
            quote_string(downcast::<LargeStringArray>(array, DataType::LargeUtf8)?.value(row))?
        }
        DataType::Utf8View => {
            quote_string(downcast::<StringViewArray>(array, DataType::Utf8View)?.value(row))?
        }
        DataType::Binary => {
            blob_literal(downcast::<BinaryArray>(array, DataType::Binary)?.value(row))
        }
        DataType::LargeBinary => {
            blob_literal(downcast::<LargeBinaryArray>(array, DataType::LargeBinary)?.value(row))
        }
        DataType::BinaryView => {
            blob_literal(downcast::<BinaryViewArray>(array, DataType::BinaryView)?.value(row))
        }
        DataType::FixedSizeBinary(_) => blob_literal(
            downcast::<FixedSizeBinaryArray>(array, array.data_type().clone())?.value(row),
        ),
        DataType::Date32 => {
            date_literal(downcast::<Date32Array>(array, DataType::Date32)?.value(row))
        }
        DataType::Date64 => {
            let millis = downcast::<Date64Array>(array, DataType::Date64)?.value(row);
            if millis % 86_400_000 != 0 {
                return Err(error(
                    "Date64 parameter is not a whole day",
                    Status::InvalidData,
                ));
            }
            date_literal(
                i32::try_from(millis / 86_400_000)
                    .map_err(|_| error("Date64 parameter is out of range", Status::InvalidData))?,
            )
        }
        DataType::Time32(unit) => time32_literal(array, *unit, row)?,
        DataType::Time64(unit) => time64_literal(array, *unit, row)?,
        DataType::Timestamp(unit, timezone) => {
            timestamp_literal(array, *unit, timezone.is_some(), row)?
        }
        DataType::Duration(unit) => duration_literal(array, *unit, row)?,
        DataType::Dictionary(key, _) => dictionary_literal(array, key, row)?,
        data_type => {
            return Err(error(
                format!("Arrow parameter type {data_type} is not supported"),
                Status::NotImplemented,
            ));
        }
    })
}

fn downcast<T: 'static>(array: &dyn Array, expected: DataType) -> Result<&T> {
    array.as_any().downcast_ref().ok_or_else(|| {
        error(
            format!(
                "expected Arrow parameter type {expected}, found {}",
                array.data_type()
            ),
            Status::InvalidData,
        )
    })
}

fn float_literal(value: f64) -> Result<String> {
    if value.is_nan() {
        Ok("NULL".into())
    } else if value.is_finite() {
        Ok(format!("{value:e}"))
    } else {
        Err(error(
            "infinite float parameters are not supported by MonetDB",
            Status::InvalidData,
        ))
    }
}

fn decimal_literal(value: i128, scale: i8) -> Result<String> {
    if scale < 0 {
        return Err(error(
            "negative-scale decimal parameters are not supported",
            Status::NotImplemented,
        ));
    }
    let scale = usize::try_from(scale).expect("nonnegative i8 fits usize");
    let negative = value < 0;
    let mut digits = value.unsigned_abs().to_string();
    if scale > 0 {
        if digits.len() <= scale {
            digits.insert_str(0, &"0".repeat(scale + 1 - digits.len()));
        }
        digits.insert(digits.len() - scale, '.');
    }
    if negative {
        digits.insert(0, '-');
    }
    Ok(digits)
}

fn quote_string(value: &str) -> Result<String> {
    if value.as_bytes().contains(&0) {
        return Err(error(
            "string parameter contains a NUL byte",
            Status::InvalidData,
        ));
    }
    Ok(format!(
        "'{}'",
        value.replace('\\', "\\\\").replace('\'', "\\'")
    ))
}

fn blob_literal(value: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(7 + value.len() * 2);
    output.push_str("BLOB '");
    for &byte in value {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output.push('\'');
    output
}

fn date_literal(days: i32) -> String {
    let (year, month, day) = civil_from_days(days);
    format!("DATE '{year:04}-{month:02}-{day:02}'")
}

fn time32_literal(array: &dyn Array, unit: TimeUnit, row: usize) -> Result<String> {
    let micros = match unit {
        TimeUnit::Second => {
            i64::from(downcast::<Time32SecondArray>(array, DataType::Time32(unit))?.value(row))
                * 1_000_000
        }
        TimeUnit::Millisecond => {
            i64::from(downcast::<Time32MillisecondArray>(array, DataType::Time32(unit))?.value(row))
                * 1_000
        }
        _ => {
            return Err(error(
                format!("invalid Time32 unit {unit:?}"),
                Status::InvalidData,
            ));
        }
    };
    format_time(micros).map(|value| format!("TIME '{value}'"))
}

fn time64_literal(array: &dyn Array, unit: TimeUnit, row: usize) -> Result<String> {
    let micros = match unit {
        TimeUnit::Microsecond => {
            downcast::<Time64MicrosecondArray>(array, DataType::Time64(unit))?.value(row)
        }
        TimeUnit::Nanosecond => {
            let nanos =
                downcast::<Time64NanosecondArray>(array, DataType::Time64(unit))?.value(row);
            if nanos % 1_000 != 0 {
                return Err(error(
                    "time parameter is not microsecond-aligned",
                    Status::InvalidData,
                ));
            }
            nanos / 1_000
        }
        _ => {
            return Err(error(
                format!("invalid Time64 unit {unit:?}"),
                Status::InvalidData,
            ));
        }
    };
    format_time(micros).map(|value| format!("TIME '{value}'"))
}

fn timestamp_literal(
    array: &dyn Array,
    unit: TimeUnit,
    timezone: bool,
    row: usize,
) -> Result<String> {
    macro_rules! value {
        ($array:ty, $factor:expr) => {{
            downcast::<$array>(array, array.data_type().clone())?
                .value(row)
                .checked_mul($factor)
                .ok_or_else(|| error("timestamp parameter overflows", Status::InvalidData))?
        }};
    }
    let micros = match unit {
        TimeUnit::Second => value!(TimestampSecondArray, 1_000_000),
        TimeUnit::Millisecond => value!(TimestampMillisecondArray, 1_000),
        TimeUnit::Microsecond => value!(TimestampMicrosecondArray, 1),
        TimeUnit::Nanosecond => {
            let nanos =
                downcast::<TimestampNanosecondArray>(array, array.data_type().clone())?.value(row);
            if nanos % 1_000 != 0 {
                return Err(error(
                    "timestamp parameter is not microsecond-aligned",
                    Status::InvalidData,
                ));
            }
            nanos / 1_000
        }
    };
    let days = i32::try_from(micros.div_euclid(86_400_000_000))
        .map_err(|_| error("timestamp parameter is out of range", Status::InvalidData))?;
    let (year, month, day) = civil_from_days(days);
    let time = format_time(micros.rem_euclid(86_400_000_000))?;
    Ok(format!(
        "{} '{year:04}-{month:02}-{day:02} {time}{}'",
        if timezone { "TIMESTAMPTZ" } else { "TIMESTAMP" },
        if timezone { "+00:00" } else { "" }
    ))
}

fn duration_literal(array: &dyn Array, unit: TimeUnit, row: usize) -> Result<String> {
    macro_rules! duration {
        ($array:ty) => {{ downcast::<$array>(array, DataType::Duration(unit))?.value(row) }};
    }
    let millis = match unit {
        TimeUnit::Second => duration!(DurationSecondArray)
            .checked_mul(1_000)
            .ok_or_else(|| error("duration parameter overflows", Status::InvalidData))?,
        TimeUnit::Millisecond => duration!(DurationMillisecondArray),
        TimeUnit::Microsecond => {
            let value = duration!(DurationMicrosecondArray);
            if value % 1_000 != 0 {
                return Err(error(
                    "duration parameter is not millisecond-aligned",
                    Status::InvalidData,
                ));
            }
            value / 1_000
        }
        TimeUnit::Nanosecond => {
            let value = duration!(DurationNanosecondArray);
            if value % 1_000_000 != 0 {
                return Err(error(
                    "duration parameter is not millisecond-aligned",
                    Status::InvalidData,
                ));
            }
            value / 1_000_000
        }
    };
    let negative = millis < 0;
    let absolute = millis.unsigned_abs();
    let seconds = absolute / 1_000;
    let fraction = absolute % 1_000;
    Ok(format!(
        "INTERVAL '{}{}.{fraction:03}' SECOND",
        if negative { "-" } else { "" },
        seconds
    ))
}

fn format_time(micros: i64) -> Result<String> {
    if !(0..86_400_000_000).contains(&micros) {
        return Err(error(
            "time parameter is outside one day",
            Status::InvalidData,
        ));
    }
    let seconds = micros / 1_000_000;
    Ok(format!(
        "{:02}:{:02}:{:02}.{:06}",
        seconds / 3_600,
        (seconds / 60) % 60,
        seconds % 60,
        micros % 1_000_000
    ))
}

fn dictionary_literal(array: &dyn Array, key: &DataType, row: usize) -> Result<String> {
    macro_rules! keys {
        ($type:ty) => {{
            dictionary_value(
                downcast::<DictionaryArray<$type>>(array, array.data_type().clone())?,
                row,
            )
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
        _ => Err(error(
            format!("dictionary key type {key} is not supported"),
            Status::NotImplemented,
        )),
    }
}

fn dictionary_value<K: ArrowDictionaryKeyType>(
    array: &DictionaryArray<K>,
    row: usize,
) -> Result<String> {
    match array.key(row) {
        Some(key) => literal(array.values().as_ref(), key),
        None => Ok("NULL".into()),
    }
}

fn civil_from_days(days: i32) -> (i32, u8, u8) {
    let days = i64::from(days) + 719_468;
    let era = if days >= 0 { days } else { days - 146_096 } / 146_097;
    let doe = days - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let mut year = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = mp + if mp < 10 { 3 } else { -9 };
    year += i64::from(month <= 2);
    (year as i32, month as u8, day as u8)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{Int32Array, StringArray};
    use arrow_schema::{Field, Schema};

    use super::*;

    #[test]
    fn renders_only_real_qmark_parameters() {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("0", DataType::Int32, false),
                Field::new("1", DataType::Utf8, false),
            ])),
            vec![
                Arc::new(Int32Array::from(vec![42])),
                Arc::new(StringArray::from(vec!["a'b"])),
            ],
        )
        .unwrap();
        assert_eq!(
            render_row("SELECT ?, '?', \"?\", ? -- ?", &batch, 0).unwrap(),
            "SELECT 42, '?', \"?\", 'a\\'b' -- ?"
        );
    }

    #[test]
    fn validates_parameter_counts_and_lexing() {
        assert_eq!(parameter_count("SELECT ? /* ? */").unwrap(), 1);
        assert!(parameter_count("SELECT 'unterminated").is_err());
        let batch = RecordBatch::new_empty(Arc::new(Schema::empty()));
        assert!(render_row("SELECT ?", &batch, 0).is_err());
    }

    #[test]
    fn formats_decimal_and_temporal_literals() {
        assert_eq!(decimal_literal(-123, 2).unwrap(), "-1.23");
        assert_eq!(float_literal(3.5).unwrap(), "3.5e0");
        assert_eq!(float_literal(f64::MAX).unwrap(), "1.7976931348623157e308");
        assert_eq!(date_literal(0), "DATE '1970-01-01'");
        assert_eq!(format_time(3_723_123_456).unwrap(), "01:02:03.123456");
    }
}
