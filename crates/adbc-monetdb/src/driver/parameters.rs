use std::{
    collections::{HashMap, HashSet},
    ops::Range,
};

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
use arrow_schema::{DataType, Field, TimeUnit};
use chrono::{DateTime, NaiveDate, TimeDelta, Utc};

use super::error;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum ParameterLayout {
    Positional(usize),
    Named(Vec<String>),
}

impl ParameterLayout {
    pub(super) fn count(&self) -> usize {
        match self {
            Self::Positional(count) => *count,
            Self::Named(names) => names.len(),
        }
    }

    pub(super) fn field_names(&self) -> Vec<String> {
        match self {
            Self::Positional(count) => (0..*count).map(|index| index.to_string()).collect(),
            Self::Named(names) => names.clone(),
        }
    }

    pub(super) fn is_named(&self) -> bool {
        matches!(self, Self::Named(_))
    }
}

#[derive(Debug, Clone, Copy)]
enum ParameterSlot {
    Positional(usize),
    Named(usize),
}

pub(super) struct QueryTemplate {
    query: String,
    segments: Vec<Range<usize>>,
    slots: Vec<ParameterSlot>,
    layout: ParameterLayout,
}

impl QueryTemplate {
    pub(super) fn parse(query: &str) -> Result<Self> {
        compile_query(query)
    }

    pub(super) fn layout(&self) -> &ParameterLayout {
        &self.layout
    }

    pub(super) fn render_nulls(&self) -> Result<String> {
        self.render(|_| Ok("NULL"))
    }

    pub(super) fn render_row(
        &self,
        batch: &RecordBatch,
        row: usize,
        bind_by_name: bool,
    ) -> Result<String> {
        let values = render_arguments(batch, row)?;
        match &self.layout {
            ParameterLayout::Positional(count) => {
                if bind_by_name {
                    return Err(error(
                        "positional parameters cannot be bound by name",
                        Status::InvalidArguments,
                    ));
                }
                if *count != values.len() {
                    return Err(error(
                        format!(
                            "query has {count} positional parameters but {} values were bound",
                            values.len()
                        ),
                        Status::InvalidArguments,
                    ));
                }
                self.render(|slot| match slot {
                    ParameterSlot::Positional(index) => Ok(values[*index].as_str()),
                    ParameterSlot::Named(_) => unreachable!("layout and slots agree"),
                })
            }
            ParameterLayout::Named(names) => {
                if !bind_by_name {
                    return Err(error(
                        "named parameters require adbc.statement.bind_by_name",
                        Status::InvalidArguments,
                    ));
                }
                let schema = batch.schema();
                let mut named_values = HashMap::with_capacity(values.len());
                for (field, value) in schema.fields().iter().zip(&values) {
                    if named_values
                        .insert(field.name().as_str(), value.as_str())
                        .is_some()
                    {
                        return Err(error(
                            format!("bound parameter name '{}' is duplicated", field.name()),
                            Status::InvalidArguments,
                        ));
                    }
                }
                if let Some(missing) = names
                    .iter()
                    .find(|name| !named_values.contains_key(name.as_str()))
                {
                    return Err(error(
                        format!("named parameter '{missing}' was not bound"),
                        Status::InvalidArguments,
                    ));
                }
                if named_values.len() > names.len() {
                    let expected: HashSet<&str> = names.iter().map(String::as_str).collect();
                    let extra = named_values
                        .keys()
                        .find(|name| !expected.contains(**name))
                        .expect("more bound names guarantees an unexpected name");
                    return Err(error(
                        format!("bound parameter '{extra}' is not present in the query"),
                        Status::InvalidArguments,
                    ));
                }
                self.render(|slot| match slot {
                    ParameterSlot::Named(index) => named_values
                        .get(names[*index].as_str())
                        .copied()
                        .ok_or_else(|| {
                            error(
                                format!("named parameter '{}' was not bound", names[*index]),
                                Status::InvalidArguments,
                            )
                        }),
                    ParameterSlot::Positional(_) => unreachable!("layout and slots agree"),
                })
            }
        }
    }

    fn render<'a>(
        &self,
        mut resolve: impl FnMut(&ParameterSlot) -> Result<&'a str>,
    ) -> Result<String> {
        let mut output = String::with_capacity(self.query.len());
        for (segment, slot) in self.segments.iter().zip(&self.slots) {
            output.push_str(&self.query[segment.clone()]);
            output.push_str(resolve(slot)?);
        }
        if let Some(segment) = self.segments.last() {
            output.push_str(&self.query[segment.clone()]);
        }
        Ok(output)
    }
}

pub(super) fn parameter_layout(query: &str) -> Result<ParameterLayout> {
    Ok(QueryTemplate::parse(query)?.layout)
}

pub(super) fn render_null_parameters(query: &str) -> Result<String> {
    QueryTemplate::parse(query)?.render_nulls()
}

#[cfg(test)]
pub(super) fn render_row(
    query: &str,
    batch: &RecordBatch,
    row: usize,
    bind_by_name: bool,
) -> Result<String> {
    QueryTemplate::parse(query)?.render_row(batch, row, bind_by_name)
}

pub(super) fn render_arguments(batch: &RecordBatch, row: usize) -> Result<Vec<String>> {
    if row >= batch.num_rows() {
        return Err(error(
            "parameter row is out of bounds",
            Status::InvalidArguments,
        ));
    }
    batch
        .columns()
        .iter()
        .zip(batch.schema().fields())
        .map(|(array, field)| literal(field, array.as_ref(), row))
        .collect()
}

#[derive(Clone, Copy)]
enum LexState {
    Normal,
    SingleQuote,
    EscapedSingleQuote,
    RawSingleQuote,
    DoubleQuote,
    LineComment,
    BlockComment,
}

struct ScannedSlot {
    range: Range<usize>,
    name: Option<Range<usize>>,
}

struct QueryScan {
    statements: Vec<Range<usize>>,
    slots: Vec<ScannedSlot>,
}

fn scan_query(query: &str) -> Result<QueryScan> {
    let bytes = query.as_bytes();
    let mut state = LexState::Normal;
    let mut index = 0;
    let mut statement_start = 0;
    let mut statement_has_code = false;
    let mut statements = Vec::new();
    let mut slots = Vec::new();
    while index < bytes.len() {
        let current = bytes[index];
        let next = bytes.get(index + 1).copied();
        let third = bytes.get(index + 2).copied();
        match state {
            LexState::Normal => match (current, next, third) {
                (b';', _, _) => {
                    if statement_has_code {
                        statements.push(statement_start..index);
                    }
                    statement_start = index + 1;
                    statement_has_code = false;
                    index += 1;
                }
                (_, _, _) if current.is_ascii_whitespace() => {
                    index = next_char(query, index);
                }
                (b'-', Some(b'-'), _) => {
                    state = LexState::LineComment;
                    index += 2;
                }
                (b'#', _, _) => {
                    state = LexState::LineComment;
                    index += 1;
                }
                (b'/', Some(b'*'), _) => {
                    state = LexState::BlockComment;
                    index += 2;
                }
                (b'?', _, _) => {
                    statement_has_code = true;
                    slots.push(ScannedSlot {
                        range: index..index + 1,
                        name: None,
                    });
                    index += 1;
                }
                (b':', Some(next), _)
                    if is_identifier_start(next)
                        && index.checked_sub(1).and_then(|index| bytes.get(index))
                            != Some(&b':') =>
                {
                    statement_has_code = true;
                    let mut end = index + 2;
                    while bytes
                        .get(end)
                        .is_some_and(|byte| is_identifier_continue(*byte))
                    {
                        end += 1;
                    }
                    slots.push(ScannedSlot {
                        range: index..end,
                        name: Some(index + 1..end),
                    });
                    index = end;
                }
                (b'r' | b'R' | b'x' | b'X', Some(b'\''), _) => {
                    statement_has_code = true;
                    state = LexState::RawSingleQuote;
                    index += 2;
                }
                (b'e' | b'E', Some(b'\''), _) => {
                    statement_has_code = true;
                    state = LexState::EscapedSingleQuote;
                    index += 2;
                }
                (b'u' | b'U', Some(b'&'), Some(b'\'')) => {
                    statement_has_code = true;
                    state = LexState::RawSingleQuote;
                    index += 3;
                }
                (b'u' | b'U', Some(b'&'), Some(b'"')) => {
                    statement_has_code = true;
                    state = LexState::DoubleQuote;
                    index += 3;
                }
                (b'\'', _, _) => {
                    statement_has_code = true;
                    state = LexState::SingleQuote;
                    index += 1;
                }
                (b'"', _, _) => {
                    statement_has_code = true;
                    state = LexState::DoubleQuote;
                    index += 1;
                }
                _ => {
                    statement_has_code = true;
                    index = next_char(query, index);
                }
            },
            LexState::SingleQuote => {
                if current == b'\\' && next == Some(b'\'') {
                    return Err(error(
                        "backslash-escaped quotes in ordinary SQL strings depend on the server's raw_strings setting; use E'...' or R'...'",
                        Status::InvalidArguments,
                    ));
                }
                index = next_char(query, index);
                if current == b'\'' {
                    if next == Some(b'\'') {
                        index += 1;
                    } else {
                        state = LexState::Normal;
                    }
                }
            }
            LexState::EscapedSingleQuote => {
                index = next_char(query, index);
                if current == b'\\' && index < bytes.len() {
                    index = next_char(query, index);
                } else if current == b'\'' {
                    if next == Some(b'\'') {
                        index += 1;
                    } else {
                        state = LexState::Normal;
                    }
                }
            }
            LexState::RawSingleQuote => {
                index = next_char(query, index);
                if current == b'\'' {
                    if next == Some(b'\'') {
                        index += 1;
                    } else {
                        state = LexState::Normal;
                    }
                }
            }
            LexState::DoubleQuote => {
                index = next_char(query, index);
                if current == b'"' {
                    if next == Some(b'"') {
                        index += 1;
                    } else {
                        state = LexState::Normal;
                    }
                }
            }
            LexState::LineComment => {
                index = next_char(query, index);
                if current == b'\n' {
                    state = LexState::Normal;
                }
            }
            LexState::BlockComment => {
                index = next_char(query, index);
                if current == b'*' && next == Some(b'/') {
                    index += 1;
                    state = LexState::Normal;
                }
            }
        }
    }
    if !matches!(state, LexState::Normal | LexState::LineComment) {
        return Err(error(
            "query contains an unterminated quoted string, identifier, or comment",
            Status::InvalidArguments,
        ));
    }
    if statement_has_code {
        statements.push(statement_start..query.len());
    }
    Ok(QueryScan { statements, slots })
}

pub(super) fn unbound_statements(query: &str) -> Result<Vec<&str>> {
    let scan = scan_query(query)?;
    if !scan.slots.is_empty() {
        return Err(error("parameters are not bound", Status::InvalidState));
    }
    if scan.statements.is_empty() {
        return Err(error("SQL query is empty", Status::InvalidArguments));
    }
    Ok(scan
        .statements
        .into_iter()
        .map(|range| query[range].trim())
        .collect())
}

fn compile_query(query: &str) -> Result<QueryTemplate> {
    let scan = scan_query(query)?;
    if scan.statements.len() > 1 {
        return Err(error(
            "multiple SQL statements are not supported",
            Status::InvalidArguments,
        ));
    }
    let mut segment_start = 0;
    let mut segments = Vec::with_capacity(scan.slots.len() + 1);
    let mut slots = Vec::with_capacity(scan.slots.len());
    let mut positional_count = 0;
    let mut named = Vec::new();
    let mut named_indices = HashMap::new();
    for scanned in scan.slots {
        segments.push(segment_start..scanned.range.start);
        match scanned.name {
            None => {
                if !named.is_empty() {
                    return Err(error(
                        "positional and named parameters cannot be mixed",
                        Status::InvalidArguments,
                    ));
                }
                slots.push(ParameterSlot::Positional(positional_count));
                positional_count += 1;
            }
            Some(name_range) => {
                if positional_count > 0 {
                    return Err(error(
                        "positional and named parameters cannot be mixed",
                        Status::InvalidArguments,
                    ));
                }
                let name = &query[name_range];
                let name_index = *named_indices.entry(name).or_insert_with(|| {
                    named.push(name.to_owned());
                    named.len() - 1
                });
                slots.push(ParameterSlot::Named(name_index));
            }
        }
        segment_start = scanned.range.end;
    }
    let layout = if named.is_empty() {
        ParameterLayout::Positional(positional_count)
    } else {
        ParameterLayout::Named(named)
    };
    segments.push(segment_start..query.len());
    Ok(QueryTemplate {
        query: query.to_owned(),
        segments,
        slots,
        layout,
    })
}

fn is_identifier_start(byte: u8) -> bool {
    byte.is_ascii_alphabetic() || byte == b'_'
}

fn is_identifier_continue(byte: u8) -> bool {
    is_identifier_start(byte) || byte.is_ascii_digit()
}

fn next_char(query: &str, index: usize) -> usize {
    let character = query[index..]
        .chars()
        .next()
        .expect("index is within the UTF-8 query");
    index + character.len_utf8()
}

fn literal(field: &Field, array: &dyn Array, row: usize) -> Result<String> {
    if array.is_null(row) {
        return Ok("NULL".into());
    }
    let extension = field
        .metadata()
        .get("ARROW:extension:name")
        .map(String::as_str);
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
        DataType::Int32 if extension == Some("monetdb.interval_month") => format!(
            "INTERVAL '{}' MONTH",
            downcast::<Int32Array>(array, DataType::Int32)?.value(row)
        ),
        DataType::Int32 => primitive!(Int32Array, DataType::Int32),
        DataType::Int64 => primitive!(Int64Array, DataType::Int64),
        DataType::UInt8 => primitive!(UInt8Array, DataType::UInt8),
        DataType::UInt16 => primitive!(UInt16Array, DataType::UInt16),
        DataType::UInt32 => primitive!(UInt32Array, DataType::UInt32),
        DataType::UInt64 if extension == Some("monetdb.oid") => {
            let value = downcast::<UInt64Array>(array, DataType::UInt64)?.value(row);
            if value >= 1 << 63 {
                return Err(error(
                    "OID parameter must be less than 2^63",
                    Status::InvalidData,
                ));
            }
            value.to_string()
        }
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
        DataType::FixedSizeBinary(16) if extension == Some("arrow.uuid") => uuid_literal(
            downcast::<FixedSizeBinaryArray>(array, DataType::FixedSizeBinary(16))?.value(row),
        ),
        DataType::FixedSizeBinary(_) => blob_literal(
            downcast::<FixedSizeBinaryArray>(array, array.data_type().clone())?.value(row),
        ),
        DataType::Date32 => date_literal(i64::from(
            downcast::<Date32Array>(array, DataType::Date32)?.value(row),
        ))?,
        DataType::Date64 => {
            let millis = downcast::<Date64Array>(array, DataType::Date64)?.value(row);
            if millis % 86_400_000 != 0 {
                return Err(error(
                    "Date64 parameter is not a whole day",
                    Status::InvalidData,
                ));
            }
            date_literal(millis / 86_400_000)?
        }
        DataType::Time32(unit) => time32_literal(array, *unit, row)?,
        DataType::Time64(unit) => {
            time64_literal(array, *unit, extension == Some("monetdb.timetz"), row)?
        }
        DataType::Timestamp(unit, timezone) => {
            timestamp_literal(array, *unit, timezone.is_some(), row)?
        }
        DataType::Duration(unit) if extension == Some("monetdb.interval_day") => {
            day_interval_literal(array, *unit, row)?
        }
        DataType::Duration(unit) => duration_literal(array, *unit, row)?,
        DataType::Dictionary(key, _) => dictionary_literal(field, array, key, row)?,
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
    if value.is_finite() {
        Ok(format!("{value:e}"))
    } else {
        Err(error(
            "non-finite float parameters are not supported by MonetDB",
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
    Ok(format!("R'{}'", value.replace('\'', "''")))
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

fn uuid_literal(value: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(43);
    output.push_str("UUID '");
    for (index, &byte) in value.iter().enumerate() {
        if matches!(index, 4 | 6 | 8 | 10) {
            output.push('-');
        }
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output.push('\'');
    output
}

fn date_literal(days: i64) -> Result<String> {
    Ok(format!("DATE '{}'", date_from_days(days)?))
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

fn time64_literal(array: &dyn Array, unit: TimeUnit, timezone: bool, row: usize) -> Result<String> {
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
    format_time(micros).map(|value| {
        if timezone {
            format!("TIMETZ '{value}+00:00'")
        } else {
            format!("TIME '{value}'")
        }
    })
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
    let timestamp = DateTime::<Utc>::from_timestamp_micros(micros)
        .ok_or_else(|| error("timestamp parameter is out of range", Status::InvalidData))?;
    let date = timestamp.date_naive();
    let time = format_time(micros.rem_euclid(86_400_000_000))?;
    Ok(format!(
        "{} '{date} {time}{}'",
        if timezone { "TIMESTAMPTZ" } else { "TIMESTAMP" },
        if timezone { "+00:00" } else { "" }
    ))
}

fn duration_literal(array: &dyn Array, unit: TimeUnit, row: usize) -> Result<String> {
    let millis = duration_millis(array, unit, row)?;
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

fn day_interval_literal(array: &dyn Array, unit: TimeUnit, row: usize) -> Result<String> {
    let millis = duration_millis(array, unit, row)?;
    let negative = millis < 0;
    let absolute = millis.unsigned_abs();
    let total_seconds = absolute / 1_000;
    let days = total_seconds / 86_400;
    let hours = total_seconds / 3_600 % 24;
    let minutes = total_seconds / 60 % 60;
    let seconds = total_seconds % 60;
    let fraction = absolute % 1_000;
    Ok(format!(
        "INTERVAL '{}{days} {hours:02}:{minutes:02}:{seconds:02}.{fraction:03}' DAY TO SECOND",
        if negative { "-" } else { "" }
    ))
}

fn duration_millis(array: &dyn Array, unit: TimeUnit, row: usize) -> Result<i64> {
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
    Ok(millis)
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

fn dictionary_literal(
    field: &Field,
    array: &dyn Array,
    key: &DataType,
    row: usize,
) -> Result<String> {
    macro_rules! keys {
        ($type:ty) => {{
            dictionary_value(
                field,
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
    field: &Field,
    array: &DictionaryArray<K>,
    row: usize,
) -> Result<String> {
    match array.key(row) {
        Some(key) => {
            let value_field = Field::new("", array.values().data_type().clone(), true)
                .with_metadata(field.metadata().clone());
            literal(&value_field, array.values().as_ref(), key)
        }
        None => Ok("NULL".into()),
    }
}

fn date_from_days(days: i64) -> Result<NaiveDate> {
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1)
        .ok_or_else(|| error("could not construct Unix epoch date", Status::Internal))?;
    let delta = TimeDelta::try_days(days)
        .ok_or_else(|| error("date parameter is out of range", Status::InvalidData))?;
    epoch
        .checked_add_signed(delta)
        .ok_or_else(|| error("date parameter is out of range", Status::InvalidData))
}

#[cfg(test)]
mod tests {
    use std::{fmt::Write, sync::Arc, time::Duration};

    use arrow_array::{
        DictionaryArray, DurationMillisecondArray, Float64Array, Int8Array, Int32Array,
        StringArray, Time64MicrosecondArray, UInt64Array, builder::FixedSizeBinaryBuilder,
        types::Int8Type,
    };
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
            render_row("SELECT ?, '?', \"?\", ? -- ?", &batch, 0, false).unwrap(),
            "SELECT 42, '?', \"?\", R'a''b' -- ?"
        );
    }

    #[test]
    fn validates_parameter_counts_and_lexing() {
        assert_eq!(parameter_layout("SELECT ? /* ? */").unwrap().count(), 1);
        assert_eq!(
            parameter_layout("SELECT R';?' /* ; */;").unwrap().count(),
            0
        );
        assert!(parameter_layout("SELECT 1; DELETE FROM important").is_err());
        assert!(parameter_layout("SELECT 'unterminated").is_err());
        assert!(parameter_layout("SELECT '\\''").is_err());
        assert_eq!(
            parameter_layout("SELECT E'a\\'b?', R'\\?'")
                .unwrap()
                .count(),
            0
        );
        assert_eq!(
            parameter_layout("SELECT 1::INT").unwrap(),
            ParameterLayout::Positional(0)
        );
        let batch = RecordBatch::new_empty(Arc::new(Schema::empty()));
        assert!(render_row("SELECT ?", &batch, 0, false).is_err());
        assert_eq!(
            render_null_parameters("SELECT ?, '?' /* ? */").unwrap(),
            "SELECT NULL, '?' /* ? */"
        );
        assert_eq!(
            unbound_statements(
                "/* ÅÄÖ */ CREATE TABLE \"MiXeD;ÅÄÖ\"(value STRING); \
                 INSERT INTO \"MiXeD;ÅÄÖ\" VALUES ('SeLeCt;ÅäÖ'); -- trailing ;"
            )
            .unwrap(),
            [
                "/* ÅÄÖ */ CREATE TABLE \"MiXeD;ÅÄÖ\"(value STRING)",
                "INSERT INTO \"MiXeD;ÅÄÖ\" VALUES ('SeLeCt;ÅäÖ')"
            ]
        );
        assert!(unbound_statements("SELECT ?; SELECT 1").is_err());
    }

    #[test]
    fn renders_named_parameters_by_field_name() {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("right", DataType::Int32, false),
                Field::new("left", DataType::Utf8, false),
            ])),
            vec![
                Arc::new(Int32Array::from(vec![7])),
                Arc::new(StringArray::from(vec!["a'b"])),
            ],
        )
        .unwrap();
        let query = "SELECT :left, :right + :right, ':ignored', TIME '12:34:56' -- :ignored";
        assert_eq!(
            parameter_layout(query).unwrap(),
            ParameterLayout::Named(vec!["left".into(), "right".into()])
        );
        assert_eq!(
            render_row(query, &batch, 0, true).unwrap(),
            "SELECT R'a''b', 7 + 7, ':ignored', TIME '12:34:56' -- :ignored"
        );
        assert_eq!(
            render_null_parameters(query).unwrap(),
            "SELECT NULL, NULL + NULL, ':ignored', TIME '12:34:56' -- :ignored"
        );
        assert!(render_row(query, &batch, 0, false).is_err());
        assert!(parameter_layout("SELECT ?, :named").is_err());
    }

    #[test]
    fn preserves_utf8_in_every_lexer_state() {
        let query = "sElEcT ÅÄÖ, åäö, 'SeLeCt ö\\界', R'tail\\', X'00ff', U&'\\0061', \"MiXeD_列\", ? -- коммент\n/* 注釈 */ # ?";
        let template = QueryTemplate::parse(query).unwrap();
        let rendered = template.render(|_| Ok("42")).unwrap();
        assert_eq!(
            rendered,
            "sElEcT ÅÄÖ, åäö, 'SeLeCt ö\\界', R'tail\\', X'00ff', U&'\\0061', \"MiXeD_列\", 42 -- коммент\n/* 注釈 */ # ?"
        );
        assert_eq!(template.layout(), &ParameterLayout::Positional(1));
    }

    #[test]
    fn compiles_massive_bi_query_without_quadratic_parameter_lookup() {
        let mut query = String::with_capacity(6 * 1024 * 1024);
        query.push_str("/* BI generated ÅÄÖ */\nsElEcT\n");
        for index in 0..75_000 {
            if index > 0 {
                query.push_str(",\n");
            }
            write!(
                query,
                "  :parameter_{index:05} AS \"MiXeD_ÅÄÖ_{index:05}\", 'SeLeCt ÅäÖ' AS \"LiTeRaL_{index:05}\""
            )
            .unwrap();
        }
        query.push_str("\nFROM \"FaCt_ÅÄÖ\"\n/* end BI query ÅÄÖ */");
        assert!(query.len() > 4 * 1024 * 1024);

        let started = std::time::Instant::now();
        let template = QueryTemplate::parse(&query).unwrap();
        let rendered = template.render_nulls().unwrap();
        assert!(started.elapsed() < Duration::from_secs(10));
        assert_eq!(template.layout().count(), 75_000);
        assert!(rendered.starts_with(
            "/* BI generated ÅÄÖ */\nsElEcT\n  NULL AS \"MiXeD_ÅÄÖ_00000\", 'SeLeCt ÅäÖ' AS \"LiTeRaL_00000\""
        ));
        assert!(rendered.ends_with(
            "NULL AS \"MiXeD_ÅÄÖ_74999\", 'SeLeCt ÅäÖ' AS \"LiTeRaL_74999\"\nFROM \"FaCt_ÅÄÖ\"\n/* end BI query ÅÄÖ */"
        ));
    }

    #[test]
    fn formats_decimal_and_temporal_literals() {
        assert_eq!(decimal_literal(-123, 2).unwrap(), "-1.23");
        assert_eq!(float_literal(3.5).unwrap(), "3.5e0");
        assert_eq!(float_literal(f64::MAX).unwrap(), "1.7976931348623157e308");
        assert_eq!(date_literal(0).unwrap(), "DATE '1970-01-01'");
        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        for (date, expected) in [
            (
                NaiveDate::from_ymd_opt(0, 2, 29).unwrap(),
                "DATE '0000-02-29'",
            ),
            (
                NaiveDate::from_ymd_opt(-1, 12, 31).unwrap(),
                "DATE '-0001-12-31'",
            ),
        ] {
            let days = date.signed_duration_since(epoch).num_days();
            assert_eq!(date_literal(days).unwrap(), expected);
        }
        assert_eq!(format_time(3_723_123_456).unwrap(), "01:02:03.123456");
    }

    #[test]
    fn renders_extension_parameters_without_losing_their_monetdb_type() {
        let metadata = |name: &str| [("ARROW:extension:name".to_owned(), name.to_owned())].into();
        let month = Field::new("m", DataType::Int32, false)
            .with_metadata(metadata("monetdb.interval_month"));
        assert_eq!(
            literal(&month, &Int32Array::from(vec![18]), 0).unwrap(),
            "INTERVAL '18' MONTH"
        );

        let day = Field::new("d", DataType::Duration(TimeUnit::Millisecond), false)
            .with_metadata(metadata("monetdb.interval_day"));
        assert_eq!(
            literal(&day, &DurationMillisecondArray::from(vec![86_401_500]), 0).unwrap(),
            "INTERVAL '1 00:00:01.500' DAY TO SECOND"
        );

        let mut uuids = FixedSizeBinaryBuilder::with_capacity(1, 16);
        uuids.append_value([0x12; 16]).unwrap();
        let uuid = Field::new("u", DataType::FixedSizeBinary(16), false)
            .with_metadata(metadata("arrow.uuid"));
        assert_eq!(
            literal(&uuid, &uuids.finish(), 0).unwrap(),
            "UUID '12121212-1212-1212-1212-121212121212'"
        );

        let oid = Field::new("o", DataType::UInt64, false).with_metadata(metadata("monetdb.oid"));
        assert!(literal(&oid, &UInt64Array::from(vec![1u64 << 63]), 0).is_err());

        let timetz = Field::new("t", DataType::Time64(TimeUnit::Microsecond), false)
            .with_metadata(metadata("monetdb.timetz"));
        assert_eq!(
            literal(
                &timetz,
                &Time64MicrosecondArray::from(vec![3_723_000_000]),
                0
            )
            .unwrap(),
            "TIMETZ '01:02:03.000000+00:00'"
        );
    }

    #[test]
    fn rejects_non_finite_and_supports_non_string_dictionary_parameters() {
        let float = Field::new("f", DataType::Float64, false);
        assert!(literal(&float, &Float64Array::from(vec![f64::NAN]), 0).is_err());

        let keys = Int8Array::from(vec![1, 0]);
        let values = Arc::new(Int32Array::from(vec![10, 20]));
        let dictionary = DictionaryArray::<Int8Type>::try_new(keys, values).unwrap();
        let field = Field::new("d", dictionary.data_type().clone(), false);
        assert_eq!(literal(&field, &dictionary, 0).unwrap(), "20");
    }
}
