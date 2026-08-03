use std::{
    cell::RefCell,
    collections::{HashMap, HashSet, VecDeque},
    fmt,
    io::{Read, Write},
    num::NonZeroUsize,
    os::raw::c_char,
    path::Path,
    sync::{
        Arc, Mutex, MutexGuard, Weak,
        atomic::{AtomicU64, AtomicUsize, Ordering},
        mpsc::{Receiver, RecvTimeoutError, SyncSender, sync_channel},
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use adbc_core::error::{Error, Result, Status};
use adbc_core::options::{
    InfoCode, ObjectDepth, OptionConnection, OptionDatabase, OptionStatement, OptionValue,
};
use adbc_core::schemas::{GET_INFO_SCHEMA, GET_TABLE_TYPES_SCHEMA};
use adbc_core::{
    Connection, Database, Driver, Optionable, PartitionedResult, Statement, StatementResult,
};
use arrow_array::{
    Array, ArrayRef, BooleanArray, Int64Array, RecordBatch, RecordBatchReader, StringArray,
    UInt32Array, UnionArray, new_empty_array,
};
use arrow_schema::{ArrowError, DataType, Field, Schema, SchemaRef};
use arrow_select::concat::concat_batches;
use memmap2::MmapMut;
use monetdb::{
    CancelHandle, CursorError, DEFAULT_UPLOAD_CHUNK_SIZE_BYTES, Endian, MonetType, Parameters,
    ResultColumn, Timeouts, parms::Parm,
};
use percent_encoding::percent_decode_str;

mod metadata;
mod parameters;
use metadata::{like_pattern_matches, load_objects, objects_batch, table_schema};
use parameters::{
    ParameterLayout, QueryTemplate, parameter_layout, render_arguments, render_null_parameters,
    unbound_statements,
};

const DEFAULT_READ_WINDOW_BYTES: usize = 64 * 1024 * 1024;
const REMOTE_READ_WINDOW_BYTES: usize = 128 * 1024 * 1024;
const MIN_READ_WINDOW_BYTES: usize = 1024 * 1024;
const MAX_AUTOMATIC_READ_GRANULE_BYTES: usize = 512 * 1024 * 1024;
const EXPORT_GRANULE_ROWS: usize = 131_072;
const VARIABLE_READ_COLUMN_BYTES: usize = 1024;
const INITIAL_VARIABLE_READ_ROWS: usize = 65_536;
const MAX_READ_WINDOW_GROWTH: usize = 4;
const DEFAULT_WRITE_WINDOW_BYTES: usize = 512 * 1024 * 1024;
const LOCAL_PHYSICAL_WINDOW_BYTES: usize = 128 * 1024 * 1024;
const WIDE_LOCAL_PHYSICAL_WINDOW_BYTES: usize = 48 * 1024 * 1024;
const WIDE_MEMORY_BOUND_COLUMNS: usize = 512;
const INCOMPRESSIBLE_WRITE_WINDOW_BYTES: usize = 64 * 1024 * 1024;
const MIN_WRITE_WINDOW_BYTES: usize = 4 * 1024 * 1024;
const ENCODE_CHUNK_BYTES: usize = 1024 * 1024;
const MIN_ENCODE_BUFFER_BYTES: usize = 64 * 1024;
const COMPRESSED_ARENA_SLAB_BYTES: usize = 16 * 1024 * 1024;
const COMPRESSION_PROBE_BYTES: usize = 256 * 1024;
const ENCODE_STAGING_BYTES: usize = 16 * 1024 * 1024;
const INGEST_RESERVATION_SOURCE_BYTES: usize = 64 * 1024 * 1024;
const PENDING_BATCH_COMPACTION_FANOUT: usize = 32;
const PENDING_BATCH_COMPACTION_MAX_BYTES: usize = 16 * 1024 * 1024;
const DEFAULT_PREPARED_CACHE_CAPACITY: usize = 512;
const ADAPTIVE_NETWORK_RTT: Duration = Duration::from_micros(500);
const REMOTE_WRITE_WINDOW_RTT: Duration = Duration::from_millis(5);
const INSERT_VALUE_ENCODE_MICROS: u128 = 2;
const COPY_FILE_EXCHANGES_PER_COLUMN: u128 = 2;
const MAX_ADAPTIVE_INSERT_ROWS: usize = 10_000;
const PREFETCH_DROP_GRACE: Duration = Duration::from_millis(250);
const METADATA_REPLY_ROWS: usize = 1024;
const METADATA_READ_BATCH_ROWS: usize = 131_072;
const INLINE_REPLY_ROWS: i64 = 100;
const READ_BATCH_ROWS_OPTION: &str = "adbc.monetdb.read_batch_rows";
const READ_WINDOW_BYTES_OPTION: &str = "adbc.monetdb.read_window_bytes";
const READ_PREFETCH_OPTION: &str = "adbc.monetdb.read_prefetch";
const WRITE_BATCH_ROWS_OPTION: &str = "adbc.monetdb.write_batch_rows";
const WRITE_WINDOW_BYTES_OPTION: &str = "adbc.monetdb.write_window_bytes";
const INGEST_INSERT_ROWS_OPTION: &str = "adbc.monetdb.ingest_insert_rows";
const PREPARED_CACHE_CAPACITY_OPTION: &str = "adbc.monetdb.prepared_cache_capacity";
const WIRE_COMPRESSION_OPTION: &str = "adbc.monetdb.wire_compression";
const INGEST_PARTIAL_OPTION: &str = "adbc.monetdb.ingest_partial";
const INGEST_ATOMICITY_OPTION: &str = "adbc.monetdb.ingest_atomicity";
const CONSTRAINED_APPEND_OPTION: &str = "adbc.monetdb.constrained_append";
const INGEST_STATS_OPTION: &str = "adbc.monetdb.ingest_stats";
const READ_STATS_OPTION: &str = "adbc.monetdb.read_stats";
const BIND_BY_NAME_OPTION: &str = "adbc.monetdb.bind_by_name";
const DRIVER_MANAGER_BIND_BY_NAME_OPTION: &str = "adbc.statement.bind_by_name";
const CONNECT_TIMEOUT_OPTION: &str = "adbc.monetdb.connect_timeout_seconds";
const READ_TIMEOUT_OPTION: &str = "adbc.monetdb.read_timeout_seconds";
const WRITE_TIMEOUT_OPTION: &str = "adbc.monetdb.write_timeout_seconds";
const OPERATION_TIMEOUT_OPTION: &str = "adbc.monetdb.operation_timeout_seconds";
const CLIENT_APPLICATION_OPTION: &str = "adbc.monetdb.client_application";
const CLIENT_REMARK_OPTION: &str = "adbc.monetdb.client_remark";
const CLIENT_INFO_OPTION: &str = "adbc.monetdb.client_info";
const TERMINAL_ERROR_DETAIL: &str = "adbc.monetdb.connection_terminal";
const MINIMUM_VERSION: (u16, u16, u16) = (11, 55, 0);
const LOCAL_TEMP_STAGING_MINIMUM_VERSION: (u16, u16, u16) = (11, 55, 7);
static SAVEPOINT_ID: AtomicU64 = AtomicU64::new(0);
static RESERVED_INGEST_MEMORY_BYTES: AtomicUsize = AtomicUsize::new(0);
thread_local! {
    static COMPRESSION_WORKSPACE: RefCell<CompressionWorkspace> =
        RefCell::new(CompressionWorkspace::default());
}

const URI_QUERY_KEYS: &[&str] = &[
    "user",
    "password",
    "cert",
    "certhash",
    "clientcert",
    "clientkey",
    "schema",
    "sock",
    "sockdir",
    "connect_timeout",
    "read_timeout",
    "write_timeout",
    "operation_timeout",
    "client_info",
    "client_application",
    "client_remark",
    "max_response_size",
    "read_window_bytes",
    "write_window_bytes",
    "ingest_insert_rows",
    "prepared_cache_capacity",
    "wire_compression",
    "constrained_append",
];

fn tuning_option_from_uri_key(key: &str) -> Option<&'static str> {
    match key {
        "read_window_bytes" => Some(READ_WINDOW_BYTES_OPTION),
        "write_window_bytes" => Some(WRITE_WINDOW_BYTES_OPTION),
        "ingest_insert_rows" => Some(INGEST_INSERT_ROWS_OPTION),
        "prepared_cache_capacity" => Some(PREPARED_CACHE_CAPACITY_OPTION),
        "wire_compression" => Some(WIRE_COMPRESSION_OPTION),
        "constrained_append" => Some(CONSTRAINED_APPEND_OPTION),
        _ => None,
    }
}

fn tuning_uri_key(option: &str) -> Option<&'static str> {
    match option {
        READ_WINDOW_BYTES_OPTION => Some("read_window_bytes"),
        WRITE_WINDOW_BYTES_OPTION => Some("write_window_bytes"),
        INGEST_INSERT_ROWS_OPTION => Some("ingest_insert_rows"),
        PREPARED_CACHE_CAPACITY_OPTION => Some("prepared_cache_capacity"),
        WIRE_COMPRESSION_OPTION => Some("wire_compression"),
        CONSTRAINED_APPEND_OPTION => Some("constrained_append"),
        _ => None,
    }
}

fn validate_tuning_option(key: &str, value: &OptionValue) -> Result<()> {
    match key {
        READ_WINDOW_BYTES_OPTION => {
            read_window_bytes_option(value)?;
        }
        WRITE_WINDOW_BYTES_OPTION => {
            write_window_bytes_option(value)?;
        }
        INGEST_INSERT_ROWS_OPTION => {
            nonnegative_usize_option(key, value)?;
        }
        PREPARED_CACHE_CAPACITY_OPTION => {
            if nonnegative_usize_option(key, value)? == 0 {
                return Err(error(
                    format!("option '{key}' must be positive"),
                    Status::InvalidArguments,
                ));
            }
        }
        WIRE_COMPRESSION_OPTION => {
            WireCompression::parse(value)?;
        }
        CONSTRAINED_APPEND_OPTION => {
            ConstrainedAppend::parse(value)?;
        }
        _ => return Err(not_implemented(key)),
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TimeoutOption {
    Connect,
    Read,
    Write,
    Operation,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ClientInfoOption {
    Application,
    Remark,
    Enabled,
}

impl ClientInfoOption {
    fn from_key(key: &str) -> Option<Self> {
        match key {
            CLIENT_APPLICATION_OPTION => Some(Self::Application),
            CLIENT_REMARK_OPTION => Some(Self::Remark),
            CLIENT_INFO_OPTION => Some(Self::Enabled),
            _ => None,
        }
    }

    fn uri_key(self) -> &'static str {
        match self {
            Self::Application => "client_application",
            Self::Remark => "client_remark",
            Self::Enabled => "client_info",
        }
    }

    fn default_value(self) -> &'static str {
        match self {
            Self::Application | Self::Remark => "",
            Self::Enabled => "true",
        }
    }

    fn validate(self, key: &str, value: &OptionValue) -> Result<OptionValue> {
        let OptionValue::String(value) = value else {
            return Err(error(
                format!("option '{key}' must be a string"),
                Status::InvalidArguments,
            ));
        };
        match self {
            Self::Application | Self::Remark => {
                if value.contains('\n') {
                    return Err(error(
                        format!("option '{key}' must not contain newlines"),
                        Status::InvalidArguments,
                    ));
                }
                Ok(value.clone().into())
            }
            Self::Enabled => Ok(parse_bool_option(value)?.to_string().into()),
        }
    }

    fn apply(self, parameters: &mut Parameters, value: &OptionValue) -> Result<()> {
        let OptionValue::String(value) = value else {
            return Err(error(
                "validated client-info option is not a string",
                Status::Internal,
            ));
        };
        let result = match self {
            Self::Application => parameters.set_client_application(value),
            Self::Remark => parameters.set_client_remark(value),
            Self::Enabled => parameters.set_client_info(value),
        };
        result.map_err(|error| map_display(error, Status::InvalidArguments))
    }
}

impl TimeoutOption {
    fn from_key(key: &str) -> Option<Self> {
        match key {
            CONNECT_TIMEOUT_OPTION => Some(Self::Connect),
            READ_TIMEOUT_OPTION => Some(Self::Read),
            WRITE_TIMEOUT_OPTION => Some(Self::Write),
            OPERATION_TIMEOUT_OPTION => Some(Self::Operation),
            _ => None,
        }
    }

    fn uri_key(self) -> &'static str {
        match self {
            Self::Connect => "connect_timeout",
            Self::Read => "read_timeout",
            Self::Write => "write_timeout",
            Self::Operation => "operation_timeout",
        }
    }
}

fn timeout_seconds(key: &str, value: &OptionValue) -> Result<i64> {
    let seconds = integer_option(key, value)?;
    if seconds < 0 {
        return Err(error(
            format!("option '{key}' must be non-negative"),
            Status::InvalidArguments,
        ));
    }
    if seconds > monetdb::MAX_TIMEOUT_SECONDS {
        return Err(error(
            format!(
                "option '{key}' must not exceed {} seconds",
                monetdb::MAX_TIMEOUT_SECONDS
            ),
            Status::InvalidArguments,
        ));
    }
    Ok(seconds)
}

fn integer_option(key: &str, value: &OptionValue) -> Result<i64> {
    let value = match value {
        OptionValue::String(value) => value.parse::<i64>().map_err(|_| {
            error(
                format!("option '{key}' must be an integer"),
                Status::InvalidArguments,
            )
        })?,
        OptionValue::Int(value) => *value,
        _ => {
            return Err(error(
                format!("option '{key}' must be an integer"),
                Status::InvalidArguments,
            ));
        }
    };
    Ok(value)
}

fn read_batch_rows_option(value: &OptionValue) -> Result<usize> {
    let rows = integer_option(READ_BATCH_ROWS_OPTION, value)?;
    let rows = usize::try_from(rows).map_err(|_| {
        error(
            format!("option '{READ_BATCH_ROWS_OPTION}' must be non-negative"),
            Status::InvalidArguments,
        )
    })?;
    let rounded = round_read_batch_rows(rows);
    if rounded != rows {
        eprintln!(
            "warning: option '{READ_BATCH_ROWS_OPTION}' was rounded from {rows} to {rounded}; \
             reads prefer {EXPORT_GRANULE_ROWS}-row export boundaries or power-of-two divisors"
        );
    }
    Ok(rounded)
}

fn round_read_batch_rows(rows: usize) -> usize {
    if rows.is_multiple_of(EXPORT_GRANULE_ROWS) {
        return rows;
    }
    if rows < EXPORT_GRANULE_ROWS {
        let lower = export_alignment_rows(rows);
        if lower == rows {
            return rows;
        }
        let upper = lower.saturating_mul(2).min(EXPORT_GRANULE_ROWS);
        return if rows - lower >= upper - rows {
            upper
        } else {
            lower
        };
    }
    let lower = rows / EXPORT_GRANULE_ROWS * EXPORT_GRANULE_ROWS;
    let upper = lower.checked_add(EXPORT_GRANULE_ROWS);
    if lower == 0 || rows - lower >= EXPORT_GRANULE_ROWS / 2 {
        upper
            .filter(|upper| i64::try_from(*upper).is_ok())
            .unwrap_or(lower)
    } else {
        lower
    }
}

fn read_window_bytes_option(value: &OptionValue) -> Result<Option<usize>> {
    let bytes = integer_option(READ_WINDOW_BYTES_OPTION, value)?;
    if bytes == 0 {
        return Ok(None);
    }
    usize::try_from(bytes)
        .ok()
        .filter(|bytes| *bytes >= MIN_READ_WINDOW_BYTES)
        .map(Some)
        .ok_or_else(|| {
            error(
                format!(
                    "option '{READ_WINDOW_BYTES_OPTION}' must be 0 or at least {MIN_READ_WINDOW_BYTES}"
                ),
                Status::InvalidArguments,
            )
        })
}

fn write_batch_rows_option(value: &OptionValue) -> Result<Option<usize>> {
    let rows = integer_option(WRITE_BATCH_ROWS_OPTION, value)?;
    if rows == 0 {
        return Ok(None);
    }
    usize::try_from(rows).map(Some).map_err(|_| {
        error(
            format!("option '{WRITE_BATCH_ROWS_OPTION}' must be non-negative"),
            Status::InvalidArguments,
        )
    })
}

fn write_window_bytes_option(value: &OptionValue) -> Result<Option<usize>> {
    let bytes = integer_option(WRITE_WINDOW_BYTES_OPTION, value)?;
    if bytes == 0 {
        return Ok(None);
    }
    usize::try_from(bytes)
        .ok()
        .filter(|bytes| *bytes >= MIN_WRITE_WINDOW_BYTES)
        .map(Some)
        .ok_or_else(|| {
            error(
                format!(
                    "option '{WRITE_WINDOW_BYTES_OPTION}' must be 0 or at least {MIN_WRITE_WINDOW_BYTES}"
                ),
                Status::InvalidArguments,
            )
        })
}

fn nonnegative_usize_option(key: &str, value: &OptionValue) -> Result<usize> {
    usize::try_from(integer_option(key, value)?).map_err(|_| {
        error(
            format!("option '{key}' must be non-negative"),
            Status::InvalidArguments,
        )
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IngestPartial {
    Block,
    Allow,
}

impl IngestPartial {
    fn parse(value: &OptionValue) -> Result<Self> {
        let OptionValue::String(value) = value else {
            return Err(error(
                format!("option '{INGEST_PARTIAL_OPTION}' must be a string"),
                Status::InvalidArguments,
            ));
        };
        match value.as_str() {
            "block" => Ok(Self::Block),
            "allow" => Ok(Self::Allow),
            _ => Err(error(
                format!("option '{INGEST_PARTIAL_OPTION}' must be 'block' or 'allow'"),
                Status::InvalidArguments,
            )),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Block => "block",
            Self::Allow => "allow",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IngestAtomicity {
    Transaction,
    Savepoint,
}

impl IngestAtomicity {
    fn parse(value: &OptionValue) -> Result<Self> {
        let OptionValue::String(value) = value else {
            return Err(error(
                format!("option '{INGEST_ATOMICITY_OPTION}' must be a string"),
                Status::InvalidArguments,
            ));
        };
        match value.as_str() {
            "transaction" => Ok(Self::Transaction),
            "savepoint" => Ok(Self::Savepoint),
            _ => Err(error(
                format!("option '{INGEST_ATOMICITY_OPTION}' must be 'transaction' or 'savepoint'"),
                Status::InvalidArguments,
            )),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Transaction => "transaction",
            Self::Savepoint => "savepoint",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConstrainedAppend {
    Auto,
    Direct,
}

impl ConstrainedAppend {
    fn parse(value: &OptionValue) -> Result<Self> {
        let OptionValue::String(value) = value else {
            return Err(error(
                format!("option '{CONSTRAINED_APPEND_OPTION}' must be a string"),
                Status::InvalidArguments,
            ));
        };
        match value.as_str() {
            "auto" => Ok(Self::Auto),
            "direct" => Ok(Self::Direct),
            _ => Err(error(
                format!("option '{CONSTRAINED_APPEND_OPTION}' must be 'auto' or 'direct'"),
                Status::InvalidArguments,
            )),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::Direct => "direct",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WireCompression {
    None,
    Auto,
    Lz4,
}

impl WireCompression {
    fn parse(value: &OptionValue) -> Result<Self> {
        let OptionValue::String(value) = value else {
            return Err(error(
                format!("option '{WIRE_COMPRESSION_OPTION}' must be a string"),
                Status::InvalidArguments,
            ));
        };
        match value.as_str() {
            "none" => Ok(Self::None),
            "auto" => Ok(Self::Auto),
            "lz4" => Ok(Self::Lz4),
            _ => Err(error(
                format!("option '{WIRE_COMPRESSION_OPTION}' must be 'none', 'auto', or 'lz4'"),
                Status::InvalidArguments,
            )),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Auto => "auto",
            Self::Lz4 => "lz4",
        }
    }
}

fn apply_parameter_timeout(
    parameters: &mut Parameters,
    timeout: TimeoutOption,
    key: &str,
    value: &OptionValue,
) -> Result<()> {
    let seconds = timeout_seconds(key, value)?;
    let result = match timeout {
        TimeoutOption::Connect => parameters.set_connect_timeout(seconds),
        TimeoutOption::Read => parameters.set_read_timeout(seconds),
        TimeoutOption::Write => parameters.set_write_timeout(seconds),
        TimeoutOption::Operation => parameters.set_operation_timeout(seconds),
    };
    result.map_err(|value| map_display(value, Status::InvalidArguments))
}

fn set_runtime_timeout(
    timeouts: &mut Timeouts,
    timeout: TimeoutOption,
    key: &str,
    value: &OptionValue,
) -> Result<()> {
    let seconds = timeout_seconds(key, value)?;
    let duration = (seconds != 0).then(|| Duration::from_secs(seconds as u64));
    match timeout {
        TimeoutOption::Connect => {
            return Err(error(
                format!("option '{key}' is only valid as a database option"),
                Status::InvalidState,
            ));
        }
        TimeoutOption::Read => timeouts.read = duration,
        TimeoutOption::Write => timeouts.write = duration,
        TimeoutOption::Operation => timeouts.operation = duration,
    }
    Ok(())
}

fn configured_timeouts(parameters: &Parameters) -> Result<(Option<Duration>, Timeouts)> {
    let validated = parameters
        .validate()
        .map_err(|value| map_display(value, Status::InvalidArguments))?;
    Ok((
        validated.connect_timeout,
        Timeouts {
            read: validated.read_timeout,
            write: validated.write_timeout,
            operation: validated.operation_timeout,
        },
    ))
}

fn store_runtime_timeouts(options: &mut Options, timeouts: Timeouts) {
    for (key, timeout) in [
        (READ_TIMEOUT_OPTION, timeouts.read),
        (WRITE_TIMEOUT_OPTION, timeouts.write),
        (OPERATION_TIMEOUT_OPTION, timeouts.operation),
    ] {
        let seconds = timeout.map_or(0, |timeout| timeout.as_secs());
        options.set(key, seconds.to_string().into());
    }
}

fn initialization_timeouts(timeouts: Timeouts, deadline: Option<Instant>) -> Result<Timeouts> {
    let Some(deadline) = deadline else {
        return Ok(timeouts);
    };
    let remaining = deadline
        .checked_duration_since(Instant::now())
        .ok_or_else(|| {
            error(
                "connection initialization deadline expired",
                Status::Timeout,
            )
        })?;
    let bounded = |timeout: Option<Duration>| Some(timeout.unwrap_or(remaining).min(remaining));
    Ok(Timeouts {
        read: bounded(timeouts.read),
        write: bounded(timeouts.write),
        operation: bounded(timeouts.operation),
    })
}

fn savepoint_name(purpose: &str) -> String {
    // The outer connection mutex serializes driver-created savepoints. MonetDB's
    // RELEASE semantics therefore cannot discard a later driver savepoint.
    format!(
        "adbc_{purpose}_{}_{}",
        std::process::id(),
        SAVEPOINT_ID.fetch_add(1, Ordering::Relaxed)
    )
}

fn error(message: impl Into<String>, status: Status) -> Error {
    Error::with_message_and_status(message, status)
}

fn not_implemented(what: &str) -> Error {
    error(
        format!("adbc-monetdb: {what} is not implemented"),
        Status::NotImplemented,
    )
}

fn unknown_option(key: &str) -> Error {
    error(format!("unknown or unset option '{key}'"), Status::NotFound)
}

fn quote_raw_string(value: &str) -> String {
    format!("R'{}'", value.replace('\'', "''"))
}

fn map_display(error: impl fmt::Display, status: Status) -> Error {
    Error::with_message_and_status(error.to_string(), status)
}

struct DriverConnection {
    inner: Mutex<monetdb::Connection>,
    pending_deallocations: Mutex<Vec<u64>>,
    prepared_generation: AtomicU64,
    ingest_poison: Mutex<Option<IngestPoison>>,
}

impl DriverConnection {
    fn new(connection: monetdb::Connection) -> Self {
        Self {
            inner: Mutex::new(connection),
            pending_deallocations: Mutex::new(Vec::new()),
            prepared_generation: AtomicU64::new(0),
            ingest_poison: Mutex::new(None),
        }
    }

    fn poison_ingest(&self, target: &str, windows: usize) {
        let mut poison = self
            .ingest_poison
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if poison.is_none() {
            *poison = Some(IngestPoison {
                target: target.to_owned(),
                windows,
            });
        }
    }

    fn commit_error(&self) -> Option<Error> {
        self.ingest_poison
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .map(|poison| {
                let mut result = error(
                    format!(
                        "ingest into {} failed after {} COPY windows; the transaction contains a partial append; ROLLBACK is required",
                        poison.target, poison.windows
                    ),
                    Status::InvalidState,
                );
                result.sqlstate =
                    parse_sqlstate("25000").expect("the rollback-only SQLSTATE is valid");
                result
            })
    }

    fn clear_ingest_poison(&self) {
        *self
            .ingest_poison
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
    }
}

struct IngestPoison {
    target: String,
    windows: usize,
}

type SharedConnection = Arc<DriverConnection>;

fn lock_connection(connection: &SharedConnection) -> Result<MutexGuard<'_, monetdb::Connection>> {
    let connection_guard = connection
        .inner
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    queue_pending_deallocations(connection, &connection_guard);
    Ok(connection_guard)
}

fn queue_pending_deallocations(
    connection: &DriverConnection,
    connection_guard: &monetdb::Connection,
) {
    let pending = connection
        .pending_deallocations
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .drain(..)
        .collect::<Vec<_>>();
    for id in pending {
        connection_guard.try_deallocate(id);
    }
}

fn invalidate_prepared_cache(connection: &DriverConnection, prepared_cache: &SharedPreparedCache) {
    connection
        .prepared_generation
        .fetch_add(1, Ordering::AcqRel);
    prepared_cache
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clear();
}

fn map_cursor_error(value: CursorError) -> Error {
    let terminal = matches!(
        &value,
        CursorError::Closed
            | CursorError::Cancelled
            | CursorError::Timeout
            | CursorError::IO(_)
            | CursorError::Framing(_)
            | CursorError::BadReply(_)
    );
    let status = match value {
        CursorError::Closed | CursorError::NoResultSet => Status::InvalidState,
        CursorError::Cancelled => Status::Cancelled,
        CursorError::Timeout => Status::Timeout,
        CursorError::IO(_) => Status::IO,
        CursorError::Framing(_) | CursorError::BadReply(_) => Status::InvalidData,
        CursorError::Conversion { .. }
        | CursorError::InvalidRange { .. }
        | CursorError::ResultNotResident { .. } => Status::InvalidData,
        CursorError::FileTransfer(_)
        | CursorError::UploadComplete
        | CursorError::UploadRefused { .. } => Status::InvalidData,
        CursorError::PreparedResult => Status::InvalidState,
        CursorError::Metadata(_) | CursorError::Poisoned => Status::Internal,
        CursorError::Server(ref error) => sqlstate_status(error.sqlstate(), error.message()),
    };
    let mut result = error(value.to_string(), status);
    if let CursorError::Server(error) = value
        && let Some(sqlstate) = error.sqlstate().and_then(parse_sqlstate)
    {
        result.sqlstate = sqlstate;
    }
    if terminal {
        result.vendor_code = adbc_core::constants::ADBC_ERROR_VENDOR_CODE_PRIVATE_DATA;
        result.details = Some(vec![(TERMINAL_ERROR_DETAIL.to_owned(), b"true".to_vec())]);
    }
    result
}

fn sqlstate_status(sqlstate: Option<&str>, message: &str) -> Status {
    let Some(sqlstate) = sqlstate.map(str::as_bytes) else {
        return Status::Unknown;
    };
    if sqlstate == b"42000" && message.contains("access denied") {
        return Status::Unauthorized;
    }
    match sqlstate {
        b"42S02" | b"42S22" | b"3F000" | b"42703" => Status::NotFound,
        b"42S01" | b"42710" => Status::AlreadyExists,
        b"40002" => Status::Integrity,
        b"42501" => Status::Unauthorized,
        b"28000" => Status::Unauthenticated,
        b"57014" => Status::Cancelled,
        b"HYT00" | b"HYT01" => Status::Timeout,
        b"2DM30" => Status::InvalidState,
        code if code.starts_with(b"23") => Status::Integrity,
        code if code.starts_with(b"25") || code.starts_with(b"2D") => Status::InvalidState,
        code if code.starts_with(b"28") => Status::Unauthorized,
        code if code.starts_with(b"42") => Status::InvalidArguments,
        _ => Status::Unknown,
    }
}

fn parse_sqlstate(sqlstate: &str) -> Option<[c_char; 5]> {
    let code: [u8; 5] = sqlstate.as_bytes().try_into().ok()?;
    if !code.iter().all(u8::is_ascii_alphanumeric) {
        return None;
    }
    Some(code.map(|byte| byte as c_char))
}

#[derive(Debug, Default)]
struct Options(HashMap<String, OptionValue>);

impl Options {
    fn set(&mut self, key: impl AsRef<str>, value: OptionValue) {
        self.0.insert(key.as_ref().to_owned(), value);
    }

    fn remove(&mut self, key: impl AsRef<str>) {
        self.0.remove(key.as_ref());
    }

    fn get(&self, key: impl AsRef<str>) -> Option<&OptionValue> {
        self.0.get(key.as_ref())
    }

    fn optional_string(&self, key: impl AsRef<str>) -> Option<&str> {
        match self.get(key) {
            Some(OptionValue::String(value)) => Some(value),
            _ => None,
        }
    }

    fn get_string(&self, key: impl AsRef<str>) -> Result<String> {
        match self.get(&key) {
            Some(OptionValue::String(value)) => Ok(value.clone()),
            Some(value) => Err(option_type_error(key.as_ref(), "string", value)),
            None => Err(unknown_option(key.as_ref())),
        }
    }

    fn get_bytes(&self, key: impl AsRef<str>) -> Result<Vec<u8>> {
        match self.get(&key) {
            Some(OptionValue::Bytes(value)) => Ok(value.clone()),
            Some(value) => Err(option_type_error(key.as_ref(), "bytes", value)),
            None => Err(unknown_option(key.as_ref())),
        }
    }

    fn get_int(&self, key: impl AsRef<str>) -> Result<i64> {
        match self.get(&key) {
            Some(OptionValue::Int(value)) => Ok(*value),
            Some(value) => Err(option_type_error(key.as_ref(), "integer", value)),
            None => Err(unknown_option(key.as_ref())),
        }
    }

    fn get_double(&self, key: impl AsRef<str>) -> Result<f64> {
        match self.get(&key) {
            Some(OptionValue::Double(value)) => Ok(*value),
            Some(value) => Err(option_type_error(key.as_ref(), "double", value)),
            None => Err(unknown_option(key.as_ref())),
        }
    }
}

fn option_type_error(key: &str, expected: &str, value: &OptionValue) -> Error {
    let actual = match value {
        OptionValue::String(_) => "string",
        OptionValue::Bytes(_) => "bytes",
        OptionValue::Int(_) => "integer",
        OptionValue::Double(_) => "double",
        _ => "an unsupported value type",
    };
    error(
        format!("option '{key}' is {actual}, not {expected}"),
        Status::InvalidData,
    )
}

#[derive(Debug, Default)]
pub struct MonetdbDriver;

impl Driver for MonetdbDriver {
    type DatabaseType = MonetdbDatabase;

    fn new_database(&mut self) -> Result<Self::DatabaseType> {
        self.new_database_with_opts([])
    }

    fn new_database_with_opts(
        &mut self,
        opts: impl IntoIterator<Item = (OptionDatabase, OptionValue)>,
    ) -> Result<Self::DatabaseType> {
        let mut database = MonetdbDatabase::default();
        for (key, value) in opts {
            database.set_option(key, value)?;
        }
        Ok(database)
    }
}

#[derive(Debug, Default)]
pub struct MonetdbDatabase {
    options: Options,
}

impl Optionable for MonetdbDatabase {
    type Option = OptionDatabase;

    fn set_option(&mut self, key: Self::Option, value: OptionValue) -> Result<()> {
        if matches!(
            key,
            OptionDatabase::Uri | OptionDatabase::Username | OptionDatabase::Password
        ) && !matches!(value, OptionValue::String(_))
        {
            return Err(error(
                format!("database option '{}' must be a string", key.as_ref()),
                Status::InvalidArguments,
            ));
        }
        if key == OptionDatabase::Uri {
            let OptionValue::String(uri) = &value else {
                unreachable!("the standard URI type was validated above");
            };
            parse_driver_uri(uri)?;
        }
        if let OptionDatabase::Other(name) = &key {
            if TimeoutOption::from_key(name).is_some() {
                timeout_seconds(name, &value)?;
            } else if tuning_uri_key(name).is_some() {
                validate_tuning_option(name, &value)?;
            } else if let Some(option) = ClientInfoOption::from_key(name) {
                let value = option.validate(name, &value)?;
                self.options.set(key, value);
                return Ok(());
            } else {
                return Err(not_implemented(name));
            }
        }
        self.options.set(key, value);
        Ok(())
    }

    fn get_option_string(&self, key: Self::Option) -> Result<String> {
        if key == OptionDatabase::Password {
            return Err(error(
                "password credentials cannot be read back",
                Status::NotImplemented,
            ));
        }
        if let OptionDatabase::Other(name) = &key
            && self.options.get(&key).is_none()
        {
            let timeout = TimeoutOption::from_key(name);
            let client_info = ClientInfoOption::from_key(name);
            let tuning = tuning_uri_key(name);
            if let Some(uri_key) = timeout
                .map(TimeoutOption::uri_key)
                .or_else(|| client_info.map(ClientInfoOption::uri_key))
                .or(tuning)
                && let Some(uri) = self.options.optional_string(OptionDatabase::Uri)
            {
                let parsed = parse_driver_uri(uri)?;
                if let Some((_, value)) = parsed
                    .query_pairs()
                    .filter(|(query_key, _)| query_key == uri_key)
                    .last()
                {
                    return Ok(value.into_owned());
                }
            }
            if let Some(client_info) = client_info {
                return Ok(client_info.default_value().to_owned());
            }
            if tuning.is_some() {
                return Ok(match name.as_str() {
                    READ_WINDOW_BYTES_OPTION => "0",
                    WRITE_WINDOW_BYTES_OPTION => "0",
                    INGEST_INSERT_ROWS_OPTION => "100",
                    PREPARED_CACHE_CAPACITY_OPTION => {
                        return Ok(DEFAULT_PREPARED_CACHE_CAPACITY.to_string());
                    }
                    WIRE_COMPRESSION_OPTION => "auto",
                    CONSTRAINED_APPEND_OPTION => "auto",
                    _ => unreachable!("tuning option was recognized"),
                }
                .to_owned());
            }
        }
        let value = self.options.get_string(&key)?;
        if key == OptionDatabase::Uri {
            return uri_without_userinfo(&value);
        }
        Ok(value)
    }

    fn get_option_bytes(&self, key: Self::Option) -> Result<Vec<u8>> {
        self.options.get_bytes(key)
    }

    fn get_option_int(&self, key: Self::Option) -> Result<i64> {
        self.options.get_int(key)
    }

    fn get_option_double(&self, key: Self::Option) -> Result<f64> {
        self.options.get_double(key)
    }
}

impl Database for MonetdbDatabase {
    type ConnectionType = MonetdbConnection;

    fn new_connection(&self) -> Result<Self::ConnectionType> {
        self.new_connection_with_opts([])
    }

    fn new_connection_with_opts(
        &self,
        opts: impl IntoIterator<Item = (OptionConnection, OptionValue)>,
    ) -> Result<Self::ConnectionType> {
        let initialization_started = Instant::now();
        let opts = opts.into_iter().collect::<Vec<_>>();
        let uri = self
            .options
            .optional_string(OptionDatabase::Uri)
            .ok_or_else(|| {
                error(
                    "database option 'uri' is required",
                    Status::InvalidArguments,
                )
            })?;
        let mut parsed_uri = parse_driver_uri(uri)?;
        let uri_username = (!parsed_uri.username().is_empty())
            .then(|| decode_userinfo(parsed_uri.username()))
            .transpose()?;
        let uri_password = parsed_uri.password().map(decode_userinfo).transpose()?;
        if !parsed_uri.username().is_empty() {
            parsed_uri
                .set_username("")
                .map_err(|()| error("URI user information is invalid", Status::InvalidArguments))?;
        }
        if parsed_uri.password().is_some() {
            parsed_uri
                .set_password(None)
                .map_err(|()| error("URI user information is invalid", Status::InvalidArguments))?;
        }
        let mut uri_tuning_options = Vec::new();
        let mut server_query = Vec::new();
        for (key, value) in parsed_uri.query_pairs() {
            if let Some(option) = tuning_option_from_uri_key(&key) {
                uri_tuning_options.push((
                    OptionConnection::Other(option.to_owned()),
                    OptionValue::String(value.into_owned()),
                ));
            } else {
                server_query.push((key.into_owned(), value.into_owned()));
            }
        }
        parsed_uri.set_query(None);
        if !server_query.is_empty() {
            parsed_uri.query_pairs_mut().extend_pairs(server_query);
        }
        let mut parameters = Parameters::from_url(parsed_uri.as_str())
            .map_err(|value| map_display(value, Status::InvalidArguments))?;
        if let Some(username) = self
            .options
            .optional_string(OptionDatabase::Username)
            .or(uri_username.as_deref())
        {
            parameters
                .set_user(username)
                .map_err(|value| map_display(value, Status::InvalidArguments))?;
        }
        if let Some(password) = self
            .options
            .optional_string(OptionDatabase::Password)
            .or(uri_password.as_deref())
        {
            parameters
                .set_password(password)
                .map_err(|value| map_display(value, Status::InvalidArguments))?;
        }
        for key in [
            CONNECT_TIMEOUT_OPTION,
            READ_TIMEOUT_OPTION,
            WRITE_TIMEOUT_OPTION,
            OPERATION_TIMEOUT_OPTION,
        ] {
            if let Some(value) = self.options.get(key) {
                apply_parameter_timeout(
                    &mut parameters,
                    TimeoutOption::from_key(key).expect("constant is a timeout option"),
                    key,
                    value,
                )?;
            }
        }
        for key in [
            CLIENT_APPLICATION_OPTION,
            CLIENT_REMARK_OPTION,
            CLIENT_INFO_OPTION,
        ] {
            if let Some(value) = self.options.get(key) {
                ClientInfoOption::from_key(key)
                    .expect("constant is a client-info option")
                    .apply(&mut parameters, value)?;
            }
        }
        for (key, value) in &opts {
            if let OptionConnection::Other(name) = key
                && let Some(timeout) = TimeoutOption::from_key(name)
            {
                if timeout == TimeoutOption::Connect {
                    return Err(error(
                        format!("option '{name}' is only valid as a database option"),
                        Status::InvalidState,
                    ));
                }
                apply_parameter_timeout(&mut parameters, timeout, name, value)?;
            }
            if let OptionConnection::Other(name) = key
                && ClientInfoOption::from_key(name).is_some()
            {
                return Err(error(
                    format!("option '{name}' is only valid as a database option"),
                    Status::InvalidState,
                ));
            }
        }
        // Small results are cheapest when MonetDB returns them inline. Larger results remain
        // server-resident and switch to Xexportbin after this prefix.
        parameters
            .set_replysize(INLINE_REPLY_ROWS)
            .map_err(|value| map_display(value, Status::InvalidArguments))?;
        parameters
            .set_binary("on")
            .map_err(|value| map_display(value, Status::InvalidArguments))?;
        parameters
            .set_autocommit(true)
            .map_err(|value| map_display(value, Status::InvalidArguments))?;
        parameters
            .set_timezone(0)
            .map_err(|value| map_display(value, Status::InvalidArguments))?;
        parameters
            .set_client_prefix(concat!("adbc_driver_monetdb ", env!("CARGO_PKG_VERSION")))
            .map_err(|value| map_display(value, Status::InvalidArguments))?;
        let catalog = parameters
            .get_str(Parm::Database)
            .map_err(|value| map_display(value, Status::InvalidArguments))?
            .into_owned();
        let (connect_timeout, timeouts) = configured_timeouts(&parameters)?;
        let initialization_deadline = connect_timeout
            .map(|timeout| {
                initialization_started.checked_add(timeout).ok_or_else(|| {
                    error(
                        "connection timeout is too large to represent",
                        Status::InvalidArguments,
                    )
                })
            })
            .transpose()?;

        let connection_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            monetdb::Connection::new(parameters)
        }))
        .map_err(|_| {
            error(
                "MonetDB connection initialization panicked",
                Status::Internal,
            )
        })?;
        let connection =
            connection_result.map_err(|value| map_display(&value, connect_error_status(&value)))?;
        let server_info = connection.server_info().map_err(map_cursor_error)?;
        if server_info.endian != Endian::Lit {
            return Err(error(
                "adbc-monetdb supports only little-endian MonetDB servers",
                Status::NotImplemented,
            ));
        }
        if server_info.binary_level < 1 {
            return Err(error(
                "MonetDB server does not advertise BINARY=1; Dec2025 or newer is required",
                Status::NotImplemented,
            ));
        }
        let metadata_timeouts = initialization_timeouts(timeouts, initialization_deadline)?;
        let metadata_started = Instant::now();
        let version = connection
            .metadata_with_timeouts(metadata_timeouts)
            .map_err(map_cursor_error)?
            .version();
        let metadata_round_trip = metadata_started.elapsed();
        if version < MINIMUM_VERSION {
            return Err(error(
                format!(
                    "MonetDB {}.{}.{} is unsupported; Dec2025 (11.55) or newer is required",
                    version.0, version.1, version.2
                ),
                Status::NotImplemented,
            ));
        }

        let cancel = connection.cancel_handle();
        let inner = Arc::new(DriverConnection::new(connection));
        let schema_started = Instant::now();
        let current_schema = scalar_string(
            &inner,
            "SELECT current_schema AS \"__adbc_current_schema\"",
            initialization_timeouts(timeouts, initialization_deadline)?,
        )?;
        let measured_round_trip = metadata_round_trip.min(schema_started.elapsed());
        let mut options = Options::default();
        options.set(OptionConnection::CurrentCatalog, catalog.clone().into());
        options.set(OptionConnection::CurrentSchema, current_schema.into());
        options.set(OptionConnection::AutoCommit, "true".into());
        options.set(OptionConnection::ReadOnly, "false".into());
        store_runtime_timeouts(&mut options, timeouts);
        let mut result = MonetdbConnection {
            inner,
            prepared_cache: Arc::new(Mutex::new(PreparedCache::new(
                DEFAULT_PREPARED_CACHE_CAPACITY,
            ))),
            cancel,
            timeouts,
            read_batch_rows: 0,
            read_window_bytes: None,
            read_prefetch: true,
            write_batch_rows: None,
            write_window_bytes: None,
            ingest_insert_rows: 100,
            wire_compression: WireCompression::Auto,
            measured_round_trip,
            ingest_partial: IngestPartial::Block,
            ingest_atomicity: IngestAtomicity::Transaction,
            constrained_append: ConstrainedAppend::Auto,
            options,
            version,
            catalog,
        };
        for (key, value) in uri_tuning_options {
            result.set_option(key, value)?;
        }
        for key in [
            READ_WINDOW_BYTES_OPTION,
            WRITE_WINDOW_BYTES_OPTION,
            INGEST_INSERT_ROWS_OPTION,
            PREPARED_CACHE_CAPACITY_OPTION,
            WIRE_COMPRESSION_OPTION,
            CONSTRAINED_APPEND_OPTION,
        ] {
            if let Some(value) = self.options.get(key) {
                result.set_option(OptionConnection::Other(key.to_owned()), value.clone())?;
            }
        }
        for (key, value) in opts {
            result.set_option(key, value)?;
        }
        Ok(result)
    }
}

fn connect_error_status(value: &monetdb::ConnectError) -> Status {
    match value {
        monetdb::ConnectError::Rejected(_) => Status::Unauthenticated,
        monetdb::ConnectError::Timeout => Status::Timeout,
        monetdb::ConnectError::IO(_) => Status::IO,
        monetdb::ConnectError::SocketAttempts { tcp, .. } => connect_error_status(tcp),
        monetdb::ConnectError::Utf(_)
        | monetdb::ConnectError::InvalidChallenge(_)
        | monetdb::ConnectError::UnsupportedHashAlgo(_)
        | monetdb::ConnectError::TlsDowngrade
        | monetdb::ConnectError::UnexpectedResponse(_) => Status::InvalidData,
        monetdb::ConnectError::TooManyRedirects => Status::IO,
        monetdb::ConnectError::TlsNotSupported | monetdb::ConnectError::UnixDomain => {
            Status::NotImplemented
        }
        monetdb::ConnectError::Parm(_)
        | monetdb::ConnectError::TlsError(_)
        | monetdb::ConnectError::OnlySqlSupported => Status::InvalidArguments,
    }
}

fn decode_userinfo(value: &str) -> Result<String> {
    percent_decode_str(value)
        .decode_utf8()
        .map(|value| value.into_owned())
        .map_err(|value| map_display(value, Status::InvalidArguments))
}

fn parse_driver_uri(uri: &str) -> Result<url::Url> {
    let parsed =
        url::Url::parse(uri).map_err(|value| map_display(value, Status::InvalidArguments))?;
    for (key, _) in parsed.query_pairs() {
        if !URI_QUERY_KEYS.contains(&key.as_ref()) {
            return Err(error(
                format!("unknown MonetDB URI query parameter '{key}'"),
                Status::InvalidArguments,
            ));
        }
    }
    Ok(parsed)
}

fn uri_without_userinfo(uri: &str) -> Result<String> {
    let mut parsed = parse_driver_uri(uri)?;
    parsed
        .set_username("")
        .map_err(|()| error("URI user information is invalid", Status::InvalidArguments))?;
    parsed
        .set_password(None)
        .map_err(|()| error("URI user information is invalid", Status::InvalidArguments))?;
    let query = parsed
        .query_pairs()
        .filter(|(key, _)| key != "password")
        .map(|(key, value)| (key.into_owned(), value.into_owned()))
        .collect::<Vec<_>>();
    parsed.set_query(None);
    if !query.is_empty() {
        parsed.query_pairs_mut().extend_pairs(query);
    }
    Ok(parsed.into())
}

pub struct MonetdbConnection {
    inner: SharedConnection,
    prepared_cache: Arc<Mutex<PreparedCache>>,
    cancel: CancelHandle,
    timeouts: Timeouts,
    read_batch_rows: usize,
    read_window_bytes: Option<usize>,
    read_prefetch: bool,
    write_batch_rows: Option<usize>,
    write_window_bytes: Option<usize>,
    ingest_insert_rows: usize,
    wire_compression: WireCompression,
    measured_round_trip: Duration,
    ingest_partial: IngestPartial,
    ingest_atomicity: IngestAtomicity,
    constrained_append: ConstrainedAppend,
    options: Options,
    version: (u16, u16, u16),
    catalog: String,
}

type SharedPreparedCache = Arc<Mutex<PreparedCache>>;
type PreparedSlot = Arc<Mutex<Arc<PreparedEntry>>>;

struct PreparedEntry {
    id: u64,
    generation: u64,
    parameters: Schema,
    result: Schema,
    connection: Weak<DriverConnection>,
}

impl PreparedEntry {
    fn new(metadata: PreparedMetadata, connection: &SharedConnection) -> Self {
        Self {
            id: metadata.id,
            generation: connection.prepared_generation.load(Ordering::Acquire),
            parameters: metadata.parameters,
            result: metadata.result,
            connection: Arc::downgrade(connection),
        }
    }
}

impl Drop for PreparedEntry {
    fn drop(&mut self) {
        let Some(connection) = self.connection.upgrade() else {
            return;
        };
        if self.generation != connection.prepared_generation.load(Ordering::Acquire) {
            return;
        }
        match connection.inner.try_lock() {
            Ok(connection) => {
                connection.try_deallocate(self.id);
            }
            Err(std::sync::TryLockError::Poisoned(poisoned)) => {
                poisoned.into_inner().try_deallocate(self.id);
            }
            Err(std::sync::TryLockError::WouldBlock) => {
                connection
                    .pending_deallocations
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .push(self.id);
            }
        }
    }
}

struct PreparedCache {
    capacity: usize,
    entries: HashMap<String, CachedPrepared>,
    use_counter: u64,
}

struct CachedPrepared {
    entry: Arc<PreparedEntry>,
    last_used: u64,
}

impl PreparedCache {
    fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "prepared cache capacity must be positive");
        Self {
            capacity,
            entries: HashMap::new(),
            use_counter: 0,
        }
    }

    fn get(&mut self, query: &str) -> Option<Arc<PreparedEntry>> {
        let query = normalize_prepared_query(query);
        let last_used = self.next_use();
        let cached = self.entries.get_mut(query)?;
        cached.last_used = last_used;
        Some(Arc::clone(&cached.entry))
    }

    fn insert(&mut self, query: String, candidate: Arc<PreparedEntry>) -> Arc<PreparedEntry> {
        let query = normalize_prepared_query(&query).to_owned();
        if let Some(existing) = self.get(&query) {
            return existing;
        }
        let last_used = self.next_use();
        self.entries.insert(
            query,
            CachedPrepared {
                entry: Arc::clone(&candidate),
                last_used,
            },
        );
        while self.entries.len() > self.capacity {
            let oldest = self
                .entries
                .iter()
                .min_by_key(|(_, cached)| cached.last_used)
                .map(|(query, _)| query.clone())
                .expect("a nonempty cache has an LRU key");
            self.entries.remove(&oldest);
        }
        candidate
    }

    fn remove_if_id(&mut self, query: &str, id: u64) {
        let query = normalize_prepared_query(query);
        if self
            .entries
            .get(query)
            .is_some_and(|cached| cached.entry.id == id)
        {
            self.entries.remove(query);
        }
    }

    fn clear(&mut self) {
        self.entries.clear();
    }

    fn set_capacity(&mut self, capacity: usize) {
        assert!(capacity > 0, "prepared cache capacity must be positive");
        self.capacity = capacity;
        while self.entries.len() > capacity {
            let oldest = self
                .entries
                .iter()
                .min_by_key(|(_, cached)| cached.last_used)
                .map(|(query, _)| query.clone())
                .expect("a nonempty cache has an LRU key");
            self.entries.remove(&oldest);
        }
    }

    fn next_use(&mut self) -> u64 {
        self.use_counter = self.use_counter.saturating_add(1);
        self.use_counter
    }
}

impl Optionable for MonetdbConnection {
    type Option = OptionConnection;

    fn set_option(&mut self, key: Self::Option, value: OptionValue) -> Result<()> {
        if let OptionConnection::Other(name) = &key
            && let Some(timeout) = TimeoutOption::from_key(name)
        {
            set_runtime_timeout(&mut self.timeouts, timeout, name, &value)?;
            self.options.set(key, value);
            return Ok(());
        }
        if key.as_ref() == READ_BATCH_ROWS_OPTION {
            self.read_batch_rows = read_batch_rows_option(&value)?;
            self.options.set(key, value);
            return Ok(());
        }
        if key.as_ref() == READ_WINDOW_BYTES_OPTION {
            self.read_window_bytes = read_window_bytes_option(&value)?;
            self.options.set(key, value);
            return Ok(());
        }
        if key.as_ref() == READ_PREFETCH_OPTION {
            self.read_prefetch = option_bool(&value)?;
            self.options.set(key, self.read_prefetch.to_string().into());
            return Ok(());
        }
        if key.as_ref() == WRITE_BATCH_ROWS_OPTION {
            self.write_batch_rows = write_batch_rows_option(&value)?;
            self.options.set(key, value);
            return Ok(());
        }
        if key.as_ref() == WRITE_WINDOW_BYTES_OPTION {
            self.write_window_bytes = write_window_bytes_option(&value)?;
            self.options.set(key, value);
            return Ok(());
        }
        if key.as_ref() == INGEST_INSERT_ROWS_OPTION {
            self.ingest_insert_rows = nonnegative_usize_option(INGEST_INSERT_ROWS_OPTION, &value)?;
            self.options.set(key, value);
            return Ok(());
        }
        if key.as_ref() == PREPARED_CACHE_CAPACITY_OPTION {
            let capacity = nonnegative_usize_option(PREPARED_CACHE_CAPACITY_OPTION, &value)?;
            if capacity == 0 {
                return Err(error(
                    format!("option '{PREPARED_CACHE_CAPACITY_OPTION}' must be positive"),
                    Status::InvalidArguments,
                ));
            }
            self.prepared_cache
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .set_capacity(capacity);
            self.options.set(key, value);
            return Ok(());
        }
        if key.as_ref() == WIRE_COMPRESSION_OPTION {
            self.wire_compression = WireCompression::parse(&value)?;
            self.options.set(key, self.wire_compression.as_str().into());
            return Ok(());
        }
        if key.as_ref() == INGEST_PARTIAL_OPTION {
            self.ingest_partial = IngestPartial::parse(&value)?;
            self.options.set(key, value);
            return Ok(());
        }
        if key.as_ref() == INGEST_ATOMICITY_OPTION {
            self.ingest_atomicity = IngestAtomicity::parse(&value)?;
            self.options.set(key, value);
            return Ok(());
        }
        if key.as_ref() == CONSTRAINED_APPEND_OPTION {
            self.constrained_append = ConstrainedAppend::parse(&value)?;
            self.options
                .set(key, self.constrained_append.as_str().into());
            return Ok(());
        }
        match &key {
            OptionConnection::AutoCommit => {
                let enabled = option_bool(&value)?;
                if enabled && let Some(poison) = self.inner.commit_error() {
                    return Err(poison);
                }
                let connection = lock_connection(&self.inner)?;
                let current = connection
                    .server_info()
                    .map_err(map_cursor_error)?
                    .autocommit;
                if enabled && !current {
                    let mut cursor = connection.cursor();
                    cursor.set_timeouts(self.timeouts);
                    cursor.execute("COMMIT").map_err(map_cursor_error)?;
                }
                if enabled != current {
                    connection
                        .set_autocommit_with_timeouts(enabled, self.timeouts)
                        .map_err(map_cursor_error)?;
                }
                self.options.set(key, enabled.to_string().into());
                return Ok(());
            }
            OptionConnection::CurrentSchema => {
                let OptionValue::String(schema) = &value else {
                    return Err(error(
                        "current schema must be a string",
                        Status::InvalidArguments,
                    ));
                };
                execute_update(
                    &self.inner,
                    &format!("SET SCHEMA {}", quote_identifier(schema)?),
                    self.timeouts,
                )?;
                invalidate_prepared_cache(&self.inner, &self.prepared_cache);
            }
            OptionConnection::ReadOnly => {
                let enabled = option_bool(&value)?;
                if enabled {
                    return Err(not_implemented("read-only connections"));
                }
                self.options.set(key, "false".into());
                return Ok(());
            }
            OptionConnection::IsolationLevel | OptionConnection::CurrentCatalog => {
                return Err(not_implemented(key.as_ref()));
            }
            OptionConnection::Other(_) => return Err(not_implemented(key.as_ref())),
            _ => return Err(not_implemented(key.as_ref())),
        }
        self.options.set(key, value);
        Ok(())
    }

    fn get_option_string(&self, key: Self::Option) -> Result<String> {
        if key == OptionConnection::AutoCommit {
            return Ok(lock_connection(&self.inner)?
                .server_info()
                .map_err(map_cursor_error)?
                .autocommit
                .to_string());
        }
        if key == OptionConnection::CurrentSchema {
            return scalar_string(
                &self.inner,
                "SELECT current_schema AS \"__adbc_current_schema\"",
                self.timeouts,
            );
        }
        if key.as_ref() == READ_PREFETCH_OPTION {
            return Ok(self.read_prefetch.to_string());
        }
        if key.as_ref() == READ_BATCH_ROWS_OPTION {
            return Ok(self.read_batch_rows.to_string());
        }
        if key.as_ref() == READ_WINDOW_BYTES_OPTION {
            return Ok(self.read_window_bytes.unwrap_or(0).to_string());
        }
        if key.as_ref() == WRITE_BATCH_ROWS_OPTION {
            return Ok(self.write_batch_rows.unwrap_or(0).to_string());
        }
        if key.as_ref() == WRITE_WINDOW_BYTES_OPTION {
            return Ok(self.write_window_bytes.unwrap_or(0).to_string());
        }
        if key.as_ref() == INGEST_INSERT_ROWS_OPTION {
            return Ok(self.ingest_insert_rows.to_string());
        }
        if key.as_ref() == PREPARED_CACHE_CAPACITY_OPTION {
            return Ok(self
                .prepared_cache
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .capacity
                .to_string());
        }
        if key.as_ref() == WIRE_COMPRESSION_OPTION {
            return Ok(self.wire_compression.as_str().to_owned());
        }
        if key.as_ref() == INGEST_PARTIAL_OPTION {
            return Ok(self.ingest_partial.as_str().to_owned());
        }
        if key.as_ref() == INGEST_ATOMICITY_OPTION {
            return Ok(self.ingest_atomicity.as_str().to_owned());
        }
        if key.as_ref() == CONSTRAINED_APPEND_OPTION {
            return Ok(self.constrained_append.as_str().to_owned());
        }
        self.options.get_string(key)
    }

    fn get_option_bytes(&self, key: Self::Option) -> Result<Vec<u8>> {
        self.options.get_bytes(key)
    }

    fn get_option_int(&self, key: Self::Option) -> Result<i64> {
        if key.as_ref() == READ_BATCH_ROWS_OPTION {
            return i64::try_from(self.read_batch_rows)
                .map_err(|_| error("read_batch_rows exceeds i64", Status::Internal));
        }
        if key.as_ref() == READ_WINDOW_BYTES_OPTION {
            return self
                .read_window_bytes
                .map(i64::try_from)
                .transpose()
                .map_err(|_| error("read_window_bytes exceeds i64", Status::Internal))
                .map(|bytes| bytes.unwrap_or(0));
        }
        if key.as_ref() == WRITE_BATCH_ROWS_OPTION {
            return self
                .write_batch_rows
                .map(i64::try_from)
                .transpose()
                .map_err(|_| error("write_batch_rows exceeds i64", Status::Internal))
                .map(|rows| rows.unwrap_or(0));
        }
        if key.as_ref() == WRITE_WINDOW_BYTES_OPTION {
            return self
                .write_window_bytes
                .map(i64::try_from)
                .transpose()
                .map_err(|_| error("write_window_bytes exceeds i64", Status::Internal))
                .map(|bytes| bytes.unwrap_or(0));
        }
        if key.as_ref() == INGEST_INSERT_ROWS_OPTION {
            return i64::try_from(self.ingest_insert_rows)
                .map_err(|_| error("ingest_insert_rows exceeds i64", Status::Internal));
        }
        if key.as_ref() == PREPARED_CACHE_CAPACITY_OPTION {
            return i64::try_from(
                self.prepared_cache
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .capacity,
            )
            .map_err(|_| error("prepared_cache_capacity exceeds i64", Status::Internal));
        }
        self.options.get_int(key)
    }

    fn get_option_double(&self, key: Self::Option) -> Result<f64> {
        self.options.get_double(key)
    }
}

impl Connection for MonetdbConnection {
    type StatementType = MonetdbStatement;

    fn new_statement(&mut self) -> Result<Self::StatementType> {
        let mut options = Options::default();
        store_runtime_timeouts(&mut options, self.timeouts);
        Ok(MonetdbStatement {
            connection: Arc::clone(&self.inner),
            prepared_cache: Arc::clone(&self.prepared_cache),
            cancel: self.cancel.clone(),
            timeouts: self.timeouts,
            options,
            query: None,
            read_batch_rows: self.read_batch_rows,
            read_window_bytes: self.read_window_bytes,
            read_prefetch: self.read_prefetch,
            write_batch_rows: self.write_batch_rows,
            write_window_bytes: self.write_window_bytes,
            ingest_insert_rows: self.ingest_insert_rows,
            wire_compression: self.wire_compression,
            measured_round_trip: self.measured_round_trip,
            ingest_partial: self.ingest_partial,
            ingest_atomicity: self.ingest_atomicity,
            constrained_append: self.constrained_append,
            server_version: self.version,
            ingest_stats: None,
            read_stats: None,
            bound: None,
            prepared: false,
            prepared_entry: None,
            prepared_parameter_schema: None,
            prepared_result_schema: None,
            bind_by_name: false,
        })
    }

    fn cancel(&mut self) -> Result<()> {
        self.cancel.cancel().map_err(map_cursor_error)
    }

    fn get_info(
        &self,
        codes: Option<HashSet<InfoCode>>,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        Ok(Box::new(SingleBatchReader::new(info_batch(
            self.version,
            codes,
        )?)))
    }

    fn get_objects(
        &self,
        depth: ObjectDepth,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: Option<&str>,
        table_type: Option<Vec<&str>>,
        column_name: Option<&str>,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        let include_catalog = catalog
            .map(|pattern| like_pattern_matches(pattern, &self.catalog))
            .transpose()?
            .unwrap_or(true);
        let schemas = if include_catalog && depth != ObjectDepth::Catalogs {
            load_objects(
                &self.inner,
                depth,
                db_schema,
                table_name,
                table_type.as_deref(),
                column_name,
                self.timeouts,
            )?
        } else {
            Vec::new()
        };
        let batch = objects_batch(&self.catalog, include_catalog, depth, &schemas)?;
        Ok(Box::new(SingleBatchReader::new(batch)))
    }

    fn get_table_schema(
        &self,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: &str,
    ) -> Result<Schema> {
        if catalog.is_some_and(|catalog| catalog != self.catalog) {
            return Err(error(
                format!("catalog '{}' does not exist", catalog.unwrap_or_default()),
                Status::NotFound,
            ));
        }
        let current_schema = if db_schema.is_none() {
            Some(scalar_string(
                &self.inner,
                "SELECT current_schema AS \"__adbc_current_schema\"",
                self.timeouts,
            )?)
        } else {
            None
        };
        let schema_name = db_schema
            .or(current_schema.as_deref())
            .ok_or_else(|| error("current schema is not set", Status::InvalidState))?;
        table_schema(&self.inner, schema_name, table_name, self.timeouts)
    }

    fn get_table_types(&self) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        let values = StringArray::from(vec![
            "TABLE",
            "VIEW",
            "MERGE TABLE",
            "REMOTE TABLE",
            "REPLICA TABLE",
            "UNLOGGED TABLE",
            "SYSTEM TABLE",
            "SYSTEM VIEW",
            "GLOBAL TEMPORARY TABLE",
            "LOCAL TEMPORARY TABLE",
            "LOCAL TEMPORARY VIEW",
        ]);
        let batch = RecordBatch::try_new(GET_TABLE_TYPES_SCHEMA.clone(), vec![Arc::new(values)])?;
        Ok(Box::new(SingleBatchReader::new(batch)))
    }

    fn get_statistic_names(&self) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        Err(not_implemented("get_statistic_names"))
    }

    fn get_statistics(
        &self,
        _catalog: Option<&str>,
        _db_schema: Option<&str>,
        _table_name: Option<&str>,
        _approximate: bool,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        Err(not_implemented("get_statistics"))
    }

    fn commit(&mut self) -> Result<()> {
        if let Some(poison) = self.inner.commit_error() {
            return Err(poison);
        }
        execute_update(&self.inner, "COMMIT", self.timeouts).map(|_| ())
    }

    fn rollback(&mut self) -> Result<()> {
        execute_update(&self.inner, "ROLLBACK", self.timeouts).map(|_| ())
    }

    fn read_partition(
        &self,
        _partition: impl AsRef<[u8]>,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        Err(not_implemented("partitioned results"))
    }
}

fn option_bool(value: &OptionValue) -> Result<bool> {
    let OptionValue::String(value) = value else {
        return Err(error(
            "boolean option must be a string",
            Status::InvalidArguments,
        ));
    };
    parse_bool_option(value)
}

fn parse_bool_option(value: &str) -> Result<bool> {
    match value.to_ascii_lowercase().as_str() {
        "true" | "1" | "enabled" | "on" => Ok(true),
        "false" | "0" | "disabled" | "off" => Ok(false),
        _ => Err(error(
            format!("invalid boolean option value '{value}'"),
            Status::InvalidArguments,
        )),
    }
}

pub struct MonetdbStatement {
    connection: SharedConnection,
    prepared_cache: SharedPreparedCache,
    cancel: CancelHandle,
    timeouts: Timeouts,
    options: Options,
    query: Option<String>,
    read_batch_rows: usize,
    read_window_bytes: Option<usize>,
    read_prefetch: bool,
    write_batch_rows: Option<usize>,
    write_window_bytes: Option<usize>,
    ingest_insert_rows: usize,
    wire_compression: WireCompression,
    measured_round_trip: Duration,
    ingest_partial: IngestPartial,
    ingest_atomicity: IngestAtomicity,
    constrained_append: ConstrainedAppend,
    server_version: (u16, u16, u16),
    ingest_stats: Option<String>,
    read_stats: Option<SharedReadStats>,
    bound: Option<Box<dyn RecordBatchReader + Send>>,
    prepared: bool,
    prepared_entry: Option<PreparedSlot>,
    prepared_parameter_schema: Option<Schema>,
    prepared_result_schema: Option<Schema>,
    bind_by_name: bool,
}

impl Optionable for MonetdbStatement {
    type Option = OptionStatement;

    fn set_option(&mut self, key: Self::Option, value: OptionValue) -> Result<()> {
        if let Some(timeout) = TimeoutOption::from_key(key.as_ref()) {
            set_runtime_timeout(&mut self.timeouts, timeout, key.as_ref(), &value)?;
            self.options.set(key, value);
            return Ok(());
        }
        if key.as_ref() == READ_BATCH_ROWS_OPTION {
            self.read_batch_rows = read_batch_rows_option(&value)?;
            self.options.set(key, value);
            return Ok(());
        }
        if key.as_ref() == READ_WINDOW_BYTES_OPTION {
            self.read_window_bytes = read_window_bytes_option(&value)?;
            self.options.set(key, value);
            return Ok(());
        }
        if key.as_ref() == READ_PREFETCH_OPTION {
            self.read_prefetch = option_bool(&value)?;
            self.options.set(key, self.read_prefetch.to_string().into());
            return Ok(());
        }
        if key.as_ref() == WRITE_BATCH_ROWS_OPTION {
            self.write_batch_rows = write_batch_rows_option(&value)?;
            self.options.set(key, value);
            return Ok(());
        }
        if key.as_ref() == WRITE_WINDOW_BYTES_OPTION {
            self.write_window_bytes = write_window_bytes_option(&value)?;
            self.options.set(key, value);
            return Ok(());
        }
        if key.as_ref() == INGEST_INSERT_ROWS_OPTION {
            self.ingest_insert_rows = nonnegative_usize_option(INGEST_INSERT_ROWS_OPTION, &value)?;
            self.options.set(key, value);
            return Ok(());
        }
        if key.as_ref() == WIRE_COMPRESSION_OPTION {
            self.wire_compression = WireCompression::parse(&value)?;
            self.options.set(key, self.wire_compression.as_str().into());
            return Ok(());
        }
        if key.as_ref() == INGEST_PARTIAL_OPTION {
            self.ingest_partial = IngestPartial::parse(&value)?;
            self.options.set(key, value);
            return Ok(());
        }
        if key.as_ref() == INGEST_ATOMICITY_OPTION {
            self.ingest_atomicity = IngestAtomicity::parse(&value)?;
            self.options.set(key, value);
            return Ok(());
        }
        if key.as_ref() == CONSTRAINED_APPEND_OPTION {
            self.constrained_append = ConstrainedAppend::parse(&value)?;
            self.options
                .set(key, self.constrained_append.as_str().into());
            return Ok(());
        }
        if matches!(key.as_ref(), INGEST_STATS_OPTION | READ_STATS_OPTION) {
            return Err(error(
                format!("option '{}' is read-only", key.as_ref()),
                Status::InvalidArguments,
            ));
        }
        if matches!(
            key.as_ref(),
            BIND_BY_NAME_OPTION | DRIVER_MANAGER_BIND_BY_NAME_OPTION
        ) {
            self.bind_by_name = option_bool(&value)?;
            self.options.set(key, value);
            return Ok(());
        }
        match &key {
            OptionStatement::IngestMode => {
                let OptionValue::String(mode) = &value else {
                    return Err(error(
                        "ingest mode must be a string",
                        Status::InvalidArguments,
                    ));
                };
                if !matches!(
                    mode.as_str(),
                    "adbc.ingest.mode.create"
                        | "adbc.ingest.mode.append"
                        | "adbc.ingest.mode.replace"
                        | "adbc.ingest.mode.create_append"
                ) {
                    return Err(error(
                        format!("unknown ingest mode '{mode}'"),
                        Status::InvalidArguments,
                    ));
                }
            }
            OptionStatement::TargetTable | OptionStatement::TargetDbSchema => {
                let OptionValue::String(name) = &value else {
                    return Err(error(
                        format!("{} must be a string", key.as_ref()),
                        Status::InvalidArguments,
                    ));
                };
                if name.is_empty() {
                    return Err(error(
                        format!("{} must not be empty", key.as_ref()),
                        Status::InvalidArguments,
                    ));
                }
            }
            OptionStatement::Temporary => {
                option_bool(&value)?;
            }
            OptionStatement::TargetCatalog => {
                return Err(not_implemented("ingest target catalogs"));
            }
            OptionStatement::Incremental => {
                return Err(not_implemented("incremental execution"));
            }
            OptionStatement::Progress | OptionStatement::MaxProgress => {
                return Err(error(
                    format!("option '{}' is read-only", key.as_ref()),
                    Status::InvalidArguments,
                ));
            }
            _ => return Err(not_implemented(key.as_ref())),
        }
        self.options.set(key, value);
        Ok(())
    }

    fn get_option_string(&self, key: Self::Option) -> Result<String> {
        if matches!(
            key.as_ref(),
            BIND_BY_NAME_OPTION | DRIVER_MANAGER_BIND_BY_NAME_OPTION
        ) {
            return Ok(self.bind_by_name.to_string());
        }
        if key.as_ref() == READ_PREFETCH_OPTION {
            return Ok(self.read_prefetch.to_string());
        }
        if key.as_ref() == READ_BATCH_ROWS_OPTION {
            return Ok(self.read_batch_rows.to_string());
        }
        if key.as_ref() == READ_WINDOW_BYTES_OPTION {
            return Ok(self.read_window_bytes.unwrap_or(0).to_string());
        }
        if key.as_ref() == WRITE_BATCH_ROWS_OPTION {
            return Ok(self.write_batch_rows.unwrap_or(0).to_string());
        }
        if key.as_ref() == WRITE_WINDOW_BYTES_OPTION {
            return Ok(self.write_window_bytes.unwrap_or(0).to_string());
        }
        if key.as_ref() == INGEST_INSERT_ROWS_OPTION {
            return Ok(self.ingest_insert_rows.to_string());
        }
        if key.as_ref() == WIRE_COMPRESSION_OPTION {
            return Ok(self.wire_compression.as_str().to_owned());
        }
        if key.as_ref() == INGEST_PARTIAL_OPTION {
            return Ok(self.ingest_partial.as_str().to_owned());
        }
        if key.as_ref() == INGEST_ATOMICITY_OPTION {
            return Ok(self.ingest_atomicity.as_str().to_owned());
        }
        if key.as_ref() == CONSTRAINED_APPEND_OPTION {
            return Ok(self.constrained_append.as_str().to_owned());
        }
        if key.as_ref() == INGEST_STATS_OPTION {
            return self
                .ingest_stats
                .clone()
                .ok_or_else(|| unknown_option(INGEST_STATS_OPTION));
        }
        if key.as_ref() == READ_STATS_OPTION {
            return self
                .read_stats
                .as_ref()
                .and_then(|stats| {
                    stats
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .as_ref()
                        .map(ReadStats::to_json)
                })
                .ok_or_else(|| unknown_option(READ_STATS_OPTION));
        }
        if key == OptionStatement::IngestMode {
            return Ok(self
                .options
                .optional_string(OptionStatement::IngestMode)
                .unwrap_or("adbc.ingest.mode.create")
                .to_owned());
        }
        if key == OptionStatement::Temporary {
            return Ok(self
                .options
                .optional_string(OptionStatement::Temporary)
                .unwrap_or("false")
                .to_owned());
        }
        self.options.get_string(key)
    }

    fn get_option_bytes(&self, key: Self::Option) -> Result<Vec<u8>> {
        self.options.get_bytes(key)
    }

    fn get_option_int(&self, key: Self::Option) -> Result<i64> {
        if key.as_ref() == READ_BATCH_ROWS_OPTION {
            return i64::try_from(self.read_batch_rows)
                .map_err(|_| error("read_batch_rows exceeds i64", Status::Internal));
        }
        if key.as_ref() == READ_WINDOW_BYTES_OPTION {
            return self
                .read_window_bytes
                .map(i64::try_from)
                .transpose()
                .map_err(|_| error("read_window_bytes exceeds i64", Status::Internal))
                .map(|bytes| bytes.unwrap_or(0));
        }
        if key.as_ref() == WRITE_BATCH_ROWS_OPTION {
            return self
                .write_batch_rows
                .map(i64::try_from)
                .transpose()
                .map_err(|_| error("write_batch_rows exceeds i64", Status::Internal))
                .map(|rows| rows.unwrap_or(0));
        }
        if key.as_ref() == WRITE_WINDOW_BYTES_OPTION {
            return self
                .write_window_bytes
                .map(i64::try_from)
                .transpose()
                .map_err(|_| error("write_window_bytes exceeds i64", Status::Internal))
                .map(|bytes| bytes.unwrap_or(0));
        }
        if key.as_ref() == INGEST_INSERT_ROWS_OPTION {
            return i64::try_from(self.ingest_insert_rows)
                .map_err(|_| error("ingest_insert_rows exceeds i64", Status::Internal));
        }
        self.options.get_int(key)
    }

    fn get_option_double(&self, key: Self::Option) -> Result<f64> {
        if matches!(
            key,
            OptionStatement::Progress | OptionStatement::MaxProgress
        ) {
            return Err(not_implemented("statement progress reporting"));
        }
        self.options.get_double(key)
    }
}

impl Statement for MonetdbStatement {
    fn bind(&mut self, batch: RecordBatch) -> Result<()> {
        validate_record_batch(&batch)?;
        self.bound = Some(Box::new(SingleBatchReader::new(batch)));
        Ok(())
    }

    fn bind_stream(&mut self, reader: Box<dyn RecordBatchReader + Send>) -> Result<()> {
        self.bound = Some(reader);
        Ok(())
    }

    fn execute(&mut self) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        self.execute_with_rows_affected()
            .map(|result| result.reader)
    }

    fn execute_with_rows_affected(&mut self) -> Result<StatementResult> {
        self.read_stats = None;
        if self.bound.is_some() {
            if self
                .options
                .optional_string(OptionStatement::TargetTable)
                .is_some()
            {
                return Err(error(
                    "bulk ingestion requires ExecuteUpdate",
                    Status::InvalidState,
                ));
            }
            if !self.prepared {
                self.prepare()?;
            }
            let mut queries = self.take_bound_queries()?;
            if queries.is_empty()? {
                let schema = match self.prepared_result_schema.clone() {
                    Some(schema) => schema,
                    None => {
                        let schema = self.execute_schema()?;
                        self.prepared_result_schema = Some(schema.clone());
                        schema
                    }
                };
                let schema = Arc::new(schema);
                let rows_affected = schema.fields().is_empty().then_some(0);
                return Ok(StatementResult {
                    reader: Box::new(EmptyReader::new(schema)),
                    rows_affected,
                });
            }
            if self
                .prepared_result_schema
                .as_ref()
                .is_some_and(|schema| schema.fields().is_empty())
            {
                let rows_affected =
                    execute_updates_atomic(&self.connection, &mut queries, self.timeouts)?;
                return Ok(StatementResult {
                    reader: Box::new(EmptyReader::default()),
                    rows_affected,
                });
            }
            if self.prepared_result_schema.is_none() {
                let schema = self.execute_schema()?;
                if schema.fields().is_empty() {
                    let rows_affected =
                        execute_updates_atomic(&self.connection, &mut queries, self.timeouts)?;
                    self.prepared_result_schema = Some(schema);
                    return Ok(StatementResult {
                        reader: Box::new(EmptyReader::default()),
                        rows_affected,
                    });
                }
                self.prepared_result_schema = Some(schema);
            }
            let read_stats = Arc::new(Mutex::new(None));
            self.read_stats = Some(Arc::clone(&read_stats));
            return Ok(StatementResult {
                reader: parameter_query_reader(
                    &self.connection,
                    queries,
                    ReadExecutionOptions {
                        batch_rows: self.read_batch_rows,
                        window_bytes: self.read_window_bytes,
                        prefetch: self.read_prefetch,
                        measured_round_trip: self.measured_round_trip,
                        stats: Some(read_stats),
                    },
                    self.timeouts,
                )?,
                rows_affected: None,
            });
        }
        let query = self
            .query
            .as_deref()
            .ok_or_else(|| error("SQL query is not set", Status::InvalidState))?;
        validate_unbound_query(query)?;
        let invalidates_cache = query_invalidates_prepared_cache(query)?;
        if invalidates_cache {
            invalidate_prepared_cache(&self.connection, &self.prepared_cache);
        }
        let read_stats = Arc::new(Mutex::new(None));
        let result = query_result_with_timeouts(
            &self.connection,
            query,
            ReadExecutionOptions {
                batch_rows: self.read_batch_rows,
                window_bytes: self.read_window_bytes,
                prefetch: self.read_prefetch,
                measured_round_trip: self.measured_round_trip,
                stats: Some(Arc::clone(&read_stats)),
            },
            self.timeouts,
        );
        self.read_stats = Some(read_stats);
        if invalidates_cache && result.is_ok() {
            invalidate_prepared_cache(&self.connection, &self.prepared_cache);
        }
        result
    }

    fn execute_update(&mut self) -> Result<Option<i64>> {
        if self.bound.is_some()
            && self
                .options
                .optional_string(OptionStatement::TargetTable)
                .is_some()
        {
            return self.ingest();
        }
        if self.bound.is_some() {
            let mut queries = self.take_bound_queries()?;
            return execute_updates_atomic(&self.connection, &mut queries, self.timeouts);
        }
        let query = self
            .query
            .as_deref()
            .ok_or_else(|| error("SQL query is not set", Status::InvalidState))?;
        let invalidates_cache = query_invalidates_prepared_cache(query)?;
        if invalidates_cache {
            invalidate_prepared_cache(&self.connection, &self.prepared_cache);
        }
        let result = execute_update_script(&self.connection, query, self.timeouts);
        if invalidates_cache && result.is_ok() {
            invalidate_prepared_cache(&self.connection, &self.prepared_cache);
        }
        result
    }

    fn execute_schema(&mut self) -> Result<Schema> {
        if let Some(slot) = self.prepared_entry.clone() {
            let query = self
                .query
                .as_deref()
                .ok_or_else(|| error("SQL query is not set", Status::InvalidState))?;
            let entry = prepare_cached(
                &self.connection,
                &self.prepared_cache,
                query,
                parameter_layout(query)?.count(),
                self.timeouts,
            )?;
            *slot
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Arc::clone(&entry);
            self.prepared_parameter_schema = Some(entry.parameters.clone());
            self.prepared_result_schema = Some(entry.result.clone());
            return Ok(entry.result.clone());
        }
        if let Some(schema) = &self.prepared_result_schema {
            return Ok(schema.clone());
        }
        let query = self
            .query
            .as_deref()
            .ok_or_else(|| error("SQL query is not set", Status::InvalidState))?;
        let layout = parameter_layout(query)?;
        let metadata = if layout.is_named() {
            let query = render_null_parameters(query)?;
            prepare_query_allowing_any(&self.connection, &query, 0, self.timeouts)?
        } else {
            match prepare_query(&self.connection, query, layout.count(), self.timeouts) {
                Ok(metadata) => metadata,
                Err(value) if requires_literal_parameter_fallback(&value) => {
                    let query = render_null_parameters(query)?;
                    prepare_query_allowing_any(&self.connection, &query, 0, self.timeouts)?
                }
                Err(value) => return Err(value),
            }
        };
        let connection = lock_connection(&self.connection)?;
        connection.try_deallocate(metadata.id);
        Ok(metadata.result)
    }

    fn execute_partitions(&mut self) -> Result<PartitionedResult> {
        Err(not_implemented("partitioned results"))
    }

    fn get_parameter_schema(&self) -> Result<Schema> {
        if let Some(slot) = &self.prepared_entry {
            let query = self
                .query
                .as_deref()
                .ok_or_else(|| error("SQL query is not set", Status::InvalidState))?;
            let entry = prepare_cached(
                &self.connection,
                &self.prepared_cache,
                query,
                parameter_layout(query)?.count(),
                self.timeouts,
            )?;
            *slot
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Arc::clone(&entry);
            return Ok(entry.parameters.clone());
        }
        if let Some(schema) = &self.prepared_parameter_schema {
            return Ok(schema.clone());
        }
        if let Some(bound) = &self.bound {
            return Ok(bound.schema().as_ref().clone());
        }
        let query = self
            .query
            .as_deref()
            .ok_or_else(|| error("SQL query is not set", Status::InvalidState))?;
        let fields = parameter_layout(query)?
            .field_names()
            .into_iter()
            .map(|name| Field::new(name, DataType::Null, true))
            .collect::<Vec<_>>();
        Ok(Schema::new(fields))
    }

    fn prepare(&mut self) -> Result<()> {
        if self.prepared {
            return Ok(());
        }
        let query = self
            .query
            .as_deref()
            .ok_or_else(|| error("SQL query is not set", Status::InvalidState))?;
        let layout = parameter_layout(query)?;
        let parameter_count = layout.count();
        if parameter_count == 0 {
            self.prepared_parameter_schema = Some(Schema::empty());
            self.prepared = true;
            return Ok(());
        }
        if let ParameterLayout::Named(names) = layout {
            self.prepared_parameter_schema = Some(Schema::new(
                names
                    .into_iter()
                    .map(|name| Field::new(name, DataType::Null, true))
                    .collect::<Vec<_>>(),
            ));
            self.prepared_result_schema = Some(self.execute_schema()?);
            self.prepared = true;
            return Ok(());
        }
        match prepare_cached(
            &self.connection,
            &self.prepared_cache,
            query,
            parameter_count,
            self.timeouts,
        ) {
            Ok(entry) => {
                self.prepared_parameter_schema = Some(entry.parameters.clone());
                self.prepared_result_schema = Some(entry.result.clone());
                self.prepared_entry = Some(Arc::new(Mutex::new(entry)));
            }
            Err(value) if requires_literal_parameter_fallback(&value) => {
                self.prepared_parameter_schema = Some(Schema::new(
                    (0..parameter_count)
                        .map(|index| Field::new(index.to_string(), DataType::Null, true))
                        .collect::<Vec<_>>(),
                ));
            }
            Err(value) => return Err(value),
        }
        self.prepared = true;
        Ok(())
    }

    fn set_sql_query(&mut self, query: impl AsRef<str>) -> Result<()> {
        self.clear_prepared();
        for key in [
            OptionStatement::IngestMode,
            OptionStatement::TargetCatalog,
            OptionStatement::TargetDbSchema,
            OptionStatement::TargetTable,
            OptionStatement::Temporary,
        ] {
            self.options.remove(key);
        }
        self.query = Some(query.as_ref().to_owned());
        Ok(())
    }

    fn set_substrait_plan(&mut self, _plan: impl AsRef<[u8]>) -> Result<()> {
        Err(not_implemented("Substrait plans"))
    }

    fn cancel(&mut self) -> Result<()> {
        self.cancel.cancel().map_err(map_cursor_error)
    }
}

fn requires_literal_parameter_fallback(value: &Error) -> bool {
    let parameter_sqlstate = value.sqlstate.map(|value| value as u8) == *b"42000";
    (parameter_sqlstate
        && (value
            .message
            .contains("Could not determine type for argument number")
            || value
                .message
                .contains("parameters not allowed as arguments to")))
        || value.message == "unknown MonetDB prepared type 'any'"
}

impl MonetdbStatement {
    fn clear_prepared(&mut self) {
        self.prepared_entry = None;
        self.prepared_parameter_schema = None;
        self.prepared_result_schema = None;
        self.prepared = false;
    }

    fn take_bound_queries(&mut self) -> Result<BoundQueryStream> {
        let query = self
            .query
            .as_deref()
            .ok_or_else(|| error("SQL query is not set", Status::InvalidState))?
            .to_owned();
        let reader = self
            .bound
            .take()
            .ok_or_else(|| error("no Arrow parameters are bound", Status::InvalidState))?;
        let schema = reader.schema();
        let template = QueryTemplate::parse(&query)?;
        let layout = template.layout();
        if layout.is_named() != self.bind_by_name {
            return Err(error(
                if layout.is_named() {
                    "named parameters require adbc.monetdb.bind_by_name"
                } else {
                    "positional parameters cannot be bound by name"
                },
                Status::InvalidArguments,
            ));
        }
        match &layout {
            ParameterLayout::Positional(expected) if schema.fields().len() != *expected => {
                return Err(error(
                    format!(
                        "query has {expected} positional parameters but the bound stream has {} columns",
                        schema.fields().len()
                    ),
                    Status::InvalidArguments,
                ));
            }
            ParameterLayout::Named(names) => {
                let fields = schema.fields();
                let field_names = fields
                    .iter()
                    .map(|field| field.name().as_str())
                    .collect::<HashSet<_>>();
                if fields.len() != names.len()
                    || field_names.len() != fields.len()
                    || names
                        .iter()
                        .any(|name| !field_names.contains(name.as_str()))
                {
                    return Err(error(
                        "bound parameter names do not match the query",
                        Status::InvalidArguments,
                    ));
                }
            }
            ParameterLayout::Positional(_) => {}
        }
        let prepared = self.prepared_entry.as_ref().map(|entry| PreparedPlan {
            query: Arc::from(normalize_prepared_query(&query)),
            parameter_count: layout.count(),
            entry: Arc::clone(entry),
            cache: Arc::clone(&self.prepared_cache),
        });
        Ok(BoundQueryStream {
            reader,
            schema,
            template,
            prepared,
            bind_by_name: self.bind_by_name,
            batch: None,
            next_row: 0,
            pending: None,
            finished: false,
        })
    }

    fn ingest(&mut self) -> Result<Option<i64>> {
        self.ingest_stats = None;
        let reader = self
            .bound
            .take()
            .ok_or_else(|| error("no Arrow data is bound", Status::InvalidState))?;
        let table = self
            .options
            .optional_string(OptionStatement::TargetTable)
            .ok_or_else(|| error("ingest target table is required", Status::InvalidState))?;
        let mode = self
            .options
            .optional_string(OptionStatement::IngestMode)
            .unwrap_or("adbc.ingest.mode.create");
        if mode != "adbc.ingest.mode.append" {
            invalidate_prepared_cache(&self.connection, &self.prepared_cache);
        }
        let temporary = self
            .options
            .get(OptionStatement::Temporary)
            .map(option_bool)
            .transpose()?
            .unwrap_or(false);
        if self
            .options
            .optional_string(OptionStatement::TargetCatalog)
            .is_some()
        {
            return Err(not_implemented("ingest target catalogs"));
        }
        let schema_name = self
            .options
            .optional_string(OptionStatement::TargetDbSchema);
        if temporary && schema_name.is_some() {
            return Err(error(
                "temporary ingestion cannot specify a schema",
                Status::InvalidArguments,
            ));
        }
        let target = qualified_name(schema_name, table)?;
        let operation_target = if temporary {
            qualified_name(Some("tmp"), table)?
        } else {
            target.clone()
        };
        let schema = reader.schema();
        if schema.fields().is_empty() {
            return Err(error(
                "cannot ingest a zero-column stream",
                Status::InvalidArguments,
            ));
        }
        for field in schema.fields() {
            let data_type = monetdb_arrow::monet_type_for_field(field)
                .map_err(|value| map_display(value, Status::NotImplemented))?;
            if data_type == MonetType::Oid {
                return Err(error(
                    "COPY BINARY ingestion for OID columns is not supported by MonetDB",
                    Status::NotImplemented,
                ));
            }
            if data_type == MonetType::Inet {
                return Err(error(
                    "COPY BINARY ingestion for INET columns is not supported by MonetDB",
                    Status::NotImplemented,
                ));
            }
        }

        let columns = ingest_column_definitions(&schema, true)?;
        let create = format!(
            "CREATE {}TABLE {} ({}){}",
            if temporary { "LOCAL TEMPORARY " } else { "" },
            target,
            columns.join(", "),
            if temporary {
                " ON COMMIT PRESERVE ROWS"
            } else {
                ""
            }
        );
        let insert_rows = if self.write_batch_rows.is_none() {
            adaptive_insert_rows(self.ingest_insert_rows, self.measured_round_trip)
        } else {
            0
        };
        let (reader, insert_candidate) = route_ingest_reader(reader, &schema, insert_rows)?;

        let connection = lock_connection(&self.connection)?;
        let temporary_state = if temporary
            && matches!(
                mode,
                "adbc.ingest.mode.append" | "adbc.ingest.mode.create_append"
            ) {
            Some(transaction_scoped_temporary_table_state(
                &connection,
                Some(table),
                self.timeouts,
            )?)
        } else {
            None
        };
        if temporary_state.is_some_and(|state| state.target == Some(true))
            && connection
                .server_info()
                .map_err(map_cursor_error)?
                .autocommit
        {
            return Err(error(
                "cannot append to a temporary table declared ON COMMIT DELETE ROWS while autocommit is enabled; disable autocommit or declare ON COMMIT PRESERVE ROWS",
                Status::InvalidState,
            ));
        }
        let append_to_existing = mode == "adbc.ingest.mode.append"
            || (mode == "adbc.ingest.mode.create_append"
                && table_exists(
                    &connection,
                    if temporary { Some("tmp") } else { schema_name },
                    table,
                    self.timeouts,
                )?);
        let constrained_append_candidate = insert_candidate.is_none()
            && append_to_existing
            && self.constrained_append == ConstrainedAppend::Auto
            && (!temporary || self.server_version >= LOCAL_TEMP_STAGING_MINIMUM_VERSION);
        let constraint_state = if constrained_append_candidate {
            let mut metadata_cursor = connection.cursor();
            metadata_cursor.set_timeouts(self.timeouts);
            Some(table_constraint_state(
                &mut metadata_cursor,
                if temporary { Some("tmp") } else { schema_name },
                table,
            )?)
        } else {
            None
        };
        let staged_constrained_append = constraint_state.is_some_and(|state| state.constrained);
        let caller_scope = if insert_candidate.is_some()
            || staged_constrained_append
            || constraint_state.is_some_and(|state| !state.exists)
        {
            CallerTransactionScope::Savepoint
        } else if append_to_existing && self.ingest_atomicity == IngestAtomicity::Transaction {
            CallerTransactionScope::Direct
        } else {
            CallerTransactionScope::Savepoint
        };
        let (mut cursor, atomic_scope) = begin_atomic(
            &connection,
            "ingest",
            caller_scope,
            temporary_state.map(|state| state.any),
            self.timeouts,
        )?;
        if staged_constrained_append {
            invalidate_prepared_cache(&self.connection, &self.prepared_cache);
        }
        let caller_transaction = matches!(&atomic_scope, AtomicScope::CallerTransaction);
        let scope_name = atomic_scope.name();
        let window_budget = self
            .write_window_bytes
            .unwrap_or_else(automatic_write_window_bytes);
        let physical_window_budget = self.write_window_bytes.unwrap_or_else(|| {
            adaptive_physical_window_bytes(
                window_budget,
                self.measured_round_trip,
                schema.fields().len(),
            )
        });
        let incompressible_window_budget = self.write_window_bytes.unwrap_or_else(|| {
            adaptive_incompressible_window_bytes(physical_window_budget, self.measured_round_trip)
        });
        let mut stats = IngestStats::new(
            window_budget,
            physical_window_budget,
            incompressible_window_budget,
            insert_rows,
            self.measured_round_trip,
            scope_name,
        );
        if let Some(candidate) = insert_candidate {
            let target = IngestTarget {
                mode,
                create: &create,
                operation_target: &operation_target,
                schema_name,
                table,
                temporary,
            };
            let setup = execute_ingest_target_mode(&mut cursor, &target);
            let mut prepared_cache_hit = false;
            let inserted = setup.and_then(|()| {
                let stream_names = schema
                    .fields()
                    .iter()
                    .map(|field| field.name().clone())
                    .collect::<Vec<_>>();
                type PreparedAttempt = (Option<(Arc<PreparedEntry>, bool)>, Option<Error>);
                let prepare = |names: &[String]| -> Result<PreparedAttempt> {
                    let mut prepared = None;
                    let mut prepare_error = None;
                    for insert_query in insert_parameter_queries(&operation_target, names)? {
                        match prepare_cached_with_status_locked(
                            &self.connection,
                            &connection,
                            &self.prepared_cache,
                            &insert_query,
                            names.len(),
                            self.timeouts,
                        ) {
                            Ok(value) => {
                                prepared = Some(value);
                                break;
                            }
                            Err(value) => prepare_error = Some(value),
                        }
                    }
                    Ok((prepared, prepare_error))
                };
                let (mut prepared, prepare_error) = prepare(&stream_names)?;
                if prepared.is_none()
                    && matches!(
                        target.mode,
                        "adbc.ingest.mode.append" | "adbc.ingest.mode.create_append"
                    )
                {
                    let mismatch_status = append_mismatch_status(target.mode);
                    let destination = append_table_columns(
                        &mut cursor,
                        if target.temporary {
                            Some("tmp")
                        } else {
                            target.schema_name
                        },
                        target.table,
                        mismatch_status,
                    )?;
                    let aligned = align_append_schema(&schema, &destination, mismatch_status)?;
                    if aligned != stream_names {
                        prepared = prepare(&aligned)?.0;
                    }
                }
                let (entry, cache_hit) = match prepared {
                    Some(value) => value,
                    None => {
                        return Err(prepare_error.unwrap_or_else(|| {
                            error("no INSERT form was available for ingest", Status::Internal)
                        }));
                    }
                };
                if matches!(
                    mode,
                    "adbc.ingest.mode.append" | "adbc.ingest.mode.create_append"
                ) {
                    validate_prepared_insert_schema(
                        &schema,
                        &entry.parameters,
                        append_mismatch_status(mode),
                    )?;
                }
                prepared_cache_hit = cache_hit;
                let queries = candidate
                    .arguments
                    .iter()
                    .map(|arguments| BoundQuery {
                        sql: format!("EXECUTE {}({arguments})", entry.id),
                        prepared: None,
                    })
                    .collect::<Vec<_>>();
                let mut total = 0i64;
                let mut has_count = false;
                execute_update_batch(&mut cursor, &queries, &mut total, &mut has_count)?;
                has_count
                    .then_some(total)
                    .ok_or_else(|| {
                        error(
                            "MonetDB did not report the INSERT row count",
                            Status::InvalidData,
                        )
                    })
                    .and_then(|rows| {
                        let expected = i64::try_from(candidate.batch.num_rows()).map_err(|_| {
                            error("insert row count exceeds i64", Status::Internal)
                        })?;
                        if rows != expected {
                            return Err(error(
                                format!(
                                    "MonetDB inserted {rows} rows from a batch containing {expected} rows"
                                ),
                                Status::InvalidData,
                            ));
                        }
                        Ok(rows)
                    })
            });
            let result = finish_atomic(
                &connection,
                &mut cursor,
                atomic_scope,
                inserted,
                self.timeouts,
            )
            .map(Some);
            stats.input_batches = 1;
            stats.encoded_bytes = candidate.estimated_bytes;
            stats.prepared_cache_hits = usize::from(prepared_cache_hit);
            stats.path = "insert";
            self.ingest_stats = Some(stats.to_json());
            drop(cursor);
            drop(connection);
            if mode != "adbc.ingest.mode.append" && result.is_ok() {
                invalidate_prepared_cache(&self.connection, &self.prepared_cache);
            }
            return result;
        }
        let result = (|| {
            let target = IngestTarget {
                mode,
                create: &create,
                operation_target: &operation_target,
                schema_name,
                table,
                temporary,
            };
            let destination = prepare_ingest_target(&mut cursor, &target, &schema)?;
            let stage = if staged_constrained_append {
                let destination = destination.as_deref().ok_or_else(|| {
                    error(
                        "constrained append staging requires an existing destination",
                        Status::Internal,
                    )
                })?;
                let local = self.server_version >= LOCAL_TEMP_STAGING_MINIMUM_VERSION;
                let persistent_schema = if local {
                    None
                } else {
                    Some(match schema_name {
                        Some(value) => value.to_owned(),
                        None => current_schema_name(&mut cursor)?,
                    })
                };
                let stage = constrained_append_stage(
                    &schema,
                    destination,
                    &operation_target,
                    local,
                    persistent_schema.as_deref(),
                )?;
                cursor.execute(&stage.create).map_err(map_cursor_error)?;
                stats.path = "staged_copy";
                Some(stage)
            } else {
                None
            };
            let copy_target = stage.as_ref().map_or(operation_target.as_str(), |stage| {
                stage.qualified_name.as_str()
            });

            let files = (0..schema.fields().len())
                .map(|index| format!("'c{index}'"))
                .collect::<Vec<_>>()
                .join(", ");
            // The staging table is created from the stream's own schema, so only a direct append to
            // an existing table needs the destination column list that makes COPY match by name.
            let copy_columns = match destination.as_deref().filter(|_| stage.is_none()) {
                Some(names) => format!(" ({})", quoted_column_list(names)?),
                None => String::new(),
            };
            let copy = format!(
                "COPY LITTLE ENDIAN BINARY INTO {copy_target}{copy_columns} FROM {files} ON CLIENT"
            );
            let copy_lz4 = format!(
                "COPY LITTLE ENDIAN BINARY INTO {copy_target}{copy_columns} FROM {files} ON 'lz4' CLIENT"
            );
            let mut windows = start_ingest_window_prefetch(
                reader,
                Arc::clone(&schema),
                window_budget,
                physical_window_budget,
                incompressible_window_budget,
                self.write_batch_rows,
                self.wire_compression,
            )?;
            let operation_deadline = self
                .timeouts
                .operation
                .and_then(|timeout| Instant::now().checked_add(timeout));
            let mut rows = 0i64;
            let upload_result = (|| {
                while let Some(mut window) =
                    windows.next_window(remaining_ingest_timeout(operation_deadline)?)?
                {
                    cursor.set_timeouts(ingest_timeouts(self.timeouts, operation_deadline)?);
                    let wire_lz4 = window.uses_wire_lz4();
                    let stored_bytes = window.stored_bytes();
                    let storage = window.storage_mode();
                    let memory_usage = window.memory_usage(wire_lz4);
                    let mut window_bytes = window.encoded_bytes;
                    let mut largest_chunk = window.largest_chunk;
                    let mut largest_column_bytes = window.largest_column_bytes;
                    let (retained_request, retained_receiver, retained_encoder) =
                        if window.retained_batches.is_empty() {
                            (None, None, None)
                        } else {
                            let (request, receiver, encoder) = start_retained_encoder(
                                Arc::clone(&schema),
                                window.retained_batches.clone(),
                            );
                            (Some(request), Some(receiver), Some(encoder))
                        };
                    let upload_result = cursor
                    .execute_with_streaming_uploads(
                        if wire_lz4 { &copy_lz4 } else { &copy },
                        |filename, sink| {
                        let index = upload_column_index(filename, schema.fields().len())?;
                        let mut decoded = None;
                        let mut unshuffled = None;
                        let column = &mut window.columns[index];
                        let chunks = std::mem::take(&mut column.chunks);
                        for chunk in &chunks {
                            match chunk {
                                EncodedChunk::Raw(data) => sink.write_chunk(data)?,
                                EncodedChunk::Lz4 {
                                    data,
                                    len,
                                    decoded_len,
                                    transform,
                                    format,
                                } => {
                                    let data = data.slice(&window.compressed_arena, *len);
                                    if wire_lz4 {
                                        if *transform != CompressionTransform::Plain
                                            || *format != CompressionFormat::Frame
                                        {
                                            return Err(CursorError::FileTransfer(
                                                "wire-compressed ingest contains a client-only transform"
                                                    .into(),
                                                ));
                                        }
                                        sink.write_chunk(data)?;
                                        continue;
                                    }
                                    if decoded
                                        .as_ref()
                                        .is_none_or(|scratch: &MmapMut| scratch.len() < *decoded_len)
                                    {
                                        decoded = Some(MmapMut::map_anon(*decoded_len).map_err(
                                            |value| {
                                                CursorError::FileTransfer(format!(
                                                    "allocating an ingest decode buffer failed: {value}"
                                                ))
                                            },
                                        )?);
                                    }
                                    let scratch = decoded
                                        .as_mut()
                                        .expect("decode buffer was just initialized");
                                    let written = match format {
                                        CompressionFormat::Block => {
                                            lz4_flex::block::decompress_into(
                                                data,
                                                &mut scratch[..*decoded_len],
                                            )
                                            .map_err(|value| {
                                                CursorError::FileTransfer(format!(
                                                    "decompressing an ingest buffer failed: {value}"
                                                ))
                                            })?
                                        }
                                        CompressionFormat::Frame => decompress_frame_into(
                                            data,
                                            &mut scratch[..*decoded_len],
                                        )?,
                                    };
                                    if written != *decoded_len {
                                        return Err(CursorError::FileTransfer(format!(
                                            "decompressed ingest buffer has {written} bytes, expected {decoded_len}"
                                        )));
                                    }
                                    match transform {
                                        CompressionTransform::Plain => {
                                            sink.write_chunk(&scratch[..written])?
                                        }
                                        CompressionTransform::Shuffle(width) => {
                                            if unshuffled
                                                .as_ref()
                                                .is_none_or(|buffer: &MmapMut| {
                                                    buffer.len() < *decoded_len
                                                })
                                            {
                                                unshuffled =
                                                    Some(MmapMut::map_anon(*decoded_len).map_err(
                                                        |value| {
                                                            CursorError::FileTransfer(format!(
                                                                "allocating an ingest unshuffle buffer failed: {value}"
                                                            ))
                                                        },
                                                    )?);
                                            }
                                            let output = unshuffled
                                                .as_mut()
                                                .expect("unshuffle buffer was just initialized");
                                            unshuffle_bytes(
                                                &scratch[..written],
                                                *width,
                                                &mut output[..written],
                                            );
                                            sink.write_chunk(&output[..written])?;
                                        }
                                    }
                                }
                            }
                        }
                        if let (Some(request), Some(receiver)) =
                            (&retained_request, &retained_receiver)
                        {
                            request.send(index).map_err(|_| {
                                CursorError::FileTransfer(
                                    "ingest encoder stopped before a column was requested".into(),
                                )
                            })?;
                            let retained_bytes = upload_retained_column(
                                receiver,
                                index,
                                sink,
                                &mut largest_chunk,
                            )?;
                            let column_bytes = window.columns[index]
                                .bytes
                                .checked_add(retained_bytes)
                                .ok_or_else(|| {
                                    CursorError::FileTransfer(
                                        "encoded ingest byte count overflows".into(),
                                    )
                                })?;
                            largest_column_bytes = largest_column_bytes.max(column_bytes);
                            window_bytes =
                                window_bytes.checked_add(retained_bytes).ok_or_else(|| {
                                    CursorError::FileTransfer(
                                        "encoded ingest byte count overflows".into(),
                                    )
                                })?;
                        }
                        Ok(())
                    },
                    )
                    .map_err(map_cursor_error);
                    drop(retained_request);
                    drop(retained_receiver);
                    let encoder_result = retained_encoder
                        .map(|encoder| {
                            encoder.join().map_err(|_| {
                                error("ingest encoder thread panicked", Status::Internal)
                            })
                        })
                        .transpose();
                    combine_error(upload_result, encoder_result, "ingest encoder cleanup")?;
                    stats.record_window(
                        CompletedIngestWindow {
                            rows: window.rows,
                            bytes: window_bytes,
                            stored_bytes,
                            storage,
                            wire_lz4,
                            memory: memory_usage,
                            streaming: IngestStreamingUsage {
                                largest_chunk,
                                largest_column_bytes,
                            },
                        },
                        staged_constrained_append,
                    );
                    let server_rows = cursor.affected_rows().ok_or_else(|| {
                        error(
                            "MonetDB did not report the COPY row count",
                            Status::InvalidData,
                        )
                    })?;
                    let expected_rows = i64::try_from(window.rows)
                        .map_err(|_| error("window row count exceeds i64", Status::Internal))?;
                    if server_rows != expected_rows {
                        return Err(error(
                            format!(
                                "MonetDB copied {server_rows} rows from a window containing {expected_rows} rows"
                            ),
                            Status::InvalidData,
                        ));
                    }
                    rows = rows.checked_add(server_rows).ok_or_else(|| {
                        error("ingested row count overflows i64", Status::Internal)
                    })?;
                }
                Ok(rows)
            })();
            if upload_result.is_err() {
                windows.detach();
            }
            let scheduler_result = windows.finish();
            if let Ok(Some(scheduler_stats)) = &scheduler_result {
                stats.input_batches = scheduler_stats.input_batches;
                stats.coalesced_windows = scheduler_stats.coalesced_windows;
                stats.split_windows = scheduler_stats.split_windows;
            }
            let upload_result =
                combine_error(upload_result, scheduler_result, "ingest prefetch cleanup");
            let moved = upload_result.and_then(|rows| {
                let Some(stage) = &stage else {
                    return Ok(rows);
                };
                cursor.set_timeouts(ingest_timeouts(self.timeouts, operation_deadline)?);
                cursor
                    .execute(&stage.move_to_target)
                    .map_err(map_cursor_error)?;
                stats.final_move_count = 1;
                let moved = cursor.affected_rows().ok_or_else(|| {
                    error(
                        "MonetDB did not report the constrained append row count",
                        Status::InvalidData,
                    )
                })?;
                if moved != rows {
                    return Err(error(
                        format!(
                            "MonetDB moved {moved} staged rows from an ingest containing {rows} rows"
                        ),
                        Status::InvalidData,
                    ));
                }
                Ok(rows)
            });
            match (moved, &stage) {
                (Ok(rows), Some(stage)) => {
                    cursor.set_timeouts(ingest_timeouts(self.timeouts, operation_deadline)?);
                    combine_atomic_error(
                        Ok(rows),
                        cursor.execute(&stage.drop),
                        "dropping the constrained append staging table",
                    )
                }
                (result, _) => result,
            }
        })();
        cursor.set_timeouts(self.timeouts);
        let operation_failed = result.is_err();
        let result = finish_atomic(
            &connection,
            &mut cursor,
            atomic_scope,
            result,
            self.timeouts,
        )
        .map(Some);
        if operation_failed
            && caller_transaction
            && stats.target_copy_count > 0
            && self.ingest_partial == IngestPartial::Block
        {
            self.connection
                .poison_ingest(&operation_target, stats.target_copy_count);
            stats.poisoned = true;
        }
        self.ingest_stats = Some(stats.to_json());
        drop(cursor);
        drop(connection);
        if mode != "adbc.ingest.mode.append" && result.is_ok() {
            invalidate_prepared_cache(&self.connection, &self.prepared_cache);
        }
        result
    }
}

fn ingest_column_definitions(
    schema: &SchemaRef,
    preserve_nullability: bool,
) -> Result<Vec<String>> {
    schema
        .fields()
        .iter()
        .map(|field| {
            let sql_type = monetdb_arrow::sql_type_for_field(field)
                .map_err(|value| map_display(value, Status::NotImplemented))?;
            Ok(format!(
                "{} {}{}",
                quote_identifier(field.name())?,
                sql_type,
                if preserve_nullability && !field.is_nullable() {
                    " NOT NULL"
                } else {
                    ""
                }
            ))
        })
        .collect()
}

struct IngestTarget<'a> {
    mode: &'a str,
    create: &'a str,
    operation_target: &'a str,
    schema_name: Option<&'a str>,
    table: &'a str,
    temporary: bool,
}

struct ConstrainedAppendStage {
    qualified_name: String,
    create: String,
    move_to_target: String,
    drop: String,
}

fn constrained_append_stage(
    schema: &SchemaRef,
    destination: &[String],
    operation_target: &str,
    local: bool,
    persistent_schema: Option<&str>,
) -> Result<ConstrainedAppendStage> {
    let name = if local {
        savepoint_name("ingest_stage")
    } else {
        format!(
            "{}_{}",
            savepoint_name("ingest_stage"),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        )
    };
    let quoted_name = quote_identifier(&name)?;
    let qualified_name = qualified_name(
        if local {
            Some("tmp")
        } else {
            Some(persistent_schema.ok_or_else(|| {
                error(
                    "persistent constrained append staging requires a schema",
                    Status::Internal,
                )
            })?)
        },
        &name,
    )?;
    let create_target = if local {
        quoted_name
    } else {
        qualified_name.clone()
    };
    let source_columns = schema
        .fields()
        .iter()
        .map(|field| quote_identifier(field.name()))
        .collect::<Result<Vec<_>>>()?
        .join(", ");
    let destination_columns = quoted_column_list(destination)?;
    Ok(ConstrainedAppendStage {
        create: format!(
            "CREATE {}TABLE {create_target} ({}){}",
            if local {
                "LOCAL TEMPORARY "
            } else {
                "UNLOGGED "
            },
            ingest_column_definitions(schema, false)?.join(", "),
            if local {
                " ON COMMIT PRESERVE ROWS"
            } else {
                ""
            }
        ),
        move_to_target: format!(
            "INSERT INTO {operation_target} ({destination_columns}) \
             SELECT {source_columns} FROM {qualified_name}"
        ),
        drop: format!("DROP TABLE {qualified_name}"),
        qualified_name,
    })
}

fn prepare_ingest_target(
    cursor: &mut monetdb::Cursor,
    target: &IngestTarget<'_>,
    schema: &SchemaRef,
) -> Result<Option<Vec<String>>> {
    execute_ingest_target_mode(cursor, target)?;
    if matches!(
        target.mode,
        "adbc.ingest.mode.append" | "adbc.ingest.mode.create_append"
    ) {
        let mismatch_status = append_mismatch_status(target.mode);
        let destination = append_table_columns(
            cursor,
            if target.temporary {
                Some("tmp")
            } else {
                target.schema_name
            },
            target.table,
            mismatch_status,
        )?;
        return align_append_schema(schema, &destination, mismatch_status).map(Some);
    }
    Ok(None)
}

fn execute_ingest_target_mode(
    cursor: &mut monetdb::Cursor,
    target: &IngestTarget<'_>,
) -> Result<()> {
    match target.mode {
        "adbc.ingest.mode.create" => cursor.execute(target.create).map_err(map_cursor_error)?,
        "adbc.ingest.mode.append" => {}
        "adbc.ingest.mode.replace" => {
            cursor
                .execute(&format!("DROP TABLE IF EXISTS {}", target.operation_target))
                .map_err(map_cursor_error)?;
            cursor.execute(target.create).map_err(map_cursor_error)?;
        }
        "adbc.ingest.mode.create_append" => cursor
            .execute(&target.create.replacen("TABLE ", "TABLE IF NOT EXISTS ", 1))
            .map_err(map_cursor_error)?,
        value => {
            return Err(error(
                format!("unknown ingest mode '{value}'"),
                Status::InvalidArguments,
            ));
        }
    }

    Ok(())
}

fn insert_parameter_queries(target: &str, names: &[String]) -> Result<Vec<String>> {
    let placeholders = std::iter::repeat_n("?", names.len())
        .collect::<Vec<_>>()
        .join(", ");
    let mut queries = Vec::with_capacity(2);
    if names.iter().all(|name| is_simple_unquoted_identifier(name)) {
        queries.push(format!(
            "INSERT INTO {target} ({}) VALUES ({placeholders})",
            names
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    queries.push(format!(
        "INSERT INTO {target} ({}) VALUES ({placeholders})",
        quoted_column_list(names)?
    ));
    Ok(queries)
}

fn is_simple_unquoted_identifier(name: &str) -> bool {
    let mut bytes = name.bytes();
    bytes.next().is_some_and(|byte| byte.is_ascii_alphabetic())
        && bytes.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}

fn append_mismatch_status(mode: &str) -> Status {
    if mode == "adbc.ingest.mode.create_append" {
        Status::AlreadyExists
    } else {
        Status::InvalidArguments
    }
}

fn upload_column_index(filename: &str, columns: usize) -> std::result::Result<usize, CursorError> {
    filename
        .strip_prefix('c')
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|index| *index < columns)
        .ok_or_else(|| {
            CursorError::FileTransfer(format!("server requested unknown file {filename:?}"))
        })
}

fn map_chunked_encode_error(value: monetdb_arrow::ChunkedEncodeError<CursorError>) -> CursorError {
    match value {
        monetdb_arrow::ChunkedEncodeError::Encode(value) => {
            CursorError::FileTransfer(value.to_string())
        }
        monetdb_arrow::ChunkedEncodeError::Sink(value) => value,
    }
}

struct IngestWindowScheduler<'a> {
    reader: &'a mut (dyn RecordBatchReader + Send),
    schema: SchemaRef,
    pending: VecDeque<PendingBatch>,
    pending_rows: usize,
    pending_bytes: usize,
    exhausted: bool,
    window_budget: usize,
    physical_window_budget: usize,
    incompressible_window_budget: usize,
    configured_rows: Option<usize>,
    wire_compression: WireCompression,
    input_batches: usize,
    coalesced_windows: usize,
    split_windows: usize,
}

struct IngestSchedulerStats {
    input_batches: usize,
    coalesced_windows: usize,
    split_windows: usize,
}

struct IngestWindowPrefetch {
    receiver: Option<Receiver<Result<Option<EncodedIngestWindow>>>>,
    worker: Option<JoinHandle<IngestSchedulerStats>>,
}

impl IngestWindowPrefetch {
    fn next_window(
        &mut self,
        operation_timeout: Option<Duration>,
    ) -> Result<Option<EncodedIngestWindow>> {
        let receiver = self.receiver.as_ref().ok_or_else(|| {
            error(
                "ingest window prefetch is no longer available",
                Status::InvalidState,
            )
        })?;
        let received = match operation_timeout {
            Some(timeout) => receiver.recv_timeout(timeout).map_err(|value| match value {
                RecvTimeoutError::Timeout => error(
                    "ingest source did not produce a window before the operation timeout",
                    Status::Timeout,
                ),
                RecvTimeoutError::Disconnected => error(
                    "ingest window prefetch stopped before producing a result",
                    Status::Internal,
                ),
            }),
            None => receiver.recv().map_err(|_| {
                error(
                    "ingest window prefetch stopped before producing a result",
                    Status::Internal,
                )
            }),
        };
        if received
            .as_ref()
            .is_err_and(|error| error.status == Status::Timeout)
        {
            self.receiver.take();
        }
        received?
    }

    fn finish(mut self) -> Result<Option<IngestSchedulerStats>> {
        let detached = self.receiver.is_none();
        let receiver = self.receiver.take();
        let worker = self.worker.take().expect("prefetch worker is present");
        drop(receiver);
        if detached {
            drop(worker);
            return Ok(None);
        }
        let result = worker
            .join()
            .map_err(|_| error("ingest window prefetch thread panicked", Status::Internal));
        result.map(Some)
    }

    fn detach(&mut self) {
        self.receiver.take();
    }
}

fn remaining_ingest_timeout(deadline: Option<Instant>) -> Result<Option<Duration>> {
    deadline
        .map(|deadline| {
            deadline
                .checked_duration_since(Instant::now())
                .ok_or_else(|| error("ingest operation timeout expired", Status::Timeout))
        })
        .transpose()
}

fn ingest_timeouts(timeouts: Timeouts, deadline: Option<Instant>) -> Result<Timeouts> {
    let Some(remaining) = remaining_ingest_timeout(deadline)? else {
        return Ok(timeouts);
    };
    let bounded = |timeout: Option<Duration>| Some(timeout.unwrap_or(remaining).min(remaining));
    Ok(Timeouts {
        read: bounded(timeouts.read),
        write: bounded(timeouts.write),
        operation: Some(remaining),
    })
}

fn start_ingest_window_prefetch(
    mut reader: Box<dyn RecordBatchReader + Send>,
    schema: SchemaRef,
    window_budget: usize,
    physical_window_budget: usize,
    incompressible_window_budget: usize,
    configured_rows: Option<usize>,
    wire_compression: WireCompression,
) -> Result<IngestWindowPrefetch> {
    let memory_reservation = reserve_ingest_memory(physical_window_budget)?;
    let (sender, receiver) = sync_channel(0);
    let worker = thread::Builder::new()
        .name("monetdb-ingest-window".into())
        .spawn(move || {
            let _memory_reservation = memory_reservation;
            let mut scheduler = IngestWindowScheduler::new(
                reader.as_mut(),
                schema,
                window_budget,
                physical_window_budget,
                incompressible_window_budget,
                configured_rows,
                wire_compression,
            );
            loop {
                let next = scheduler.next_window();
                let finished = !matches!(next, Ok(Some(_)));
                if sender.send(next).is_err() || finished {
                    break;
                }
            }
            IngestSchedulerStats {
                input_batches: scheduler.input_batches,
                coalesced_windows: scheduler.coalesced_windows,
                split_windows: scheduler.split_windows,
            }
        })
        .map_err(|value| {
            error(
                format!("starting ingest window prefetch failed: {value}"),
                Status::Internal,
            )
        })?;
    Ok(IngestWindowPrefetch {
        receiver: Some(receiver),
        worker: Some(worker),
    })
}

struct IngestMemoryReservation {
    bytes: usize,
}

impl Drop for IngestMemoryReservation {
    fn drop(&mut self) {
        if self.bytes > 0 {
            RESERVED_INGEST_MEMORY_BYTES.fetch_sub(self.bytes, Ordering::AcqRel);
        }
    }
}

fn ingest_memory_reservation_bytes(physical_window_budget: usize) -> usize {
    physical_window_budget
        .saturating_add(ENCODE_STAGING_BYTES)
        .saturating_mul(2)
        .saturating_add(INGEST_RESERVATION_SOURCE_BYTES)
}

fn reserve_ingest_memory(physical_window_budget: usize) -> Result<IngestMemoryReservation> {
    let bytes = ingest_memory_reservation_bytes(physical_window_budget);
    let Some((limit, current)) = cgroup_memory_limit_and_usage_bytes() else {
        return Ok(IngestMemoryReservation { bytes: 0 });
    };
    let allowed = limit.saturating_mul(9) / 10;
    let mut reserved = RESERVED_INGEST_MEMORY_BYTES.load(Ordering::Acquire);
    loop {
        if !ingest_memory_reservation_fits(allowed, current, reserved, bytes) {
            let mut reservation_error = error(
                format!(
                    "ingest requires a {bytes}-byte memory reservation, but the cgroup has \
                     {current} bytes in use, {reserved} bytes reserved by other ingests, and \
                     a {limit}-byte limit"
                ),
                Status::Internal,
            );
            reservation_error.sqlstate =
                parse_sqlstate("HY001").expect("HY001 is a valid SQLSTATE");
            return Err(reservation_error);
        }
        match RESERVED_INGEST_MEMORY_BYTES.compare_exchange_weak(
            reserved,
            reserved.saturating_add(bytes),
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => return Ok(IngestMemoryReservation { bytes }),
            Err(updated) => reserved = updated,
        }
    }
}

fn ingest_memory_reservation_fits(
    allowed: usize,
    current: usize,
    reserved: usize,
    requested: usize,
) -> bool {
    current
        .checked_add(reserved)
        .and_then(|total| total.checked_add(requested))
        .is_some_and(|required| required <= allowed)
}

struct PendingBatch {
    batch: RecordBatch,
    estimated_bytes: usize,
    compaction_level: usize,
}

struct InsertCandidate {
    batch: RecordBatch,
    arguments: Vec<String>,
    estimated_bytes: usize,
}

enum RetainedEncodeMessage {
    Start(usize),
    Chunk(Vec<u8>),
    Finish(usize),
    Error(CursorError),
}

fn start_retained_encoder(
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
) -> (
    SyncSender<usize>,
    Receiver<RetainedEncodeMessage>,
    JoinHandle<()>,
) {
    let (request_sender, request_receiver) = sync_channel(0);
    let (sender, receiver) = sync_channel(1);
    let handle = thread::spawn(move || {
        encode_retained_columns(&request_receiver, &sender, &schema, &batches)
    });
    (request_sender, receiver, handle)
}

fn encode_retained_columns(
    requests: &Receiver<usize>,
    sender: &SyncSender<RetainedEncodeMessage>,
    schema: &SchemaRef,
    batches: &[RecordBatch],
) {
    while let Ok(index) = requests.recv() {
        if sender.send(RetainedEncodeMessage::Start(index)).is_err() {
            return;
        }
        let arrays = batches
            .iter()
            .map(|batch| batch.column(index).as_ref())
            .collect::<Vec<_>>();
        let encoded = monetdb_arrow::encode_column_chunks(
            &schema.fields()[index],
            &arrays,
            NonZeroUsize::new(ENCODE_CHUNK_BYTES).expect("encode chunk size is positive"),
            |chunk| -> std::result::Result<(), CursorError> {
                sender
                    .send(RetainedEncodeMessage::Chunk(chunk.to_vec()))
                    .map_err(|_| {
                        CursorError::FileTransfer(
                            "ingest upload stopped before encoding completed".into(),
                        )
                    })
            },
        )
        .map_err(map_chunked_encode_error);
        match encoded {
            Ok(bytes) => {
                if sender.send(RetainedEncodeMessage::Finish(bytes)).is_err() {
                    return;
                }
            }
            Err(value) => {
                let _ = sender.send(RetainedEncodeMessage::Error(value));
                return;
            }
        }
    }
}

fn upload_retained_column(
    receiver: &Receiver<RetainedEncodeMessage>,
    expected_index: usize,
    sink: &mut dyn monetdb::UploadSink,
    largest_chunk: &mut usize,
) -> std::result::Result<usize, CursorError> {
    match receiver.recv() {
        Ok(RetainedEncodeMessage::Start(index)) if index == expected_index => {}
        Ok(RetainedEncodeMessage::Start(index)) => {
            return Err(CursorError::FileTransfer(format!(
                "ingest encoder produced column {index}, expected {expected_index}"
            )));
        }
        Ok(RetainedEncodeMessage::Error(value)) => return Err(value),
        Ok(_) => {
            return Err(CursorError::FileTransfer(
                "ingest encoder produced an out-of-order message".into(),
            ));
        }
        Err(_) => {
            return Err(CursorError::FileTransfer(
                "ingest encoder stopped before the upload completed".into(),
            ));
        }
    }
    loop {
        match receiver.recv() {
            Ok(RetainedEncodeMessage::Chunk(chunk)) => {
                *largest_chunk = (*largest_chunk).max(chunk.len());
                sink.write_chunk(&chunk)?;
            }
            Ok(RetainedEncodeMessage::Finish(bytes)) => return Ok(bytes),
            Ok(RetainedEncodeMessage::Error(value)) => return Err(value),
            Ok(RetainedEncodeMessage::Start(index)) => {
                return Err(CursorError::FileTransfer(format!(
                    "ingest encoder started column {index} before finishing {expected_index}"
                )));
            }
            Err(_) => {
                return Err(CursorError::FileTransfer(
                    "ingest encoder stopped before the upload completed".into(),
                ));
            }
        }
    }
}

struct PrefixedBatchReader {
    schema: SchemaRef,
    prefix: VecDeque<RecordBatch>,
    reader: Box<dyn RecordBatchReader + Send>,
}

impl Iterator for PrefixedBatchReader {
    type Item = std::result::Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.prefix
            .pop_front()
            .map(Ok)
            .or_else(|| self.reader.next())
    }
}

impl RecordBatchReader for PrefixedBatchReader {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

fn route_ingest_reader(
    mut reader: Box<dyn RecordBatchReader + Send>,
    schema: &SchemaRef,
    insert_rows: usize,
) -> Result<(Box<dyn RecordBatchReader + Send>, Option<InsertCandidate>)> {
    if insert_rows == 0 {
        return Ok((reader, None));
    }
    let Some(first) = reader.next() else {
        return Ok((reader, None));
    };
    let first = first.map_err(Error::from)?;
    validate_record_batch(&first)?;
    if first.schema() != *schema {
        return Err(error(
            "record batch schema changed within ingest stream",
            Status::InvalidData,
        ));
    }
    let second = reader.next().transpose().map_err(Error::from)?;
    let estimated_bytes = estimated_batch_size(schema, &first)?;
    if insert_rows > 0
        && first.num_rows() > 0
        && first.num_rows() <= insert_rows
        && second.is_none()
        && estimated_bytes <= PARAMETER_UPDATE_BATCH_BYTES / 2
    {
        for (field, column) in schema.fields().iter().zip(first.columns()) {
            monetdb_arrow::encode_column(field, column.as_ref())
                .map_err(|value| map_display(value, Status::InvalidData))?;
        }
        let mut arguments = Vec::with_capacity(first.num_rows());
        let mut rendered_bytes = 0usize;
        for row in 0..first.num_rows() {
            let row_arguments = render_arguments(&first, row)?.join(", ");
            rendered_bytes = rendered_bytes
                .checked_add(row_arguments.len().saturating_add(32))
                .ok_or_else(|| error("rendered INSERT byte count overflows", Status::Internal))?;
            if rendered_bytes > PARAMETER_UPDATE_BATCH_BYTES {
                arguments.clear();
                break;
            }
            arguments.push(row_arguments);
        }
        if !arguments.is_empty() {
            return Ok((
                reader,
                Some(InsertCandidate {
                    batch: first,
                    arguments,
                    estimated_bytes,
                }),
            ));
        }
    }
    let mut prefix = VecDeque::from([first]);
    if let Some(second) = second {
        prefix.push_back(second);
    }
    Ok((
        Box::new(PrefixedBatchReader {
            schema: Arc::clone(schema),
            prefix,
            reader,
        }),
        None,
    ))
}

struct EncodedColumn {
    chunks: Vec<EncodedChunk>,
    pending: Option<PendingEncodedChunk>,
    compression: CompressionMode,
    fixed_width: Option<usize>,
    wire_compression: WireCompression,
    bytes: usize,
}

struct PendingEncodedChunk {
    data: MmapMut,
    len: usize,
}

enum EncodedChunk {
    Raw(MmapMut),
    Lz4 {
        data: CompressedData,
        len: usize,
        decoded_len: usize,
        transform: CompressionTransform,
        format: CompressionFormat,
    },
}

struct CompressedSlab {
    data: MmapMut,
    used: usize,
}

#[derive(Default)]
struct CompressedArena {
    slabs: Vec<CompressedSlab>,
}

impl CompressedArena {
    fn store(&mut self, bytes: &[u8]) -> std::result::Result<CompressedData, CursorError> {
        let reusable = self
            .slabs
            .last()
            .filter(|slab| slab.data.len() - slab.used >= bytes.len())
            .map(|_| self.slabs.len() - 1);
        let slab = match reusable {
            Some(index) => index,
            None => {
                let capacity = COMPRESSED_ARENA_SLAB_BYTES.max(bytes.len());
                let data = MmapMut::map_anon(capacity).map_err(|value| {
                    CursorError::FileTransfer(format!(
                        "allocating an ingest compression arena failed: {value}"
                    ))
                })?;
                self.slabs.push(CompressedSlab { data, used: 0 });
                self.slabs.len() - 1
            }
        };
        let storage = &mut self.slabs[slab];
        let offset = storage.used;
        storage.data[offset..offset + bytes.len()].copy_from_slice(bytes);
        storage.used += bytes.len();
        Ok(CompressedData { slab, offset })
    }

    fn slice(&self, data: &CompressedData, len: usize) -> &[u8] {
        &self.slabs[data.slab].data[data.offset..data.offset + len]
    }

    fn physical_bytes(&self) -> usize {
        self.slabs.iter().map(|slab| slab.data.len()).sum()
    }

    #[cfg(test)]
    fn used_bytes(&self) -> usize {
        self.slabs.iter().map(|slab| slab.used).sum()
    }
}

struct CompressedData {
    slab: usize,
    offset: usize,
}

#[derive(Default)]
struct CompressionWorkspace {
    transformed: Vec<u8>,
    compressed: Vec<u8>,
}

impl CompressedData {
    fn slice<'a>(&self, arena: &'a CompressedArena, len: usize) -> &'a [u8] {
        arena.slice(self, len)
    }
}

impl EncodedChunk {
    fn stored_len(&self) -> usize {
        match self {
            Self::Raw(data) => data.len(),
            Self::Lz4 { len, .. } => *len,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CompressionTransform {
    Plain,
    Shuffle(usize),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CompressionFormat {
    Block,
    Frame,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CompressionMode {
    Unknown,
    Enabled(CompressionTransform),
    Disabled,
}

impl EncodedColumn {
    fn new(fixed_width: Option<usize>, wire_compression: WireCompression) -> Self {
        Self {
            chunks: Vec::new(),
            pending: None,
            compression: CompressionMode::Unknown,
            fixed_width,
            wire_compression,
            bytes: 0,
        }
    }

    fn push(
        &mut self,
        mut bytes: &[u8],
        compressed_arena: &mut CompressedArena,
    ) -> std::result::Result<(), CursorError> {
        while !bytes.is_empty() {
            if self.pending.is_none() {
                let capacity = bytes
                    .len()
                    .clamp(MIN_ENCODE_BUFFER_BYTES, ENCODE_CHUNK_BYTES);
                self.pending = Some(PendingEncodedChunk {
                    data: MmapMut::map_anon(capacity).map_err(|value| {
                        CursorError::FileTransfer(format!(
                            "allocating an encoded ingest buffer failed: {value}"
                        ))
                    })?,
                    len: 0,
                });
            }
            let pending = self
                .pending
                .as_mut()
                .expect("pending encoded chunk was just initialized");
            let copied = bytes.len().min(pending.data.len() - pending.len);
            pending.data[pending.len..pending.len + copied].copy_from_slice(&bytes[..copied]);
            pending.len += copied;
            bytes = &bytes[copied..];
            if pending.len == pending.data.len() {
                self.finish_pending(compressed_arena)?;
            }
        }
        Ok(())
    }

    fn finish(
        &mut self,
        compressed_arena: &mut CompressedArena,
    ) -> std::result::Result<(), CursorError> {
        self.finish_pending(compressed_arena)
    }

    fn finish_pending(
        &mut self,
        compressed_arena: &mut CompressedArena,
    ) -> std::result::Result<(), CursorError> {
        let Some(pending) = self.pending.take() else {
            return Ok(());
        };
        let input = &pending.data[..pending.len];
        if self.compression == CompressionMode::Disabled {
            self.store_raw(input)?;
            return Ok(());
        }
        let force_compression = self.wire_compression == WireCompression::Lz4;
        let probe_format = if self.wire_compression == WireCompression::Lz4 {
            CompressionFormat::Frame
        } else {
            CompressionFormat::Block
        };
        let compressed = COMPRESSION_WORKSPACE.with(
            |workspace| -> std::result::Result<
                Option<(
                    CompressedData,
                    usize,
                    CompressionTransform,
                    CompressionFormat,
                )>,
                CursorError,
            > {
                let mut workspace = workspace.borrow_mut();
                let transform = match self.compression {
                    CompressionMode::Unknown => {
                        if force_compression {
                            CompressionTransform::Plain
                        } else {
                            let Some(transform) = select_compression_transform(
                                input,
                                self.fixed_width,
                                self.wire_compression != WireCompression::Lz4,
                                probe_format,
                                &mut workspace,
                            )?
                            else {
                                return Ok(None);
                            };
                            transform
                        }
                    }
                    CompressionMode::Enabled(transform) => transform,
                    CompressionMode::Disabled => {
                        unreachable!("disabled compression returned above")
                    }
                };
                let format = if self.wire_compression != WireCompression::None
                    && transform == CompressionTransform::Plain
                {
                    CompressionFormat::Frame
                } else {
                    CompressionFormat::Block
                };
                let compressed_len = compress_bytes(input, transform, format, &mut workspace)?;
                if !force_compression
                    && compressed_len.saturating_mul(8) > input.len().saturating_mul(7)
                {
                    return Ok(None);
                }
                let data = compressed_arena.store(&workspace.compressed[..compressed_len])?;
                Ok(Some((data, compressed_len, transform, format)))
            },
        )?;
        if let Some((data, compressed_len, transform, format)) = compressed {
            self.compression = CompressionMode::Enabled(transform);
            self.chunks.push(EncodedChunk::Lz4 {
                data,
                len: compressed_len,
                decoded_len: input.len(),
                transform,
                format,
            });
        } else {
            self.compression = CompressionMode::Disabled;
            self.store_raw(input)?;
        }
        Ok(())
    }

    fn store_raw(&mut self, input: &[u8]) -> std::result::Result<(), CursorError> {
        let mut raw = MmapMut::map_anon(input.len()).map_err(|value| {
            CursorError::FileTransfer(format!(
                "allocating an encoded ingest buffer failed: {value}"
            ))
        })?;
        raw.copy_from_slice(input);
        self.chunks.push(EncodedChunk::Raw(raw));
        Ok(())
    }

    fn largest_chunk(&self) -> usize {
        self.chunks
            .iter()
            .map(|chunk| match chunk {
                EncodedChunk::Raw(data) => data.len(),
                EncodedChunk::Lz4 { decoded_len, .. } => *decoded_len,
            })
            .max()
            .unwrap_or(0)
    }

    fn stored_bytes(&self) -> usize {
        self.chunks.iter().map(EncodedChunk::stored_len).sum()
    }
}

fn select_compression_transform(
    input: &[u8],
    fixed_width: Option<usize>,
    allow_shuffle: bool,
    format: CompressionFormat,
    workspace: &mut CompressionWorkspace,
) -> std::result::Result<Option<CompressionTransform>, CursorError> {
    let sample_len = COMPRESSION_PROBE_BYTES.min(input.len());
    let plain = &input[..sample_len];
    let plain_len = compressed_size(plain, format, &mut workspace.compressed)?;
    let mut best = (CompressionTransform::Plain, plain_len);
    if allow_shuffle && let Some(width) = fixed_width.filter(|width| *width > 1) {
        let aligned_len = sample_len - sample_len % width;
        if aligned_len > 0 {
            shuffle_bytes_into(plain, width, &mut workspace.transformed);
            let shuffled_len =
                compressed_size(&workspace.transformed, format, &mut workspace.compressed)?;
            if shuffled_len < best.1 {
                best = (CompressionTransform::Shuffle(width), shuffled_len);
            }
        }
    }
    Ok((best.1.saturating_mul(8) <= plain.len().saturating_mul(7)).then_some(best.0))
}

fn compressed_size(
    input: &[u8],
    format: CompressionFormat,
    output: &mut Vec<u8>,
) -> std::result::Result<usize, CursorError> {
    match format {
        CompressionFormat::Block => {
            output.resize(lz4_flex::block::get_maximum_output_size(input.len()), 0);
            let compressed_len =
                lz4_flex::block::compress_into(input, output).map_err(|value| {
                    CursorError::FileTransfer(format!(
                        "compressing an ingest buffer failed: {value}"
                    ))
                })?;
            output.truncate(compressed_len);
            Ok(compressed_len)
        }
        CompressionFormat::Frame => compress_frame_bytes(input, output),
    }
}

fn compress_frame_bytes(
    input: &[u8],
    output: &mut Vec<u8>,
) -> std::result::Result<usize, CursorError> {
    output.clear();
    let mut encoder = lz4_flex::frame::FrameEncoder::new(std::mem::take(output));
    encoder.write_all(input).map_err(|value| {
        CursorError::FileTransfer(format!("compressing an ingest buffer failed: {value}"))
    })?;
    *output = encoder.finish().map_err(|value| {
        CursorError::FileTransfer(format!(
            "finishing an ingest compression frame failed: {value}"
        ))
    })?;
    Ok(output.len())
}

fn compress_bytes(
    input: &[u8],
    transform: CompressionTransform,
    format: CompressionFormat,
    workspace: &mut CompressionWorkspace,
) -> std::result::Result<usize, CursorError> {
    match transform {
        CompressionTransform::Plain => compressed_size(input, format, &mut workspace.compressed),
        CompressionTransform::Shuffle(width) => {
            shuffle_bytes_into(input, width, &mut workspace.transformed);
            compressed_size(&workspace.transformed, format, &mut workspace.compressed)
        }
    }
}

fn decompress_frame_into(
    input: &[u8],
    output: &mut [u8],
) -> std::result::Result<usize, CursorError> {
    let mut decoder = lz4_flex::frame::FrameDecoder::new(input);
    decoder.read_exact(output).map_err(|value| {
        CursorError::FileTransfer(format!("decompressing an ingest buffer failed: {value}"))
    })?;
    let mut trailing = [0u8; 1];
    let trailing_bytes = decoder.read(&mut trailing).map_err(|value| {
        CursorError::FileTransfer(format!("finishing ingest decompression failed: {value}"))
    })?;
    if trailing_bytes != 0 {
        return Err(CursorError::FileTransfer(
            "decompressed ingest buffer exceeds its declared size".into(),
        ));
    }
    Ok(output.len())
}

fn shuffle_bytes_into(input: &[u8], width: usize, output: &mut Vec<u8>) {
    assert!(width > 0);
    let aligned_len = input.len() - input.len() % width;
    let rows = aligned_len / width;
    output.clear();
    output.reserve(input.len());
    for byte in 0..width {
        output.extend((0..rows).map(|row| input[row * width + byte]));
    }
    output.extend_from_slice(&input[aligned_len..]);
}

fn unshuffle_bytes(input: &[u8], width: usize, output: &mut [u8]) {
    assert!(width > 0);
    assert_eq!(input.len(), output.len());
    let aligned_len = input.len() - input.len() % width;
    let rows = aligned_len / width;
    for byte in 0..width {
        for row in 0..rows {
            output[row * width + byte] = input[byte * rows + row];
        }
    }
    output[aligned_len..].copy_from_slice(&input[aligned_len..]);
}

fn pinned_record_batch_buffers<'a>(
    batches: impl IntoIterator<Item = &'a RecordBatch>,
) -> HashMap<usize, usize> {
    let mut buffers = HashMap::new();
    for batch in batches {
        for column in batch.columns() {
            add_array_buffers(&column.to_data(), &mut buffers);
        }
    }
    buffers
}

fn add_array_buffers(data: &arrow_data::ArrayData, buffers: &mut HashMap<usize, usize>) {
    for buffer in data.buffers() {
        add_buffer(buffer, buffers);
    }
    if let Some(nulls) = data.nulls() {
        add_buffer(nulls.buffer(), buffers);
    }
    for child in data.child_data() {
        add_array_buffers(child, buffers);
    }
}

fn add_buffer(buffer: &arrow_buffer::Buffer, buffers: &mut HashMap<usize, usize>) {
    let bytes = buffer
        .capacity()
        .max(buffer.ptr_offset().saturating_add(buffer.len()));
    if bytes > 0 {
        buffers
            .entry(buffer.data_ptr().as_ptr() as usize)
            .and_modify(|current| *current = (*current).max(bytes))
            .or_insert(bytes);
    }
}

fn buffer_bytes(buffers: &HashMap<usize, usize>) -> usize {
    buffers
        .values()
        .fold(0usize, |total, bytes| total.saturating_add(*bytes))
}

fn merged_buffers(
    first: &HashMap<usize, usize>,
    second: &HashMap<usize, usize>,
) -> HashMap<usize, usize> {
    let mut merged = first.clone();
    for (pointer, bytes) in second {
        merged
            .entry(*pointer)
            .and_modify(|current| *current = (*current).max(*bytes))
            .or_insert(*bytes);
    }
    merged
}

struct EncodedIngestWindow {
    columns: Vec<EncodedColumn>,
    compressed_arena: CompressedArena,
    retained_batches: Vec<RecordBatch>,
    retained_estimated_bytes: usize,
    retained_buffers: HashMap<usize, usize>,
    peak_staging_bytes: usize,
    peak_build_bytes: usize,
    peak_build_buffers: HashMap<usize, usize>,
    incompressible: bool,
    retain_arrow_eligible: bool,
    retain_arrow: bool,
    rows: usize,
    estimated_bytes: usize,
    encoded_bytes: usize,
    largest_chunk: usize,
    largest_column_bytes: usize,
    coalesced: bool,
    wire_compression: WireCompression,
}

impl EncodedIngestWindow {
    #[cfg(test)]
    fn new(columns: usize) -> Self {
        Self {
            columns: (0..columns)
                .map(|_| EncodedColumn::new(None, WireCompression::None))
                .collect(),
            compressed_arena: CompressedArena::default(),
            retained_batches: Vec::new(),
            retained_estimated_bytes: 0,
            retained_buffers: HashMap::new(),
            peak_staging_bytes: 0,
            peak_build_bytes: 0,
            peak_build_buffers: HashMap::new(),
            incompressible: false,
            retain_arrow_eligible: true,
            retain_arrow: false,
            rows: 0,
            estimated_bytes: 0,
            encoded_bytes: 0,
            largest_chunk: 0,
            largest_column_bytes: 0,
            coalesced: false,
            wire_compression: WireCompression::None,
        }
    }

    fn for_schema(schema: &SchemaRef, wire_compression: WireCompression) -> Result<Self> {
        let columns = schema
            .fields()
            .iter()
            .map(|field| {
                monetdb_arrow::fixed_encoded_width(field)
                    .map_err(|value| map_display(value, Status::NotImplemented))
                    .map(|width| EncodedColumn::new(width, wire_compression))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            columns,
            compressed_arena: CompressedArena::default(),
            retained_batches: Vec::new(),
            retained_estimated_bytes: 0,
            retained_buffers: HashMap::new(),
            peak_staging_bytes: 0,
            peak_build_bytes: 0,
            peak_build_buffers: HashMap::new(),
            incompressible: false,
            retain_arrow_eligible: true,
            retain_arrow: false,
            rows: 0,
            estimated_bytes: 0,
            encoded_bytes: 0,
            largest_chunk: 0,
            largest_column_bytes: 0,
            coalesced: false,
            wire_compression,
        })
    }

    fn append(
        &mut self,
        schema: &SchemaRef,
        pending: Vec<PendingBatch>,
        estimated_bytes: usize,
    ) -> Result<()> {
        let staging_buffers =
            pinned_record_batch_buffers(pending.iter().map(|pending| &pending.batch));
        let staging_bytes = buffer_bytes(&staging_buffers);
        self.peak_staging_bytes = self.peak_staging_bytes.max(staging_bytes);
        self.retain_arrow_eligible &= pending
            .iter()
            .flat_map(|pending| pending.batch.columns())
            .all(|column| column.null_count() == 0);
        self.coalesced |=
            pending.len() > 1 || pending.iter().any(|batch| batch.compaction_level > 0);
        let rows = pending
            .iter()
            .map(|pending| pending.batch.num_rows())
            .sum::<usize>();
        if self.retain_arrow {
            self.retained_estimated_bytes = self
                .retained_estimated_bytes
                .checked_add(estimated_bytes)
                .ok_or_else(|| error("retained Arrow byte estimate overflows", Status::Internal))?;
            self.retained_batches
                .extend(pending.into_iter().map(|pending| pending.batch));
            self.retained_buffers = pinned_record_batch_buffers(self.retained_batches.iter());
        } else {
            for index in 0..schema.fields().len() {
                let arrays = pending
                    .iter()
                    .map(|pending| pending.batch.column(index).as_ref())
                    .collect::<Vec<_>>();
                let column = &mut self.columns[index];
                let compressed_arena = &mut self.compressed_arena;
                let column_bytes = monetdb_arrow::encode_column_chunks(
                    &schema.fields()[index],
                    &arrays,
                    NonZeroUsize::new(ENCODE_CHUNK_BYTES).expect("encode chunk size is positive"),
                    |chunk| -> std::result::Result<(), CursorError> {
                        column.push(chunk, compressed_arena)
                    },
                )
                .map_err(map_chunked_encode_error)
                .map_err(map_cursor_error)?;
                column.finish(compressed_arena).map_err(map_cursor_error)?;
                column.bytes = column.bytes.checked_add(column_bytes).ok_or_else(|| {
                    error("encoded column byte count overflows", Status::Internal)
                })?;
                self.largest_column_bytes = self.largest_column_bytes.max(column.bytes);
                self.encoded_bytes =
                    self.encoded_bytes
                        .checked_add(column_bytes)
                        .ok_or_else(|| {
                            error("encoded ingest byte count overflows", Status::Internal)
                        })?;
            }
            let stored_bytes = self
                .columns
                .iter()
                .map(EncodedColumn::stored_bytes)
                .sum::<usize>();
            self.incompressible = self.wire_compression != WireCompression::Lz4
                && stored_bytes.saturating_mul(8) > self.encoded_bytes.saturating_mul(7);
            self.retain_arrow = self.incompressible && self.retain_arrow_eligible;
        }
        let build_buffers = merged_buffers(&self.retained_buffers, &staging_buffers);
        let build_bytes = self
            .encoded_stored_bytes()
            .saturating_add(buffer_bytes(&build_buffers));
        if build_bytes > self.peak_build_bytes {
            self.peak_build_bytes = build_bytes;
            self.peak_build_buffers = build_buffers;
        }
        self.rows = self
            .rows
            .checked_add(rows)
            .ok_or_else(|| error("ingest row count overflows", Status::Internal))?;
        self.estimated_bytes = self
            .estimated_bytes
            .checked_add(estimated_bytes)
            .ok_or_else(|| error("estimated ingest byte count overflows", Status::Internal))?;
        Ok(())
    }

    fn finish(&mut self) -> Result<()> {
        for column in &mut self.columns {
            column
                .finish(&mut self.compressed_arena)
                .map_err(map_cursor_error)?;
            self.largest_chunk = self.largest_chunk.max(column.largest_chunk());
        }
        Ok(())
    }

    fn stored_bytes(&self) -> usize {
        self.encoded_stored_bytes()
            .saturating_add(self.retained_estimated_bytes)
    }

    fn encoded_stored_bytes(&self) -> usize {
        self.columns.iter().map(EncodedColumn::stored_bytes).sum()
    }

    fn encoded_physical_stored_bytes(&self) -> usize {
        self.columns
            .iter()
            .flat_map(|column| &column.chunks)
            .map(|chunk| match chunk {
                EncodedChunk::Raw(data) => data.len(),
                EncodedChunk::Lz4 { .. } => 0,
            })
            .sum::<usize>()
            .saturating_add(self.compressed_arena.physical_bytes())
    }

    fn physical_stored_bytes(&self) -> usize {
        self.encoded_physical_stored_bytes()
            .saturating_add(buffer_bytes(&self.retained_buffers))
    }

    fn scratch_bytes(&self) -> usize {
        self.columns
            .iter()
            .flat_map(|column| &column.chunks)
            .map(|chunk| match chunk {
                EncodedChunk::Raw(_) => 0,
                EncodedChunk::Lz4 {
                    decoded_len,
                    transform,
                    ..
                } => match transform {
                    CompressionTransform::Plain => *decoded_len,
                    CompressionTransform::Shuffle(_) => decoded_len.saturating_mul(2),
                },
            })
            .max()
            .unwrap_or(0)
    }

    fn memory_usage(&self, wire_lz4: bool) -> WindowMemoryUsage {
        WindowMemoryUsage {
            encoded_stored_bytes: self.encoded_physical_stored_bytes(),
            retained_buffers: self.retained_buffers.clone(),
            peak_staging_bytes: self.peak_staging_bytes,
            peak_build_bytes: self.peak_build_bytes,
            peak_build_buffers: self.peak_build_buffers.clone(),
            scratch_bytes: if wire_lz4 { 0 } else { self.scratch_bytes() },
        }
    }

    fn storage_mode(&self) -> WindowStorage {
        let mut raw = 0usize;
        let mut lz4 = 0usize;
        for chunk in self.columns.iter().flat_map(|column| &column.chunks) {
            match chunk {
                EncodedChunk::Raw(data) => raw = raw.saturating_add(data.len()),
                EncodedChunk::Lz4 { len, .. } => lz4 = lz4.saturating_add(*len),
            }
        }
        [
            (raw, WindowStorage::Raw),
            (lz4, WindowStorage::Lz4),
            (self.retained_estimated_bytes, WindowStorage::Arrow),
        ]
        .into_iter()
        .max_by_key(|(bytes, _)| *bytes)
        .map(|(_, mode)| mode)
        .unwrap_or(WindowStorage::Raw)
    }

    fn uses_wire_lz4(&self) -> bool {
        match self.wire_compression {
            WireCompression::None => false,
            WireCompression::Lz4 => true,
            WireCompression::Auto => {
                self.retained_batches.is_empty()
                    && self.columns.iter().all(|column| {
                        !column.chunks.is_empty()
                            && column.chunks.iter().all(|chunk| {
                                matches!(
                                    chunk,
                                    EncodedChunk::Lz4 {
                                        transform: CompressionTransform::Plain,
                                        format: CompressionFormat::Frame,
                                        ..
                                    }
                                )
                            })
                    })
            }
        }
    }
}

fn should_finish_automatic_window(
    configured_rows: Option<usize>,
    physical_window_budget: usize,
    incompressible_window_budget: usize,
    window: &EncodedIngestWindow,
) -> bool {
    configured_rows.is_none()
        && (window.physical_stored_bytes() >= physical_window_budget
            || (window.incompressible && window.estimated_bytes >= incompressible_window_budget))
}

fn remaining_staging_bytes(
    configured_rows: Option<usize>,
    remaining_window_bytes: usize,
    incompressible_window_budget: usize,
    window: &EncodedIngestWindow,
) -> usize {
    if configured_rows.is_none() && window.incompressible {
        incompressible_window_budget
            .saturating_sub(window.estimated_bytes)
            .min(remaining_window_bytes)
    } else {
        remaining_window_bytes
    }
}

impl<'a> IngestWindowScheduler<'a> {
    fn new(
        reader: &'a mut (dyn RecordBatchReader + Send),
        schema: SchemaRef,
        window_budget: usize,
        physical_window_budget: usize,
        incompressible_window_budget: usize,
        configured_rows: Option<usize>,
        wire_compression: WireCompression,
    ) -> Self {
        Self {
            reader,
            schema,
            pending: VecDeque::new(),
            pending_rows: 0,
            pending_bytes: 0,
            exhausted: false,
            window_budget,
            physical_window_budget,
            incompressible_window_budget,
            configured_rows,
            wire_compression,
            input_batches: 0,
            coalesced_windows: 0,
            split_windows: 0,
        }
    }

    fn next_window(&mut self) -> Result<Option<EncodedIngestWindow>> {
        let mut window = EncodedIngestWindow::for_schema(&self.schema, self.wire_compression)?;
        loop {
            let remaining_rows = self
                .configured_rows
                .map(|rows| rows - window.rows)
                .unwrap_or(usize::MAX);
            if remaining_rows == 0 {
                break;
            }
            let remaining_bytes = if self.configured_rows.is_some() {
                PENDING_BATCH_COMPACTION_MAX_BYTES
            } else {
                self.window_budget.saturating_sub(window.estimated_bytes)
            };
            if remaining_bytes == 0 {
                break;
            }
            let remaining_staging_bytes = remaining_staging_bytes(
                self.configured_rows,
                remaining_bytes,
                self.incompressible_window_budget,
                &window,
            );
            if remaining_staging_bytes == 0 {
                break;
            }
            let staging_bytes = remaining_staging_bytes.min(ENCODE_STAGING_BYTES);
            self.fill_staging(remaining_rows, staging_bytes)?;
            if self.pending_rows == 0 {
                break;
            }
            let pending = self.take_staging(remaining_rows, staging_bytes, window.rows == 0)?;
            if pending.is_empty() {
                break;
            }
            let estimated_bytes = pending.iter().try_fold(0usize, |total, pending| {
                total
                    .checked_add(pending.estimated_bytes)
                    .ok_or_else(|| error("pending ingest byte count overflows", Status::Internal))
            })?;
            window.append(&self.schema, pending, estimated_bytes)?;
            if should_finish_automatic_window(
                self.configured_rows,
                self.physical_window_budget,
                self.incompressible_window_budget,
                &window,
            ) {
                break;
            }
        }
        if window.rows == 0 {
            return Ok(None);
        }
        window.finish()?;
        if window.coalesced {
            self.coalesced_windows += 1;
        }
        Ok(Some(window))
    }

    fn fill_staging(&mut self, desired_rows: usize, desired_bytes: usize) -> Result<()> {
        while self.pending_rows < desired_rows
            && self.pending_bytes < desired_bytes
            && !self.exhausted
        {
            self.read_batch()?;
        }
        Ok(())
    }

    fn take_staging(
        &mut self,
        target_rows: usize,
        target_bytes: usize,
        allow_oversized_row: bool,
    ) -> Result<Vec<PendingBatch>> {
        let mut remaining_rows = target_rows;
        let mut remaining_bytes = target_bytes;
        let mut staging = Vec::new();

        while remaining_rows > 0 {
            let Some(pending) = self.pop_front() else {
                break;
            };
            let rows_by_bytes = if pending.estimated_bytes <= remaining_bytes {
                pending.batch.num_rows()
            } else {
                self.rows_within_budget(&pending.batch, remaining_bytes)?
            };
            let rows = rows_by_bytes.min(remaining_rows);
            if rows == 0 && (!staging.is_empty() || !allow_oversized_row) {
                self.push_front(pending);
                break;
            }

            let rows = rows.max(1);
            let selected = if rows == pending.batch.num_rows() {
                pending
            } else {
                let (prefix, remainder) = self.split_pending(pending, rows)?;
                self.push_front(remainder);
                self.split_windows += 1;
                prefix
            };
            remaining_rows -= selected.batch.num_rows();
            remaining_bytes = remaining_bytes.saturating_sub(selected.estimated_bytes);
            staging.push(selected);
            if remaining_bytes == 0 {
                break;
            }
        }
        Ok(staging)
    }

    fn read_batch(&mut self) -> Result<()> {
        let Some(batch) = self.reader.next() else {
            self.exhausted = true;
            return Ok(());
        };
        let batch = batch.map_err(Error::from)?;
        self.input_batches += 1;
        validate_record_batch(&batch)?;
        if batch.schema() != self.schema {
            return Err(error(
                "record batch schema changed within ingest stream",
                Status::InvalidData,
            ));
        }
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let estimated_bytes = estimated_batch_size(&self.schema, &batch)?;
        self.push_back(PendingBatch {
            batch,
            estimated_bytes,
            compaction_level: 0,
        });
        self.compact_pending_batches()?;
        Ok(())
    }

    fn compact_pending_batches(&mut self) -> Result<()> {
        loop {
            if self.pending.len() < PENDING_BATCH_COMPACTION_FANOUT {
                return Ok(());
            }
            let level = self
                .pending
                .back()
                .expect("the compaction fanout guarantees a pending batch")
                .compaction_level;
            if !self
                .pending
                .iter()
                .rev()
                .take(PENDING_BATCH_COMPACTION_FANOUT)
                .all(|pending| pending.compaction_level == level)
            {
                return Ok(());
            }
            let estimated_bytes = self
                .pending
                .iter()
                .rev()
                .take(PENDING_BATCH_COMPACTION_FANOUT)
                .try_fold(0usize, |total, pending| {
                    total.checked_add(pending.estimated_bytes).ok_or_else(|| {
                        error("pending ingest byte count overflows", Status::Internal)
                    })
                })?;
            if estimated_bytes > PENDING_BATCH_COMPACTION_MAX_BYTES {
                return Ok(());
            }
            let compacted = self
                .pending
                .split_off(self.pending.len() - PENDING_BATCH_COMPACTION_FANOUT);
            let batch =
                concat_batches(&self.schema, compacted.iter().map(|pending| &pending.batch))
                    .map_err(Error::from)?;
            self.pending.push_back(PendingBatch {
                batch,
                estimated_bytes,
                compaction_level: level + 1,
            });
        }
    }

    fn rows_within_budget(&self, batch: &RecordBatch, budget: usize) -> Result<usize> {
        let mut low = 0usize;
        let mut high = batch.num_rows();
        while low < high {
            let middle = low + (high - low).div_ceil(2);
            let bytes = estimated_batch_size(&self.schema, &batch.slice(0, middle))?;
            if bytes <= budget {
                low = middle;
            } else {
                high = middle - 1;
            }
        }
        Ok(low)
    }

    fn split_pending(
        &self,
        pending: PendingBatch,
        rows: usize,
    ) -> Result<(PendingBatch, PendingBatch)> {
        let prefix = pending.batch.slice(0, rows);
        let remainder = pending.batch.slice(rows, pending.batch.num_rows() - rows);
        let prefix_bytes = estimated_batch_size(&self.schema, &prefix)?;
        let remainder_bytes = pending
            .estimated_bytes
            .checked_sub(prefix_bytes)
            .ok_or_else(|| error("pending ingest byte count underflows", Status::Internal))?;
        Ok((
            PendingBatch {
                batch: prefix,
                estimated_bytes: prefix_bytes,
                compaction_level: pending.compaction_level,
            },
            PendingBatch {
                batch: remainder,
                estimated_bytes: remainder_bytes,
                compaction_level: pending.compaction_level,
            },
        ))
    }

    fn pop_front(&mut self) -> Option<PendingBatch> {
        let pending = self.pending.pop_front()?;
        self.pending_rows -= pending.batch.num_rows();
        self.pending_bytes -= pending.estimated_bytes;
        Some(pending)
    }

    fn push_front(&mut self, pending: PendingBatch) {
        self.pending_rows += pending.batch.num_rows();
        self.pending_bytes += pending.estimated_bytes;
        self.pending.push_front(pending);
    }

    fn push_back(&mut self, pending: PendingBatch) {
        self.pending_rows += pending.batch.num_rows();
        self.pending_bytes += pending.estimated_bytes;
        self.pending.push_back(pending);
    }
}

fn estimated_batch_size(schema: &SchemaRef, batch: &RecordBatch) -> Result<usize> {
    schema
        .fields()
        .iter()
        .zip(batch.columns())
        .try_fold(0usize, |total, (field, column)| {
            let bytes = monetdb_arrow::estimated_encoded_size(field, column.as_ref())
                .map_err(|value| map_cursor_error(CursorError::FileTransfer(value.to_string())))?;
            total
                .checked_add(bytes)
                .ok_or_else(|| error("estimated ingest byte count overflows", Status::Internal))
        })
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum WindowStorage {
    Lz4,
    Raw,
    Arrow,
}

impl WindowStorage {
    fn as_str(self) -> &'static str {
        match self {
            Self::Lz4 => "lz4",
            Self::Raw => "raw",
            Self::Arrow => "arrow",
        }
    }
}

struct IngestStats {
    input_batches: usize,
    coalesced_windows: usize,
    split_windows: usize,
    copy_count: usize,
    target_copy_count: usize,
    staging_copy_count: usize,
    final_move_count: usize,
    encoded_bytes: usize,
    stored_bytes: usize,
    prepared_cache_hits: usize,
    peak_in_flight_bytes: usize,
    window_budget_bytes: usize,
    physical_window_budget_bytes: usize,
    incompressible_window_budget_bytes: usize,
    insert_rows_threshold: usize,
    measured_round_trip_us: u128,
    window_rows: Vec<usize>,
    window_bytes: Vec<usize>,
    window_physical_stored_bytes: Vec<usize>,
    window_staging_bytes: Vec<usize>,
    window_retained_arrow_pinned_bytes: Vec<usize>,
    window_scratch_bytes: Vec<usize>,
    window_storage: Vec<WindowStorage>,
    window_wire_compression: Vec<bool>,
    window_memory: Vec<WindowMemoryUsage>,
    path: &'static str,
    scope: &'static str,
    poisoned: bool,
}

struct IngestStreamingUsage {
    largest_chunk: usize,
    largest_column_bytes: usize,
}

struct CompletedIngestWindow {
    rows: usize,
    bytes: usize,
    stored_bytes: usize,
    storage: WindowStorage,
    wire_lz4: bool,
    memory: WindowMemoryUsage,
    streaming: IngestStreamingUsage,
}

struct WindowMemoryUsage {
    encoded_stored_bytes: usize,
    retained_buffers: HashMap<usize, usize>,
    peak_staging_bytes: usize,
    peak_build_bytes: usize,
    peak_build_buffers: HashMap<usize, usize>,
    scratch_bytes: usize,
}

impl WindowMemoryUsage {
    fn physical_stored_bytes(&self) -> usize {
        self.encoded_stored_bytes
            .saturating_add(buffer_bytes(&self.retained_buffers))
    }
}

impl IngestStats {
    fn new(
        window_budget_bytes: usize,
        physical_window_budget_bytes: usize,
        incompressible_window_budget_bytes: usize,
        insert_rows_threshold: usize,
        measured_round_trip: Duration,
        scope: &'static str,
    ) -> Self {
        Self {
            input_batches: 0,
            coalesced_windows: 0,
            split_windows: 0,
            copy_count: 0,
            target_copy_count: 0,
            staging_copy_count: 0,
            final_move_count: 0,
            encoded_bytes: 0,
            stored_bytes: 0,
            prepared_cache_hits: 0,
            peak_in_flight_bytes: 0,
            window_budget_bytes,
            physical_window_budget_bytes,
            incompressible_window_budget_bytes,
            insert_rows_threshold,
            measured_round_trip_us: measured_round_trip.as_micros(),
            window_rows: Vec::new(),
            window_bytes: Vec::new(),
            window_physical_stored_bytes: Vec::new(),
            window_staging_bytes: Vec::new(),
            window_retained_arrow_pinned_bytes: Vec::new(),
            window_scratch_bytes: Vec::new(),
            window_storage: Vec::new(),
            window_wire_compression: Vec::new(),
            window_memory: Vec::new(),
            path: "copy",
            scope,
            poisoned: false,
        }
    }

    fn record_window(&mut self, window: CompletedIngestWindow, staging: bool) {
        let CompletedIngestWindow {
            rows,
            bytes,
            stored_bytes,
            storage,
            wire_lz4,
            memory,
            streaming,
        } = window;
        self.copy_count += 1;
        if staging {
            self.staging_copy_count += 1;
        } else {
            self.target_copy_count += 1;
        }
        self.encoded_bytes = self.encoded_bytes.saturating_add(bytes);
        self.stored_bytes = self.stored_bytes.saturating_add(stored_bytes);
        let upload_message_bytes = streaming
            .largest_column_bytes
            .min(DEFAULT_UPLOAD_CHUNK_SIZE_BYTES);
        let framing_bytes =
            2usize.saturating_mul(upload_message_bytes.div_ceil(monetdb::MAPI_BLOCK_SIZE_BYTES));
        let streaming_bytes = streaming
            .largest_chunk
            .saturating_add(upload_message_bytes)
            .saturating_add(framing_bytes);
        self.peak_in_flight_bytes = self.peak_in_flight_bytes.max(streaming_bytes);
        self.window_rows.push(rows);
        self.window_bytes.push(bytes);
        self.window_physical_stored_bytes
            .push(memory.physical_stored_bytes());
        self.window_staging_bytes.push(memory.peak_staging_bytes);
        self.window_retained_arrow_pinned_bytes
            .push(buffer_bytes(&memory.retained_buffers));
        self.window_scratch_bytes
            .push(memory.scratch_bytes.saturating_add(streaming_bytes));
        self.window_storage.push(storage);
        self.window_wire_compression.push(wire_lz4);
        self.window_memory.push(memory);
    }

    fn to_json(&self) -> String {
        let rows = self
            .window_rows
            .iter()
            .map(usize::to_string)
            .collect::<Vec<_>>()
            .join(",");
        let bytes = self
            .window_bytes
            .iter()
            .map(usize::to_string)
            .collect::<Vec<_>>()
            .join(",");
        let physical_stored_bytes = self
            .window_physical_stored_bytes
            .iter()
            .map(usize::to_string)
            .collect::<Vec<_>>()
            .join(",");
        let staging_bytes = self
            .window_staging_bytes
            .iter()
            .map(usize::to_string)
            .collect::<Vec<_>>()
            .join(",");
        let retained_arrow_pinned_bytes = self
            .window_retained_arrow_pinned_bytes
            .iter()
            .map(usize::to_string)
            .collect::<Vec<_>>()
            .join(",");
        let scratch_bytes = self
            .window_scratch_bytes
            .iter()
            .map(usize::to_string)
            .collect::<Vec<_>>()
            .join(",");
        let storage = self
            .window_storage
            .iter()
            .map(|mode| format!("\"{}\"", mode.as_str()))
            .collect::<Vec<_>>()
            .join(",");
        let wire_compression = self
            .window_wire_compression
            .iter()
            .map(|enabled| if *enabled { "\"lz4\"" } else { "\"none\"" })
            .collect::<Vec<_>>()
            .join(",");
        let (peak_window_physical_bytes, peak_prefetch_physical_bytes) = self.physical_peaks();
        format!(
            concat!(
                "{{\"input_batches\":{},\"coalesced_windows\":{},\"split_windows\":{},",
                "\"copy_count\":{},\"target_copy_count\":{},\"staging_copy_count\":{},",
                "\"final_move_count\":{},\"encoded_bytes\":{},\"stored_bytes\":{},",
                "\"prepared_cache_hits\":{},",
                "\"peak_in_flight_bytes\":{},\"window_budget_bytes\":{},",
                "\"physical_window_budget_bytes\":{},\"incompressible_window_budget_bytes\":{},",
                "\"insert_rows_threshold\":{},\"measured_round_trip_us\":{},",
                "\"peak_window_physical_bytes\":{},\"peak_prefetch_physical_bytes\":{},",
                "\"window_rows\":[{}],\"window_bytes\":[{}],",
                "\"window_physical_stored_bytes\":[{}],\"window_staging_bytes\":[{}],",
                "\"window_retained_arrow_pinned_bytes\":[{}],\"window_scratch_bytes\":[{}],",
                "\"window_storage\":[{}],",
                "\"window_wire_compression\":[{}],",
                "\"path\":\"{}\",\"scope\":\"{}\",\"poisoned\":{}}}"
            ),
            self.input_batches,
            self.coalesced_windows,
            self.split_windows,
            self.copy_count,
            self.target_copy_count,
            self.staging_copy_count,
            self.final_move_count,
            self.encoded_bytes,
            self.stored_bytes,
            self.prepared_cache_hits,
            self.peak_in_flight_bytes,
            self.window_budget_bytes,
            self.physical_window_budget_bytes,
            self.incompressible_window_budget_bytes,
            self.insert_rows_threshold,
            self.measured_round_trip_us,
            peak_window_physical_bytes,
            peak_prefetch_physical_bytes,
            rows,
            bytes,
            physical_stored_bytes,
            staging_bytes,
            retained_arrow_pinned_bytes,
            scratch_bytes,
            storage,
            wire_compression,
            self.path,
            self.scope,
            self.poisoned,
        )
    }

    fn physical_peaks(&self) -> (usize, usize) {
        let mut peak_window = 0usize;
        let mut peak_prefetch = 0usize;
        for (index, memory) in self.window_memory.iter().enumerate() {
            let upload_bytes = memory
                .physical_stored_bytes()
                .saturating_add(self.window_scratch_bytes[index]);
            let build_bytes = memory.peak_build_bytes.saturating_add(memory.scratch_bytes);
            peak_window = peak_window.max(upload_bytes).max(build_bytes);
            peak_prefetch = peak_prefetch.max(upload_bytes).max(build_bytes);

            let Some(next) = self.window_memory.get(index + 1) else {
                continue;
            };
            let live_buffers = merged_buffers(&memory.retained_buffers, &next.peak_build_buffers);
            let current_fixed = memory
                .encoded_stored_bytes
                .saturating_add(self.window_scratch_bytes[index]);
            let next_fixed = next
                .peak_build_bytes
                .saturating_sub(buffer_bytes(&next.peak_build_buffers))
                .saturating_add(next.scratch_bytes);
            let overlap = current_fixed
                .saturating_add(next_fixed)
                .saturating_add(buffer_bytes(&live_buffers));
            peak_prefetch = peak_prefetch.max(overlap);
        }
        (peak_window, peak_prefetch)
    }
}

fn adaptive_insert_rows(configured_rows: usize, measured_round_trip: Duration) -> usize {
    if configured_rows == 0 || measured_round_trip < ADAPTIVE_NETWORK_RTT {
        return configured_rows;
    }
    let measured_crossover = measured_round_trip
        .as_micros()
        .saturating_mul(COPY_FILE_EXCHANGES_PER_COLUMN)
        / INSERT_VALUE_ENCODE_MICROS;
    configured_rows.max(
        usize::try_from(measured_crossover)
            .unwrap_or(usize::MAX)
            .min(MAX_ADAPTIVE_INSERT_ROWS),
    )
}

fn adaptive_incompressible_window_bytes(
    physical_window_budget: usize,
    measured_round_trip: Duration,
) -> usize {
    if measured_round_trip >= REMOTE_WRITE_WINDOW_RTT {
        physical_window_budget
    } else {
        INCOMPRESSIBLE_WRITE_WINDOW_BYTES.min(physical_window_budget)
    }
}

fn adaptive_physical_window_bytes(
    window_budget: usize,
    measured_round_trip: Duration,
    columns: usize,
) -> usize {
    if measured_round_trip >= REMOTE_WRITE_WINDOW_RTT {
        window_budget
    } else {
        let local_budget = if columns >= WIDE_MEMORY_BOUND_COLUMNS {
            WIDE_LOCAL_PHYSICAL_WINDOW_BYTES
        } else {
            LOCAL_PHYSICAL_WINDOW_BYTES
        };
        local_budget.min(window_budget)
    }
}

fn automatic_write_window_bytes() -> usize {
    automatic_write_window_bytes_for_memory(system_memory_limit_bytes())
}

fn automatic_read_window_bytes(measured_round_trip: Duration) -> usize {
    automatic_read_window_bytes_for_memory(system_memory_limit_bytes(), measured_round_trip)
}

fn effective_read_window_bytes(
    configured_window_bytes: Option<usize>,
    measured_round_trip: Duration,
    fixed_granule_capacity: Option<usize>,
) -> usize {
    let base =
        configured_window_bytes.unwrap_or_else(|| automatic_read_window_bytes(measured_round_trip));
    if configured_window_bytes.is_some() {
        return base;
    }
    effective_automatic_read_window_bytes(base, fixed_granule_capacity, system_memory_limit_bytes())
}

fn effective_automatic_read_window_bytes(
    base: usize,
    fixed_granule_capacity: Option<usize>,
    memory_limit: Option<usize>,
) -> usize {
    let maximum = memory_limit.map_or(MAX_AUTOMATIC_READ_GRANULE_BYTES, |limit| {
        MAX_AUTOMATIC_READ_GRANULE_BYTES.min((limit / 8).max(MIN_READ_WINDOW_BYTES))
    });
    fixed_granule_capacity
        .filter(|capacity| *capacity <= maximum)
        .map_or(base, |capacity| base.max(capacity))
}

fn automatic_read_window_bytes_for_memory(
    memory_limit: Option<usize>,
    measured_round_trip: Duration,
) -> usize {
    let target = if measured_round_trip >= REMOTE_WRITE_WINDOW_RTT {
        REMOTE_READ_WINDOW_BYTES
    } else {
        DEFAULT_READ_WINDOW_BYTES
    };
    memory_limit.map_or(target, |limit| {
        target.min((limit / 8).max(MIN_READ_WINDOW_BYTES))
    })
}

fn automatic_write_window_bytes_for_memory(memory_limit: Option<usize>) -> usize {
    let target = DEFAULT_WRITE_WINDOW_BYTES;
    let divisor = 8;
    memory_limit.map_or(target, |limit| {
        target.min((limit / divisor).max(MIN_WRITE_WINDOW_BYTES))
    })
}

fn system_memory_limit_bytes() -> Option<usize> {
    [cgroup_memory_limit_bytes(), total_system_memory_bytes()]
        .into_iter()
        .flatten()
        .min()
}

fn cgroup_memory_limit_bytes() -> Option<usize> {
    [
        Path::new("/sys/fs/cgroup/memory.max"),
        Path::new("/sys/fs/cgroup/memory/memory.limit_in_bytes"),
    ]
    .into_iter()
    .filter_map(|path| std::fs::read_to_string(path).ok())
    .filter_map(|value| value.trim().parse::<usize>().ok())
    .find(|limit| *limit > 0 && *limit < (1usize << (usize::BITS - 2)))
}

fn cgroup_memory_limit_and_usage_bytes() -> Option<(usize, usize)> {
    let limit = cgroup_memory_limit_bytes()?;
    let current = [
        Path::new("/sys/fs/cgroup/memory.current"),
        Path::new("/sys/fs/cgroup/memory/memory.usage_in_bytes"),
    ]
    .into_iter()
    .filter_map(|path| std::fs::read_to_string(path).ok())
    .filter_map(|value| value.trim().parse::<usize>().ok())
    .next()?;
    Some((limit, current))
}

#[cfg(unix)]
fn total_system_memory_bytes() -> Option<usize> {
    // SAFETY: sysconf reads immutable process-wide system configuration.
    let pages = unsafe { libc::sysconf(libc::_SC_PHYS_PAGES) };
    // SAFETY: sysconf reads immutable process-wide system configuration.
    let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    if pages <= 0 || page_size <= 0 {
        return None;
    }
    usize::try_from(pages)
        .ok()?
        .checked_mul(usize::try_from(page_size).ok()?)
}

#[cfg(not(unix))]
fn total_system_memory_bytes() -> Option<usize> {
    None
}

fn validate_record_batch(batch: &RecordBatch) -> Result<()> {
    for (index, column) in batch.columns().iter().enumerate() {
        column.to_data().validate_full().map_err(|value| {
            error(
                format!("invalid Arrow data in column {index}: {value}"),
                Status::InvalidData,
            )
        })?;
    }
    Ok(())
}

#[derive(Debug, PartialEq, Eq)]
struct AppendColumn {
    name: String,
    data_type: MonetType,
    nullable: bool,
}

fn append_table_columns(
    cursor: &mut monetdb::Cursor,
    schema_name: Option<&str>,
    table_name: &str,
    mismatch_status: Status,
) -> Result<Vec<AppendColumn>> {
    let table = metadata::raw_string_literal(table_name)?;
    let (columns_catalog, tables_catalog, schema_join, table_selector) = match schema_name {
        Some("tmp") => (
            "tmp._columns",
            "tmp._tables",
            "",
            format!("t.name = {table}"),
        ),
        Some(schema) => (
            "sys._columns",
            "sys._tables",
            "JOIN sys.schemas AS s ON s.id = t.schema_id",
            format!(
                "t.name = {table} AND s.name = {}",
                metadata::raw_string_literal(schema)?
            ),
        ),
        None => (
            "sys._columns",
            "sys._tables",
            "JOIN sys.schemas AS s ON s.id = t.schema_id",
            format!("t.name = {table} AND s.name = current_schema"),
        ),
    };
    cursor
        .execute(&format!(
            "SELECT c.name, c.type, c.type_digits, c.type_scale, c.\"null\", \
                    tt.table_type_name \
             FROM {columns_catalog} AS c \
             JOIN {tables_catalog} AS t ON t.id = c.table_id \
             {schema_join} \
             JOIN sys.table_types AS tt ON tt.table_type_id = t.type \
             WHERE {table_selector} \
             ORDER BY c.number"
        ))
        .map_err(map_cursor_error)?;
    let mut columns = Vec::new();
    let mut table_type = None;
    while cursor.next_row().map_err(map_cursor_error)? {
        let name = cursor
            .get_str(0)
            .map_err(map_cursor_error)?
            .ok_or_else(|| error("catalog column name is NULL", Status::InvalidData))?;
        let code = cursor
            .get_str(1)
            .map_err(map_cursor_error)?
            .ok_or_else(|| error("catalog column type is NULL", Status::InvalidData))?;
        let digits = cursor
            .get::<i32>(2)
            .map_err(map_cursor_error)?
            .ok_or_else(|| error("catalog type digits are NULL", Status::InvalidData))?;
        let scale = cursor
            .get::<i32>(3)
            .map_err(map_cursor_error)?
            .ok_or_else(|| error("catalog type scale is NULL", Status::InvalidData))?;
        let nullable = cursor
            .get::<bool>(4)
            .map_err(map_cursor_error)?
            .ok_or_else(|| error("catalog nullability is NULL", Status::InvalidData))?;
        let current_type = cursor
            .get_str(5)
            .map_err(map_cursor_error)?
            .ok_or_else(|| error("catalog table type is NULL", Status::InvalidData))?;
        table_type.get_or_insert_with(|| current_type.to_owned());
        columns.push(AppendColumn {
            name: name.to_owned(),
            data_type: prepared_monet_type(code, digits, scale)?,
            nullable,
        });
    }
    let Some(table_type) = table_type else {
        return Err(error(
            format!("append target table {table_name:?} does not exist"),
            Status::NotFound,
        ));
    };
    if !matches!(
        table_type.as_str(),
        "TABLE" | "UNLOGGED TABLE" | "GLOBAL TEMPORARY TABLE" | "LOCAL TEMPORARY TABLE"
    ) {
        return Err(error(
            format!("append target {table_name:?} is a {table_type}, not a writable table"),
            mismatch_status,
        ));
    }
    Ok(columns)
}

fn align_append_schema(
    schema: &SchemaRef,
    columns: &[AppendColumn],
    mismatch_status: Status,
) -> Result<Vec<String>> {
    let mut matched = vec![false; columns.len()];
    let mut targets = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        let Some(index) = append_column_index(field.name(), columns) else {
            return Err(error(
                format!(
                    "append column {:?} does not exist in the destination table; it has columns {}",
                    field.name(),
                    columns
                        .iter()
                        .map(|column| format!("{:?}", column.name))
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
                mismatch_status,
            ));
        };
        if std::mem::replace(&mut matched[index], true) {
            return Err(error(
                format!(
                    "append stream names destination column {:?} more than once",
                    columns[index].name
                ),
                mismatch_status,
            ));
        }
        let source = monetdb_arrow::monet_type_for_field(field)
            .map_err(|value| map_display(value, Status::NotImplemented))?;
        let destination = &columns[index].data_type;
        if !append_monet_types_match(&source, destination) {
            return Err(error(
                format!(
                    "append column {:?} has Arrow/MonetDB type {source}, but destination type is {destination}",
                    field.name()
                ),
                mismatch_status,
            ));
        }
        targets.push(columns[index].name.clone());
    }
    Ok(targets)
}

fn quoted_column_list(names: &[String]) -> Result<String> {
    Ok(names
        .iter()
        .map(|name| quote_identifier(name))
        .collect::<Result<Vec<_>>>()?
        .join(", "))
}

fn validate_prepared_insert_schema(
    source: &SchemaRef,
    destination: &Schema,
    mismatch_status: Status,
) -> Result<()> {
    if source.fields().len() != destination.fields().len() {
        return Err(error(
            format!(
                "append stream has {} columns but destination table has {}",
                source.fields().len(),
                destination.fields().len()
            ),
            mismatch_status,
        ));
    }
    for (field, parameter) in source.fields().iter().zip(destination.fields()) {
        let source_type = monetdb_arrow::monet_type_for_field(field)
            .map_err(|value| map_display(value, Status::NotImplemented))?;
        let destination_type = monetdb_arrow::monet_type_for_field(parameter)
            .map_err(|value| map_display(value, Status::InvalidData))?;
        if !append_monet_types_match(&source_type, &destination_type) {
            return Err(error(
                format!(
                    "append column {:?} has Arrow/MonetDB type {source_type}, but destination type is {destination_type}",
                    field.name()
                ),
                mismatch_status,
            ));
        }
    }
    Ok(())
}

fn append_monet_types_match(source: &MonetType, destination: &MonetType) -> bool {
    let string_wire = |data_type: &MonetType| {
        matches!(
            data_type,
            MonetType::Varchar(_) | MonetType::Url | MonetType::Json
        )
    };
    source == destination || (string_wire(source) && string_wire(destination))
}

fn append_column_index(source: &str, columns: &[AppendColumn]) -> Option<usize> {
    columns
        .iter()
        .position(|column| source == column.name)
        .or_else(|| {
            columns
                .iter()
                .position(|column| source.eq_ignore_ascii_case(&column.name))
        })
}

#[derive(Clone)]
struct PreparedPlan {
    query: Arc<str>,
    parameter_count: usize,
    entry: PreparedSlot,
    cache: SharedPreparedCache,
}

#[derive(Clone)]
struct PreparedInvocation {
    plan: PreparedPlan,
    entry: Arc<PreparedEntry>,
    arguments: String,
}

#[derive(Clone)]
struct BoundQuery {
    sql: String,
    prepared: Option<PreparedInvocation>,
}

impl BoundQuery {
    fn prepared(plan: PreparedPlan, entry: Arc<PreparedEntry>, arguments: String) -> Self {
        Self {
            sql: format!("EXECUTE {}({arguments})", entry.id),
            prepared: Some(PreparedInvocation {
                plan,
                entry,
                arguments,
            }),
        }
    }
}

struct BoundQueryStream {
    reader: Box<dyn RecordBatchReader + Send>,
    schema: SchemaRef,
    template: QueryTemplate,
    prepared: Option<PreparedPlan>,
    bind_by_name: bool,
    batch: Option<RecordBatch>,
    next_row: usize,
    pending: Option<BoundQuery>,
    finished: bool,
}

impl BoundQueryStream {
    fn is_empty(&mut self) -> Result<bool> {
        match self.next().transpose()? {
            Some(query) => {
                self.pending = Some(query);
                Ok(false)
            }
            None => Ok(true),
        }
    }
}

impl Iterator for BoundQueryStream {
    type Item = Result<BoundQuery>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(query) = self.pending.take() {
            return Some(Ok(query));
        }
        if self.finished {
            return None;
        }
        loop {
            if let Some(batch) = &self.batch
                && self.next_row < batch.num_rows()
            {
                let row = self.next_row;
                self.next_row += 1;
                let query = match &self.prepared {
                    Some(plan) => render_arguments(batch, row).map(|values| {
                        let entry = Arc::clone(
                            &plan
                                .entry
                                .lock()
                                .unwrap_or_else(std::sync::PoisonError::into_inner),
                        );
                        BoundQuery::prepared(plan.clone(), entry, values.join(", "))
                    }),
                    None => self
                        .template
                        .render_row(batch, row, self.bind_by_name)
                        .map(|sql| BoundQuery {
                            sql,
                            prepared: None,
                        }),
                };
                return Some(query);
            }
            match self.reader.next() {
                Some(Ok(batch)) if batch.schema() != self.schema => {
                    self.finished = true;
                    return Some(Err(error(
                        "parameter schema changed within the bound stream",
                        Status::InvalidData,
                    )));
                }
                Some(Ok(batch)) => {
                    if let Err(value) = validate_record_batch(&batch) {
                        self.finished = true;
                        return Some(Err(value));
                    }
                    self.batch = Some(batch);
                    self.next_row = 0;
                }
                Some(Err(value)) => {
                    self.finished = true;
                    return Some(Err(Error::from(value)));
                }
                None => {
                    self.finished = true;
                    return None;
                }
            }
        }
    }
}

fn prepared_statement_missing(value: &Error) -> bool {
    value.sqlstate.map(|value| value as u8) == *b"42000"
        && value.message.contains("EXEC: PREPARED Statement missing")
}

fn current_bound_query(
    connection: &SharedConnection,
    query: &BoundQuery,
    timeouts: Timeouts,
) -> Result<BoundQuery> {
    let Some(invocation) = &query.prepared else {
        return Ok(query.clone());
    };
    let cached = invocation
        .plan
        .cache
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .get(&invocation.plan.query);
    let entry = match cached {
        Some(entry) => entry,
        None => prepare_cached(
            connection,
            &invocation.plan.cache,
            &invocation.plan.query,
            invocation.plan.parameter_count,
            timeouts,
        )?,
    };
    if entry.id != invocation.entry.id {
        *invocation
            .plan
            .entry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Arc::clone(&entry);
    }
    Ok(BoundQuery::prepared(
        invocation.plan.clone(),
        entry,
        invocation.arguments.clone(),
    ))
}

fn retry_bound_query(
    connection: &SharedConnection,
    query: &BoundQuery,
    timeouts: Timeouts,
) -> Result<BoundQuery> {
    let invocation = query.prepared.as_ref().ok_or_else(|| {
        error(
            "cannot recover an unprepared parameter query",
            Status::Internal,
        )
    })?;
    let current = Arc::clone(
        &invocation
            .plan
            .entry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner),
    );
    let entry = if current.id != invocation.entry.id {
        current
    } else {
        invocation
            .plan
            .cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove_if_id(&invocation.plan.query, invocation.entry.id);
        let refreshed = prepare_cached(
            connection,
            &invocation.plan.cache,
            &invocation.plan.query,
            invocation.plan.parameter_count,
            timeouts,
        )?;
        let mut slot = invocation
            .plan
            .entry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if slot.id == invocation.entry.id {
            *slot = Arc::clone(&refreshed);
            refreshed
        } else {
            Arc::clone(&slot)
        }
    };
    Ok(BoundQuery::prepared(
        invocation.plan.clone(),
        entry,
        invocation.arguments.clone(),
    ))
}

enum InfoValue {
    String(Option<String>),
    Bool(bool),
    Int(i64),
}

fn info_value(code: InfoCode, version: (u16, u16, u16)) -> InfoValue {
    match code {
        InfoCode::VendorName => InfoValue::String(Some("MonetDB".into())),
        InfoCode::VendorVersion => {
            InfoValue::String(Some(format!("{}.{}.{}", version.0, version.1, version.2)))
        }
        InfoCode::VendorArrowVersion => InfoValue::String(None),
        InfoCode::VendorSql => InfoValue::Bool(true),
        InfoCode::VendorSubstrait => InfoValue::Bool(false),
        InfoCode::VendorSubstraitMinVersion | InfoCode::VendorSubstraitMaxVersion => {
            InfoValue::String(None)
        }
        InfoCode::DriverName => InfoValue::String(Some("adbc-driver-monetdb".into())),
        InfoCode::DriverVersion => InfoValue::String(Some(env!("CARGO_PKG_VERSION").into())),
        InfoCode::DriverArrowVersion => {
            InfoValue::String(Some(env!("ADBC_MONETDB_ARROW_VERSION").into()))
        }
        InfoCode::DriverAdbcVersion => InfoValue::Int(1_001_000),
        _ => InfoValue::String(None),
    }
}

fn info_batch(version: (u16, u16, u16), codes: Option<HashSet<InfoCode>>) -> Result<RecordBatch> {
    let mut codes = match codes {
        Some(codes) => codes.into_iter().collect::<Vec<_>>(),
        None => vec![
            InfoCode::VendorName,
            InfoCode::VendorVersion,
            InfoCode::VendorArrowVersion,
            InfoCode::VendorSql,
            InfoCode::VendorSubstrait,
            InfoCode::VendorSubstraitMinVersion,
            InfoCode::VendorSubstraitMaxVersion,
            InfoCode::DriverName,
            InfoCode::DriverVersion,
            InfoCode::DriverArrowVersion,
            InfoCode::DriverAdbcVersion,
        ],
    };
    codes.sort_by_key(|code| u32::from(code));

    let mut names = Vec::with_capacity(codes.len());
    let mut type_ids = Vec::with_capacity(codes.len());
    let mut offsets = Vec::with_capacity(codes.len());
    let mut strings = Vec::new();
    let mut bools = Vec::new();
    let mut ints = Vec::new();
    for code in codes {
        names.push(u32::from(&code));
        let (type_id, offset) = match info_value(code, version) {
            InfoValue::String(value) => {
                strings.push(value);
                (0, strings.len() - 1)
            }
            InfoValue::Bool(value) => {
                bools.push(Some(value));
                (1, bools.len() - 1)
            }
            InfoValue::Int(value) => {
                ints.push(Some(value));
                (2, ints.len() - 1)
            }
        };
        type_ids.push(type_id);
        offsets.push(
            i32::try_from(offset)
                .map_err(|_| error("get_info union offset exceeds i32", Status::Internal))?,
        );
    }

    let DataType::Union(fields, _) = GET_INFO_SCHEMA.field(1).data_type() else {
        return Err(error("invalid canonical get_info schema", Status::Internal));
    };
    let string_child: ArrayRef = Arc::new(StringArray::from(strings));
    let bool_child: ArrayRef = Arc::new(BooleanArray::from(bools));
    let int_child: ArrayRef = Arc::new(Int64Array::from(ints));
    let children = fields
        .iter()
        .map(|(type_id, field)| match type_id {
            0 => Arc::clone(&string_child),
            1 => Arc::clone(&bool_child),
            2 => Arc::clone(&int_child),
            _ => new_empty_array(field.data_type()),
        })
        .collect();
    let values = UnionArray::try_new(
        fields.clone(),
        type_ids.into(),
        Some(offsets.into()),
        children,
    )?;
    Ok(RecordBatch::try_new(
        GET_INFO_SCHEMA.clone(),
        vec![Arc::new(UInt32Array::from(names)), Arc::new(values)],
    )?)
}

struct PreparedMetadata {
    id: u64,
    parameters: Schema,
    result: Schema,
}

fn prepare_cached(
    connection: &SharedConnection,
    cache: &SharedPreparedCache,
    query: &str,
    parameter_count: usize,
    timeouts: Timeouts,
) -> Result<Arc<PreparedEntry>> {
    prepare_cached_with_status(connection, cache, query, parameter_count, timeouts)
        .map(|(entry, _)| entry)
}

fn prepare_cached_with_status(
    connection: &SharedConnection,
    cache: &SharedPreparedCache,
    query: &str,
    parameter_count: usize,
    timeouts: Timeouts,
) -> Result<(Arc<PreparedEntry>, bool)> {
    let query = normalize_prepared_query(query);
    if let Some(entry) = cache
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .get(query)
    {
        return Ok((entry, true));
    }
    let metadata = prepare_query(connection, query, parameter_count, timeouts)?;
    let candidate = Arc::new(PreparedEntry::new(metadata, connection));
    Ok((
        cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(query.to_owned(), candidate),
        false,
    ))
}

fn prepare_cached_with_status_locked(
    shared_connection: &SharedConnection,
    connection: &monetdb::Connection,
    cache: &SharedPreparedCache,
    query: &str,
    parameter_count: usize,
    timeouts: Timeouts,
) -> Result<(Arc<PreparedEntry>, bool)> {
    let query = normalize_prepared_query(query);
    if let Some(entry) = cache
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .get(query)
    {
        return Ok((entry, true));
    }
    let metadata = prepare_query_inner_locked(connection, query, parameter_count, false, timeouts)?;
    let candidate = Arc::new(PreparedEntry::new(metadata, shared_connection));
    Ok((
        cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(query.to_owned(), candidate),
        false,
    ))
}

struct PreparedField {
    data_type: MonetType,
    undetermined: bool,
    name: Option<String>,
    origin_schema: Option<String>,
    origin_table: Option<String>,
}

fn prepare_query(
    connection: &SharedConnection,
    query: &str,
    parameter_count: usize,
    timeouts: Timeouts,
) -> Result<PreparedMetadata> {
    prepare_query_inner(connection, query, parameter_count, false, timeouts)
}

fn prepare_query_allowing_any(
    connection: &SharedConnection,
    query: &str,
    parameter_count: usize,
    timeouts: Timeouts,
) -> Result<PreparedMetadata> {
    prepare_query_inner(connection, query, parameter_count, true, timeouts)
}

fn prepare_query_inner(
    connection: &SharedConnection,
    query: &str,
    parameter_count: usize,
    allow_any: bool,
    timeouts: Timeouts,
) -> Result<PreparedMetadata> {
    let query = normalize_prepared_query(query);
    let connection = lock_connection(connection)?;
    prepare_query_inner_locked(&connection, query, parameter_count, allow_any, timeouts)
}

fn prepare_query_inner_locked(
    connection: &monetdb::Connection,
    query: &str,
    parameter_count: usize,
    allow_any: bool,
    timeouts: Timeouts,
) -> Result<PreparedMetadata> {
    let use_savepoint = !connection
        .server_info()
        .map_err(map_cursor_error)?
        .autocommit;
    let savepoint = use_savepoint.then(|| savepoint_name("prepare_probe"));
    let mut cursor = connection.cursor();
    cursor.set_timeouts(timeouts);
    cursor.set_reply_size(NonZeroUsize::new(METADATA_REPLY_ROWS).expect("constant is positive"));
    if let Some(savepoint) = &savepoint {
        cursor
            .execute(&format!("SAVEPOINT {savepoint}"))
            .map_err(map_cursor_error)?;
    }
    if let Err(root_cause) = cursor.execute(&format!("PREPARE {query}")) {
        if let Some(savepoint) = &savepoint {
            let recovery = cursor
                .execute(&format!("ROLLBACK TO SAVEPOINT {savepoint}"))
                .and_then(|_| cursor.execute(&format!("RELEASE SAVEPOINT {savepoint}")));
            return combine_atomic_error(
                Err(map_cursor_error(root_cause)),
                recovery,
                "restoring the transaction after PREPARE",
            );
        }
        return Err(map_cursor_error(root_cause));
    }
    let id = cursor.prepared_statement_id().ok_or_else(|| {
        error(
            "MonetDB did not return prepared statement metadata",
            Status::InvalidData,
        )
    })?;
    let parsed = (|| {
        let mut fields = Vec::new();
        while cursor.next_row().map_err(map_cursor_error)? {
            let code = cursor
                .get_str(0)
                .map_err(map_cursor_error)?
                .ok_or_else(|| error("prepared type is NULL", Status::InvalidData))?
                .to_owned();
            let digits = cursor
                .get::<i32>(1)
                .map_err(map_cursor_error)?
                .ok_or_else(|| error("prepared type digits are NULL", Status::InvalidData))?;
            let scale = cursor
                .get::<i32>(2)
                .map_err(map_cursor_error)?
                .ok_or_else(|| error("prepared type scale is NULL", Status::InvalidData))?;
            let name = cursor
                .get_str(5)
                .map_err(map_cursor_error)?
                .map(str::to_owned);
            let origin_schema = cursor
                .get_str(3)
                .map_err(map_cursor_error)?
                .filter(|value| !value.is_empty())
                .map(str::to_owned);
            let origin_table = cursor
                .get_str(4)
                .map_err(map_cursor_error)?
                .filter(|value| !value.is_empty())
                .map(str::to_owned);
            let provisional = allow_any
                && (code == "any"
                    || code.eq_ignore_ascii_case("decimal")
                        && (digits <= 0 || scale < 0 || scale > digits));
            fields.push(PreparedField {
                undetermined: provisional,
                data_type: if provisional {
                    MonetType::Varchar(0)
                } else {
                    prepared_monet_type(&code, digits, scale)?
                },
                name,
                origin_schema,
                origin_table,
            });
        }
        let result_count = fields.len().checked_sub(parameter_count).ok_or_else(|| {
            error(
                format!(
                    "PREPARE returned {} metadata rows for {parameter_count} parameters",
                    fields.len()
                ),
                Status::InvalidData,
            )
        })?;
        restore_declared_result_types(&mut cursor, &mut fields[..result_count])?;
        let parameter_fields = fields
            .drain(result_count..)
            .enumerate()
            .map(|(index, field)| {
                prepared_parameter_arrow_field(index.to_string(), &field.data_type)
            })
            .collect::<Result<Vec<_>>>()?;
        let result_fields = fields
            .into_iter()
            .enumerate()
            .map(|(index, field)| {
                let name = field
                    .name
                    .filter(|name| !name.is_empty())
                    .unwrap_or_else(|| format!("column_{index}"));
                if field.undetermined {
                    Ok(Field::new(name, DataType::Null, true))
                } else {
                    prepared_arrow_field(name, &field.data_type)
                }
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(PreparedMetadata {
            id,
            parameters: Schema::new(parameter_fields),
            result: Schema::new(result_fields),
        })
    })();
    if parsed.is_err() {
        let _ = cursor.execute(&format!("DEALLOCATE {id}"));
    }
    let release = match &savepoint {
        Some(savepoint) => cursor.execute(&format!("RELEASE SAVEPOINT {savepoint}")),
        None => Ok(()),
    };
    combine_atomic_error(parsed, release, "releasing the PREPARE savepoint")
}

fn normalize_prepared_query(query: &str) -> &str {
    query.trim().trim_end_matches(';').trim_end()
}

fn restore_declared_result_types(
    cursor: &mut monetdb::Cursor,
    fields: &mut [PreparedField],
) -> Result<()> {
    let table_names = fields
        .iter()
        .filter_map(|field| field.origin_table.as_deref())
        .collect::<HashSet<_>>();
    if table_names.is_empty() {
        return Ok(());
    }
    let table_names = table_names
        .into_iter()
        .map(metadata::raw_string_literal)
        .collect::<Result<Vec<_>>>()?;
    cursor
        .execute(&format!(
            "SELECT s.name, t.name, c.name, c.type, c.type_digits, c.type_scale \
             FROM sys.columns AS c \
             JOIN sys.tables AS t ON t.id = c.table_id \
             JOIN sys.schemas AS s ON s.id = t.schema_id \
             WHERE t.name IN ({})",
            table_names.join(", ")
        ))
        .map_err(map_cursor_error)?;

    let mut declared = HashMap::<(String, String, String), MonetType>::new();
    while cursor.next_row().map_err(map_cursor_error)? {
        let schema = cursor
            .get_str(0)
            .map_err(map_cursor_error)?
            .ok_or_else(|| error("catalog schema name is NULL", Status::InvalidData))?;
        let table = cursor
            .get_str(1)
            .map_err(map_cursor_error)?
            .ok_or_else(|| error("catalog table name is NULL", Status::InvalidData))?;
        let column = cursor
            .get_str(2)
            .map_err(map_cursor_error)?
            .ok_or_else(|| error("catalog column name is NULL", Status::InvalidData))?;
        let code = cursor
            .get_str(3)
            .map_err(map_cursor_error)?
            .ok_or_else(|| error("catalog column type is NULL", Status::InvalidData))?;
        let digits = cursor
            .get::<i32>(4)
            .map_err(map_cursor_error)?
            .ok_or_else(|| error("catalog type digits are NULL", Status::InvalidData))?;
        let scale = cursor
            .get::<i32>(5)
            .map_err(map_cursor_error)?
            .ok_or_else(|| error("catalog type scale is NULL", Status::InvalidData))?;
        declared.insert(
            (schema.to_owned(), table.to_owned(), column.to_owned()),
            prepared_monet_type(code, digits, scale)?,
        );
    }

    for field in fields {
        if let Some(data_type) = declared_result_type(field, &declared) {
            field.data_type = data_type;
        }
    }
    Ok(())
}

fn declared_result_type(
    field: &PreparedField,
    declared: &HashMap<(String, String, String), MonetType>,
) -> Option<MonetType> {
    let (Some(table), Some(column)) = (field.origin_table.as_deref(), field.name.as_deref()) else {
        return None;
    };
    let mut candidate = None;
    for ((schema, declared_table, declared_column), data_type) in declared {
        if declared_table != table
            || declared_column != column
            || field
                .origin_schema
                .as_deref()
                .is_some_and(|origin| origin != schema)
        {
            continue;
        }
        match candidate {
            None => candidate = Some(*data_type),
            Some(previous) if previous == *data_type => {}
            Some(_) => return None,
        }
    }
    candidate
}

fn prepared_monet_type(code: &str, digits: i32, scale: i32) -> Result<MonetType> {
    let mut data_type = if code.eq_ignore_ascii_case("clob") {
        MonetType::Varchar(0)
    } else {
        MonetType::from_mapi_code(code).ok_or_else(|| {
            error(
                format!("unknown MonetDB prepared type '{code}'"),
                Status::InvalidData,
            )
        })?
    };
    match &mut data_type {
        MonetType::Decimal(precision, decimal_scale) => {
            *precision = u8::try_from(digits)
                .map_err(|_| error("prepared decimal precision is invalid", Status::InvalidData))?;
            *decimal_scale = u8::try_from(scale)
                .map_err(|_| error("prepared decimal scale is invalid", Status::InvalidData))?;
        }
        MonetType::Varchar(width) => {
            *width = u32::try_from(digits)
                .map_err(|_| error("prepared string width is invalid", Status::InvalidData))?;
        }
        _ => {}
    }
    Ok(data_type)
}

fn prepared_arrow_field(name: String, data_type: &MonetType) -> Result<Field> {
    monetdb_arrow::field_for_monet_type(name, data_type)
        .map_err(|value| map_display(value, Status::InvalidData))
}

fn prepared_parameter_arrow_field(name: String, data_type: &MonetType) -> Result<Field> {
    if matches!(
        data_type,
        MonetType::Geometry | MonetType::Inet | MonetType::Xml
    ) {
        return Ok(Field::new(name, DataType::Utf8, true));
    }
    prepared_arrow_field(name, data_type)
}

#[derive(Clone)]
struct ReadExecutionOptions {
    batch_rows: usize,
    window_bytes: Option<usize>,
    prefetch: bool,
    measured_round_trip: Duration,
    stats: Option<SharedReadStats>,
}

fn metadata_read_options() -> ReadExecutionOptions {
    ReadExecutionOptions {
        batch_rows: METADATA_READ_BATCH_ROWS,
        window_bytes: None,
        prefetch: false,
        measured_round_trip: Duration::ZERO,
        stats: None,
    }
}

fn query_reader_with_timeouts(
    connection: &SharedConnection,
    query: &str,
    read_options: ReadExecutionOptions,
    timeouts: Timeouts,
) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
    query_result_with_timeouts(connection, query, read_options, timeouts)
        .map(|result| result.reader)
}

fn query_result_with_timeouts(
    connection: &SharedConnection,
    query: &str,
    read_options: ReadExecutionOptions,
    timeouts: Timeouts,
) -> Result<StatementResult> {
    if is_prepare_statement(query) {
        return Err(error(
            "execute() does not accept a PREPARE statement; call Statement::prepare() instead",
            Status::InvalidArguments,
        ));
    }
    let transaction_effects = transaction_effects(query)?;
    guard_transaction_commit(connection, transaction_effects)?;
    let connection_guard = lock_connection(connection)?;
    let mut cursor = connection_guard.cursor();
    cursor.set_timeouts(timeouts);
    cursor.execute(query).map_err(map_cursor_error)?;
    apply_transaction_rollback(connection, transaction_effects);
    if !cursor.has_result_set() {
        return Ok(StatementResult {
            reader: Box::new(EmptyReader::default()),
            rows_affected: cursor.affected_rows(),
        });
    }
    let result = cursor.binary_result().map_err(map_cursor_error)?;
    let total_rows = result.total_rows;
    let rows_affected = i64::try_from(total_rows).ok();
    if result.is_server_resident()
        && result
            .columns
            .iter()
            .any(|column| *column.sql_type() == MonetType::Oid)
    {
        return Err(error(
            "multi-row OID results are unavailable through Xexportbin; cast OID columns to VARCHAR in SQL",
            Status::NotImplemented,
        ));
    }
    if !result.is_server_resident() {
        if let Some(stats) = &read_options.stats {
            let scheduler = ReadWindowScheduler::new(
                &result.columns,
                read_options.batch_rows,
                read_options.window_bytes,
                read_options.measured_round_trip,
            );
            *stats
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) =
                Some(ReadStats::new(&scheduler, false));
        }
        let rows = usize::try_from(total_rows).map_err(|_| {
            error(
                "inline result row count exceeds this platform",
                Status::InvalidData,
            )
        })?;
        if rows == 0 {
            return Ok(StatementResult {
                reader: Box::new(EmptyReader::new(schema_for_columns(&result.columns)?)),
                rows_affected,
            });
        }
        let batch = monetdb_arrow::decode_inline_rows(&mut cursor, &result.columns, rows)
            .map_err(|value| map_display(value, Status::InvalidData))?;
        let batch_rows = if read_options.batch_rows == 0 {
            batch.num_rows()
        } else {
            read_options.batch_rows
        };
        return Ok(StatementResult {
            reader: Box::new(SlicedBatchReader::new(batch, batch_rows)),
            rows_affected,
        });
    }
    let schema = schema_for_columns(&result.columns)?;
    let adopt_frame = monetdb_arrow::prefers_owned_frame(&result.columns);
    let mut scheduler = ReadWindowScheduler::new(
        &result.columns,
        read_options.batch_rows,
        read_options.window_bytes,
        read_options.measured_round_trip,
    );
    if total_rows == 0 {
        if let Some(stats) = &read_options.stats {
            *stats
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) =
                Some(ReadStats::new(&scheduler, false));
        }
        return Ok(StatementResult {
            reader: Box::new(EmptyReader::new(schema)),
            rows_affected,
        });
    }
    let first_window_rows = scheduler.next_rows(total_rows);
    let prefetch_engaged = read_options.prefetch && total_rows > first_window_rows as u64;
    if let Some(stats) = &read_options.stats {
        *stats
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) =
            Some(ReadStats::new(&scheduler, prefetch_engaged));
    }
    drop(connection_guard);
    if prefetch_engaged {
        return Ok(StatementResult {
            reader: Box::new(PrefetchedBinaryReader::new(
                cursor,
                PrefetchPlan {
                    result_id: result.result_id,
                    columns: result.columns,
                    schema,
                    total_rows,
                    scheduler,
                    adopt_frame,
                    stats: read_options.stats,
                },
            )?),
            rows_affected,
        });
    }
    Ok(StatementResult {
        reader: Box::new(BinaryReader {
            cursor,
            result_id: result.result_id,
            columns: result.columns,
            schema,
            next_row: 0,
            total_rows,
            scheduler,
            response: Vec::new(),
            adopt_frame,
            stats: read_options.stats,
            finished: false,
        }),
        rows_affected,
    })
}

fn is_prepare_statement(query: &str) -> bool {
    leading_sql_keyword(query).is_some_and(|keyword| keyword.eq_ignore_ascii_case("prepare"))
}

fn query_invalidates_prepared_cache(query: &str) -> Result<bool> {
    Ok(unbound_statements(query)?.into_iter().any(|statement| {
        leading_sql_keyword(statement).is_some_and(|keyword| {
            matches!(
                keyword.to_ascii_uppercase().as_str(),
                "ALTER"
                    | "ANALYZE"
                    | "CALL"
                    | "COMMENT"
                    | "CREATE"
                    | "DEALLOCATE"
                    | "DROP"
                    | "GRANT"
                    | "RENAME"
                    | "REVOKE"
                    | "SET"
                    | "TRUNCATE"
            )
        })
    }))
}

fn leading_sql_keyword(query: &str) -> Option<&str> {
    let bytes = query.as_bytes();
    let mut index = 0;
    loop {
        while bytes.get(index).is_some_and(u8::is_ascii_whitespace) {
            index += 1;
        }
        match (bytes.get(index), bytes.get(index + 1)) {
            (Some(b'-'), Some(b'-')) => {
                index += 2;
                while bytes.get(index).is_some_and(|byte| *byte != b'\n') {
                    index += 1;
                }
            }
            (Some(b'#'), _) => {
                index += 1;
                while bytes.get(index).is_some_and(|byte| *byte != b'\n') {
                    index += 1;
                }
            }
            (Some(b'/'), Some(b'*')) => {
                let end = bytes[index + 2..]
                    .windows(2)
                    .position(|pair| pair == b"*/")?;
                index += end + 4;
            }
            _ => break,
        }
    }
    let start = index;
    while bytes
        .get(index)
        .is_some_and(|byte| byte.is_ascii_alphabetic() || *byte == b'_')
    {
        index += 1;
    }
    (index > start).then(|| &query[start..index])
}

#[derive(Clone, Copy, Default)]
struct TransactionEffects {
    commit: bool,
    rollback: bool,
}

fn transaction_effects(query: &str) -> Result<TransactionEffects> {
    if !contains_ascii_case_insensitive(query.as_bytes(), b"commit")
        && !contains_ascii_case_insensitive(query.as_bytes(), b"rollback")
    {
        return Ok(TransactionEffects::default());
    }
    let mut effects = TransactionEffects::default();
    for statement in unbound_statements(query)? {
        match leading_sql_keyword(statement).map(str::to_ascii_uppercase) {
            Some(keyword) if keyword == "COMMIT" => effects.commit = true,
            Some(keyword) if keyword == "ROLLBACK" => {
                effects.rollback |= is_transaction_rollback(statement)
            }
            _ => {}
        }
    }
    Ok(effects)
}

fn is_transaction_rollback(statement: &str) -> bool {
    let Some(keyword) = leading_sql_keyword(statement) else {
        return false;
    };
    if !keyword.eq_ignore_ascii_case("rollback") {
        return false;
    }
    let keyword_end = keyword.as_ptr() as usize - statement.as_ptr() as usize + keyword.len();
    !leading_sql_keyword(&statement[keyword_end..])
        .is_some_and(|next| next.eq_ignore_ascii_case("to"))
}

fn contains_ascii_case_insensitive(haystack: &[u8], needle: &[u8]) -> bool {
    haystack
        .windows(needle.len())
        .any(|window| window.eq_ignore_ascii_case(needle))
}

fn guard_transaction_commit(
    connection: &SharedConnection,
    effects: TransactionEffects,
) -> Result<()> {
    if effects.commit
        && let Some(poison) = connection.commit_error()
    {
        return Err(poison);
    }
    Ok(())
}

fn apply_transaction_rollback(connection: &SharedConnection, effects: TransactionEffects) {
    if effects.rollback {
        connection.clear_ingest_poison();
    }
}

fn validate_unbound_query(query: &str) -> Result<()> {
    if is_prepare_statement(query) {
        return Err(error(
            "execution does not accept a PREPARE statement; call Statement::prepare() instead",
            Status::InvalidArguments,
        ));
    }
    if parameter_layout(query)?.count() != 0 {
        return Err(error("parameters are not bound", Status::InvalidState));
    }
    Ok(())
}

fn parameter_query_reader(
    connection: &SharedConnection,
    mut queries: BoundQueryStream,
    read_options: ReadExecutionOptions,
    timeouts: Timeouts,
) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
    let first = queries
        .next()
        .transpose()?
        .ok_or_else(|| error("no parameter rows to execute", Status::InvalidArguments))?;
    let current =
        bound_query_reader_with_retry(connection, &first, read_options.clone(), timeouts)?;
    let schema = current.schema();
    Ok(Box::new(ParameterQueryReader {
        connection: Arc::clone(connection),
        queries,
        current: Some(current),
        schema,
        read_options,
        timeouts,
        finished: false,
    }))
}

fn bound_query_reader_with_retry(
    connection: &SharedConnection,
    query: &BoundQuery,
    read_options: ReadExecutionOptions,
    timeouts: Timeouts,
) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
    let query = current_bound_query(connection, query, timeouts)?;
    match query_reader_with_timeouts(connection, &query.sql, read_options.clone(), timeouts) {
        Err(value) if query.prepared.is_some() && prepared_statement_missing(&value) => {
            if !lock_connection(connection)?
                .server_info()
                .map_err(map_cursor_error)?
                .autocommit
            {
                return Err(value);
            }
            let retry = retry_bound_query(connection, &query, timeouts)?;
            query_reader_with_timeouts(connection, &retry.sql, read_options, timeouts)
        }
        result => result,
    }
}

fn scalar_string(connection: &SharedConnection, query: &str, timeouts: Timeouts) -> Result<String> {
    let mut reader = query_reader_with_timeouts(
        connection,
        query,
        ReadExecutionOptions {
            batch_rows: 1,
            window_bytes: None,
            prefetch: false,
            measured_round_trip: Duration::ZERO,
            stats: None,
        },
        timeouts,
    )?;
    let batch = reader
        .next()
        .transpose()
        .map_err(Error::from)?
        .ok_or_else(|| error("scalar query returned no rows", Status::Internal))?;
    let values = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| error("scalar query did not return a string", Status::Internal))?;
    if values.is_null(0) {
        return Err(error("scalar query returned NULL", Status::Internal));
    }
    Ok(values.value(0).to_owned())
}

fn execute_update(
    connection: &SharedConnection,
    query: &str,
    timeouts: Timeouts,
) -> Result<Option<i64>> {
    if is_prepare_statement(query) {
        return Err(error(
            "execute_update() does not accept a PREPARE statement; call Statement::prepare() instead",
            Status::InvalidArguments,
        ));
    }
    let transaction_effects = transaction_effects(query)?;
    guard_transaction_commit(connection, transaction_effects)?;
    let connection_guard = lock_connection(connection)?;
    let mut cursor = connection_guard.cursor();
    cursor.set_timeouts(timeouts);
    cursor.execute(query).map_err(map_cursor_error)?;
    apply_transaction_rollback(connection, transaction_effects);
    let affected_rows = (!cursor.has_result_set())
        .then(|| cursor.affected_rows())
        .flatten();
    drop(connection_guard);
    Ok(affected_rows)
}

fn execute_update_script(
    connection: &SharedConnection,
    query: &str,
    timeouts: Timeouts,
) -> Result<Option<i64>> {
    let statements = unbound_statements(query)?;
    if statements.iter().any(|query| is_prepare_statement(query)) {
        return Err(error(
            "execute_update() does not accept a PREPARE statement; call Statement::prepare() instead",
            Status::InvalidArguments,
        ));
    }
    let transaction_effects = transaction_effects(query)?;
    guard_transaction_commit(connection, transaction_effects)?;
    let connection_guard = lock_connection(connection)?;
    let mut cursor = connection_guard.cursor();
    cursor.set_timeouts(timeouts);
    let mut affected_rows = None;
    for statement in statements {
        cursor.execute(statement).map_err(map_cursor_error)?;
        apply_transaction_rollback(
            connection,
            TransactionEffects {
                rollback: is_transaction_rollback(statement),
                ..TransactionEffects::default()
            },
        );
        if !cursor.has_result_set()
            && let Some(rows) = cursor.affected_rows()
        {
            affected_rows = Some(
                affected_rows
                    .unwrap_or(0i64)
                    .checked_add(rows)
                    .ok_or_else(|| error("affected row count overflows i64", Status::Internal))?,
            );
        }
    }
    drop(connection_guard);
    Ok(affected_rows)
}

enum AtomicScope {
    Autocommit,
    CallerTransaction,
    Savepoint {
        name: String,
        retain_until_transaction_end: bool,
    },
}

impl AtomicScope {
    fn name(&self) -> &'static str {
        match self {
            Self::Autocommit => "autocommit",
            Self::CallerTransaction => "transaction",
            Self::Savepoint { .. } => "savepoint",
        }
    }
}

#[derive(Clone, Copy)]
enum CallerTransactionScope {
    Direct,
    Savepoint,
}

fn begin_atomic(
    connection: &monetdb::Connection,
    purpose: &str,
    caller_scope: CallerTransactionScope,
    transaction_scoped_temporary_exists: Option<bool>,
    timeouts: Timeouts,
) -> Result<(monetdb::Cursor, AtomicScope)> {
    let originally_autocommit = connection
        .server_info()
        .map_err(map_cursor_error)?
        .autocommit;
    if originally_autocommit {
        connection
            .set_autocommit_with_timeouts(false, timeouts)
            .map_err(map_cursor_error)?;
        let mut cursor = connection.cursor();
        cursor.set_timeouts(timeouts);
        return Ok((cursor, AtomicScope::Autocommit));
    }
    let mut cursor = connection.cursor();
    cursor.set_timeouts(timeouts);
    if matches!(caller_scope, CallerTransactionScope::Direct) {
        return Ok((cursor, AtomicScope::CallerTransaction));
    }
    let retain_until_transaction_end = match transaction_scoped_temporary_exists {
        Some(exists) => exists,
        None => transaction_scoped_temporary_table_state(connection, None, timeouts)?.any,
    };
    let savepoint = savepoint_name(purpose);
    cursor
        .execute(&format!("SAVEPOINT {savepoint}"))
        .map_err(map_cursor_error)?;
    Ok((
        cursor,
        AtomicScope::Savepoint {
            name: savepoint,
            retain_until_transaction_end,
        },
    ))
}

fn current_schema_name(cursor: &mut monetdb::Cursor) -> Result<String> {
    cursor
        .execute("SELECT current_schema")
        .map_err(map_cursor_error)?;
    if !cursor.next_row().map_err(map_cursor_error)? {
        return Err(error(
            "current-schema query returned no row",
            Status::InvalidData,
        ));
    }
    cursor
        .get_str(0)
        .map_err(map_cursor_error)?
        .map(str::to_owned)
        .ok_or_else(|| error("current schema is NULL", Status::InvalidData))
}

fn table_exists(
    connection: &monetdb::Connection,
    schema_name: Option<&str>,
    table_name: &str,
    timeouts: Timeouts,
) -> Result<bool> {
    let schema_filter = schema_name
        .map(metadata::raw_string_literal)
        .transpose()?
        .unwrap_or_else(|| "current_schema".to_owned());
    let table = metadata::raw_string_literal(table_name)?;
    let mut cursor = connection.cursor();
    cursor.set_timeouts(timeouts);
    cursor
        .execute(&format!(
            "SELECT CAST(COUNT(*) AS VARCHAR(32)) \
             FROM sys.tables AS t \
             JOIN sys.schemas AS s ON s.id = t.schema_id \
             WHERE s.name = {schema_filter} AND t.name = {table}"
        ))
        .map_err(map_cursor_error)?;
    count_query_result(&mut cursor, "table")
}

#[derive(Clone, Copy)]
struct TableConstraintState {
    exists: bool,
    constrained: bool,
}

fn table_constraint_state(
    cursor: &mut monetdb::Cursor,
    schema_name: Option<&str>,
    table_name: &str,
) -> Result<TableConstraintState> {
    let schema_filter = schema_name
        .map(metadata::raw_string_literal)
        .transpose()?
        .unwrap_or_else(|| "current_schema".to_owned());
    let table = metadata::raw_string_literal(table_name)?;
    let keys = if schema_name == Some("tmp") {
        "tmp.keys"
    } else {
        "sys.keys"
    };
    cursor
        .execute(&format!(
            "SELECT CAST(COUNT(DISTINCT t.id) AS VARCHAR(32)), \
                    CAST(COUNT(k.id) AS VARCHAR(32)) \
             FROM sys.tables AS t \
             JOIN sys.schemas AS s ON s.id = t.schema_id \
             LEFT JOIN {keys} AS k ON k.table_id = t.id \
             WHERE s.name = {schema_filter} AND t.name = {table}"
        ))
        .map_err(map_cursor_error)?;
    if !cursor.next_row().map_err(map_cursor_error)? {
        return Err(error(
            "table constraint metadata query returned no row",
            Status::InvalidData,
        ));
    }
    let parse_count = |index| -> Result<u64> {
        cursor
            .get_str(index)
            .map_err(map_cursor_error)?
            .ok_or_else(|| error("table constraint count is NULL", Status::InvalidData))?
            .parse::<u64>()
            .map_err(|value| {
                error(
                    format!("invalid table constraint count: {value}"),
                    Status::InvalidData,
                )
            })
    };
    Ok(TableConstraintState {
        exists: parse_count(0)? != 0,
        constrained: parse_count(1)? != 0,
    })
}

/// Whether a temporary table exists whose lifetime ends with the transaction.
///
/// MonetDB source: [`ca_t`](https://github.com/MonetDB/MonetDB/blob/Dec2025_7/sql/include/sql_catalog.h#L186-L191).
/// `sys.tables.commit_action` encodes the temporary-table commit action:
/// 1 = ON COMMIT DELETE ROWS, 2 = ON COMMIT PRESERVE ROWS, 3 = ON COMMIT DROP.
/// Both 1 and 3 are transaction-scoped: ending the transaction, including
/// releasing a savepoint taken inside it, empties or destroys the table.
#[derive(Clone, Copy)]
struct TransactionScopedTemporaryTableState {
    any: bool,
    target: Option<bool>,
}

fn transaction_scoped_temporary_table_state(
    connection: &monetdb::Connection,
    table_name: Option<&str>,
    timeouts: Timeouts,
) -> Result<TransactionScopedTemporaryTableState> {
    let target_count = table_name
        .map(|table| {
            metadata::raw_string_literal(table).map(|table| {
                format!(", CAST(COUNT(CASE WHEN t.name = {table} THEN 1 END) AS VARCHAR(32))")
            })
        })
        .transpose()?
        .unwrap_or_default();
    let mut cursor = connection.cursor();
    cursor.set_timeouts(timeouts);
    cursor
        .execute(&format!(
            "SELECT CAST(COUNT(*) AS VARCHAR(32)){target_count} \
             FROM sys.tables AS t \
             JOIN sys.schemas AS s ON s.id = t.schema_id \
             WHERE s.name = 'tmp' AND t.commit_action IN (1, 3)"
        ))
        .map_err(map_cursor_error)?;
    if !cursor.next_row().map_err(map_cursor_error)? {
        return Err(error(
            "temporary table metadata query returned no row",
            Status::InvalidData,
        ));
    }
    let parse_count = |index| -> Result<bool> {
        cursor
            .get_str(index)
            .map_err(map_cursor_error)?
            .ok_or_else(|| error("temporary table count is NULL", Status::InvalidData))?
            .parse::<u64>()
            .map(|count| count != 0)
            .map_err(|value| {
                error(
                    format!("invalid temporary table count: {value}"),
                    Status::InvalidData,
                )
            })
    };
    Ok(TransactionScopedTemporaryTableState {
        any: parse_count(0)?,
        target: table_name.map(|_| parse_count(1)).transpose()?,
    })
}

fn count_query_result(cursor: &mut monetdb::Cursor, object: &str) -> Result<bool> {
    if !cursor.next_row().map_err(map_cursor_error)? {
        return Err(error(
            format!("{object} metadata query returned no row"),
            Status::InvalidData,
        ));
    }
    let count = cursor
        .get_str(0)
        .map_err(map_cursor_error)?
        .ok_or_else(|| error("temporary table count is NULL", Status::InvalidData))?
        .parse::<u64>()
        .map_err(|value| {
            error(
                format!("invalid {object} count: {value}"),
                Status::InvalidData,
            )
        })?;
    Ok(count != 0)
}

fn combine_atomic_error<T>(
    result: Result<T>,
    secondary: std::result::Result<(), CursorError>,
    context: &str,
) -> Result<T> {
    match (result, secondary) {
        (result, Ok(())) => result,
        (Err(mut root), Err(secondary)) => {
            root.message = format!("{}; {context} also failed: {secondary}", root.message);
            Err(root)
        }
        (Ok(_), Err(secondary)) => {
            let mut secondary = map_cursor_error(secondary);
            secondary.message = format!("{context} failed: {}", secondary.message);
            Err(secondary)
        }
    }
}

fn combine_error<T, U>(result: Result<T>, secondary: Result<U>, context: &str) -> Result<T> {
    match (result, secondary) {
        (result, Ok(_)) => result,
        (Err(mut root), Err(secondary)) => {
            root.message = format!(
                "{}; {context} also failed: {}",
                root.message, secondary.message
            );
            Err(root)
        }
        (Ok(_), Err(mut secondary)) => {
            secondary.message = format!("{context} failed: {}", secondary.message);
            Err(secondary)
        }
    }
}

fn finish_atomic<T>(
    connection: &monetdb::Connection,
    cursor: &mut monetdb::Cursor,
    scope: AtomicScope,
    operation: Result<T>,
    timeouts: Timeouts,
) -> Result<T> {
    let mut result = match operation {
        Ok(value) => {
            let finalize = match &scope {
                AtomicScope::Autocommit => cursor.execute("COMMIT"),
                AtomicScope::CallerTransaction => Ok(()),
                AtomicScope::Savepoint {
                    name,
                    retain_until_transaction_end,
                } => {
                    if *retain_until_transaction_end {
                        Ok(())
                    } else {
                        cursor.execute(&format!("RELEASE SAVEPOINT {name}"))
                    }
                }
            };
            finalize.map(|()| value).map_err(map_cursor_error)
        }
        Err(root) => Err(root),
    };

    if result.is_err() {
        let recovery = match &scope {
            AtomicScope::Autocommit => cursor.execute("ROLLBACK"),
            AtomicScope::CallerTransaction => Ok(()),
            AtomicScope::Savepoint {
                name,
                retain_until_transaction_end,
            } => {
                let rollback = cursor.execute(&format!("ROLLBACK TO SAVEPOINT {name}"));
                if *retain_until_transaction_end {
                    rollback
                } else {
                    rollback.and_then(|()| cursor.execute(&format!("RELEASE SAVEPOINT {name}")))
                }
            }
        };
        result = combine_atomic_error(result, recovery, "transaction recovery");
    }
    if matches!(scope, AtomicScope::Autocommit) {
        result = combine_atomic_error(
            result,
            connection.set_autocommit_with_timeouts(true, timeouts),
            "restoring autocommit",
        );
    }
    result
}

fn execute_updates_atomic(
    connection: &SharedConnection,
    queries: &mut BoundQueryStream,
    timeouts: Timeouts,
) -> Result<Option<i64>> {
    let Some(first) = queries.next().transpose()? else {
        return Ok(Some(0));
    };
    if queries.is_empty()? {
        return execute_single_bound_update(connection, &first, timeouts);
    }
    execute_updates_atomic_from_first(connection, queries, first, timeouts, true)
}

fn execute_single_bound_update(
    connection: &SharedConnection,
    query: &BoundQuery,
    timeouts: Timeouts,
) -> Result<Option<i64>> {
    let query = current_bound_query(connection, query, timeouts)?;
    let can_retry = query.prepared.is_some()
        && lock_connection(connection)?
            .server_info()
            .map_err(map_cursor_error)?
            .autocommit;
    match execute_update(connection, &query.sql, timeouts) {
        Err(value) if can_retry && prepared_statement_missing(&value) => {
            let retry = retry_bound_query(connection, &query, timeouts)?;
            execute_update(connection, &retry.sql, timeouts)
        }
        result => result,
    }
}

fn execute_updates_atomic_from_first(
    connection: &SharedConnection,
    queries: &mut BoundQueryStream,
    first: BoundQuery,
    timeouts: Timeouts,
    allow_retry: bool,
) -> Result<Option<i64>> {
    let first = current_bound_query(connection, &first, timeouts)?;
    let connection_guard = lock_connection(connection)?;
    let (mut cursor, atomic_scope) = begin_atomic(
        &connection_guard,
        "parameter_batch",
        CallerTransactionScope::Savepoint,
        None,
        timeouts,
    )?;
    if let Err(root) = cursor.execute(&first.sql).map_err(map_cursor_error) {
        let retryable =
            allow_retry && first.prepared.is_some() && prepared_statement_missing(&root);
        let root_message = root.message.clone();
        let result = finish_atomic(
            &connection_guard,
            &mut cursor,
            atomic_scope,
            Err(root),
            timeouts,
        );
        drop(cursor);
        drop(connection_guard);
        return match result {
            Err(value) if retryable && value.message == root_message => {
                let retry = retry_bound_query(connection, &first, timeouts)?;
                execute_updates_atomic_from_first(connection, queries, retry, timeouts, false)
            }
            result => result,
        };
    }
    let result = (|| {
        let mut total = 0i64;
        let mut has_count = false;
        add_current_affected_rows(&cursor, &mut total, &mut has_count)?;
        let mut pending: Option<BoundQuery> = None;
        loop {
            let mut batch = Vec::with_capacity(PARAMETER_UPDATE_BATCH_ROWS);
            let mut sql_bytes = 0usize;
            if let Some(query) = pending.take() {
                sql_bytes = query.sql.len();
                batch.push(query);
            }
            while batch.len() < PARAMETER_UPDATE_BATCH_ROWS {
                let Some(query) = queries.next().transpose()? else {
                    break;
                };
                let next_bytes = sql_bytes.saturating_add(query.sql.len()).saturating_add(2);
                if !batch.is_empty() && next_bytes > PARAMETER_UPDATE_BATCH_BYTES {
                    pending = Some(query);
                    break;
                }
                sql_bytes = next_bytes;
                batch.push(query);
            }
            if batch.is_empty() {
                break;
            }
            execute_update_batch(&mut cursor, &batch, &mut total, &mut has_count)?;
        }
        Ok(has_count.then_some(total))
    })();
    finish_atomic(
        &connection_guard,
        &mut cursor,
        atomic_scope,
        result,
        timeouts,
    )
}

const PARAMETER_UPDATE_BATCH_ROWS: usize = 1_024;
const PARAMETER_UPDATE_BATCH_BYTES: usize = 8 * 1024 * 1024;

fn execute_update_batch(
    cursor: &mut monetdb::Cursor,
    queries: &[BoundQuery],
    total: &mut i64,
    has_count: &mut bool,
) -> Result<()> {
    let sql_bytes = queries
        .iter()
        .map(|query| query.sql.len().saturating_add(2))
        .sum();
    let mut script = String::with_capacity(sql_bytes);
    for query in queries {
        script.push_str(query.sql.trim_end().trim_end_matches(';'));
        script.push_str(";\n");
    }
    cursor.execute(&script).map_err(map_cursor_error)?;
    loop {
        add_current_affected_rows(cursor, total, has_count)?;
        if !cursor.next_reply().map_err(map_cursor_error)? {
            break;
        }
    }
    Ok(())
}

fn add_current_affected_rows(
    cursor: &monetdb::Cursor,
    total: &mut i64,
    has_count: &mut bool,
) -> Result<()> {
    if !cursor.has_result_set()
        && let Some(rows) = cursor.affected_rows()
    {
        *total = total
            .checked_add(rows)
            .ok_or_else(|| error("affected row count overflows i64", Status::Internal))?;
        *has_count = true;
    }
    Ok(())
}

fn schema_for_columns(columns: &[ResultColumn]) -> Result<SchemaRef> {
    let fields = columns
        .iter()
        .map(monetdb_arrow::field_for_column)
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|value| map_display(value, Status::InvalidData))?;
    Ok(Arc::new(Schema::new(fields)))
}

type SharedReadStats = Arc<Mutex<Option<ReadStats>>>;

#[derive(Debug)]
struct ReadStats {
    window_budget_bytes: usize,
    exact_batch_rows: usize,
    initial_estimated_bytes_per_row: usize,
    observed_bytes_per_row: Vec<usize>,
    window_rows: Vec<usize>,
    window_bytes: Vec<usize>,
    window_capacity_bytes: Vec<usize>,
    window_adopted: Vec<bool>,
    prefetch_engaged: bool,
    recycled_buffers: usize,
}

impl ReadStats {
    fn new(scheduler: &ReadWindowScheduler, prefetch_engaged: bool) -> Self {
        Self {
            window_budget_bytes: scheduler.window_budget_bytes,
            exact_batch_rows: scheduler.exact_batch_rows.unwrap_or(0),
            initial_estimated_bytes_per_row: scheduler.estimated_bytes_per_row,
            observed_bytes_per_row: Vec::new(),
            window_rows: Vec::new(),
            window_bytes: Vec::new(),
            window_capacity_bytes: Vec::new(),
            window_adopted: Vec::new(),
            prefetch_engaged,
            recycled_buffers: 0,
        }
    }

    fn record_window(&mut self, rows: usize, bytes: usize, capacity: usize, adopted: bool) {
        self.window_rows.push(rows);
        self.window_bytes.push(bytes);
        self.window_capacity_bytes.push(capacity);
        self.window_adopted.push(adopted);
        if let Some(observed) = bytes
            .saturating_add(rows.saturating_sub(1))
            .checked_div(rows)
        {
            self.observed_bytes_per_row.push(observed);
        }
    }

    fn to_json(&self) -> String {
        fn usize_array(values: &[usize]) -> String {
            values
                .iter()
                .map(usize::to_string)
                .collect::<Vec<_>>()
                .join(",")
        }
        fn bool_array(values: &[bool]) -> String {
            values
                .iter()
                .map(bool::to_string)
                .collect::<Vec<_>>()
                .join(",")
        }
        format!(
            concat!(
                "{{\"windows_fetched\":{},\"window_budget_bytes\":{},",
                "\"exact_batch_rows\":{},\"initial_estimated_bytes_per_row\":{},",
                "\"observed_bytes_per_row\":[{}],\"window_rows\":[{}],",
                "\"window_bytes\":[{}],\"window_capacity_bytes\":[{}],",
                "\"window_adopted\":[{}],\"prefetch_engaged\":{},",
                "\"recycled_buffers\":{}}}"
            ),
            self.window_rows.len(),
            self.window_budget_bytes,
            self.exact_batch_rows,
            self.initial_estimated_bytes_per_row,
            usize_array(&self.observed_bytes_per_row),
            usize_array(&self.window_rows),
            usize_array(&self.window_bytes),
            usize_array(&self.window_capacity_bytes),
            bool_array(&self.window_adopted),
            self.prefetch_engaged,
            self.recycled_buffers,
        )
    }
}

#[derive(Debug, Clone)]
struct ReadWindowScheduler {
    exact_batch_rows: Option<usize>,
    window_budget_bytes: usize,
    full_granule_fits: bool,
    estimated_bytes_per_row: usize,
    initial_rows_limit: Option<usize>,
    previous_rows: Option<usize>,
    observed_rows: usize,
}

impl ReadWindowScheduler {
    fn new(
        columns: &[ResultColumn],
        batch_rows: usize,
        configured_window_bytes: Option<usize>,
        measured_round_trip: Duration,
    ) -> Self {
        let fixed_granule_capacity =
            monetdb_arrow::owned_frame_capacity(columns, EXPORT_GRANULE_ROWS);
        let window_budget_bytes = effective_read_window_bytes(
            configured_window_bytes,
            measured_round_trip,
            fixed_granule_capacity,
        );
        Self {
            exact_batch_rows: (batch_rows > 0).then_some(batch_rows),
            window_budget_bytes,
            full_granule_fits: fixed_granule_capacity
                .is_some_and(|capacity| capacity <= window_budget_bytes),
            estimated_bytes_per_row: monetdb_arrow::estimated_frame_bytes_per_row(
                columns,
                VARIABLE_READ_COLUMN_BYTES,
            )
            .max(1),
            initial_rows_limit: monetdb_arrow::owned_frame_capacity(columns, 1)
                .is_none()
                .then_some(INITIAL_VARIABLE_READ_ROWS),
            previous_rows: None,
            observed_rows: 0,
        }
    }

    fn next_rows(&mut self, remaining: u64) -> usize {
        let remaining = usize::try_from(remaining).unwrap_or(usize::MAX);
        let rows = if let Some(exact) = self.exact_batch_rows {
            exact.min(remaining)
        } else {
            let target = (self.window_budget_bytes / self.estimated_bytes_per_row)
                .max(1)
                .max(if self.full_granule_fits {
                    EXPORT_GRANULE_ROWS
                } else {
                    1
                });
            let target = self.previous_rows.map_or_else(
                || target.min(self.initial_rows_limit.unwrap_or(usize::MAX)),
                |previous| target.min(previous.saturating_mul(MAX_READ_WINDOW_GROWTH)),
            );
            // MonetDB's Dec2025 sql_result.c::mvc_export_bin_chunk forwards each
            // Xexportbin range to sql_bincopy.c::dump_binary_column without a wire-
            // level alignment restriction. Driver-selected boundaries use the
            // measured 131,072-row export granule; sub-granule powers of two
            // preserve the byte budget when a complete granule is too large.
            let alignment = export_alignment_rows(target);
            let target_end = self.observed_rows.saturating_add(target);
            let aligned_end = target_end - target_end % alignment;
            aligned_end
                .saturating_sub(self.observed_rows)
                .max(1)
                .min(remaining)
        };
        self.previous_rows = Some(rows);
        rows
    }

    fn observe(&mut self, bytes: usize, rows: usize) {
        self.observed_rows = self.observed_rows.saturating_add(rows);
        if self.exact_batch_rows.is_none() && rows > 0 {
            self.estimated_bytes_per_row = bytes.saturating_add(rows - 1) / rows;
            self.estimated_bytes_per_row = self.estimated_bytes_per_row.max(1);
        }
    }

    fn response_capacity(&self, requested_rows: usize, adopt_frame: bool) -> usize {
        if adopt_frame {
            return 0;
        }
        self.estimated_bytes_per_row
            .saturating_mul(requested_rows)
            .saturating_add(4096)
    }
}

fn export_alignment_rows(target: usize) -> usize {
    let target = target.clamp(1, EXPORT_GRANULE_ROWS);
    1usize << (usize::BITS - 1 - target.leading_zeros())
}

fn shrink_owned_response(response: &mut Vec<u8>) {
    let retained_limit = response.len().saturating_add(response.len() / 8);
    if response.capacity() > retained_limit {
        response.shrink_to_fit();
    }
}

struct BinaryFrame {
    start_row: u64,
    requested_rows: usize,
    response: Vec<u8>,
}

type BinaryFrameResult = std::result::Result<BinaryFrame, ArrowError>;

struct PrefetchPlan {
    result_id: u64,
    columns: Vec<ResultColumn>,
    schema: SchemaRef,
    total_rows: u64,
    scheduler: ReadWindowScheduler,
    adopt_frame: bool,
    stats: Option<SharedReadStats>,
}

struct PrefetchedBinaryReader {
    result_id: u64,
    columns: Vec<ResultColumn>,
    schema: SchemaRef,
    total_rows: u64,
    decoded_rows: u64,
    adopt_frame: bool,
    recycle_sender: std::sync::mpsc::SyncSender<Vec<u8>>,
    receiver: Option<std::sync::mpsc::Receiver<BinaryFrameResult>>,
    completion: std::sync::mpsc::Receiver<()>,
    worker: Option<std::thread::JoinHandle<()>>,
    finished: bool,
}

impl PrefetchedBinaryReader {
    fn new(cursor: monetdb::Cursor, plan: PrefetchPlan) -> Result<Self> {
        let (sender, receiver) = std::sync::mpsc::sync_channel(1);
        let (completion_sender, completion) = std::sync::mpsc::sync_channel(1);
        let worker_columns = plan.columns.clone();
        let result_id = plan.result_id;
        let total_rows = plan.total_rows;
        let scheduler = plan.scheduler;
        let adopt_frame = plan.adopt_frame;
        let worker_stats = plan.stats;
        let (recycle_sender, recycle_receiver) = std::sync::mpsc::sync_channel(2);
        let worker = std::thread::Builder::new()
            .name("adbc-monetdb-prefetch".into())
            .spawn(move || {
                let panic_sender = sender.clone();
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    fetch_binary_frames(BinaryFetchTask {
                        cursor,
                        result_id,
                        columns: worker_columns,
                        total_rows,
                        scheduler,
                        adopt_frame,
                        recycle_receiver,
                        stats: worker_stats,
                        sender,
                    });
                }));
                if result.is_err() {
                    let _ = panic_sender.send(Err(ArrowError::ParseError(
                        "MonetDB result fetching panicked".into(),
                    )));
                }
                let _ = completion_sender.send(());
            })
            .map_err(|value| map_display(value, Status::Internal))?;
        Ok(Self {
            result_id,
            columns: plan.columns,
            schema: plan.schema,
            total_rows,
            decoded_rows: 0,
            adopt_frame,
            recycle_sender,
            receiver: Some(receiver),
            completion,
            worker: Some(worker),
            finished: false,
        })
    }

    fn finish(&mut self) {
        self.finished = true;
        self.receiver.take();
    }

    fn next_inner(&mut self) -> Option<std::result::Result<RecordBatch, ArrowError>> {
        if self.finished || self.decoded_rows >= self.total_rows {
            self.finish();
            return None;
        }
        let Some(receiver) = self.receiver.as_ref() else {
            self.finish();
            return Some(Err(ArrowError::ParseError(
                "MonetDB result prefetch receiver is unavailable".into(),
            )));
        };
        let frame = match receiver.recv() {
            Ok(Ok(frame)) => frame,
            Ok(Err(error)) => {
                self.finish();
                return Some(Err(error));
            }
            Err(_) => {
                self.finish();
                return Some(Err(ArrowError::ParseError(
                    "MonetDB result prefetch ended before the result was complete".into(),
                )));
            }
        };
        let BinaryFrame {
            start_row,
            requested_rows,
            mut response,
        } = frame;
        let result = if self.adopt_frame {
            shrink_owned_response(&mut response);
            monetdb_arrow::decode_frame_owned_with_schema(
                response,
                &self.columns,
                self.result_id,
                start_row,
                requested_rows,
                Arc::clone(&self.schema),
            )
        } else {
            let result = monetdb_arrow::decode_frame_with_schema(
                &response,
                &self.columns,
                self.result_id,
                start_row,
                requested_rows,
                Arc::clone(&self.schema),
            );
            response.clear();
            let _ = self.recycle_sender.try_send(response);
            result
        }
        .map_err(|value| ArrowError::ExternalError(Box::new(value)));
        match &result {
            Ok(batch) if batch.num_rows() > 0 => {
                self.decoded_rows += batch.num_rows() as u64;
            }
            Ok(_) => {
                self.finish();
                return Some(Err(ArrowError::ParseError(
                    "MonetDB returned an empty binary window before the result ended".into(),
                )));
            }
            Err(_) => self.finish(),
        }
        if self.decoded_rows >= self.total_rows {
            self.finish();
        }
        Some(result)
    }
}

impl Iterator for PrefetchedBinaryReader {
    type Item = std::result::Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| self.next_inner())) {
            Ok(result) => result,
            Err(_) => {
                self.finish();
                Some(Err(ArrowError::ParseError(
                    "MonetDB result decoding panicked".into(),
                )))
            }
        }
    }
}

impl RecordBatchReader for PrefetchedBinaryReader {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

impl Drop for PrefetchedBinaryReader {
    fn drop(&mut self) {
        self.receiver.take();
        if let Some(worker) = self.worker.take()
            && self.completion.recv_timeout(PREFETCH_DROP_GRACE).is_ok()
        {
            let _ = worker.join();
        }
    }
}

struct BinaryFetchTask {
    cursor: monetdb::Cursor,
    result_id: u64,
    columns: Vec<ResultColumn>,
    total_rows: u64,
    scheduler: ReadWindowScheduler,
    adopt_frame: bool,
    recycle_receiver: std::sync::mpsc::Receiver<Vec<u8>>,
    stats: Option<SharedReadStats>,
    sender: std::sync::mpsc::SyncSender<BinaryFrameResult>,
}

fn fetch_binary_frames(task: BinaryFetchTask) {
    let BinaryFetchTask {
        mut cursor,
        result_id,
        columns,
        total_rows,
        mut scheduler,
        adopt_frame,
        recycle_receiver,
        stats,
        sender,
    } = task;
    let mut next_row = 0u64;
    while next_row < total_rows {
        let remaining = total_rows - next_row;
        let requested_rows = scheduler.next_rows(remaining);
        let capacity = if adopt_frame {
            monetdb_arrow::owned_frame_capacity(&columns, requested_rows).unwrap_or(0)
        } else {
            scheduler.response_capacity(requested_rows, false)
        };
        let (mut response, recycled) = if adopt_frame {
            (Vec::new(), false)
        } else {
            match recycle_receiver.try_recv() {
                Ok(response)
                    if scheduler.exact_batch_rows.is_some()
                        || response.capacity()
                            <= scheduler.window_budget_bytes.saturating_mul(2) =>
                {
                    (response, true)
                }
                _ => (Vec::new(), false),
            }
        };
        if recycled
            && let Some(stats) = &stats
            && let Some(stats) = stats
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .as_mut()
        {
            stats.recycled_buffers += 1;
        }
        response.clear();
        if response.capacity() < capacity {
            response.reserve_exact(capacity);
        }
        if let Err(value) = cursor.fetch_binary_into(next_row, requested_rows, &mut response) {
            let _ = sender.send(Err(ArrowError::ExternalError(Box::new(value))));
            return;
        }
        let frame = match monetdb_arrow::exportbin::parse_frame_header(&response) {
            Ok(frame) => frame,
            Err(value) => {
                let _ = sender.send(Err(ArrowError::ExternalError(Box::new(value))));
                return;
            }
        };
        if u64::try_from(frame.result_id) != Ok(result_id) {
            let _ = sender.send(Err(ArrowError::ExternalError(Box::new(
                monetdb_arrow::DecodeError::ResultId {
                    expected: result_id,
                    actual: frame.result_id,
                },
            ))));
            return;
        }
        if frame.start_row != next_row {
            let _ = sender.send(Err(ArrowError::ExternalError(Box::new(
                monetdb_arrow::DecodeError::StartRow {
                    expected: next_row,
                    actual: frame.start_row,
                },
            ))));
            return;
        }
        let actual_rows = match usize::try_from(frame.row_count) {
            Ok(actual_rows) if actual_rows <= requested_rows => actual_rows,
            Ok(actual_rows) => {
                let _ = sender.send(Err(ArrowError::ExternalError(Box::new(
                    monetdb_arrow::DecodeError::RowCount {
                        requested: requested_rows,
                        actual: actual_rows,
                    },
                ))));
                return;
            }
            Err(_) => {
                let _ = sender.send(Err(ArrowError::ParseError(
                    "MonetDB binary window row count exceeds this platform".into(),
                )));
                return;
            }
        };
        scheduler.observe(response.len(), actual_rows);
        if let Some(stats) = &stats
            && let Some(stats) = stats
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .as_mut()
        {
            stats.record_window(
                actual_rows,
                response.len(),
                response.capacity(),
                adopt_frame,
            );
        }
        if sender
            .send(Ok(BinaryFrame {
                start_row: next_row,
                requested_rows,
                response,
            }))
            .is_err()
        {
            return;
        }
        if actual_rows == 0 {
            return;
        }
        next_row += actual_rows as u64;
    }
}

struct BinaryReader {
    cursor: monetdb::Cursor,
    result_id: u64,
    columns: Vec<ResultColumn>,
    schema: SchemaRef,
    next_row: u64,
    total_rows: u64,
    scheduler: ReadWindowScheduler,
    response: Vec<u8>,
    adopt_frame: bool,
    stats: Option<SharedReadStats>,
    finished: bool,
}

impl Iterator for BinaryReader {
    type Item = std::result::Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| self.next_inner())) {
            Ok(result) => result,
            Err(_) => {
                self.finished = true;
                Some(Err(ArrowError::ParseError(
                    "MonetDB result decoding panicked".into(),
                )))
            }
        }
    }
}

impl BinaryReader {
    fn next_inner(&mut self) -> Option<std::result::Result<RecordBatch, ArrowError>> {
        if self.finished || self.next_row >= self.total_rows {
            self.finished = true;
            return None;
        }
        let remaining = self.total_rows - self.next_row;
        let count = self.scheduler.next_rows(remaining);
        let result = if self.adopt_frame {
            let capacity = monetdb_arrow::owned_frame_capacity(&self.columns, count).unwrap_or(0);
            let mut response = Vec::with_capacity(capacity);
            self.cursor
                .fetch_binary_into(self.next_row, count, &mut response)
                .map_err(|value| ArrowError::ExternalError(Box::new(value)))
                .and_then(|()| {
                    self.scheduler.observe(response.len(), count);
                    shrink_owned_response(&mut response);
                    if let Some(stats) = &self.stats
                        && let Some(stats) = stats
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .as_mut()
                    {
                        stats.record_window(count, response.len(), response.capacity(), true);
                    }
                    monetdb_arrow::decode_frame_owned_with_schema(
                        response,
                        &self.columns,
                        self.result_id,
                        self.next_row,
                        count,
                        Arc::clone(&self.schema),
                    )
                    .map_err(|value| ArrowError::ExternalError(Box::new(value)))
                })
        } else {
            let capacity = self.scheduler.response_capacity(count, false);
            if self.response.capacity() < capacity {
                self.response.reserve_exact(capacity);
            }
            self.cursor
                .fetch_binary_into(self.next_row, count, &mut self.response)
                .map_err(|value| ArrowError::ExternalError(Box::new(value)))
                .and_then(|()| {
                    self.scheduler.observe(self.response.len(), count);
                    if let Some(stats) = &self.stats
                        && let Some(stats) = stats
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .as_mut()
                    {
                        stats.record_window(
                            count,
                            self.response.len(),
                            self.response.capacity(),
                            false,
                        );
                    }
                    monetdb_arrow::decode_frame_with_schema(
                        &self.response,
                        &self.columns,
                        self.result_id,
                        self.next_row,
                        count,
                        Arc::clone(&self.schema),
                    )
                    .map_err(|value| ArrowError::ExternalError(Box::new(value)))
                })
        };
        match &result {
            Ok(batch) if batch.num_rows() > 0 => {
                self.next_row += batch.num_rows() as u64;
            }
            Ok(_) => {
                self.finished = true;
                return Some(Err(ArrowError::ParseError(
                    "MonetDB returned an empty binary window before the result ended".into(),
                )));
            }
            Err(_) => self.finished = true,
        }
        Some(result)
    }
}

impl RecordBatchReader for BinaryReader {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

struct ParameterQueryReader {
    connection: SharedConnection,
    queries: BoundQueryStream,
    current: Option<Box<dyn RecordBatchReader + Send + 'static>>,
    schema: SchemaRef,
    read_options: ReadExecutionOptions,
    timeouts: Timeouts,
    finished: bool,
}

impl Iterator for ParameterQueryReader {
    type Item = std::result::Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| self.next_inner())) {
            Ok(result) => result,
            Err(_) => {
                self.finished = true;
                Some(Err(ArrowError::ParseError(
                    "MonetDB parameter result decoding panicked".into(),
                )))
            }
        }
    }
}

impl ParameterQueryReader {
    fn next_inner(&mut self) -> Option<std::result::Result<RecordBatch, ArrowError>> {
        if self.finished {
            return None;
        }
        loop {
            if let Some(reader) = &mut self.current
                && let Some(batch) = reader.next()
            {
                return Some(batch);
            }
            self.current = None;
            let query = match self.queries.next()? {
                Ok(query) => query,
                Err(value) => return Some(Err(ArrowError::ExternalError(Box::new(value)))),
            };
            match bound_query_reader_with_retry(
                &self.connection,
                &query,
                self.read_options.clone(),
                self.timeouts,
            ) {
                Ok(reader) if reader.schema() == self.schema => self.current = Some(reader),
                Ok(_) => {
                    return Some(Err(ArrowError::SchemaError(
                        "parameterized query schema changed between rows".into(),
                    )));
                }
                Err(value) => return Some(Err(ArrowError::ExternalError(Box::new(value)))),
            }
        }
    }
}

impl RecordBatchReader for ParameterQueryReader {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

struct SingleBatchReader {
    batch: Option<RecordBatch>,
    schema: SchemaRef,
}

impl SingleBatchReader {
    fn new(batch: RecordBatch) -> Self {
        let schema = batch.schema();
        Self {
            batch: Some(batch),
            schema,
        }
    }
}

impl Iterator for SingleBatchReader {
    type Item = std::result::Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.batch.take().map(Ok)
    }
}

impl RecordBatchReader for SingleBatchReader {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

struct SlicedBatchReader {
    batch: RecordBatch,
    schema: SchemaRef,
    offset: usize,
    batch_rows: usize,
}

impl SlicedBatchReader {
    fn new(batch: RecordBatch, batch_rows: usize) -> Self {
        debug_assert!(batch_rows > 0);
        let schema = batch.schema();
        Self {
            batch,
            schema,
            offset: 0,
            batch_rows: batch_rows.max(1),
        }
    }
}

impl Iterator for SlicedBatchReader {
    type Item = std::result::Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.offset >= self.batch.num_rows() {
            return None;
        }
        let rows = self.batch_rows.min(self.batch.num_rows() - self.offset);
        let batch = self.batch.slice(self.offset, rows);
        self.offset += rows;
        Some(Ok(batch))
    }
}

impl RecordBatchReader for SlicedBatchReader {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

struct EmptyReader {
    schema: SchemaRef,
}

impl Default for EmptyReader {
    fn default() -> Self {
        Self {
            schema: Arc::new(Schema::empty()),
        }
    }
}

impl EmptyReader {
    fn new(schema: SchemaRef) -> Self {
        Self { schema }
    }
}

impl Iterator for EmptyReader {
    type Item = std::result::Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        None
    }
}

impl RecordBatchReader for EmptyReader {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

fn quote_identifier(identifier: &str) -> Result<String> {
    if identifier.contains('\0') {
        return Err(error(
            "SQL identifier contains a NUL byte",
            Status::InvalidArguments,
        ));
    }
    Ok(format!("\"{}\"", identifier.replace('"', "\"\"")))
}

fn qualified_name(schema: Option<&str>, table: &str) -> Result<String> {
    match schema {
        Some(schema) => Ok(format!(
            "{}.{}",
            quote_identifier(schema)?,
            quote_identifier(table)?
        )),
        None => quote_identifier(table),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn aligns_append_streams_to_destination_columns_by_name() {
        let column = |name: &str, data_type: MonetType| AppendColumn {
            name: name.to_owned(),
            data_type,
            nullable: true,
        };
        let destination = [
            column("time", MonetType::Timestamp),
            column("a", MonetType::Real),
            column("b", MonetType::Double),
        ];
        let stream = |fields: Vec<Field>| Arc::new(Schema::new(fields));
        let time = Field::new(
            "time",
            DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None),
            true,
        );
        let a = Field::new("a", DataType::Float32, true);
        let b = Field::new("b", DataType::Float64, true);

        assert_eq!(
            align_append_schema(
                &stream(vec![b.clone(), time.clone(), a.clone()]),
                &destination,
                Status::InvalidArguments
            )
            .unwrap(),
            ["b", "time", "a"]
        );
        assert_eq!(
            align_append_schema(
                &stream(vec![b.clone()]),
                &destination,
                Status::InvalidArguments
            )
            .unwrap(),
            ["b"]
        );
        assert_eq!(
            align_append_schema(
                &stream(vec![b.clone().with_name("B")]),
                &destination,
                Status::InvalidArguments
            )
            .unwrap(),
            ["b"]
        );

        let case_distinct_destination =
            [column("a", MonetType::Real), column("A", MonetType::Double)];
        assert_eq!(
            align_append_schema(
                &stream(vec![b.clone().with_name("A"), a.clone()]),
                &case_distinct_destination,
                Status::InvalidArguments
            )
            .unwrap(),
            ["A", "a"]
        );

        let unknown = align_append_schema(
            &stream(vec![b.clone().with_name("missing")]),
            &destination,
            Status::InvalidArguments,
        )
        .unwrap_err();
        assert!(
            unknown
                .message
                .contains("does not exist in the destination")
        );

        let duplicated = align_append_schema(
            &stream(vec![b.clone(), b.clone().with_name("B")]),
            &destination,
            Status::InvalidArguments,
        )
        .unwrap_err();
        assert!(duplicated.message.contains("more than once"));

        let mistyped = align_append_schema(
            &stream(vec![a.clone().with_data_type(DataType::Float64)]),
            &destination,
            Status::InvalidArguments,
        )
        .unwrap_err();
        assert!(mistyped.message.contains("destination type is REAL"));
    }

    #[test]
    fn renders_insert_column_lists_from_the_names_it_is_given() {
        let names = ["a".to_owned(), "b".to_owned()];
        assert_eq!(
            insert_parameter_queries("t", &names).unwrap(),
            [
                "INSERT INTO t (a, b) VALUES (?, ?)",
                "INSERT INTO t (\"a\", \"b\") VALUES (?, ?)"
            ]
        );
        let quoted = ["Col One".to_owned()];
        assert_eq!(
            insert_parameter_queries("t", &quoted).unwrap(),
            ["INSERT INTO t (\"Col One\") VALUES (?)"]
        );
    }

    struct SignallingReader {
        batch: RecordBatch,
        schema: SchemaRef,
        offset: usize,
        signal: std::sync::mpsc::Sender<()>,
    }

    impl Iterator for SignallingReader {
        type Item = std::result::Result<RecordBatch, ArrowError>;

        fn next(&mut self) -> Option<Self::Item> {
            if self.offset >= self.batch.num_rows() {
                return None;
            }
            self.signal.send(()).unwrap();
            let batch = self.batch.slice(self.offset, 1);
            self.offset += 1;
            Some(Ok(batch))
        }
    }

    impl RecordBatchReader for SignallingReader {
        fn schema(&self) -> SchemaRef {
            Arc::clone(&self.schema)
        }
    }

    struct PanickingReader {
        schema: SchemaRef,
    }

    impl Iterator for PanickingReader {
        type Item = std::result::Result<RecordBatch, ArrowError>;

        fn next(&mut self) -> Option<Self::Item> {
            panic!("reader panic")
        }
    }

    impl RecordBatchReader for PanickingReader {
        fn schema(&self) -> SchemaRef {
            Arc::clone(&self.schema)
        }
    }

    struct BlockingReader {
        schema: SchemaRef,
        release: Receiver<()>,
    }

    impl Iterator for BlockingReader {
        type Item = std::result::Result<RecordBatch, ArrowError>;

        fn next(&mut self) -> Option<Self::Item> {
            let _ = self.release.recv();
            None
        }
    }

    impl RecordBatchReader for BlockingReader {
        fn schema(&self) -> SchemaRef {
            Arc::clone(&self.schema)
        }
    }

    #[test]
    fn extracts_sqlstate() {
        assert_eq!(
            parse_sqlstate("42000"),
            Some([
                b'4' as c_char,
                b'2' as c_char,
                b'0' as c_char,
                b'0' as c_char,
                b'0' as c_char
            ])
        );
        assert_eq!(parse_sqlstate("syntax error"), None);
    }

    #[test]
    fn maps_sqlstate_families_to_adbc_statuses() {
        for (message, expected) in [
            ("42000!syntax error", Status::InvalidArguments),
            (
                "42000!SELECT: access denied for user to table 'sys.private'",
                Status::Unauthorized,
            ),
            ("42S02!table not found", Status::NotFound),
            ("42S01!table exists", Status::AlreadyExists),
            ("40002!primary key violation", Status::Integrity),
            ("23503!foreign key violation", Status::Integrity),
            ("42501!permission denied", Status::Unauthorized),
            ("28000!authentication failed", Status::Unauthenticated),
            ("25000!transaction state", Status::InvalidState),
            ("HYT00!query timed out", Status::Timeout),
            ("57014!query interrupted", Status::Cancelled),
            ("XXXXX!vendor condition", Status::Unknown),
            ("unstructured server error", Status::Unknown),
        ] {
            let (sqlstate, diagnostic) = match message.split_once('!') {
                Some((sqlstate, diagnostic)) => (Some(sqlstate), diagnostic),
                None => (None, message),
            };
            assert_eq!(sqlstate_status(sqlstate, diagnostic), expected, "{message}");
        }
    }

    #[test]
    fn maps_nested_socket_attempts_from_the_tcp_error() {
        let nested = |tcp| monetdb::ConnectError::SocketAttempts {
            unix: Box::new(monetdb::ConnectError::UnixDomain),
            tcp: Box::new(tcp),
        };
        let refused = std::io::Error::from(std::io::ErrorKind::ConnectionRefused).into();
        assert_eq!(connect_error_status(&nested(refused)), Status::IO);
        assert_eq!(
            connect_error_status(&nested(monetdb::ConnectError::Timeout)),
            Status::Timeout
        );
    }

    #[test]
    fn maps_malformed_server_replies_as_invalid_data() {
        for value in [
            monetdb::ConnectError::InvalidChallenge("bad".into()),
            monetdb::ConnectError::UnsupportedHashAlgo("bad".into()),
            monetdb::ConnectError::UnexpectedResponse("bad".into()),
            monetdb::ConnectError::TlsDowngrade,
        ] {
            assert_eq!(connect_error_status(&value), Status::InvalidData);
        }
        assert_eq!(
            connect_error_status(&monetdb::ConnectError::TooManyRedirects),
            Status::IO
        );
    }

    #[test]
    fn marks_only_terminal_client_failures_with_structured_error_details() {
        let timeout = map_cursor_error(CursorError::Timeout);
        assert_eq!(timeout.status, Status::Timeout);
        assert_eq!(
            timeout.details,
            Some(vec![(TERMINAL_ERROR_DETAIL.to_owned(), b"true".to_vec())])
        );

        let producer = map_cursor_error(CursorError::FileTransfer("reader failed".into()));
        assert_eq!(producer.status, Status::InvalidData);
        assert!(producer.details.is_none());
    }

    #[test]
    fn bind_validation_rejects_corrupt_string_offsets() {
        use arrow_buffer::{Buffer, OffsetBuffer, ScalarBuffer};

        let offsets = OffsetBuffer::new(ScalarBuffer::from(vec![0i32, 100]));
        // SAFETY: This deliberately models malformed Arrow C data. No value is
        // accessed before `validate_record_batch` rejects the out-of-bounds offset.
        let values = unsafe { StringArray::new_unchecked(offsets, Buffer::from(vec![b'x']), None) };
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "value",
                DataType::Utf8,
                false,
            )])),
            vec![Arc::new(values)],
        )
        .unwrap();
        let rejected = validate_record_batch(&batch).unwrap_err();
        assert_eq!(rejected.status, Status::InvalidData);
        assert!(rejected.message.contains("invalid Arrow data in column 0"));
    }

    #[test]
    fn validates_boolean_options() {
        assert!(option_bool(&OptionValue::String("enabled".into())).unwrap());
        assert!(!option_bool(&OptionValue::String("false".into())).unwrap());
        assert!(option_bool(&OptionValue::String("maybe".into())).is_err());
    }

    #[test]
    fn validates_timeout_options_and_zero_semantics() {
        let mut parameters = Parameters::default();
        apply_parameter_timeout(
            &mut parameters,
            TimeoutOption::Connect,
            CONNECT_TIMEOUT_OPTION,
            &OptionValue::String("7".into()),
        )
        .unwrap();
        let (connect, mut timeouts) = configured_timeouts(&parameters).unwrap();
        assert_eq!(connect, Some(Duration::from_secs(7)));

        set_runtime_timeout(
            &mut timeouts,
            TimeoutOption::Operation,
            OPERATION_TIMEOUT_OPTION,
            &OptionValue::Int(0),
        )
        .unwrap();
        assert_eq!(timeouts.operation, None);
        assert!(timeout_seconds(READ_TIMEOUT_OPTION, &OptionValue::String("-1".into())).is_err());
        assert!(
            timeout_seconds(
                READ_TIMEOUT_OPTION,
                &OptionValue::Int(monetdb::MAX_TIMEOUT_SECONDS + 1),
            )
            .is_err()
        );
        assert!(timeout_seconds(WRITE_TIMEOUT_OPTION, &OptionValue::Double(1.0)).is_err());
        assert_eq!(
            initialization_timeouts(timeouts, Some(Instant::now() - Duration::from_millis(1)))
                .unwrap_err()
                .status,
            Status::Timeout
        );
    }

    #[test]
    fn validates_ingest_scheduling_and_failure_options() {
        assert_eq!(
            read_batch_rows_option(&OptionValue::String("131072".into())).unwrap(),
            131_072
        );
        assert_eq!(read_batch_rows_option(&OptionValue::Int(0)).unwrap(), 0);
        assert_eq!(
            read_batch_rows_option(&OptionValue::Int(10_000)).unwrap(),
            8_192
        );
        assert_eq!(
            read_batch_rows_option(&OptionValue::Int(32_768)).unwrap(),
            32_768
        );
        assert_eq!(
            read_batch_rows_option(&OptionValue::Int(196_608)).unwrap(),
            262_144
        );
        assert_eq!(
            read_batch_rows_option(&OptionValue::Int(300_000)).unwrap(),
            262_144
        );
        assert_eq!(write_batch_rows_option(&OptionValue::Int(0)).unwrap(), None);
        assert_eq!(
            write_batch_rows_option(&OptionValue::Int(100_000)).unwrap(),
            Some(100_000)
        );
        assert!(read_batch_rows_option(&OptionValue::Int(-1)).is_err());
        assert_eq!(
            read_window_bytes_option(&OptionValue::Int(0)).unwrap(),
            None
        );
        assert_eq!(
            read_window_bytes_option(&OptionValue::Int(DEFAULT_READ_WINDOW_BYTES as i64)).unwrap(),
            Some(DEFAULT_READ_WINDOW_BYTES)
        );
        assert!(read_window_bytes_option(&OptionValue::Int(1)).is_err());
        assert!(write_batch_rows_option(&OptionValue::Int(-1)).is_err());
        assert!(write_batch_rows_option(&OptionValue::Double(1.0)).is_err());
        assert_eq!(
            write_window_bytes_option(&OptionValue::Int(0)).unwrap(),
            None
        );
        assert_eq!(
            write_window_bytes_option(&OptionValue::Int(DEFAULT_WRITE_WINDOW_BYTES as i64))
                .unwrap(),
            Some(DEFAULT_WRITE_WINDOW_BYTES)
        );
        assert!(write_window_bytes_option(&OptionValue::Int(1)).is_err());
        assert_eq!(
            IngestPartial::parse(&OptionValue::String("block".into())).unwrap(),
            IngestPartial::Block
        );
        assert_eq!(
            IngestAtomicity::parse(&OptionValue::String("savepoint".into())).unwrap(),
            IngestAtomicity::Savepoint
        );
        assert_eq!(
            ConstrainedAppend::parse(&OptionValue::String("auto".into())).unwrap(),
            ConstrainedAppend::Auto
        );
        assert!(IngestPartial::parse(&OptionValue::String("invalid".into())).is_err());
        assert!(IngestAtomicity::parse(&OptionValue::Int(1)).is_err());
        assert!(ConstrainedAppend::parse(&OptionValue::String("stage".into())).is_err());
        for (value, expected) in [
            ("none", WireCompression::None),
            ("auto", WireCompression::Auto),
            ("lz4", WireCompression::Lz4),
        ] {
            assert_eq!(
                WireCompression::parse(&OptionValue::String(value.into())).unwrap(),
                expected
            );
        }
        assert!(WireCompression::parse(&OptionValue::String("gzip".into())).is_err());
        assert!(WireCompression::parse(&OptionValue::Int(1)).is_err());
    }

    #[test]
    fn adaptive_ingest_defaults_only_expand_for_measured_costs() {
        assert_eq!(adaptive_insert_rows(0, Duration::from_secs(1)), 0);
        assert_eq!(adaptive_insert_rows(100, Duration::from_micros(100)), 100);
        assert_eq!(adaptive_insert_rows(100, Duration::from_millis(2)), 2_000);
        assert_eq!(
            adaptive_insert_rows(100, Duration::from_secs(1)),
            MAX_ADAPTIVE_INSERT_ROWS
        );

        assert_eq!(
            adaptive_incompressible_window_bytes(
                DEFAULT_WRITE_WINDOW_BYTES,
                Duration::from_micros(100),
            ),
            INCOMPRESSIBLE_WRITE_WINDOW_BYTES
        );
        assert_eq!(
            adaptive_incompressible_window_bytes(
                DEFAULT_WRITE_WINDOW_BYTES,
                REMOTE_WRITE_WINDOW_RTT,
            ),
            DEFAULT_WRITE_WINDOW_BYTES
        );
        assert_eq!(
            adaptive_incompressible_window_bytes(MIN_WRITE_WINDOW_BYTES, Duration::ZERO),
            MIN_WRITE_WINDOW_BYTES
        );
    }

    #[test]
    fn automatic_write_windows_follow_memory_and_network_budgets() {
        assert_eq!(
            automatic_write_window_bytes_for_memory(None),
            DEFAULT_WRITE_WINDOW_BYTES
        );
        assert_eq!(
            automatic_write_window_bytes_for_memory(Some(DEFAULT_WRITE_WINDOW_BYTES)),
            INCOMPRESSIBLE_WRITE_WINDOW_BYTES
        );
        assert_eq!(
            automatic_write_window_bytes_for_memory(Some(32 * 1024 * 1024)),
            MIN_WRITE_WINDOW_BYTES
        );
        assert_eq!(
            adaptive_physical_window_bytes(DEFAULT_WRITE_WINDOW_BYTES, Duration::ZERO, 1),
            LOCAL_PHYSICAL_WINDOW_BYTES
        );
        assert_eq!(
            adaptive_physical_window_bytes(
                DEFAULT_WRITE_WINDOW_BYTES,
                Duration::ZERO,
                WIDE_MEMORY_BOUND_COLUMNS,
            ),
            WIDE_LOCAL_PHYSICAL_WINDOW_BYTES
        );
        assert_eq!(
            adaptive_physical_window_bytes(
                DEFAULT_WRITE_WINDOW_BYTES,
                REMOTE_WRITE_WINDOW_RTT,
                WIDE_MEMORY_BOUND_COLUMNS,
            ),
            DEFAULT_WRITE_WINDOW_BYTES
        );
        assert_eq!(
            adaptive_physical_window_bytes(
                MIN_WRITE_WINDOW_BYTES,
                Duration::ZERO,
                WIDE_MEMORY_BOUND_COLUMNS,
            ),
            MIN_WRITE_WINDOW_BYTES
        );
    }

    #[test]
    fn automatic_read_windows_follow_memory_and_network_budgets() {
        assert_eq!(
            automatic_read_window_bytes_for_memory(None, Duration::ZERO),
            DEFAULT_READ_WINDOW_BYTES
        );
        assert_eq!(
            automatic_read_window_bytes_for_memory(None, REMOTE_WRITE_WINDOW_RTT),
            REMOTE_READ_WINDOW_BYTES
        );
        assert_eq!(
            automatic_read_window_bytes_for_memory(Some(64 * 1024 * 1024), Duration::ZERO),
            8 * 1024 * 1024
        );
        assert_eq!(
            automatic_read_window_bytes_for_memory(Some(1024), Duration::ZERO),
            MIN_READ_WINDOW_BYTES
        );
        assert_eq!(
            effective_automatic_read_window_bytes(
                DEFAULT_READ_WINDOW_BYTES,
                Some(412 * 1024 * 1024),
                None,
            ),
            412 * 1024 * 1024
        );
        assert_eq!(
            effective_automatic_read_window_bytes(
                DEFAULT_READ_WINDOW_BYTES,
                Some(412 * 1024 * 1024),
                Some(2 * 1024 * 1024 * 1024),
            ),
            DEFAULT_READ_WINDOW_BYTES
        );
        assert_eq!(
            effective_automatic_read_window_bytes(
                DEFAULT_READ_WINDOW_BYTES,
                Some(513 * 1024 * 1024),
                None,
            ),
            DEFAULT_READ_WINDOW_BYTES
        );
    }

    #[test]
    fn read_window_alignment_uses_server_granules_and_power_of_two_divisors() {
        assert_eq!(export_alignment_rows(1), 1);
        assert_eq!(export_alignment_rows(21_311), 16_384);
        assert_eq!(export_alignment_rows(65_536), 65_536);
        assert_eq!(export_alignment_rows(131_071), 65_536);
        assert_eq!(export_alignment_rows(131_072), 131_072);
        assert_eq!(export_alignment_rows(usize::MAX), 131_072);
    }

    #[test]
    fn read_window_scheduler_adapts_without_unbounded_growth() {
        let mut scheduler = ReadWindowScheduler {
            exact_batch_rows: None,
            window_budget_bytes: 100,
            full_granule_fits: false,
            estimated_bytes_per_row: 1000,
            initial_rows_limit: Some(INITIAL_VARIABLE_READ_ROWS),
            previous_rows: None,
            observed_rows: 0,
        };
        assert_eq!(scheduler.next_rows(1000), 1);
        scheduler.observe(1, 1);
        assert_eq!(scheduler.next_rows(1000), 3);
        scheduler.observe(3, 3);
        assert_eq!(scheduler.next_rows(1000), 12);

        scheduler.observe(1_200, 12);
        assert_eq!(scheduler.next_rows(3), 1);
    }

    #[test]
    fn fixed_width_granule_fit_survives_observed_width_rounding() {
        let mut scheduler = ReadWindowScheduler {
            exact_batch_rows: None,
            window_budget_bytes: 412_651_812,
            full_granule_fits: true,
            estimated_bytes_per_row: 3_148,
            initial_rows_limit: None,
            previous_rows: None,
            observed_rows: 0,
        };
        assert_eq!(scheduler.next_rows(300_000), EXPORT_GRANULE_ROWS);
        scheduler.observe(412_627_288, EXPORT_GRANULE_ROWS);
        assert_eq!(scheduler.estimated_bytes_per_row, 3_149);
        assert_eq!(scheduler.next_rows(168_928), EXPORT_GRANULE_ROWS);
        scheduler.observe(412_627_288, EXPORT_GRANULE_ROWS);
        assert_eq!(scheduler.next_rows(37_856), 37_856);
    }

    #[test]
    fn exact_read_batch_rows_override_the_byte_scheduler() {
        let mut scheduler = ReadWindowScheduler {
            exact_batch_rows: Some(17),
            window_budget_bytes: 1,
            full_granule_fits: false,
            estimated_bytes_per_row: usize::MAX,
            initial_rows_limit: None,
            previous_rows: None,
            observed_rows: 0,
        };
        assert_eq!(scheduler.next_rows(100), 17);
        scheduler.observe(usize::MAX, 17);
        assert_eq!(scheduler.next_rows(9), 9);
    }

    #[test]
    fn ingest_memory_reservations_keep_headroom_for_concurrent_work() {
        let reservation = ingest_memory_reservation_bytes(128 * 1024 * 1024);
        assert_eq!(reservation, 352 * 1024 * 1024);
        assert!(ingest_memory_reservation_fits(
            800 * 1024 * 1024,
            128 * 1024 * 1024,
            0,
            reservation,
        ));
        assert!(!ingest_memory_reservation_fits(
            800 * 1024 * 1024,
            128 * 1024 * 1024,
            reservation,
            reservation,
        ));
        assert!(!ingest_memory_reservation_fits(
            usize::MAX,
            usize::MAX,
            1,
            1,
        ));
    }

    #[test]
    #[ignore = "run in an isolated Linux cgroup with a 512 MiB memory limit"]
    fn concurrent_ingest_reservation_fails_before_cgroup_oom() {
        let (limit, _) = cgroup_memory_limit_and_usage_bytes().expect("cgroup memory accounting");
        assert!(limit <= 512 * 1024 * 1024);

        let first = reserve_ingest_memory(128 * 1024 * 1024).expect("first reservation");
        let error = match reserve_ingest_memory(128 * 1024 * 1024) {
            Ok(_) => panic!("second reservation must fail"),
            Err(error) => error,
        };
        assert_eq!(error.status, Status::Internal);
        assert_eq!(error.sqlstate, parse_sqlstate("HY001").unwrap());
        assert!(error.message.contains("reserved by other ingests"));

        drop(first);
        assert!(reserve_ingest_memory(128 * 1024 * 1024).is_ok());
    }

    #[test]
    fn timeout_options_follow_uri_database_connection_statement_precedence() {
        let mut parameters =
            Parameters::from_url("monetdb://localhost/test?read_timeout=7").unwrap();
        apply_parameter_timeout(
            &mut parameters,
            TimeoutOption::Read,
            READ_TIMEOUT_OPTION,
            &OptionValue::String("6".into()),
        )
        .unwrap();
        apply_parameter_timeout(
            &mut parameters,
            TimeoutOption::Read,
            READ_TIMEOUT_OPTION,
            &OptionValue::String("5".into()),
        )
        .unwrap();
        let (_, mut timeouts) = configured_timeouts(&parameters).unwrap();
        assert_eq!(timeouts.read, Some(Duration::from_secs(5)));

        set_runtime_timeout(
            &mut timeouts,
            TimeoutOption::Read,
            READ_TIMEOUT_OPTION,
            &OptionValue::String("4".into()),
        )
        .unwrap();
        assert_eq!(timeouts.read, Some(Duration::from_secs(4)));
    }

    #[test]
    fn validates_uri_query_keys_and_reads_back_uri_timeouts() {
        let uri = "monetdb://localhost/test?connect_timeout=9&read_timeout=8&write_timeout=7&operation_timeout=6";
        let mut database = MonetdbDatabase::default();
        database
            .set_option(OptionDatabase::Uri, uri.into())
            .unwrap();
        for (key, expected) in [
            (CONNECT_TIMEOUT_OPTION, "9"),
            (READ_TIMEOUT_OPTION, "8"),
            (WRITE_TIMEOUT_OPTION, "7"),
            (OPERATION_TIMEOUT_OPTION, "6"),
        ] {
            assert_eq!(
                database
                    .get_option_string(OptionDatabase::Other(key.into()))
                    .unwrap(),
                expected
            );
        }

        database
            .set_option(
                OptionDatabase::Other(READ_TIMEOUT_OPTION.into()),
                "5".into(),
            )
            .unwrap();
        assert_eq!(
            database
                .get_option_string(OptionDatabase::Other(READ_TIMEOUT_OPTION.into()))
                .unwrap(),
            "5"
        );

        for key in URI_QUERY_KEYS {
            parse_driver_uri(&format!("monetdb://localhost/test?{key}=0")).unwrap();
        }
        let error = parse_driver_uri("monetdb://localhost/test?operation_timout=1").unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments);
        assert!(error.message.contains("operation_timout"));
    }

    #[test]
    fn validates_and_reads_back_client_info_database_options() {
        let mut database = MonetdbDatabase::default();
        database
            .set_option(
                OptionDatabase::Uri,
                "monetdb://localhost/test?client_application=uri-app&client_remark=uri-remark"
                    .into(),
            )
            .unwrap();
        assert_eq!(
            database
                .get_option_string(OptionDatabase::Other(CLIENT_APPLICATION_OPTION.into()))
                .unwrap(),
            "uri-app"
        );
        assert_eq!(
            database
                .get_option_string(OptionDatabase::Other(CLIENT_REMARK_OPTION.into()))
                .unwrap(),
            "uri-remark"
        );
        assert_eq!(
            database
                .get_option_string(OptionDatabase::Other(CLIENT_INFO_OPTION.into()))
                .unwrap(),
            "true"
        );

        database
            .set_option(
                OptionDatabase::Other(CLIENT_APPLICATION_OPTION.into()),
                "explicit-app".into(),
            )
            .unwrap();
        assert_eq!(
            database
                .get_option_string(OptionDatabase::Other(CLIENT_APPLICATION_OPTION.into()))
                .unwrap(),
            "explicit-app"
        );
        database
            .set_option(
                OptionDatabase::Other(CLIENT_INFO_OPTION.into()),
                "disabled".into(),
            )
            .unwrap();
        assert_eq!(
            database
                .get_option_string(OptionDatabase::Other(CLIENT_INFO_OPTION.into()))
                .unwrap(),
            "false"
        );

        let newline = database
            .set_option(
                OptionDatabase::Other(CLIENT_REMARK_OPTION.into()),
                "first\nsecond".into(),
            )
            .unwrap_err();
        assert_eq!(newline.status, Status::InvalidArguments);
        assert!(newline.message.contains("must not contain newlines"));

        let prefix =
            parse_driver_uri("monetdb://localhost/test?client_prefix=impersonate").unwrap_err();
        assert_eq!(prefix.status, Status::InvalidArguments);
        assert!(prefix.message.contains("client_prefix"));
    }

    #[test]
    fn quotes_identifiers() {
        assert_eq!(quote_identifier("a\"b").unwrap(), "\"a\"\"b\"");
        assert!(quote_identifier("a\0b").is_err());
        assert_eq!(decode_userinfo("").unwrap(), "");
        assert!(is_prepare_statement("  PrEpArE SELECT 1"));
        assert!(is_prepare_statement(
            "/* cannot obscure the statement kind */ -- comment\n PREPARE SELECT 1"
        ));
        assert!(is_prepare_statement("# comment\nPREPARE SELECT 1"));
        assert!(!is_prepare_statement("prepared_value"));
        assert_eq!(
            qualified_name(
                Some("s'; DROP SCHEMA sys; --"),
                "t\"; DELETE FROM guard; --"
            )
            .unwrap(),
            "\"s'; DROP SCHEMA sys; --\".\"t\"\"; DELETE FROM guard; --\""
        );
    }

    #[test]
    fn identifies_queries_that_can_invalidate_prepared_statements() {
        for query in [
            "ALTER TABLE t ADD COLUMN value INT",
            "/* comment */ CREATE TABLE t(value INT)",
            "INSERT INTO t VALUES (1); DROP TABLE t",
            "SET SCHEMA sys",
        ] {
            assert!(query_invalidates_prepared_cache(query).unwrap(), "{query}");
        }
        for query in [
            "SELECT * FROM t",
            "INSERT INTO t VALUES (1)",
            "UPDATE t SET value = 2",
            "DELETE FROM t",
        ] {
            assert!(!query_invalidates_prepared_cache(query).unwrap(), "{query}");
        }
    }

    #[test]
    fn identifies_transaction_commands_across_sql_scripts() {
        let effects =
            transaction_effects("SELECT 1; /* keep */ ROLLBACK; -- then\n COMMIT").unwrap();
        assert!(effects.rollback);
        assert!(effects.commit);

        let effects =
            transaction_effects("SELECT 'COMMIT'; SELECT \"ROLLBACK\" FROM sys.tables").unwrap();
        assert!(!effects.rollback);
        assert!(!effects.commit);

        for statement in [
            "ROLLBACK TO SAVEPOINT s",
            "/* recover scope */ rollback /* target */ to savepoint s",
        ] {
            assert!(!transaction_effects(statement).unwrap().rollback);
        }
    }

    #[test]
    fn database_standard_options_require_strings() {
        for key in [
            OptionDatabase::Uri,
            OptionDatabase::Username,
            OptionDatabase::Password,
        ] {
            let mut database = MonetdbDatabase::default();
            let error = database.set_option(key, OptionValue::Int(1)).unwrap_err();
            assert_eq!(error.status, Status::InvalidArguments);
            assert!(error.message.contains("must be a string"));
        }
    }

    #[test]
    fn atomic_cleanup_keeps_the_root_status_and_sqlstate() {
        let mut root = error("operation failed", Status::Cancelled);
        root.sqlstate = parse_sqlstate("57014").unwrap();
        let combined =
            combine_atomic_error::<()>(Err(root), Err(CursorError::Closed), "transaction recovery")
                .unwrap_err();
        assert_eq!(combined.status, Status::Cancelled);
        assert_eq!(combined.sqlstate, parse_sqlstate("57014").unwrap());
        assert!(
            combined
                .message
                .contains("transaction recovery also failed")
        );
    }

    #[test]
    fn option_getters_distinguish_missing_and_wrong_types() {
        let mut options = Options::default();
        options.set("value", OptionValue::Int(1));
        let wrong_type = options.get_string("value").unwrap_err();
        assert_eq!(wrong_type.status, Status::InvalidData);
        assert!(wrong_type.message.contains("integer, not string"));
        assert_eq!(
            options.get_string("missing").unwrap_err().status,
            Status::NotFound
        );
    }

    #[test]
    fn builds_canonical_info_batches() {
        let batch = info_batch((11, 55, 7), None).unwrap();
        assert_eq!(batch.schema(), GET_INFO_SCHEMA.clone());
        assert_eq!(batch.num_rows(), 11);
        let names = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap();
        assert_eq!(names.value(0), u32::from(&InfoCode::VendorName));

        let empty = info_batch((11, 55, 7), Some(HashSet::new())).unwrap();
        assert_eq!(empty.schema(), GET_INFO_SCHEMA.clone());
        assert_eq!(empty.num_rows(), 0);
    }

    #[test]
    fn slices_inline_batches_at_the_configured_read_size() {
        let batch = RecordBatch::try_from_iter([(
            "value",
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])) as ArrayRef,
        )])
        .unwrap();
        let rows = SlicedBatchReader::new(batch, 2)
            .map(|batch| batch.unwrap().num_rows())
            .collect::<Vec<_>>();
        assert_eq!(rows, [2, 2, 1]);
    }

    #[test]
    fn maps_clob_prepare_metadata_to_string() {
        assert_eq!(
            prepared_monet_type("clob", 1024, 0).unwrap(),
            MonetType::Varchar(1024)
        );
    }

    #[test]
    fn maps_text_only_prepare_parameters_without_enabling_binary_results() {
        let parameter = prepared_parameter_arrow_field("0".into(), &MonetType::Inet).unwrap();
        assert_eq!(parameter.data_type(), &DataType::Utf8);
        assert!(prepared_arrow_field("inet".into(), &MonetType::Inet).is_err());
    }

    #[test]
    fn declared_type_restoration_never_guesses_across_schemas() {
        let mut declared = HashMap::from([
            (
                ("first".into(), "shared".into(), "value".into()),
                MonetType::Decimal(10, 2),
            ),
            (
                ("second".into(), "shared".into(), "value".into()),
                MonetType::Decimal(18, 4),
            ),
        ]);
        let mut field = PreparedField {
            data_type: MonetType::Decimal(3, 2),
            undetermined: false,
            name: Some("value".into()),
            origin_schema: None,
            origin_table: Some("shared".into()),
        };
        assert_eq!(declared_result_type(&field, &declared), None);

        field.origin_schema = Some("first".into());
        assert_eq!(
            declared_result_type(&field, &declared),
            Some(MonetType::Decimal(10, 2))
        );

        field.origin_schema = None;
        declared.insert(
            ("second".into(), "shared".into(), "value".into()),
            MonetType::Decimal(10, 2),
        );
        assert_eq!(
            declared_result_type(&field, &declared),
            Some(MonetType::Decimal(10, 2))
        );
    }

    #[test]
    fn ingest_scheduler_coalesces_and_splits_upstream_batches() {
        let batch = RecordBatch::try_from_iter([(
            "value",
            Arc::new(Int64Array::from_iter_values(0..250_001)) as ArrayRef,
        )])
        .unwrap();
        let schema = batch.schema();
        let mut reader = SlicedBatchReader::new(batch, 777);
        let mut scheduler = IngestWindowScheduler::new(
            &mut reader,
            schema,
            DEFAULT_WRITE_WINDOW_BYTES,
            LOCAL_PHYSICAL_WINDOW_BYTES,
            INCOMPRESSIBLE_WRITE_WINDOW_BYTES,
            Some(100_000),
            WireCompression::None,
        );
        let mut windows = Vec::new();
        while let Some(window) = scheduler.next_window().unwrap() {
            windows.push(window.rows);
        }

        assert_eq!(windows, [100_000, 100_000, 50_001]);
        assert_eq!(scheduler.input_batches, 322);
        assert_eq!(scheduler.coalesced_windows, 3);
        assert_eq!(scheduler.split_windows, 2);
    }

    #[test]
    fn encoded_columns_keep_only_material_compression_savings() {
        let mut compressed = CompressedArena::default();
        let mut compressible = EncodedColumn::new(None, WireCompression::None);
        compressible
            .push(&vec![0; MIN_ENCODE_BUFFER_BYTES], &mut compressed)
            .unwrap();
        compressible.finish(&mut compressed).unwrap();
        assert!(matches!(
            compressible.compression,
            CompressionMode::Enabled(_)
        ));
        assert!(compressible.stored_bytes() < MIN_ENCODE_BUFFER_BYTES);

        let mut state = 0x9e3779b97f4a7c15u64;
        let incompressible_bytes = (0..MIN_ENCODE_BUFFER_BYTES)
            .map(|_| {
                state ^= state << 7;
                state ^= state >> 9;
                state ^= state << 8;
                state as u8
            })
            .collect::<Vec<_>>();
        let mut incompressible = EncodedColumn::new(None, WireCompression::None);
        incompressible
            .push(&incompressible_bytes, &mut compressed)
            .unwrap();
        incompressible.finish(&mut compressed).unwrap();
        assert!(matches!(
            incompressible.compression,
            CompressionMode::Disabled
        ));
        assert_eq!(incompressible.stored_bytes(), MIN_ENCODE_BUFFER_BYTES);
    }

    #[test]
    fn compressed_storage_coalesces_pieces_without_heap_retention() {
        let mut compressed = CompressedArena::default();
        let first = compressed
            .store(&vec![1; MIN_ENCODE_BUFFER_BYTES / 4])
            .unwrap();
        let second = compressed
            .store(&vec![2; MIN_ENCODE_BUFFER_BYTES / 4])
            .unwrap();

        assert_eq!(compressed.slabs.len(), 1);
        assert_eq!(first.slab, second.slab);
        assert_eq!(compressed.physical_bytes(), COMPRESSED_ARENA_SLAB_BYTES);
        assert_eq!(compressed.used_bytes(), MIN_ENCODE_BUFFER_BYTES / 2);
        assert_eq!(
            first.slice(&compressed, MIN_ENCODE_BUFFER_BYTES / 4),
            vec![1; MIN_ENCODE_BUFFER_BYTES / 4]
        );
    }

    #[test]
    fn compression_probe_selects_shuffle_from_observed_timestamp_bytes() {
        let timestamps = (0..32_768i64)
            .flat_map(|value| (1_700_000_000_000_000i64 + value * 1_000).to_le_bytes())
            .collect::<Vec<_>>();
        let mut column = EncodedColumn::new(Some(8), WireCompression::None);
        let mut compressed = CompressedArena::default();
        column.push(&timestamps, &mut compressed).unwrap();
        column.finish(&mut compressed).unwrap();

        assert_eq!(
            column.compression,
            CompressionMode::Enabled(CompressionTransform::Shuffle(8))
        );
        assert!(column.stored_bytes() < timestamps.len() / 4);
    }

    #[test]
    fn automatic_compression_uses_shuffle_without_claiming_wire_lz4() {
        let timestamps = arrow_array::TimestampMicrosecondArray::from_iter_values(
            (0..32_768).map(|value| 1_700_000_000_000_000i64 + value * 1_000),
        );
        let batch =
            RecordBatch::try_from_iter([("time", Arc::new(timestamps) as ArrayRef)]).unwrap();
        let schema = batch.schema();
        let estimated = estimated_batch_size(&schema, &batch).unwrap();
        let mut window = EncodedIngestWindow::for_schema(&schema, WireCompression::Auto).unwrap();
        window
            .append(
                &schema,
                vec![PendingBatch {
                    batch,
                    estimated_bytes: estimated,
                    compaction_level: 0,
                }],
                estimated,
            )
            .unwrap();
        window.finish().unwrap();

        assert!(matches!(
            window.columns[0].compression,
            CompressionMode::Enabled(CompressionTransform::Shuffle(_))
        ));
        assert!(matches!(
            window.columns[0].chunks.first(),
            Some(EncodedChunk::Lz4 {
                format: CompressionFormat::Block,
                ..
            })
        ));
        assert!(!window.uses_wire_lz4());
    }

    #[test]
    fn pinned_buffer_accounting_uses_the_parent_allocation() {
        let parent = Int64Array::from_iter_values(0..1_024);
        let sliced = parent.slice(100, 10);
        let batch = RecordBatch::try_from_iter([("value", Arc::new(sliced) as ArrayRef)]).unwrap();

        let buffers = pinned_record_batch_buffers([&batch]);

        assert_eq!(buffers.len(), 1);
        assert!(buffer_bytes(&buffers) >= 1_024 * std::mem::size_of::<i64>());
    }

    #[test]
    fn ingest_stats_attribute_prefetched_window_memory() {
        let mut stats = IngestStats::new(512, 256, 128, 0, Duration::ZERO, "savepoint");
        stats.record_window(
            CompletedIngestWindow {
                rows: 10,
                bytes: 100,
                stored_bytes: 300,
                storage: WindowStorage::Arrow,
                wire_lz4: false,
                memory: WindowMemoryUsage {
                    encoded_stored_bytes: 100,
                    retained_buffers: HashMap::from([(1, 200)]),
                    peak_staging_bytes: 200,
                    peak_build_bytes: 300,
                    peak_build_buffers: HashMap::from([(1, 200)]),
                    scratch_bytes: 10,
                },
                streaming: IngestStreamingUsage {
                    largest_chunk: 0,
                    largest_column_bytes: 0,
                },
            },
            false,
        );
        stats.record_window(
            CompletedIngestWindow {
                rows: 10,
                bytes: 100,
                stored_bytes: 250,
                storage: WindowStorage::Arrow,
                wire_lz4: false,
                memory: WindowMemoryUsage {
                    encoded_stored_bytes: 50,
                    retained_buffers: HashMap::from([(1, 200)]),
                    peak_staging_bytes: 300,
                    peak_build_bytes: 350,
                    peak_build_buffers: HashMap::from([(1, 200), (2, 100)]),
                    scratch_bytes: 20,
                },
                streaming: IngestStreamingUsage {
                    largest_chunk: 0,
                    largest_column_bytes: 0,
                },
            },
            true,
        );

        assert_eq!(stats.physical_peaks(), (370, 480));
        let json = stats.to_json();
        assert!(json.contains("\"window_physical_stored_bytes\":[300,250]"));
        assert!(json.contains("\"window_retained_arrow_pinned_bytes\":[200,200]"));
        assert!(json.contains("\"peak_prefetch_physical_bytes\":480"));
    }

    #[test]
    fn compression_probe_rejects_random_fixed_width_bytes() {
        let mut state = 0x9e3779b97f4a7c15u64;
        let values = (0..32_768)
            .flat_map(|_| {
                state ^= state << 7;
                state ^= state >> 9;
                state ^= state << 8;
                state.to_le_bytes()
            })
            .collect::<Vec<_>>();
        let mut column = EncodedColumn::new(Some(8), WireCompression::None);
        let mut compressed = CompressedArena::default();
        column.push(&values, &mut compressed).unwrap();
        column.finish(&mut compressed).unwrap();

        assert_eq!(column.compression, CompressionMode::Disabled);
        assert_eq!(column.stored_bytes(), values.len());
    }

    #[test]
    fn incompressible_nullable_batches_do_not_switch_to_retained_encoding() {
        let mut state = 0x9e3779b97f4a7c15u64;
        let values = (0..32_768)
            .map(|index| {
                state ^= state << 7;
                state ^= state >> 9;
                state ^= state << 8;
                (index != 7).then_some(state as i64)
            })
            .collect::<Vec<_>>();
        let batch =
            RecordBatch::try_from_iter([("value", Arc::new(Int64Array::from(values)) as ArrayRef)])
                .unwrap();
        let schema = batch.schema();
        let estimated = estimated_batch_size(&schema, &batch).unwrap();
        let mut window = EncodedIngestWindow::for_schema(&schema, WireCompression::None).unwrap();

        window
            .append(
                &schema,
                vec![PendingBatch {
                    batch,
                    estimated_bytes: estimated,
                    compaction_level: 0,
                }],
                estimated,
            )
            .unwrap();

        assert!(window.incompressible);
        assert!(!window.retain_arrow_eligible);
        assert!(!window.retain_arrow);
    }

    #[test]
    fn compression_probe_selects_each_column_independently() {
        let timestamps = arrow_array::TimestampMicrosecondArray::from_iter_values(
            (0..32_768).map(|value| 1_700_000_000_000_000i64 + value * 1_000),
        );
        let mut state = 0x9e3779b97f4a7c15u64;
        let random = Int64Array::from_iter_values((0..32_768).map(|_| {
            state ^= state << 7;
            state ^= state >> 9;
            state ^= state << 8;
            state as i64
        }));
        let batch = RecordBatch::try_from_iter([
            ("time", Arc::new(timestamps) as ArrayRef),
            ("value", Arc::new(random) as ArrayRef),
        ])
        .unwrap();
        let schema = batch.schema();
        let estimated = estimated_batch_size(&schema, &batch).unwrap();
        let mut window = EncodedIngestWindow::for_schema(&schema, WireCompression::None).unwrap();
        window
            .append(
                &schema,
                vec![PendingBatch {
                    batch,
                    estimated_bytes: estimated,
                    compaction_level: 0,
                }],
                estimated,
            )
            .unwrap();
        window.finish().unwrap();

        assert_eq!(
            window.columns[0].compression,
            CompressionMode::Enabled(CompressionTransform::Shuffle(12))
        );
        assert_eq!(window.columns[1].compression, CompressionMode::Disabled);
    }

    #[test]
    fn compression_falls_back_when_the_sample_does_not_represent_the_tail() {
        let mut column = EncodedColumn::new(Some(8), WireCompression::None);
        let mut compressed = CompressedArena::default();
        column
            .push(&vec![0; ENCODE_CHUNK_BYTES], &mut compressed)
            .unwrap();
        assert!(matches!(column.compression, CompressionMode::Enabled(_)));

        let mut state = 0x9e3779b97f4a7c15u64;
        let tail = (0..ENCODE_CHUNK_BYTES / 8)
            .flat_map(|_| {
                state ^= state << 7;
                state ^= state >> 9;
                state ^= state << 8;
                state.to_le_bytes()
            })
            .collect::<Vec<_>>();
        column.push(&tail, &mut compressed).unwrap();
        column.finish(&mut compressed).unwrap();

        assert_eq!(column.compression, CompressionMode::Disabled);
        assert!(matches!(column.chunks[0], EncodedChunk::Lz4 { .. }));
        assert!(matches!(column.chunks[1], EncodedChunk::Raw(_)));
    }

    struct CollectingUploadSink {
        bytes: Vec<u8>,
    }

    impl monetdb::UploadSink for CollectingUploadSink {
        fn write_chunk(&mut self, data: &[u8]) -> std::result::Result<(), monetdb::CursorError> {
            self.bytes.extend_from_slice(data);
            Ok(())
        }
    }

    #[test]
    fn retained_encoder_streams_multiple_chunks_without_deadlock() {
        let values = Int64Array::from_iter_values(0..400_000);
        let batch = RecordBatch::try_from_iter([("value", Arc::new(values) as ArrayRef)]).unwrap();
        let schema = batch.schema();
        let (request, receiver, encoder) = start_retained_encoder(Arc::clone(&schema), vec![batch]);
        let mut sink = CollectingUploadSink { bytes: Vec::new() };
        let mut largest_chunk = 0;

        request.send(0).unwrap();
        let encoded = upload_retained_column(&receiver, 0, &mut sink, &mut largest_chunk).unwrap();
        drop(request);
        encoder.join().unwrap();

        assert_eq!(encoded, sink.bytes.len());
        assert_eq!(encoded, 400_000 * 8);
        assert_eq!(largest_chunk, ENCODE_CHUNK_BYTES);
    }

    #[test]
    fn retained_encoder_propagates_encode_errors_without_deadlock() {
        let values = arrow_array::Float64Array::from(vec![1.0, f64::NAN]);
        let batch = RecordBatch::try_from_iter([("value", Arc::new(values) as ArrayRef)]).unwrap();
        let schema = batch.schema();
        let (request, receiver, encoder) = start_retained_encoder(schema, vec![batch]);
        let mut sink = CollectingUploadSink { bytes: Vec::new() };
        let mut largest_chunk = 0;

        request.send(0).unwrap();
        let rejected =
            upload_retained_column(&receiver, 0, &mut sink, &mut largest_chunk).unwrap_err();
        drop(request);
        encoder.join().unwrap();

        assert!(rejected.to_string().contains("non-finite"));
        assert!(sink.bytes.is_empty());
    }

    #[test]
    fn retained_encoder_follows_the_server_column_order() {
        let batch = RecordBatch::try_from_iter([
            ("first", Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef),
            ("second", Arc::new(Int64Array::from(vec![3, 4])) as ArrayRef),
            ("third", Arc::new(Int64Array::from(vec![5, 6])) as ArrayRef),
        ])
        .unwrap();
        let schema = batch.schema();
        let (request, receiver, encoder) = start_retained_encoder(schema, vec![batch]);

        for (index, expected) in [(2, [5i64, 6i64]), (0, [1i64, 2i64]), (1, [3i64, 4i64])] {
            request.send(index).unwrap();
            let mut sink = CollectingUploadSink { bytes: Vec::new() };
            let mut largest_chunk = 0;
            let encoded =
                upload_retained_column(&receiver, index, &mut sink, &mut largest_chunk).unwrap();
            let expected = expected
                .into_iter()
                .flat_map(i64::to_le_bytes)
                .collect::<Vec<_>>();
            assert_eq!(encoded, expected.len());
            assert_eq!(sink.bytes, expected);
        }

        drop(request);
        encoder.join().unwrap();
    }

    proptest! {
        #[test]
        fn shuffle_round_trips_arbitrary_fixed_width_values(
            width in prop::sample::select(vec![1usize, 2, 4, 8, 12, 16]),
            format in prop::sample::select(vec![CompressionFormat::Block, CompressionFormat::Frame]),
            input in prop::collection::vec(any::<u8>(), 0..4096),
        ) {
            let mut workspace = CompressionWorkspace::default();
            let compressed_len =
                compress_bytes(&input, CompressionTransform::Shuffle(width), format, &mut workspace)
                    .unwrap();
            let mut decoded = vec![0; input.len()];
            match format {
                CompressionFormat::Block => {
                    let written = lz4_flex::block::decompress_into(
                        &workspace.compressed[..compressed_len],
                        &mut decoded,
                    )
                    .unwrap();
                    prop_assert_eq!(written, input.len());
                }
                CompressionFormat::Frame => {
                    decompress_frame_into(
                        &workspace.compressed[..compressed_len],
                        &mut decoded,
                    )
                    .unwrap();
                }
            }
            let mut restored = vec![0; input.len()];
            unshuffle_bytes(&decoded, width, &mut restored);
            prop_assert_eq!(restored, input);
        }
    }

    #[test]
    fn automatic_windows_obey_physical_and_incompressible_bounds() {
        let mut window = EncodedIngestWindow::new(0);
        window.incompressible = true;
        window.estimated_bytes = INCOMPRESSIBLE_WRITE_WINDOW_BYTES - 1;
        assert!(!should_finish_automatic_window(
            None,
            LOCAL_PHYSICAL_WINDOW_BYTES,
            INCOMPRESSIBLE_WRITE_WINDOW_BYTES,
            &window
        ));

        window.estimated_bytes = INCOMPRESSIBLE_WRITE_WINDOW_BYTES;
        assert!(should_finish_automatic_window(
            None,
            LOCAL_PHYSICAL_WINDOW_BYTES,
            INCOMPRESSIBLE_WRITE_WINDOW_BYTES,
            &window
        ));
        assert!(!should_finish_automatic_window(
            Some(100_000),
            LOCAL_PHYSICAL_WINDOW_BYTES,
            INCOMPRESSIBLE_WRITE_WINDOW_BYTES,
            &window
        ));
    }

    #[test]
    fn incompressible_staging_honors_the_remaining_physical_window() {
        let mut window = EncodedIngestWindow::new(0);
        window.incompressible = true;
        window.estimated_bytes = 16 * 1024 * 1024;

        assert_eq!(
            remaining_staging_bytes(
                None,
                DEFAULT_WRITE_WINDOW_BYTES - window.estimated_bytes,
                INCOMPRESSIBLE_WRITE_WINDOW_BYTES,
                &window,
            ),
            INCOMPRESSIBLE_WRITE_WINDOW_BYTES - window.estimated_bytes
        );
        assert_eq!(
            remaining_staging_bytes(
                Some(1_000_000),
                DEFAULT_WRITE_WINDOW_BYTES - window.estimated_bytes,
                INCOMPRESSIBLE_WRITE_WINDOW_BYTES,
                &window,
            ),
            DEFAULT_WRITE_WINDOW_BYTES - window.estimated_bytes
        );
    }

    #[test]
    fn ingest_scheduler_compacts_a_million_one_row_batches() {
        let batch = RecordBatch::try_from_iter([(
            "value",
            Arc::new(Int64Array::from_iter_values(0..1_000_000)) as ArrayRef,
        )])
        .unwrap();
        let schema = batch.schema();
        let mut reader = SlicedBatchReader::new(batch, 1);
        let mut scheduler = IngestWindowScheduler::new(
            &mut reader,
            schema,
            DEFAULT_WRITE_WINDOW_BYTES,
            LOCAL_PHYSICAL_WINDOW_BYTES,
            INCOMPRESSIBLE_WRITE_WINDOW_BYTES,
            Some(1_000_000),
            WireCompression::None,
        );

        let window = scheduler.next_window().unwrap().unwrap();

        assert_eq!(window.rows, 1_000_000);
        assert!(window.columns[0].chunks.len() <= 128);
        assert_eq!(scheduler.input_batches, 1_000_000);
        assert!(scheduler.next_window().unwrap().is_none());
    }

    #[test]
    fn ingest_window_prefetch_builds_the_next_window_during_upload() {
        let batch = RecordBatch::try_from_iter([(
            "value",
            Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef,
        )])
        .unwrap();
        let schema = batch.schema();
        let (signal, signals) = std::sync::mpsc::channel();
        let reader = SignallingReader {
            batch,
            schema: Arc::clone(&schema),
            offset: 0,
            signal,
        };
        let mut windows = start_ingest_window_prefetch(
            Box::new(reader),
            schema,
            8,
            8,
            8,
            None,
            WireCompression::None,
        )
        .unwrap();

        assert_eq!(windows.next_window(None).unwrap().unwrap().rows, 1);
        signals.recv_timeout(Duration::from_secs(1)).unwrap();
        signals.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(windows.finish().unwrap().unwrap().input_batches, 2);
    }

    #[test]
    fn ingest_window_prefetch_reports_reader_panics_without_deadlocking() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let mut windows = start_ingest_window_prefetch(
            Box::new(PanickingReader {
                schema: Arc::clone(&schema),
            }),
            schema,
            8,
            8,
            8,
            None,
            WireCompression::None,
        )
        .unwrap();

        assert_eq!(
            windows
                .next_window(None)
                .err()
                .expect("prefetch must fail")
                .status,
            Status::Internal
        );
        let panic = windows.finish().err().expect("prefetch join must fail");
        assert_eq!(panic.status, Status::Internal);
        assert!(panic.message.contains("panicked"));
    }

    #[test]
    fn ingest_window_prefetch_applies_the_operation_timeout_without_joining_a_hung_reader() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let (release, wait) = std::sync::mpsc::channel();
        let mut windows = start_ingest_window_prefetch(
            Box::new(BlockingReader {
                schema: Arc::clone(&schema),
                release: wait,
            }),
            schema,
            8,
            8,
            8,
            None,
            WireCompression::None,
        )
        .unwrap();

        let timeout = windows
            .next_window(Some(Duration::from_millis(10)))
            .err()
            .expect("blocking the source must time out");
        assert_eq!(timeout.status, Status::Timeout);
        assert!(windows.finish().unwrap().is_none());
        release.send(()).unwrap();
    }

    #[test]
    fn ingest_scheduler_does_not_copy_large_upstream_batches() {
        let rows = 4_194_304usize;
        let batch = RecordBatch::try_from_iter([(
            "value",
            Arc::new(Int64Array::from_iter_values(
                0..i64::try_from(rows).unwrap(),
            )) as ArrayRef,
        )])
        .unwrap();
        let schema = batch.schema();
        let mut reader = SlicedBatchReader::new(batch, rows / 32);
        let mut scheduler = IngestWindowScheduler::new(
            &mut reader,
            schema,
            DEFAULT_WRITE_WINDOW_BYTES,
            LOCAL_PHYSICAL_WINDOW_BYTES,
            INCOMPRESSIBLE_WRITE_WINDOW_BYTES,
            Some(rows),
            WireCompression::None,
        );

        let window = scheduler.next_window().unwrap().unwrap();

        assert_eq!(window.rows, rows);
        assert_eq!(window.columns[0].chunks.len(), 32);
    }

    #[test]
    fn ingest_scheduler_honors_the_byte_budget_after_row_width_skew() {
        let narrow = RecordBatch::try_from_iter([(
            "value",
            Arc::new(StringArray::from_iter_values(std::iter::repeat_n(
                "", 4_096,
            ))) as ArrayRef,
        )])
        .unwrap();
        let wide_values = (0..100).map(|_| "x".repeat(1_024)).collect::<Vec<_>>();
        let wide = RecordBatch::try_from_iter([(
            "value",
            Arc::new(StringArray::from_iter_values(
                wide_values.iter().map(String::as_str),
            )) as ArrayRef,
        )])
        .unwrap();
        let schema = narrow.schema();
        let mut reader = arrow_array::RecordBatchIterator::new(
            vec![Ok(narrow), Ok(wide)].into_iter(),
            Arc::clone(&schema),
        );
        let budget = 16 * 1_024;
        let mut scheduler = IngestWindowScheduler::new(
            &mut reader,
            Arc::clone(&schema),
            budget,
            budget,
            budget,
            None,
            WireCompression::None,
        );
        let mut windows = Vec::new();
        while let Some(window) = scheduler.next_window().unwrap() {
            windows.push((window.rows, window.estimated_bytes));
        }

        assert!(windows.len() > 1);
        assert!(windows.iter().all(|(_, bytes)| *bytes <= budget));
        assert_eq!(windows.iter().map(|(rows, _)| rows).sum::<usize>(), 4_196);
    }

    #[test]
    fn ingest_scheduler_only_exceeds_the_budget_for_one_oversized_row() {
        let batch = RecordBatch::try_from_iter([(
            "value",
            Arc::new(StringArray::from(vec![
                Some("x".repeat(1024 * 1024)),
                Some("small".to_owned()),
            ])) as ArrayRef,
        )])
        .unwrap();
        let schema = batch.schema();
        let mut reader = SlicedBatchReader::new(batch, 2);
        let budget = 16 * 1024;
        let mut scheduler = IngestWindowScheduler::new(
            &mut reader,
            Arc::clone(&schema),
            budget,
            budget,
            budget,
            None,
            WireCompression::None,
        );

        let oversized = scheduler.next_window().unwrap().unwrap();
        let following = scheduler.next_window().unwrap().unwrap();

        assert_eq!(oversized.rows, 1);
        assert!(oversized.estimated_bytes > budget);
        assert_eq!(following.rows, 1);
        assert!(following.estimated_bytes <= budget);
        assert!(scheduler.next_window().unwrap().is_none());
    }

    #[test]
    fn append_column_lookup_falls_back_to_unquoted_identifier_case_folding() {
        let columns = [AppendColumn {
            name: "mixedcase".to_owned(),
            data_type: MonetType::Int,
            nullable: true,
        }];
        assert_eq!(append_column_index("MixedCase", &columns), Some(0));
        assert_eq!(append_column_index("first", &columns), None);
    }
}
