use std::{
    collections::{HashMap, HashSet},
    fmt,
    num::NonZeroUsize,
    ops::Range,
    os::raw::c_char,
    sync::{
        Arc, Mutex, MutexGuard, Weak,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
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
use monetdb::{
    CancelHandle, CursorError, Endian, MonetType, Parameters, ResultColumn, Timeouts, parms::Parm,
};
use percent_encoding::percent_decode_str;
use rayon::prelude::*;

mod metadata;
mod parameters;
use metadata::{like_pattern_matches, load_objects, objects_batch, table_schema};
use parameters::{
    ParameterLayout, QueryTemplate, parameter_layout, render_arguments, render_null_parameters,
    unbound_statements,
};

const DEFAULT_READ_BATCH_ROWS: usize = 131_072;
const MAX_ENCODE_BATCH_ROWS: usize = 131_072;
const PREPARED_CACHE_CAPACITY: usize = 128;
const PREFETCH_DROP_GRACE: Duration = Duration::from_millis(250);
const METADATA_REPLY_ROWS: usize = 1024;
const INLINE_REPLY_ROWS: i64 = 100;
const READ_BATCH_ROWS_OPTION: &str = "adbc.monetdb.read_batch_rows";
const READ_PREFETCH_OPTION: &str = "adbc.monetdb.read_prefetch";
const WRITE_BATCH_ROWS_OPTION: &str = "adbc.monetdb.write_batch_rows";
const BIND_BY_NAME_OPTION: &str = "adbc.statement.bind_by_name";
const CONNECT_TIMEOUT_OPTION: &str = "adbc.monetdb.connect_timeout_seconds";
const READ_TIMEOUT_OPTION: &str = "adbc.monetdb.read_timeout_seconds";
const WRITE_TIMEOUT_OPTION: &str = "adbc.monetdb.write_timeout_seconds";
const OPERATION_TIMEOUT_OPTION: &str = "adbc.monetdb.operation_timeout_seconds";
const CLIENT_APPLICATION_OPTION: &str = "adbc.monetdb.client_application";
const CLIENT_REMARK_OPTION: &str = "adbc.monetdb.client_remark";
const CLIENT_INFO_OPTION: &str = "adbc.monetdb.client_info";
const MINIMUM_VERSION: (u16, u16, u16) = (11, 55, 0);
static SAVEPOINT_ID: AtomicU64 = AtomicU64::new(0);

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
];

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
    usize::try_from(rows)
        .ok()
        .filter(|rows| *rows > 0)
        .ok_or_else(|| {
            error(
                format!("option '{READ_BATCH_ROWS_OPTION}' must be positive"),
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
}

impl DriverConnection {
    fn new(connection: monetdb::Connection) -> Self {
        Self {
            inner: Mutex::new(connection),
            pending_deallocations: Mutex::new(Vec::new()),
        }
    }
}

type SharedConnection = Arc<DriverConnection>;

fn lock_connection(connection: &SharedConnection) -> Result<MutexGuard<'_, monetdb::Connection>> {
    let connection_guard = connection
        .inner
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let pending = connection
        .pending_deallocations
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .drain(..)
        .collect::<Vec<_>>();
    for id in pending {
        connection_guard.try_deallocate(id);
    }
    Ok(connection_guard)
}

fn map_cursor_error(value: CursorError) -> Error {
    let status = match value {
        CursorError::Closed | CursorError::NoResultSet => Status::InvalidState,
        CursorError::Cancelled => Status::Cancelled,
        CursorError::Timeout => Status::Timeout,
        CursorError::IO(_) => Status::IO,
        CursorError::Framing(_) | CursorError::BadReply(_) => Status::InvalidData,
        CursorError::Conversion { .. }
        | CursorError::InvalidRange { .. }
        | CursorError::ResultNotResident { .. } => Status::InvalidData,
        CursorError::FileTransfer(_) | CursorError::UploadRefused { .. } => Status::InvalidData,
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
            if let Some(uri_key) = timeout
                .map(TimeoutOption::uri_key)
                .or_else(|| client_info.map(ClientInfoOption::uri_key))
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
        let version = connection
            .metadata_with_timeouts(metadata_timeouts)
            .map_err(map_cursor_error)?
            .version();
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
        let current_schema = scalar_string(
            &inner,
            "SELECT current_schema AS \"__adbc_current_schema\"",
            initialization_timeouts(timeouts, initialization_deadline)?,
        )?;
        let mut options = Options::default();
        options.set(OptionConnection::CurrentCatalog, catalog.clone().into());
        options.set(OptionConnection::CurrentSchema, current_schema.into());
        options.set(OptionConnection::AutoCommit, "true".into());
        options.set(OptionConnection::ReadOnly, "false".into());
        store_runtime_timeouts(&mut options, timeouts);
        let mut result = MonetdbConnection {
            inner,
            prepared_cache: Arc::new(Mutex::new(PreparedCache::new(PREPARED_CACHE_CAPACITY))),
            cancel,
            timeouts,
            read_batch_rows: DEFAULT_READ_BATCH_ROWS,
            read_prefetch: true,
            write_batch_rows: None,
            options,
            version,
            catalog,
        };
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
    read_prefetch: bool,
    write_batch_rows: Option<usize>,
    options: Options,
    version: (u16, u16, u16),
    catalog: String,
}

type SharedPreparedCache = Arc<Mutex<PreparedCache>>;
type PreparedSlot = Arc<Mutex<Arc<PreparedEntry>>>;

struct PreparedEntry {
    id: u64,
    parameters: Schema,
    result: Schema,
    connection: Weak<DriverConnection>,
}

impl PreparedEntry {
    fn new(metadata: PreparedMetadata, connection: &SharedConnection) -> Self {
        Self {
            id: metadata.id,
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
        match &key {
            OptionConnection::AutoCommit => {
                let enabled = option_bool(&value)?;
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
                self.prepared_cache
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .clear();
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
        if key.as_ref() == WRITE_BATCH_ROWS_OPTION {
            return Ok(self.write_batch_rows.unwrap_or(0).to_string());
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
        if key.as_ref() == WRITE_BATCH_ROWS_OPTION {
            return self
                .write_batch_rows
                .map(i64::try_from)
                .transpose()
                .map_err(|_| error("write_batch_rows exceeds i64", Status::Internal))
                .map(|rows| rows.unwrap_or(0));
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
            read_prefetch: self.read_prefetch,
            write_batch_rows: self.write_batch_rows,
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
    read_prefetch: bool,
    write_batch_rows: Option<usize>,
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
        if key.as_ref() == BIND_BY_NAME_OPTION {
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
        if key.as_ref() == BIND_BY_NAME_OPTION {
            return Ok(self.bind_by_name.to_string());
        }
        if key.as_ref() == READ_PREFETCH_OPTION {
            return Ok(self.read_prefetch.to_string());
        }
        if key.as_ref() == READ_BATCH_ROWS_OPTION {
            return Ok(self.read_batch_rows.to_string());
        }
        if key.as_ref() == WRITE_BATCH_ROWS_OPTION {
            return Ok(self.write_batch_rows.unwrap_or(0).to_string());
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
        if key.as_ref() == WRITE_BATCH_ROWS_OPTION {
            return self
                .write_batch_rows
                .map(i64::try_from)
                .transpose()
                .map_err(|_| error("write_batch_rows exceeds i64", Status::Internal))
                .map(|rows| rows.unwrap_or(0));
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
            return Ok(StatementResult {
                reader: parameter_query_reader(
                    &self.connection,
                    queries,
                    self.read_batch_rows,
                    self.read_prefetch,
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
            self.prepared_cache
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clear();
        }
        let result = query_result_with_timeouts(
            &self.connection,
            query,
            self.read_batch_rows,
            self.read_prefetch,
            self.timeouts,
        );
        if invalidates_cache && result.is_ok() {
            self.prepared_cache
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clear();
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
            self.prepared_cache
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clear();
        }
        let result = execute_update_script(&self.connection, query, self.timeouts);
        if invalidates_cache && result.is_ok() {
            self.prepared_cache
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clear();
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
                Err(value) if could_not_determine_parameter_type(&value) => {
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
            Err(value) if could_not_determine_parameter_type(&value) => {
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

fn could_not_determine_parameter_type(value: &Error) -> bool {
    let indeterminate_sqlstate = value.sqlstate.map(|value| value as u8) == *b"42000";
    (indeterminate_sqlstate
        && value
            .message
            .contains("Could not determine type for argument number"))
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
                    "named parameters require adbc.statement.bind_by_name"
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
        let mut reader = self
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
            self.prepared_cache
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clear();
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
                    "COPY BINARY ingestion for legacy INET columns is not supported by MonetDB",
                    Status::NotImplemented,
                ));
            }
        }

        let columns = schema
            .fields()
            .iter()
            .map(|field| {
                let sql_type = monetdb_arrow::sql_type_for_field(field)
                    .map_err(|value| map_display(value, Status::NotImplemented))?;
                Ok(format!(
                    "{} {}{}",
                    quote_identifier(field.name())?,
                    sql_type,
                    if field.is_nullable() { "" } else { " NOT NULL" }
                ))
            })
            .collect::<Result<Vec<_>>>()?;
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

        let connection = lock_connection(&self.connection)?;
        if temporary
            && matches!(
                mode,
                "adbc.ingest.mode.append" | "adbc.ingest.mode.create_append"
            )
            && delete_on_commit_temporary_table_exists(&connection, Some(table), self.timeouts)?
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
        let (mut cursor, atomic_scope) = begin_atomic(&connection, "ingest", self.timeouts)?;
        let result = (|| {
            match mode {
                "adbc.ingest.mode.create" => cursor.execute(&create).map_err(map_cursor_error)?,
                "adbc.ingest.mode.append" => {}
                "adbc.ingest.mode.replace" => {
                    cursor
                        .execute(&format!("DROP TABLE IF EXISTS {operation_target}"))
                        .map_err(map_cursor_error)?;
                    cursor.execute(&create).map_err(map_cursor_error)?;
                }
                "adbc.ingest.mode.create_append" => cursor
                    .execute(&create.replacen("TABLE ", "TABLE IF NOT EXISTS ", 1))
                    .map_err(map_cursor_error)?,
                value => {
                    return Err(error(
                        format!("unknown ingest mode '{value}'"),
                        Status::InvalidArguments,
                    ));
                }
            }

            if matches!(
                mode,
                "adbc.ingest.mode.append" | "adbc.ingest.mode.create_append"
            ) {
                cursor
                    .execute(&format!("SELECT * FROM {operation_target} WHERE FALSE"))
                    .map_err(map_cursor_error)?;
                let mismatch_status = if mode == "adbc.ingest.mode.create_append" {
                    Status::AlreadyExists
                } else {
                    Status::InvalidArguments
                };
                validate_append_schema(&schema, cursor.column_metadata(), mismatch_status)?;
            }

            let files = (0..schema.fields().len())
                .map(|index| format!("'c{index}'"))
                .collect::<Vec<_>>()
                .join(", ");
            let copy =
                format!("COPY LITTLE ENDIAN BINARY INTO {operation_target} FROM {files} ON CLIENT");
            let mut rows = 0i64;
            for batch in &mut reader {
                let batch = batch.map_err(Error::from)?;
                validate_record_batch(&batch)?;
                if batch.schema() != schema {
                    return Err(error(
                        "record batch schema changed within ingest stream",
                        Status::InvalidData,
                    ));
                }
                if batch.num_rows() == 0 {
                    continue;
                }
                let write_batch_rows = encode_batch_rows(batch.num_rows(), self.write_batch_rows);
                for range in batch_ranges(batch.num_rows(), write_batch_rows) {
                    let batch = batch.slice(range.start, range.len());
                    let encoded = schema
                        .fields()
                        .par_iter()
                        .enumerate()
                        .map(|(index, field)| {
                            monetdb_arrow::encode_column(field, batch.column(index).as_ref())
                                .map(|bytes| (format!("c{index}"), bytes))
                                .map_err(|value| {
                                    map_cursor_error(CursorError::FileTransfer(value.to_string()))
                                })
                        })
                        .collect::<Vec<_>>();
                    let uploads = encoded.into_iter().collect::<Result<HashMap<_, _>>>()?;
                    cursor
                        .execute_with_binary_uploads(&copy, &uploads)
                        .map_err(map_cursor_error)?;
                    let server_rows = cursor.affected_rows().ok_or_else(|| {
                        error(
                            "MonetDB did not report the COPY row count",
                            Status::InvalidData,
                        )
                    })?;
                    let expected_rows = i64::try_from(batch.num_rows())
                        .map_err(|_| error("batch row count exceeds i64", Status::Internal))?;
                    if server_rows != expected_rows {
                        return Err(error(
                            format!(
                                "MonetDB copied {server_rows} rows from a batch containing {expected_rows} rows"
                            ),
                            Status::InvalidData,
                        ));
                    }
                    rows = rows.checked_add(server_rows).ok_or_else(|| {
                        error("ingested row count overflows i64", Status::Internal)
                    })?;
                }
            }
            Ok(rows)
        })();
        let result = finish_atomic(
            &connection,
            &mut cursor,
            atomic_scope,
            result,
            self.timeouts,
        )
        .map(Some);
        drop(cursor);
        drop(connection);
        if mode != "adbc.ingest.mode.append" && result.is_ok() {
            self.prepared_cache
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clear();
        }
        result
    }
}

fn batch_ranges(rows: usize, batch_rows: usize) -> impl Iterator<Item = Range<usize>> {
    debug_assert!(batch_rows > 0);
    (0..rows).step_by(batch_rows).map(move |start| {
        let len = (rows - start).min(batch_rows);
        start..start + len
    })
}

fn encode_batch_rows(batch_rows: usize, configured_rows: Option<usize>) -> usize {
    configured_rows
        .unwrap_or(batch_rows)
        .min(MAX_ENCODE_BATCH_ROWS)
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

fn validate_append_schema(
    schema: &SchemaRef,
    columns: &[ResultColumn],
    mismatch_status: Status,
) -> Result<()> {
    if schema.fields().len() != columns.len() {
        return Err(error(
            format!(
                "append stream has {} columns but destination table has {}",
                schema.fields().len(),
                columns.len()
            ),
            mismatch_status,
        ));
    }
    for (index, (field, column)) in schema.fields().iter().zip(columns).enumerate() {
        if field.name() != column.name() {
            return Err(error(
                format!(
                    "append column {index} is named {:?}, but destination column is named {:?}; append schemas are positional and column order must match",
                    field.name(),
                    column.name()
                ),
                mismatch_status,
            ));
        }
        let source = monetdb_arrow::monet_type_for_field(field)
            .map_err(|value| map_display(value, Status::NotImplemented))?;
        let destination = column.sql_type();
        let string_wire = |data_type: &MonetType| {
            matches!(
                data_type,
                MonetType::Varchar(_) | MonetType::Url | MonetType::Json
            )
        };
        if source != *destination && !(string_wire(&source) && string_wire(destination)) {
            return Err(error(
                format!(
                    "append column {:?} has Arrow/MonetDB type {source}, but destination type is {destination}",
                    field.name()
                ),
                mismatch_status,
            ));
        }
    }
    Ok(())
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
    let query = normalize_prepared_query(query);
    if let Some(entry) = cache
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .get(query)
    {
        return Ok(entry);
    }
    let metadata = prepare_query(connection, query, parameter_count, timeouts)?;
    let candidate = Arc::new(PreparedEntry::new(metadata, connection));
    Ok(cache
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .insert(query.to_owned(), candidate))
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
            .map(|(index, field)| prepared_arrow_field(index.to_string(), &field.data_type))
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

fn query_reader_with_timeouts(
    connection: &SharedConnection,
    query: &str,
    batch_rows: usize,
    read_prefetch: bool,
    timeouts: Timeouts,
) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
    query_result_with_timeouts(connection, query, batch_rows, read_prefetch, timeouts)
        .map(|result| result.reader)
}

fn query_result_with_timeouts(
    connection: &SharedConnection,
    query: &str,
    batch_rows: usize,
    read_prefetch: bool,
    timeouts: Timeouts,
) -> Result<StatementResult> {
    if is_prepare_statement(query) {
        return Err(error(
            "execute() does not accept a PREPARE statement; call Statement::prepare() instead",
            Status::InvalidArguments,
        ));
    }
    let connection_guard = lock_connection(connection)?;
    let mut cursor = connection_guard.cursor();
    cursor.set_timeouts(timeouts);
    cursor.execute(query).map_err(map_cursor_error)?;
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
        return Ok(StatementResult {
            reader: Box::new(SlicedBatchReader::new(batch, batch_rows)),
            rows_affected,
        });
    }
    let schema = schema_for_columns(&result.columns)?;
    if total_rows == 0 {
        return Ok(StatementResult {
            reader: Box::new(EmptyReader::new(schema)),
            rows_affected,
        });
    }
    let adopt_frame = monetdb_arrow::prefers_owned_frame(&result.columns);
    drop(connection_guard);
    if read_prefetch && total_rows > batch_rows as u64 {
        return Ok(StatementResult {
            reader: Box::new(PrefetchedBinaryReader::new(
                cursor,
                PrefetchPlan {
                    result_id: result.result_id,
                    columns: result.columns,
                    schema,
                    total_rows,
                    batch_rows,
                    adopt_frame,
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
            batch_rows,
            response: Vec::new(),
            adopt_frame,
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
    batch_rows: usize,
    read_prefetch: bool,
    timeouts: Timeouts,
) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
    let first = queries
        .next()
        .transpose()?
        .ok_or_else(|| error("no parameter rows to execute", Status::InvalidArguments))?;
    let current =
        bound_query_reader_with_retry(connection, &first, batch_rows, read_prefetch, timeouts)?;
    let schema = current.schema();
    Ok(Box::new(ParameterQueryReader {
        connection: Arc::clone(connection),
        queries,
        current: Some(current),
        schema,
        batch_rows,
        read_prefetch,
        timeouts,
        finished: false,
    }))
}

fn bound_query_reader_with_retry(
    connection: &SharedConnection,
    query: &BoundQuery,
    batch_rows: usize,
    read_prefetch: bool,
    timeouts: Timeouts,
) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
    let query = current_bound_query(connection, query, timeouts)?;
    match query_reader_with_timeouts(connection, &query.sql, batch_rows, read_prefetch, timeouts) {
        Err(value) if query.prepared.is_some() && prepared_statement_missing(&value) => {
            if !lock_connection(connection)?
                .server_info()
                .map_err(map_cursor_error)?
                .autocommit
            {
                return Err(value);
            }
            let retry = retry_bound_query(connection, &query, timeouts)?;
            query_reader_with_timeouts(connection, &retry.sql, batch_rows, read_prefetch, timeouts)
        }
        result => result,
    }
}

fn scalar_string(connection: &SharedConnection, query: &str, timeouts: Timeouts) -> Result<String> {
    let mut reader = query_reader_with_timeouts(connection, query, 1, false, timeouts)?;
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
    let connection = lock_connection(connection)?;
    let mut cursor = connection.cursor();
    cursor.set_timeouts(timeouts);
    cursor.execute(query).map_err(map_cursor_error)?;
    let affected_rows = (!cursor.has_result_set())
        .then(|| cursor.affected_rows())
        .flatten();
    drop(connection);
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
    let connection = lock_connection(connection)?;
    let mut cursor = connection.cursor();
    cursor.set_timeouts(timeouts);
    let mut affected_rows = None;
    for statement in statements {
        cursor.execute(statement).map_err(map_cursor_error)?;
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
    drop(connection);
    Ok(affected_rows)
}

enum AtomicScope {
    Autocommit,
    Savepoint {
        name: String,
        retain_until_transaction_end: bool,
    },
}

fn begin_atomic(
    connection: &monetdb::Connection,
    purpose: &str,
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
    let retain_until_transaction_end =
        delete_on_commit_temporary_table_exists(connection, None, timeouts)?;
    let savepoint = savepoint_name(purpose);
    let mut cursor = connection.cursor();
    cursor.set_timeouts(timeouts);
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

fn delete_on_commit_temporary_table_exists(
    connection: &monetdb::Connection,
    table_name: Option<&str>,
    timeouts: Timeouts,
) -> Result<bool> {
    let table_filter = table_name
        .map(|table| {
            metadata::raw_string_literal(table).map(|table| format!(" AND t.name = {table}"))
        })
        .transpose()?
        .unwrap_or_default();
    let mut cursor = connection.cursor();
    cursor.set_timeouts(timeouts);
    cursor
        .execute(&format!(
            "SELECT CAST(COUNT(*) AS VARCHAR(32)) \
             FROM sys.tables AS t \
             JOIN sys.schemas AS s ON s.id = t.schema_id \
             WHERE s.name = 'tmp' AND t.commit_action = 1{table_filter}"
        ))
        .map_err(map_cursor_error)?;
    if !cursor.next_row().map_err(map_cursor_error)? {
        return Err(error(
            "temporary table metadata query returned no row",
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
                format!("invalid temporary table count: {value}"),
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
    let (mut cursor, atomic_scope) = begin_atomic(&connection_guard, "parameter_batch", timeouts)?;
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
    batch_rows: usize,
    adopt_frame: bool,
}

struct PrefetchedBinaryReader {
    result_id: u64,
    columns: Vec<ResultColumn>,
    schema: SchemaRef,
    total_rows: u64,
    decoded_rows: u64,
    adopt_frame: bool,
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
        let batch_rows = plan.batch_rows;
        let adopt_frame = plan.adopt_frame;
        let worker = std::thread::Builder::new()
            .name("adbc-monetdb-prefetch".into())
            .spawn(move || {
                let panic_sender = sender.clone();
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    fetch_binary_frames(
                        cursor,
                        result_id,
                        &worker_columns,
                        total_rows,
                        batch_rows,
                        adopt_frame,
                        sender,
                    );
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
        let result = if self.adopt_frame {
            monetdb_arrow::decode_frame_owned_with_schema(
                frame.response,
                &self.columns,
                self.result_id,
                frame.start_row,
                frame.requested_rows,
                Arc::clone(&self.schema),
            )
        } else {
            monetdb_arrow::decode_frame_with_schema(
                &frame.response,
                &self.columns,
                self.result_id,
                frame.start_row,
                frame.requested_rows,
                Arc::clone(&self.schema),
            )
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

fn fetch_binary_frames(
    mut cursor: monetdb::Cursor,
    result_id: u64,
    columns: &[ResultColumn],
    total_rows: u64,
    batch_rows: usize,
    adopt_frame: bool,
    sender: std::sync::mpsc::SyncSender<BinaryFrameResult>,
) {
    let mut next_row = 0u64;
    while next_row < total_rows {
        let remaining = total_rows - next_row;
        let requested_rows = batch_rows.min(usize::try_from(remaining).unwrap_or(usize::MAX));
        let capacity = if adopt_frame {
            monetdb_arrow::owned_frame_capacity(columns, requested_rows).unwrap_or(0)
        } else {
            0
        };
        let mut response = Vec::with_capacity(capacity);
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
    batch_rows: usize,
    response: Vec<u8>,
    adopt_frame: bool,
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
        let count = self
            .batch_rows
            .min(usize::try_from(remaining).unwrap_or(usize::MAX));
        let result = if self.adopt_frame {
            let capacity = monetdb_arrow::owned_frame_capacity(&self.columns, count).unwrap_or(0);
            let mut response = Vec::with_capacity(capacity);
            self.cursor
                .fetch_binary_into(self.next_row, count, &mut response)
                .map_err(|value| ArrowError::ExternalError(Box::new(value)))
                .and_then(|()| {
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
            self.cursor
                .fetch_binary_into(self.next_row, count, &mut self.response)
                .map_err(|value| ArrowError::ExternalError(Box::new(value)))
                .and_then(|()| {
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
    batch_rows: usize,
    read_prefetch: bool,
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
                self.batch_rows,
                self.read_prefetch,
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
    fn validates_separate_read_and_write_batch_options() {
        assert_eq!(
            read_batch_rows_option(&OptionValue::String("131072".into())).unwrap(),
            DEFAULT_READ_BATCH_ROWS
        );
        assert_eq!(write_batch_rows_option(&OptionValue::Int(0)).unwrap(), None);
        assert_eq!(
            write_batch_rows_option(&OptionValue::Int(100_000)).unwrap(),
            Some(100_000)
        );
        for value in [OptionValue::Int(0), OptionValue::Int(-1)] {
            assert!(read_batch_rows_option(&value).is_err());
        }
        assert!(write_batch_rows_option(&OptionValue::Int(-1)).is_err());
        assert!(write_batch_rows_option(&OptionValue::Double(1.0)).is_err());
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
    fn maps_legacy_clob_prepare_metadata_to_string() {
        assert_eq!(
            prepared_monet_type("clob", 1024, 0).unwrap(),
            MonetType::Varchar(1024)
        );
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
    fn ingest_ranges_bound_each_copy_batch() {
        assert_eq!(
            batch_ranges(250_001, 100_000).collect::<Vec<_>>(),
            [0..100_000, 100_000..200_000, 200_000..250_001]
        );
        assert!(batch_ranges(0, 100_000).next().is_none());
        assert_eq!(encode_batch_rows(10_000_000, None), MAX_ENCODE_BATCH_ROWS);
        assert_eq!(
            encode_batch_rows(10_000_000, Some(1_000_000)),
            MAX_ENCODE_BATCH_ROWS
        );
        assert_eq!(encode_batch_rows(10_000_000, Some(100_000)), 100_000);
        assert_eq!(encode_batch_rows(100_000, None), 100_000);
    }
}
