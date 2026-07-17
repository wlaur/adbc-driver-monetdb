use std::{
    collections::{HashMap, HashSet},
    fmt,
    num::NonZeroUsize,
    os::raw::c_char,
    sync::{
        Arc, Mutex, MutexGuard,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use adbc_core::error::{Error, Result, Status};
use adbc_core::options::{
    InfoCode, ObjectDepth, OptionConnection, OptionDatabase, OptionStatement, OptionValue,
};
use adbc_core::schemas::{GET_INFO_SCHEMA, GET_TABLE_TYPES_SCHEMA};
use adbc_core::{Connection, Database, Driver, Optionable, PartitionedResult, Statement};
use arrow_array::{
    Array, ArrayRef, BooleanArray, Int64Array, RecordBatch, RecordBatchReader, StringArray,
    UInt32Array, UnionArray, new_empty_array,
};
use arrow_schema::{ArrowError, DataType, Field, Schema, SchemaRef};
use monetdb::{
    CancelHandle, CursorError, Endian, MonetType, Parameters, ResultColumn, Timeouts, parms::Parm,
};
use percent_encoding::percent_decode_str;

mod metadata;
mod parameters;
use metadata::{like_pattern_matches, load_objects, objects_batch, table_schema};
use parameters::{
    ParameterLayout, QueryTemplate, parameter_layout, render_arguments, render_null_parameters,
    unbound_statements,
};

const DEFAULT_BATCH_ROWS: usize = 131_072;
const METADATA_REPLY_ROWS: usize = 1024;
const BATCH_ROWS_OPTION: &str = "adbc.monetdb.batch_rows";
const BIND_BY_NAME_OPTION: &str = "adbc.statement.bind_by_name";
const CONNECT_TIMEOUT_OPTION: &str = "adbc.monetdb.connect_timeout_seconds";
const READ_TIMEOUT_OPTION: &str = "adbc.monetdb.read_timeout_seconds";
const WRITE_TIMEOUT_OPTION: &str = "adbc.monetdb.write_timeout_seconds";
const OPERATION_TIMEOUT_OPTION: &str = "adbc.monetdb.operation_timeout_seconds";
const MINIMUM_VERSION: (u16, u16, u16) = (11, 55, 0);
static SAVEPOINT_ID: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TimeoutOption {
    Connect,
    Read,
    Write,
    Operation,
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
}

fn timeout_seconds(key: &str, value: &OptionValue) -> Result<i64> {
    let seconds = integer_option(key, value)?;
    if seconds < 0 {
        return Err(error(
            format!("option '{key}' must be nonnegative"),
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
        format!("adbc-monetdb: {what} is not implemented yet"),
        Status::NotImplemented,
    )
}

fn unknown_option(key: &str) -> Error {
    error(format!("unknown or unset option '{key}'"), Status::NotFound)
}

fn map_display(error: impl fmt::Display, status: Status) -> Error {
    Error::with_message_and_status(error.to_string(), status)
}

fn lock_connection(
    connection: &Arc<Mutex<monetdb::Connection>>,
) -> Result<MutexGuard<'_, monetdb::Connection>> {
    Ok(connection
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner))
}

fn map_cursor_error(value: CursorError) -> Error {
    let status = match value {
        CursorError::Closed | CursorError::NoResultSet | CursorError::NoActiveOperation => {
            Status::InvalidState
        }
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
        if let OptionDatabase::Other(name) = &key {
            if TimeoutOption::from_key(name).is_none() {
                return Err(not_implemented(key.as_ref()));
            }
            timeout_seconds(name, &value)?;
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
        self.options.get_string(key)
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
        let mut parsed_uri =
            url::Url::parse(uri).map_err(|value| map_display(value, Status::InvalidArguments))?;
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
        }
        // One minimizes the inline text prefix. Multi-row results remain open
        // for Xexportbin; a complete scalar row is decoded directly.
        parameters
            .set_replysize(1)
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
        let connection = connection_result.map_err(|value| {
            let status = match value {
                monetdb::ConnectError::Rejected(_) => Status::Unauthenticated,
                monetdb::ConnectError::Timeout => Status::Timeout,
                monetdb::ConnectError::IO(_) => Status::IO,
                _ => Status::InvalidArguments,
            };
            map_display(value, status)
        })?;
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
        let inner = Arc::new(Mutex::new(connection));
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
        let mut result = MonetdbConnection {
            inner,
            cancel,
            timeouts,
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

fn decode_userinfo(value: &str) -> Result<String> {
    percent_decode_str(value)
        .decode_utf8()
        .map(|value| value.into_owned())
        .map_err(|value| map_display(value, Status::InvalidArguments))
}

pub struct MonetdbConnection {
    inner: Arc<Mutex<monetdb::Connection>>,
    cancel: CancelHandle,
    timeouts: Timeouts,
    options: Options,
    version: (u16, u16, u16),
    catalog: String,
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
        if key == OptionConnection::CurrentSchema {
            return scalar_string(
                &self.inner,
                "SELECT current_schema AS \"__adbc_current_schema\"",
                self.timeouts,
            );
        }
        self.options.get_string(key)
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

impl Connection for MonetdbConnection {
    type StatementType = MonetdbStatement;

    fn new_statement(&mut self) -> Result<Self::StatementType> {
        Ok(MonetdbStatement {
            connection: Arc::clone(&self.inner),
            cancel: self.cancel.clone(),
            timeouts: self.timeouts,
            options: Options::default(),
            query: None,
            batch_rows: DEFAULT_BATCH_ROWS,
            bound: None,
            prepared: false,
            prepared_id: None,
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
    connection: Arc<Mutex<monetdb::Connection>>,
    cancel: CancelHandle,
    timeouts: Timeouts,
    options: Options,
    query: Option<String>,
    batch_rows: usize,
    bound: Option<Box<dyn RecordBatchReader + Send>>,
    prepared: bool,
    prepared_id: Option<u64>,
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
        if key.as_ref() == BATCH_ROWS_OPTION {
            let rows = integer_option(BATCH_ROWS_OPTION, &value)?;
            self.batch_rows = usize::try_from(rows)
                .ok()
                .filter(|rows| *rows > 0)
                .ok_or_else(|| error("batch_rows must be positive", Status::InvalidArguments))?;
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
        if key.as_ref() == BATCH_ROWS_OPTION {
            return i64::try_from(self.batch_rows)
                .map_err(|_| error("batch_rows exceeds i64", Status::Internal));
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
        self.bound = Some(Box::new(SingleBatchReader::new(batch)));
        Ok(())
    }

    fn bind_stream(&mut self, reader: Box<dyn RecordBatchReader + Send>) -> Result<()> {
        self.bound = Some(reader);
        Ok(())
    }

    fn execute(&mut self) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        if self.bound.is_some() {
            if !self.prepared {
                self.prepare()?;
            }
            let mut queries = self.take_bound_queries()?;
            if queries.is_empty()? {
                let schema = self
                    .prepared_result_schema
                    .clone()
                    .map(Arc::new)
                    .unwrap_or_else(|| Arc::new(Schema::empty()));
                return Ok(Box::new(EmptyReader::new(schema)));
            }
            if self
                .prepared_result_schema
                .as_ref()
                .is_some_and(|schema| schema.fields().is_empty())
            {
                execute_updates_atomic(&self.connection, &mut queries, self.timeouts)?;
                return Ok(Box::new(EmptyReader::default()));
            }
            if self.prepared_result_schema.is_none() {
                let schema = self.execute_schema()?;
                if schema.fields().is_empty() {
                    execute_updates_atomic(&self.connection, &mut queries, self.timeouts)?;
                    self.prepared_result_schema = Some(schema);
                    return Ok(Box::new(EmptyReader::default()));
                }
                self.prepared_result_schema = Some(schema);
            }
            return parameter_query_reader(
                &self.connection,
                queries,
                self.batch_rows,
                self.timeouts,
            );
        }
        let query = self
            .query
            .as_deref()
            .ok_or_else(|| error("SQL query is not set", Status::InvalidState))?;
        validate_unbound_query(query)?;
        if let Some(id) = self.prepared_id {
            return query_reader_with_timeouts(
                &self.connection,
                &format!("EXECUTE {id}()"),
                self.batch_rows,
                self.timeouts,
            );
        }
        query_reader_with_timeouts(&self.connection, query, self.batch_rows, self.timeouts)
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
        if let Some(id) = self.prepared_id {
            validate_unbound_query(query)?;
            return execute_update(&self.connection, &format!("EXECUTE {id}()"), self.timeouts);
        }
        execute_update_script(&self.connection, query, self.timeouts)
    }

    fn execute_schema(&mut self) -> Result<Schema> {
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
        match prepare_query(&self.connection, query, parameter_count, self.timeouts) {
            Ok(metadata) => {
                self.prepared_id = Some(metadata.id);
                self.prepared_parameter_schema = Some(metadata.parameters);
                self.prepared_result_schema = Some(metadata.result);
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
        if let Some(id) = self.prepared_id.take() {
            let connection = lock_connection(&self.connection)
                .expect("locking a poison-tolerant connection cannot fail");
            connection.try_deallocate(id);
        }
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
        Ok(BoundQueryStream {
            reader,
            schema,
            template,
            prepared_id: self.prepared_id,
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
                return Err(not_implemented(
                    "COPY BINARY ingestion for OID columns is not supported by MonetDB",
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
                if batch.schema() != schema {
                    return Err(error(
                        "record batch schema changed within ingest stream",
                        Status::InvalidData,
                    ));
                }
                if batch.num_rows() == 0 {
                    continue;
                }
                cursor
                    .execute_with_binary_uploads_lazy(&copy, |filename| {
                        let index = filename
                            .strip_prefix('c')
                            .and_then(|value| value.parse::<usize>().ok())
                            .filter(|index| *index < schema.fields().len())
                            .ok_or_else(|| {
                                CursorError::FileTransfer(format!(
                                    "server requested unknown file {filename:?}"
                                ))
                            })?;
                        monetdb_arrow::encode_column(
                            &schema.fields()[index],
                            batch.column(index).as_ref(),
                        )
                        .map_err(|value| CursorError::FileTransfer(value.to_string()))
                    })
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
                rows = rows
                    .checked_add(server_rows)
                    .ok_or_else(|| error("ingested row count overflows i64", Status::Internal))?;
            }
            Ok(rows)
        })();
        finish_atomic(
            &connection,
            &mut cursor,
            atomic_scope,
            result,
            self.timeouts,
        )
        .map(Some)
    }
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
                    "append column {index} is named {:?}, but destination column is named {:?}",
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

impl Drop for MonetdbStatement {
    fn drop(&mut self) {
        if let Some(id) = self.prepared_id.take() {
            match self.connection.try_lock() {
                Ok(connection) => {
                    connection.try_deallocate(id);
                }
                Err(std::sync::TryLockError::Poisoned(poisoned)) => {
                    poisoned.into_inner().try_deallocate(id);
                }
                Err(std::sync::TryLockError::WouldBlock) => {}
            }
        }
    }
}

struct BoundQueryStream {
    reader: Box<dyn RecordBatchReader + Send>,
    schema: SchemaRef,
    template: QueryTemplate,
    prepared_id: Option<u64>,
    bind_by_name: bool,
    batch: Option<RecordBatch>,
    next_row: usize,
    pending: Option<String>,
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
    type Item = Result<String>;

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
                let sql = match self.prepared_id {
                    Some(id) => render_arguments(batch, row)
                        .map(|values| format!("EXECUTE {id}({})", values.join(", "))),
                    None => self.template.render_row(batch, row, self.bind_by_name),
                };
                return Some(sql);
            }
            match self.reader.next() {
                Some(Ok(batch)) if batch.schema() == self.schema => {
                    self.batch = Some(batch);
                    self.next_row = 0;
                }
                Some(Ok(_)) => {
                    self.finished = true;
                    return Some(Err(error(
                        "parameter schema changed within the bound stream",
                        Status::InvalidData,
                    )));
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

struct PreparedField {
    data_type: MonetType,
    undetermined: bool,
    name: Option<String>,
    origin_schema: Option<String>,
    origin_table: Option<String>,
}

fn prepare_query(
    connection: &Arc<Mutex<monetdb::Connection>>,
    query: &str,
    parameter_count: usize,
    timeouts: Timeouts,
) -> Result<PreparedMetadata> {
    prepare_query_inner(connection, query, parameter_count, false, timeouts)
}

fn prepare_query_allowing_any(
    connection: &Arc<Mutex<monetdb::Connection>>,
    query: &str,
    parameter_count: usize,
    timeouts: Timeouts,
) -> Result<PreparedMetadata> {
    prepare_query_inner(connection, query, parameter_count, true, timeouts)
}

fn prepare_query_inner(
    connection: &Arc<Mutex<monetdb::Connection>>,
    query: &str,
    parameter_count: usize,
    allow_any: bool,
    timeouts: Timeouts,
) -> Result<PreparedMetadata> {
    let query = query.trim().trim_end_matches(';');
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
            fields.push(PreparedField {
                undetermined: allow_any && code == "any",
                data_type: if allow_any && code == "any" {
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
        let (Some(table), Some(column)) = (field.origin_table.as_deref(), field.name.as_deref())
        else {
            continue;
        };
        let mut candidate = None;
        let mut ambiguous = false;
        for ((schema, declared_table, declared_column), data_type) in &declared {
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
                Some(_) => {
                    ambiguous = true;
                    break;
                }
            }
        }
        if !ambiguous && let Some(data_type) = candidate {
            field.data_type = data_type;
        }
    }
    Ok(())
}

fn prepared_monet_type(code: &str, digits: i32, scale: i32) -> Result<MonetType> {
    let mut data_type = MonetType::from_mapi_code(code).ok_or_else(|| {
        error(
            format!("unknown MonetDB prepared type '{code}'"),
            Status::InvalidData,
        )
    })?;
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
    connection: &Arc<Mutex<monetdb::Connection>>,
    query: &str,
    batch_rows: usize,
    timeouts: Timeouts,
) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
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
        return Ok(Box::new(EmptyReader::default()));
    }
    let result = cursor.binary_result().map_err(map_cursor_error)?;
    let total_rows = result.total_rows;
    if result.is_server_resident()
        && result
            .columns
            .iter()
            .any(|column| *column.sql_type() == MonetType::Oid)
    {
        return Err(not_implemented(
            "multi-row OID results are unavailable through Xexportbin; cast OID columns to VARCHAR in SQL",
        ));
    }
    if total_rows >= 16_384 && result.columns.len() > 1 {
        monetdb_arrow::initialize_parallel_decoder();
    }
    if total_rows == 1 && !result.is_server_resident() {
        let batch = monetdb_arrow::decode_inline_row(&mut cursor, &result.columns)
            .map_err(|value| map_display(value, Status::InvalidData))?;
        return Ok(Box::new(SingleBatchReader::new(batch)));
    }
    let schema = schema_for_columns(&result.columns)?;
    let adopt_frame = monetdb_arrow::prefers_owned_frame(&result.columns);
    drop(connection_guard);
    Ok(Box::new(BinaryReader {
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
    }))
}

fn is_prepare_statement(query: &str) -> bool {
    leading_sql_keyword(query).is_some_and(|keyword| keyword.eq_ignore_ascii_case("prepare"))
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
    connection: &Arc<Mutex<monetdb::Connection>>,
    mut queries: BoundQueryStream,
    batch_rows: usize,
    timeouts: Timeouts,
) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
    let first = queries
        .next()
        .transpose()?
        .ok_or_else(|| error("no parameter rows to execute", Status::InvalidArguments))?;
    let current = query_reader_with_timeouts(connection, &first, batch_rows, timeouts)?;
    let schema = current.schema();
    Ok(Box::new(ParameterQueryReader {
        connection: Arc::clone(connection),
        queries,
        current: Some(current),
        schema,
        batch_rows,
        timeouts,
        finished: false,
    }))
}

fn scalar_string(
    connection: &Arc<Mutex<monetdb::Connection>>,
    query: &str,
    timeouts: Timeouts,
) -> Result<String> {
    let mut reader = query_reader_with_timeouts(connection, query, 1, timeouts)?;
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
    connection: &Arc<Mutex<monetdb::Connection>>,
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
    let affected_rows = cursor.affected_rows();
    drop(connection);
    Ok(affected_rows)
}

fn execute_update_script(
    connection: &Arc<Mutex<monetdb::Connection>>,
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
        if let Some(rows) = cursor.affected_rows() {
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
    Savepoint(String),
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
    let savepoint = savepoint_name(purpose);
    let mut cursor = connection.cursor();
    cursor.set_timeouts(timeouts);
    cursor
        .execute(&format!("SAVEPOINT {savepoint}"))
        .map_err(map_cursor_error)?;
    Ok((cursor, AtomicScope::Savepoint(savepoint)))
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
                AtomicScope::Savepoint(savepoint) => {
                    cursor.execute(&format!("RELEASE SAVEPOINT {savepoint}"))
                }
            };
            finalize.map(|()| value).map_err(map_cursor_error)
        }
        Err(root) => Err(root),
    };

    if result.is_err() {
        let recovery = match &scope {
            AtomicScope::Autocommit => cursor.execute("ROLLBACK"),
            AtomicScope::Savepoint(savepoint) => cursor
                .execute(&format!("ROLLBACK TO SAVEPOINT {savepoint}"))
                .and_then(|()| cursor.execute(&format!("RELEASE SAVEPOINT {savepoint}"))),
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
    connection: &Arc<Mutex<monetdb::Connection>>,
    queries: &mut BoundQueryStream,
    timeouts: Timeouts,
) -> Result<Option<i64>> {
    if queries.is_empty()? {
        return Ok(Some(0));
    }
    let connection = lock_connection(connection)?;
    let (mut cursor, atomic_scope) = begin_atomic(&connection, "parameter_batch", timeouts)?;
    let result = (|| {
        let mut total = 0i64;
        let mut has_count = false;
        for query in queries {
            let query = query?;
            cursor.execute(&query).map_err(map_cursor_error)?;
            if let Some(rows) = cursor.affected_rows() {
                total = total
                    .checked_add(rows)
                    .ok_or_else(|| error("affected row count overflows i64", Status::Internal))?;
                has_count = true;
            }
        }
        Ok(has_count.then_some(total))
    })();
    finish_atomic(&connection, &mut cursor, atomic_scope, result, timeouts)
}

fn schema_for_columns(columns: &[ResultColumn]) -> Result<SchemaRef> {
    let fields = columns
        .iter()
        .map(monetdb_arrow::field_for_column)
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|value| map_display(value, Status::InvalidData))?;
    Ok(Arc::new(Schema::new(fields)))
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
    connection: Arc<Mutex<monetdb::Connection>>,
    queries: BoundQueryStream,
    current: Option<Box<dyn RecordBatchReader + Send + 'static>>,
    schema: SchemaRef,
    batch_rows: usize,
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
            match query_reader_with_timeouts(
                &self.connection,
                &query,
                self.batch_rows,
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
}
