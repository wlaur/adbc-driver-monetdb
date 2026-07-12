use std::{
    collections::{HashMap, HashSet, VecDeque},
    fmt,
    os::raw::c_char,
    sync::{
        Arc, Mutex, MutexGuard,
        atomic::{AtomicU64, Ordering},
    },
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
use monetdb::{CursorError, Endian, MonetType, Parameters, ResultColumn, parms::Parm};
use percent_encoding::percent_decode_str;

mod metadata;
mod parameters;
use metadata::{like_pattern_matches, load_objects, objects_batch};
use parameters::{parameter_count, render_arguments, render_row};

const DEFAULT_BATCH_ROWS: usize = 131_072;
const BATCH_ROWS_OPTION: &str = "adbc.monetdb.batch_rows";
const MINIMUM_VERSION: (u16, u16, u16) = (11, 55, 0);
static PREPARE_SAVEPOINT_ID: AtomicU64 = AtomicU64::new(0);

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
    connection
        .lock()
        .map_err(|_| error("connection mutex is poisoned", Status::Internal))
}

fn map_cursor_error(value: CursorError) -> Error {
    let status = match value {
        CursorError::Closed | CursorError::NoResultSet => Status::InvalidState,
        CursorError::IO(_) => Status::IO,
        CursorError::Framing(_) | CursorError::BadReply(_) => Status::InvalidData,
        CursorError::Conversion { .. }
        | CursorError::InvalidRange { .. }
        | CursorError::ResultNotResident { .. } => Status::InvalidData,
        CursorError::FileTransfer(_) => Status::InvalidData,
        CursorError::PreparedResult => Status::InvalidState,
        CursorError::Metadata(_) | CursorError::Poisoned => Status::Internal,
        CursorError::Server(ref message)
            if message.starts_with("42S02!")
                || message.starts_with("42S22!")
                || message.starts_with("3F000!") =>
        {
            Status::NotFound
        }
        CursorError::Server(ref message) if message.starts_with("2DM30!") => Status::InvalidState,
        CursorError::Server(_) => Status::Unknown,
    };
    let mut result = error(value.to_string(), status);
    if let CursorError::Server(message) = value
        && let Some(sqlstate) = parse_sqlstate(&message)
    {
        result.sqlstate = sqlstate;
    }
    result
}

fn parse_sqlstate(message: &str) -> Option<[c_char; 5]> {
    let code: [u8; 5] = message.as_bytes().get(..5)?.try_into().ok()?;
    if message.as_bytes().get(5) != Some(&b'!') || !code.iter().all(u8::is_ascii_alphanumeric) {
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
        self.optional_string(&key)
            .map(str::to_owned)
            .ok_or_else(|| unknown_option(key.as_ref()))
    }

    fn get_bytes(&self, key: impl AsRef<str>) -> Result<Vec<u8>> {
        match self.get(&key) {
            Some(OptionValue::Bytes(value)) => Ok(value.clone()),
            _ => Err(unknown_option(key.as_ref())),
        }
    }

    fn get_int(&self, key: impl AsRef<str>) -> Result<i64> {
        match self.get(&key) {
            Some(OptionValue::Int(value)) => Ok(*value),
            _ => Err(unknown_option(key.as_ref())),
        }
    }

    fn get_double(&self, key: impl AsRef<str>) -> Result<f64> {
        match self.get(&key) {
            Some(OptionValue::Double(value)) => Ok(*value),
            _ => Err(unknown_option(key.as_ref())),
        }
    }
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
        if matches!(key, OptionDatabase::Other(_)) {
            return Err(not_implemented(key.as_ref()));
        }
        self.options.set(key, value);
        Ok(())
    }

    fn get_option_string(&self, key: Self::Option) -> Result<String> {
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
        let uri_username = decode_userinfo(parsed_uri.username())?;
        let uri_password = parsed_uri
            .password()
            .map(decode_userinfo)
            .transpose()?
            .flatten();
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
        let catalog = parameters
            .get_str(Parm::Database)
            .map_err(|value| map_display(value, Status::InvalidArguments))?
            .into_owned();

        let connection_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            monetdb::Connection::new(parameters)
        }))
        .map_err(|_| {
            error(
                "MonetDB connection initialization panicked",
                Status::InvalidArguments,
            )
        })?;
        let mut connection = connection_result.map_err(|value| {
            let status = match value {
                monetdb::ConnectError::Rejected(_) => Status::Unauthenticated,
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
        let version = connection.metadata().map_err(map_cursor_error)?.version();
        if version < MINIMUM_VERSION {
            return Err(error(
                format!(
                    "MonetDB {}.{}.{} is unsupported; Dec2025 (11.55) or newer is required",
                    version.0, version.1, version.2
                ),
                Status::NotImplemented,
            ));
        }

        let inner = Arc::new(Mutex::new(connection));
        let current_schema =
            scalar_string(&inner, "SELECT current_schema AS \"__adbc_current_schema\"")?;
        let mut options = Options::default();
        options.set(OptionConnection::CurrentCatalog, catalog.clone().into());
        options.set(OptionConnection::CurrentSchema, current_schema.into());
        options.set(OptionConnection::AutoCommit, "true".into());
        options.set(OptionConnection::ReadOnly, "false".into());
        let mut result = MonetdbConnection {
            inner,
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

fn decode_userinfo(value: &str) -> Result<Option<String>> {
    if value.is_empty() {
        return Ok(None);
    }
    percent_decode_str(value)
        .decode_utf8()
        .map(|value| Some(value.into_owned()))
        .map_err(|value| map_display(value, Status::InvalidArguments))
}

pub struct MonetdbConnection {
    inner: Arc<Mutex<monetdb::Connection>>,
    options: Options,
    version: (u16, u16, u16),
    catalog: String,
}

impl Optionable for MonetdbConnection {
    type Option = OptionConnection;

    fn set_option(&mut self, key: Self::Option, value: OptionValue) -> Result<()> {
        match &key {
            OptionConnection::AutoCommit => {
                let enabled = option_bool(&value)?;
                lock_connection(&self.inner)?
                    .set_autocommit(enabled)
                    .map_err(map_cursor_error)?;
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
                    &format!("SET SCHEMA {}", quote_identifier(schema)),
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
            options: Options::default(),
            query: None,
            batch_rows: DEFAULT_BATCH_ROWS,
            bound: None,
            prepared: false,
            prepared_id: None,
            prepared_parameter_schema: None,
            prepared_result_schema: None,
        })
    }

    fn cancel(&mut self) -> Result<()> {
        Err(not_implemented("cancel"))
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
            .unwrap_or(true);
        let schemas = if include_catalog && depth != ObjectDepth::Catalogs {
            load_objects(
                &self.inner,
                db_schema,
                table_name,
                table_type.as_deref(),
                column_name,
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
        schema_for_query(
            &self.inner,
            &format!(
                "SELECT * FROM {} WHERE FALSE",
                qualified_name(db_schema, table_name)
            ),
        )
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
        execute_update(&self.inner, "COMMIT").map(|_| ())
    }

    fn rollback(&mut self) -> Result<()> {
        execute_update(&self.inner, "ROLLBACK").map(|_| ())
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
    options: Options,
    query: Option<String>,
    batch_rows: usize,
    bound: Option<Box<dyn RecordBatchReader + Send>>,
    prepared: bool,
    prepared_id: Option<u64>,
    prepared_parameter_schema: Option<Schema>,
    prepared_result_schema: Option<Schema>,
}

impl Optionable for MonetdbStatement {
    type Option = OptionStatement;

    fn set_option(&mut self, key: Self::Option, value: OptionValue) -> Result<()> {
        if key.as_ref() == BATCH_ROWS_OPTION {
            let OptionValue::Int(rows) = value else {
                return Err(error(
                    "batch_rows must be an integer",
                    Status::InvalidArguments,
                ));
            };
            self.batch_rows = usize::try_from(rows)
                .ok()
                .filter(|rows| *rows > 0)
                .ok_or_else(|| error("batch_rows must be positive", Status::InvalidArguments))?;
            self.options.set(key, OptionValue::Int(rows));
            return Ok(());
        }
        match &key {
            OptionStatement::IngestMode
            | OptionStatement::TargetTable
            | OptionStatement::TargetCatalog
            | OptionStatement::TargetDbSchema
            | OptionStatement::Temporary => {}
            _ => return Err(not_implemented(key.as_ref())),
        }
        self.options.set(key, value);
        Ok(())
    }

    fn get_option_string(&self, key: Self::Option) -> Result<String> {
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
            let queries = self.take_bound_queries()?;
            if queries.is_empty() {
                return Err(error(
                    "query execution requires at least one parameter row",
                    Status::InvalidArguments,
                ));
            }
            if self
                .prepared_result_schema
                .as_ref()
                .is_some_and(|schema| schema.fields().is_empty())
            {
                execute_updates_atomic(&self.connection, &queries)?;
                return Ok(Box::new(EmptyReader::default()));
            }
            return parameter_query_reader(&self.connection, queries, self.batch_rows);
        }
        let query = self
            .query
            .as_deref()
            .ok_or_else(|| error("SQL query is not set", Status::InvalidState))?;
        if let Some(id) = self.prepared_id {
            return query_reader(
                &self.connection,
                &format!("EXECUTE {id}()"),
                self.batch_rows,
            );
        }
        query_reader(&self.connection, query, self.batch_rows)
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
            let queries = self.take_bound_queries()?;
            return execute_updates_atomic(&self.connection, &queries);
        }
        let query = self
            .query
            .as_deref()
            .ok_or_else(|| error("SQL query is not set", Status::InvalidState))?;
        if let Some(id) = self.prepared_id {
            return execute_update(&self.connection, &format!("EXECUTE {id}()"));
        }
        execute_update(&self.connection, query)
    }

    fn execute_schema(&mut self) -> Result<Schema> {
        if let Some(schema) = &self.prepared_result_schema {
            return Ok(schema.clone());
        }
        let query = self
            .query
            .as_deref()
            .ok_or_else(|| error("SQL query is not set", Status::InvalidState))?;
        let metadata = prepare_query(&self.connection, query, parameter_count(query)?)?;
        if let Ok(connection) = lock_connection(&self.connection) {
            connection.try_deallocate(metadata.id);
        }
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
        let fields = (0..parameter_count(query)?)
            .map(|index| Field::new(index.to_string(), DataType::Null, true))
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
        let parameter_count = parameter_count(query)?;
        if parameter_count == 0 {
            return Err(not_implemented("preparing parameterless statements"));
        }
        match prepare_query(&self.connection, query, parameter_count) {
            Ok(metadata) => {
                self.prepared_id = Some(metadata.id);
                self.prepared_parameter_schema = Some(metadata.parameters);
                self.prepared_result_schema = Some(metadata.result);
            }
            Err(value)
                if value
                    .message
                    .contains("Could not determine type for argument number") =>
            {
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
        Err(not_implemented("cancel"))
    }
}

impl MonetdbStatement {
    fn clear_prepared(&mut self) {
        if let Some(id) = self.prepared_id.take() {
            let _ = execute_update(&self.connection, &format!("DEALLOCATE {id}"));
        }
        self.prepared_parameter_schema = None;
        self.prepared_result_schema = None;
        self.prepared = false;
    }

    fn take_bound_queries(&mut self) -> Result<Vec<ExecutableQuery>> {
        let query = self
            .query
            .as_deref()
            .ok_or_else(|| error("SQL query is not set", Status::InvalidState))?
            .to_owned();
        let mut reader = self
            .bound
            .take()
            .ok_or_else(|| error("no Arrow parameters are bound", Status::InvalidState))?;
        let schema = reader.schema();
        let expected = parameter_count(&query)?;
        if schema.fields().len() != expected {
            return Err(error(
                format!(
                    "query has {expected} positional parameters but the bound stream has {} columns",
                    schema.fields().len()
                ),
                Status::InvalidArguments,
            ));
        }
        let mut queries = Vec::new();
        for batch in &mut reader {
            let batch = batch.map_err(Error::from)?;
            if batch.schema() != schema {
                return Err(error(
                    "parameter schema changed within the bound stream",
                    Status::InvalidData,
                ));
            }
            for row in 0..batch.num_rows() {
                let sql = match self.prepared_id {
                    Some(id) => format!(
                        "EXECUTE {id}({})",
                        render_arguments(&batch, row)?.join(", ")
                    ),
                    None => render_row(&query, &batch, row)?,
                };
                queries.push(ExecutableQuery { sql });
            }
        }
        Ok(queries)
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
        let target = qualified_name(schema_name, table);
        let schema = reader.schema();
        if schema.fields().is_empty() {
            return Err(error(
                "cannot ingest a zero-column stream",
                Status::InvalidArguments,
            ));
        }

        let columns = schema
            .fields()
            .iter()
            .map(|field| {
                let sql_type = monetdb_arrow::sql_type_for_field(field)
                    .map_err(|value| map_display(value, Status::NotImplemented))?;
                Ok(format!(
                    "{} {}{}",
                    quote_identifier(field.name()),
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

        let originally_autocommit = lock_connection(&self.connection)?
            .server_info()
            .map_err(map_cursor_error)?
            .autocommit;
        if originally_autocommit {
            lock_connection(&self.connection)?
                .set_autocommit(false)
                .map_err(map_cursor_error)?;
        }

        let result = (|| {
            let mut cursor = lock_connection(&self.connection)?.cursor();
            match mode {
                "adbc.ingest.mode.create" => cursor.execute(&create).map_err(map_cursor_error)?,
                "adbc.ingest.mode.append" => {}
                "adbc.ingest.mode.replace" => {
                    cursor
                        .execute(&format!("DROP TABLE IF EXISTS {target}"))
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
                    .execute(&format!("SELECT * FROM {target} WHERE FALSE"))
                    .map_err(map_cursor_error)?;
                validate_append_schema(&schema, cursor.column_metadata())?;
            }

            let files = (0..schema.fields().len())
                .map(|index| format!("'c{index}'"))
                .collect::<Vec<_>>()
                .join(", ");
            let copy = format!("COPY LITTLE ENDIAN BINARY INTO {target} FROM {files} ON CLIENT");
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
                rows = rows
                    .checked_add(batch.num_rows() as i64)
                    .ok_or_else(|| error("ingested row count overflows i64", Status::Internal))?;
            }
            if originally_autocommit {
                cursor.execute("COMMIT").map_err(map_cursor_error)?;
            }
            cursor.close().map_err(map_cursor_error)?;
            Ok(rows)
        })();

        if result.is_err()
            && originally_autocommit
            && let Ok(connection) = lock_connection(&self.connection)
        {
            let mut cursor = connection.cursor();
            let _ = cursor.execute("ROLLBACK");
            let _ = cursor.close();
        }
        let restore_result = if originally_autocommit {
            lock_connection(&self.connection)
                .and_then(|connection| connection.set_autocommit(true).map_err(map_cursor_error))
        } else {
            Ok(())
        };
        match result {
            Err(root_cause) => Err(root_cause),
            Ok(rows) => {
                restore_result?;
                Ok(Some(rows))
            }
        }
    }
}

fn validate_append_schema(schema: &SchemaRef, columns: &[ResultColumn]) -> Result<()> {
    if schema.fields().len() != columns.len() {
        return Err(error(
            format!(
                "append stream has {} columns but destination table has {}",
                schema.fields().len(),
                columns.len()
            ),
            Status::InvalidArguments,
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
                Status::InvalidArguments,
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
                Status::InvalidArguments,
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

struct ExecutableQuery {
    sql: String,
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
    name: Option<String>,
}

fn prepare_query(
    connection: &Arc<Mutex<monetdb::Connection>>,
    query: &str,
    parameter_count: usize,
) -> Result<PreparedMetadata> {
    let query = query.trim().trim_end_matches(';');
    let connection = lock_connection(connection)?;
    let use_savepoint = !connection
        .server_info()
        .map_err(map_cursor_error)?
        .autocommit;
    let savepoint = use_savepoint.then(|| {
        format!(
            "adbc_prepare_probe_{}",
            PREPARE_SAVEPOINT_ID.fetch_add(1, Ordering::Relaxed)
        )
    });
    let mut cursor = connection.cursor();
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
            if let Err(recovery) = recovery {
                return Err(error(
                    format!(
                        "PREPARE failed ({root_cause}); restoring the transaction also failed ({recovery})"
                    ),
                    Status::Internal,
                ));
            }
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
            fields.push(PreparedField {
                data_type: prepared_monet_type(&code, digits, scale)?,
                name,
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
        let parameter_fields = fields
            .drain(result_count..)
            .enumerate()
            .map(|(index, field)| prepared_arrow_field(index.to_string(), &field.data_type))
            .collect::<Result<Vec<_>>>()?;
        let result_fields = fields
            .into_iter()
            .enumerate()
            .map(|(index, field)| {
                prepared_arrow_field(
                    field
                        .name
                        .filter(|name| !name.is_empty())
                        .unwrap_or_else(|| format!("column_{index}")),
                    &field.data_type,
                )
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
    if let Some(savepoint) = &savepoint {
        cursor
            .execute(&format!("RELEASE SAVEPOINT {savepoint}"))
            .map_err(map_cursor_error)?;
    }
    parsed
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

fn query_reader(
    connection: &Arc<Mutex<monetdb::Connection>>,
    query: &str,
    batch_rows: usize,
) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
    let mut cursor = lock_connection(connection)?.cursor();
    cursor.execute(query).map_err(map_cursor_error)?;
    if !cursor.has_result_set() {
        return Ok(Box::new(EmptyReader::default()));
    }
    let result = cursor.binary_result().map_err(map_cursor_error)?;
    let total_rows = result.total_rows;
    if total_rows == 1 && !result.is_server_resident() {
        let batch = monetdb_arrow::decode_inline_row(&mut cursor, &result.columns)
            .map_err(|value| map_display(value, Status::InvalidData))?;
        return Ok(Box::new(SingleBatchReader::new(batch)));
    }
    let schema = schema_for_columns(&result.columns)?;
    Ok(Box::new(BinaryReader {
        cursor,
        columns: result.columns,
        schema,
        next_row: 0,
        total_rows,
        batch_rows,
        finished: false,
    }))
}

fn parameter_query_reader(
    connection: &Arc<Mutex<monetdb::Connection>>,
    queries: Vec<ExecutableQuery>,
    batch_rows: usize,
) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
    let mut queries = VecDeque::from(queries);
    let first = queries
        .pop_front()
        .ok_or_else(|| error("no parameter rows to execute", Status::InvalidArguments))?;
    let current = query_reader(connection, &first.sql, batch_rows)?;
    let schema = current.schema();
    Ok(Box::new(ParameterQueryReader {
        connection: Arc::clone(connection),
        queries,
        current: Some(current),
        schema,
        batch_rows,
    }))
}

fn scalar_string(connection: &Arc<Mutex<monetdb::Connection>>, query: &str) -> Result<String> {
    let mut reader = query_reader(connection, query, 1)?;
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

fn schema_for_query(connection: &Arc<Mutex<monetdb::Connection>>, query: &str) -> Result<Schema> {
    let mut cursor = lock_connection(connection)?.cursor();
    cursor.execute(query).map_err(map_cursor_error)?;
    let result = cursor.binary_result().map_err(map_cursor_error)?;
    Ok(schema_for_columns(&result.columns)?.as_ref().clone())
}

fn execute_update(
    connection: &Arc<Mutex<monetdb::Connection>>,
    query: &str,
) -> Result<Option<i64>> {
    let mut cursor = lock_connection(connection)?.cursor();
    cursor.execute(query).map_err(map_cursor_error)?;
    Ok(cursor.affected_rows())
}

fn execute_updates_atomic(
    connection: &Arc<Mutex<monetdb::Connection>>,
    queries: &[ExecutableQuery],
) -> Result<Option<i64>> {
    let connection = lock_connection(connection)?;
    let originally_autocommit = connection
        .server_info()
        .map_err(map_cursor_error)?
        .autocommit;
    if originally_autocommit {
        connection.set_autocommit(false).map_err(map_cursor_error)?;
    }

    let mut cursor = connection.cursor();
    let result = (|| {
        let mut total = 0i64;
        let mut has_count = false;
        for query in queries {
            cursor.execute(&query.sql).map_err(map_cursor_error)?;
            if let Some(rows) = cursor.affected_rows() {
                total = total
                    .checked_add(rows)
                    .ok_or_else(|| error("affected row count overflows i64", Status::Internal))?;
                has_count = true;
            }
        }
        if originally_autocommit {
            cursor.execute("COMMIT").map_err(map_cursor_error)?;
        }
        Ok(has_count.then_some(total))
    })();

    if result.is_err() && originally_autocommit {
        let _ = cursor.execute("ROLLBACK");
    }
    let restore_result = if originally_autocommit {
        connection.set_autocommit(true).map_err(map_cursor_error)
    } else {
        Ok(())
    };
    match result {
        Err(root_cause) => Err(root_cause),
        Ok(rows) => {
            restore_result?;
            Ok(rows)
        }
    }
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
    columns: Vec<ResultColumn>,
    schema: SchemaRef,
    next_row: u64,
    total_rows: u64,
    batch_rows: usize,
    finished: bool,
}

impl Iterator for BinaryReader {
    type Item = std::result::Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished || self.next_row >= self.total_rows {
            self.finished = true;
            return None;
        }
        let remaining = self.total_rows - self.next_row;
        let count = self
            .batch_rows
            .min(usize::try_from(remaining).unwrap_or(usize::MAX));
        let result = self
            .cursor
            .fetch_binary(self.next_row, count)
            .map_err(|value| ArrowError::ExternalError(Box::new(value)))
            .and_then(|frame| {
                monetdb_arrow::decode_frame(&frame, &self.columns)
                    .map_err(|value| ArrowError::ExternalError(Box::new(value)))
            });
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
    queries: VecDeque<ExecutableQuery>,
    current: Option<Box<dyn RecordBatchReader + Send + 'static>>,
    schema: SchemaRef,
    batch_rows: usize,
}

impl Iterator for ParameterQueryReader {
    type Item = std::result::Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(reader) = &mut self.current
                && let Some(batch) = reader.next()
            {
                return Some(batch);
            }
            self.current = None;
            let query = self.queries.pop_front()?;
            match query_reader(&self.connection, &query.sql, self.batch_rows) {
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

fn quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn qualified_name(schema: Option<&str>, table: &str) -> String {
    match schema {
        Some(schema) => format!("{}.{}", quote_identifier(schema), quote_identifier(table)),
        None => quote_identifier(table),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_sqlstate() {
        assert_eq!(
            parse_sqlstate("42000!syntax error"),
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
    fn validates_boolean_options() {
        assert!(option_bool(&OptionValue::String("enabled".into())).unwrap());
        assert!(!option_bool(&OptionValue::String("false".into())).unwrap());
        assert!(option_bool(&OptionValue::String("maybe".into())).is_err());
    }

    #[test]
    fn quotes_identifiers() {
        assert_eq!(quote_identifier("a\"b"), "\"a\"\"b\"");
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
