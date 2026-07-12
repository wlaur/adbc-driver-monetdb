use std::{
    collections::{HashMap, HashSet},
    fmt,
    os::raw::c_char,
    sync::{Arc, Mutex},
};

use adbc_core::error::{Error, Result, Status};
use adbc_core::options::{
    InfoCode, ObjectDepth, OptionConnection, OptionDatabase, OptionStatement, OptionValue,
};
use adbc_core::{Connection, Database, Driver, Optionable, PartitionedResult, Statement};
use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::{ArrowError, Schema, SchemaRef};
use monetdb::{CursorError, Endian, Parameters, ResultColumn};
use percent_encoding::percent_decode_str;

const DEFAULT_BATCH_ROWS: usize = 131_072;
const BATCH_ROWS_OPTION: &str = "adbc.monetdb.batch_rows";
const MINIMUM_VERSION: (u16, u16, u16) = (11, 55, 0);

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

fn map_cursor_error(value: CursorError) -> Error {
    let status = match value {
        CursorError::Closed | CursorError::NoResultSet => Status::InvalidState,
        CursorError::IO(_) => Status::IO,
        CursorError::Framing(_) | CursorError::BadReply(_) => Status::InvalidData,
        CursorError::Conversion { .. } | CursorError::InvalidRange { .. } => Status::InvalidData,
        CursorError::FileTransfer(_) => Status::InvalidData,
        CursorError::Metadata(_) => Status::Internal,
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
        parsed_uri
            .set_username("")
            .map_err(|()| error("URI cannot contain a username", Status::InvalidArguments))?;
        parsed_uri
            .set_password(None)
            .map_err(|()| error("URI cannot contain a password", Status::InvalidArguments))?;
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
        // Zero means "all rows" in MAPI. One keeps the result set open while
        // the driver re-fetches every row through Xexportbin, including row 0.
        parameters
            .set_replysize(1)
            .map_err(|value| map_display(value, Status::InvalidArguments))?;
        parameters
            .set_binary("on")
            .map_err(|value| map_display(value, Status::InvalidArguments))?;
        parameters
            .set_autocommit(true)
            .map_err(|value| map_display(value, Status::InvalidArguments))?;

        let mut connection = monetdb::Connection::new(parameters).map_err(|value| {
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

        let mut result = MonetdbConnection {
            inner: Arc::new(Mutex::new(connection)),
            options: Options::default(),
            _version: version,
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
    _version: (u16, u16, u16),
}

impl Optionable for MonetdbConnection {
    type Option = OptionConnection;

    fn set_option(&mut self, key: Self::Option, value: OptionValue) -> Result<()> {
        match &key {
            OptionConnection::AutoCommit => {
                let enabled = option_bool(&value)?;
                self.inner
                    .lock()
                    .expect("connection mutex is not poisoned")
                    .set_autocommit(enabled)
                    .map_err(map_cursor_error)?;
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
                if option_bool(&value)? {
                    return Err(not_implemented("read-only connections"));
                }
            }
            OptionConnection::IsolationLevel | OptionConnection::CurrentCatalog => {
                return Err(not_implemented(key.as_ref()));
            }
            OptionConnection::Other(_) => {}
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
        })
    }

    fn cancel(&mut self) -> Result<()> {
        Err(not_implemented("cancel"))
    }

    fn get_info(
        &self,
        _codes: Option<HashSet<InfoCode>>,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        Err(not_implemented("get_info"))
    }

    fn get_objects(
        &self,
        _depth: ObjectDepth,
        _catalog: Option<&str>,
        _db_schema: Option<&str>,
        _table_name: Option<&str>,
        _table_type: Option<Vec<&str>>,
        _column_name: Option<&str>,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        Err(not_implemented("get_objects"))
    }

    fn get_table_schema(
        &self,
        _catalog: Option<&str>,
        _db_schema: Option<&str>,
        _table_name: &str,
    ) -> Result<Schema> {
        Err(not_implemented("get_table_schema"))
    }

    fn get_table_types(&self) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        Err(not_implemented("get_table_types"))
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
            return Err(not_implemented("bulk ingestion"));
        }
        let query = self
            .query
            .as_deref()
            .ok_or_else(|| error("SQL query is not set", Status::InvalidState))?;
        let mut cursor = self
            .connection
            .lock()
            .expect("connection mutex is not poisoned")
            .cursor();
        cursor.execute(query).map_err(map_cursor_error)?;
        if !cursor.has_result_set() {
            return Ok(Box::new(EmptyReader::default()));
        }
        let mut result = cursor.binary_result().map_err(map_cursor_error)?;
        let total_rows = result.total_rows;
        if total_rows == 1 {
            let query = query.trim().trim_end_matches(';');
            cursor
                .execute(&format!(
                    "WITH \"__adbc_source\" AS ({query}) \
                     SELECT \"__adbc_source\".* FROM \"__adbc_source\" \
                     CROSS JOIN (VALUES (1), (2)) AS \"__adbc_duplicate\"(\"n\")"
                ))
                .map_err(map_cursor_error)?;
            result = cursor.binary_result().map_err(map_cursor_error)?;
            if result.total_rows < 2 {
                return Err(error(
                    "one-row query could not be retained for binary export",
                    Status::Internal,
                ));
            }
        }
        let schema = schema_for_columns(&result.columns)?;
        Ok(Box::new(BinaryReader {
            cursor,
            columns: result.columns,
            schema,
            next_row: 0,
            total_rows,
            batch_rows: self.batch_rows,
            finished: false,
        }))
    }

    fn execute_update(&mut self) -> Result<Option<i64>> {
        if self.bound.is_some() {
            return self.ingest();
        }
        let query = self
            .query
            .as_deref()
            .ok_or_else(|| error("SQL query is not set", Status::InvalidState))?;
        execute_update(&self.connection, query)
    }

    fn execute_schema(&mut self) -> Result<Schema> {
        Err(not_implemented("execute_schema"))
    }

    fn execute_partitions(&mut self) -> Result<PartitionedResult> {
        Err(not_implemented("partitioned results"))
    }

    fn get_parameter_schema(&self) -> Result<Schema> {
        Err(not_implemented("get_parameter_schema"))
    }

    fn prepare(&mut self) -> Result<()> {
        Err(not_implemented("prepare"))
    }

    fn set_sql_query(&mut self, query: impl AsRef<str>) -> Result<()> {
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

        let originally_autocommit = self
            .connection
            .lock()
            .expect("connection mutex is not poisoned")
            .server_info()
            .map_err(map_cursor_error)?
            .autocommit;
        if originally_autocommit {
            self.connection
                .lock()
                .expect("connection mutex is not poisoned")
                .set_autocommit(false)
                .map_err(map_cursor_error)?;
        }

        let result = (|| {
            let mut cursor = self
                .connection
                .lock()
                .expect("connection mutex is not poisoned")
                .cursor();
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
                let uploads = schema
                    .fields()
                    .iter()
                    .zip(batch.columns())
                    .enumerate()
                    .map(|(index, (field, array))| {
                        monetdb_arrow::encode_column(field, array.as_ref())
                            .map(|bytes| (format!("c{index}"), bytes))
                            .map_err(|value| map_display(value, Status::InvalidData))
                    })
                    .collect::<Result<HashMap<_, _>>>()?;
                cursor
                    .execute_with_binary_uploads(&copy, &uploads)
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

        if result.is_err() && originally_autocommit {
            let mut cursor = self
                .connection
                .lock()
                .expect("connection mutex is not poisoned")
                .cursor();
            let _ = cursor.execute("ROLLBACK");
            let _ = cursor.close();
        }
        if originally_autocommit {
            self.connection
                .lock()
                .expect("connection mutex is not poisoned")
                .set_autocommit(true)
                .map_err(map_cursor_error)?;
        }
        result.map(Some)
    }
}

fn execute_update(
    connection: &Arc<Mutex<monetdb::Connection>>,
    query: &str,
) -> Result<Option<i64>> {
    let mut cursor = connection
        .lock()
        .expect("connection mutex is not poisoned")
        .cursor();
    cursor.execute(query).map_err(map_cursor_error)?;
    Ok(cursor.affected_rows())
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
}
