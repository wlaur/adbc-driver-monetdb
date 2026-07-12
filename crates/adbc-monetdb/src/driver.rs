//! ADBC object model skeleton: driver, database, connection, statement.
//!
//! Option plumbing is functional; everything that talks to a server returns
//! `NotImplemented` until the protocol layer lands (milestones M1-M3 in
//! docs/PLAN.md).

use std::collections::{HashMap, HashSet};

use adbc_core::error::{Error, Result, Status};
use adbc_core::options::{
    InfoCode, ObjectDepth, OptionConnection, OptionDatabase, OptionStatement, OptionValue,
};
use adbc_core::{Connection, Database, Driver, Optionable, PartitionedResult, Statement};
use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::Schema;

fn not_implemented(what: &str) -> Error {
    Error::with_message_and_status(
        format!("adbc-monetdb: {what} is not implemented yet"),
        Status::NotImplemented,
    )
}

fn unknown_option(key: &str) -> Error {
    Error::with_message_and_status(format!("unknown or unset option '{key}'"), Status::NotFound)
}

/// String-keyed option storage shared by the database/connection/statement skeletons.
#[derive(Debug, Default)]
struct Options(HashMap<String, OptionValue>);

impl Options {
    fn set(&mut self, key: impl AsRef<str>, value: OptionValue) {
        self.0.insert(key.as_ref().to_owned(), value);
    }

    fn get_string(&self, key: impl AsRef<str>) -> Result<String> {
        match self.0.get(key.as_ref()) {
            Some(OptionValue::String(value)) => Ok(value.clone()),
            _ => Err(unknown_option(key.as_ref())),
        }
    }

    fn get_bytes(&self, key: impl AsRef<str>) -> Result<Vec<u8>> {
        match self.0.get(key.as_ref()) {
            Some(OptionValue::Bytes(value)) => Ok(value.clone()),
            _ => Err(unknown_option(key.as_ref())),
        }
    }

    fn get_int(&self, key: impl AsRef<str>) -> Result<i64> {
        match self.0.get(key.as_ref()) {
            Some(OptionValue::Int(value)) => Ok(*value),
            _ => Err(unknown_option(key.as_ref())),
        }
    }

    fn get_double(&self, key: impl AsRef<str>) -> Result<f64> {
        match self.0.get(key.as_ref()) {
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
        _opts: impl IntoIterator<Item = (OptionConnection, OptionValue)>,
    ) -> Result<Self::ConnectionType> {
        // MAPI connect (auth, TLS, redirects) + the Dec2025+ server check land here.
        Err(not_implemented("connecting to a MonetDB server"))
    }
}

#[derive(Debug, Default)]
pub struct MonetdbConnection {
    options: Options,
}

impl Optionable for MonetdbConnection {
    type Option = OptionConnection;

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

impl Connection for MonetdbConnection {
    type StatementType = MonetdbStatement;

    fn new_statement(&mut self) -> Result<Self::StatementType> {
        Err(not_implemented("creating a statement"))
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
        Err(not_implemented("commit"))
    }

    fn rollback(&mut self) -> Result<()> {
        Err(not_implemented("rollback"))
    }

    fn read_partition(
        &self,
        _partition: impl AsRef<[u8]>,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        // MAPI is a single sequential channel; partitioned results will stay unsupported.
        Err(not_implemented("read_partition"))
    }
}

#[derive(Debug, Default)]
pub struct MonetdbStatement {
    options: Options,
}

impl Optionable for MonetdbStatement {
    type Option = OptionStatement;

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

impl Statement for MonetdbStatement {
    fn bind(&mut self, _batch: RecordBatch) -> Result<()> {
        Err(not_implemented("bind"))
    }

    fn bind_stream(&mut self, _reader: Box<dyn RecordBatchReader + Send>) -> Result<()> {
        Err(not_implemented("bind_stream"))
    }

    fn execute(&mut self) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        Err(not_implemented("execute"))
    }

    fn execute_update(&mut self) -> Result<Option<i64>> {
        Err(not_implemented("execute_update"))
    }

    fn execute_schema(&mut self) -> Result<Schema> {
        Err(not_implemented("execute_schema"))
    }

    fn execute_partitions(&mut self) -> Result<PartitionedResult> {
        Err(not_implemented("execute_partitions"))
    }

    fn get_parameter_schema(&self) -> Result<Schema> {
        Err(not_implemented("get_parameter_schema"))
    }

    fn prepare(&mut self) -> Result<()> {
        Err(not_implemented("prepare"))
    }

    fn set_sql_query(&mut self, _query: impl AsRef<str>) -> Result<()> {
        Err(not_implemented("set_sql_query"))
    }

    fn set_substrait_plan(&mut self, _plan: impl AsRef<[u8]>) -> Result<()> {
        Err(not_implemented("set_substrait_plan"))
    }

    fn cancel(&mut self) -> Result<()> {
        Err(not_implemented("cancel"))
    }
}
