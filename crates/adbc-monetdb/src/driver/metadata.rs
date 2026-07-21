use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use adbc_core::error::{Error, Result, Status};
use adbc_core::options::ObjectDepth;
use adbc_core::schemas::GET_OBJECTS_SCHEMA;
use arrow_array::builder::{
    ArrayBuilder, BooleanBuilder, Int16Builder, Int32Builder, ListBuilder, StringBuilder,
    StructBuilder, make_builder,
};
use arrow_array::{Array, BooleanArray, Int32Array, RecordBatch, StringArray};
use arrow_schema::Schema;
use monetdb::Timeouts;

use super::{error, prepared_arrow_field, prepared_monet_type, query_reader_with_timeouts};

#[derive(Debug)]
pub(super) struct ObjectSchema {
    name: String,
    tables: Vec<ObjectTable>,
}

#[derive(Debug)]
struct ObjectTable {
    name: String,
    table_type: String,
    columns: Vec<ObjectColumn>,
    constraints: Vec<ObjectConstraint>,
}

#[derive(Debug)]
struct ObjectColumn {
    name: String,
    ordinal: i32,
    remarks: Option<String>,
    type_name: String,
    digits: i32,
    scale: i32,
    nullable: bool,
    default_value: Option<String>,
}

#[derive(Debug)]
struct ObjectConstraint {
    name: Option<String>,
    constraint_type: String,
    columns: Vec<String>,
    usage: Option<Vec<ConstraintUsage>>,
}

#[derive(Debug)]
struct ConstraintUsage {
    schema: String,
    table: String,
    column: String,
}

pub(super) fn load_objects(
    connection: &Arc<Mutex<monetdb::Connection>>,
    depth: ObjectDepth,
    schema_filter: Option<&str>,
    table_filter: Option<&str>,
    table_types: Option<&[&str]>,
    column_filter: Option<&str>,
    timeouts: Timeouts,
) -> Result<Vec<ObjectSchema>> {
    if depth == ObjectDepth::Schemas {
        return load_schemas(connection, schema_filter, timeouts);
    }
    if depth == ObjectDepth::Tables {
        return load_tables(
            connection,
            schema_filter,
            table_filter,
            table_types,
            timeouts,
        );
    }
    let schema_predicate = like_predicate("s.name", schema_filter)?;
    let table_predicate = like_predicate("t.name", table_filter)?;
    let column_predicate = like_predicate("c.name", column_filter)?;
    let type_predicate = in_predicate("tt.table_type_name", table_types)?;
    let schema_where = schema_predicate
        .as_deref()
        .map(|predicate| format!("WHERE {predicate}"))
        .unwrap_or_default();
    let table_join_filters = [table_predicate.as_deref(), type_predicate.as_deref()]
        .into_iter()
        .flatten()
        .map(|predicate| format!(" AND {predicate}"))
        .collect::<String>();
    let column_join_filter = column_predicate
        .as_deref()
        .map(|predicate| format!(" AND {predicate}"))
        .unwrap_or_default();
    let query = format!(
        r#"
        SELECT s.name AS schema_name,
               t.name AS table_name,
               tt.table_type_name AS table_type,
               c.name AS column_name,
               c.number AS ordinal_position,
               c.type AS type_name,
               c.type_digits,
               c.type_scale,
               c."null" AS is_nullable,
               c."default" AS column_default,
               cm.remark
          FROM sys.schemas AS s
          LEFT OUTER JOIN (
              SELECT t.*, tt.table_type_name
                FROM sys.tables AS t
                JOIN sys.table_types AS tt ON tt.table_type_id = t.type
               WHERE TRUE {table_join_filters}
          ) AS t ON t.schema_id = s.id
          LEFT OUTER JOIN sys.table_types AS tt ON tt.table_type_id = t.type
          LEFT OUTER JOIN sys.columns AS c ON c.table_id = t.id{column_join_filter}
          LEFT OUTER JOIN sys.comments AS cm ON cm.id = c.id
         {schema_where}
         ORDER BY s.name, t.name, c.number
    "#
    );
    let mut reader =
        query_reader_with_timeouts(connection, &query, super::DEFAULT_READ_BATCH_ROWS, timeouts)?;
    let mut schemas = Vec::<ObjectSchema>::new();
    for batch in &mut reader {
        let batch = batch.map_err(Error::from)?;
        let schema_names = array_as::<StringArray>(&batch, 0)?;
        let table_names = array_as::<StringArray>(&batch, 1)?;
        let table_type_names = array_as::<StringArray>(&batch, 2)?;
        let column_names = array_as::<StringArray>(&batch, 3)?;
        let ordinals = array_as::<Int32Array>(&batch, 4)?;
        let type_names = array_as::<StringArray>(&batch, 5)?;
        let digits = array_as::<Int32Array>(&batch, 6)?;
        let scales = array_as::<Int32Array>(&batch, 7)?;
        let nullable = array_as::<BooleanArray>(&batch, 8)?;
        let defaults = array_as::<StringArray>(&batch, 9)?;
        let remarks = array_as::<StringArray>(&batch, 10)?;

        for row in 0..batch.num_rows() {
            let schema_name = schema_names.value(row);
            if !matches_filter(schema_filter, schema_name)? {
                continue;
            }
            if schemas.last().map(|schema| schema.name.as_str()) != Some(schema_name) {
                schemas.push(ObjectSchema {
                    name: schema_name.to_owned(),
                    tables: Vec::new(),
                });
            }
            if table_names.is_null(row) {
                continue;
            }
            let table_name = table_names.value(row);
            let table_type = table_type_names.value(row);
            if !matches_filter(table_filter, table_name)?
                || !table_types
                    .map(|types| types.contains(&table_type))
                    .unwrap_or(true)
            {
                continue;
            }
            let schema = schemas.last_mut().expect("schema was just inserted");
            if schema.tables.last().map(|table| table.name.as_str()) != Some(table_name) {
                schema.tables.push(ObjectTable {
                    name: table_name.to_owned(),
                    table_type: table_type.to_owned(),
                    columns: Vec::new(),
                    constraints: Vec::new(),
                });
            }
            if column_names.is_null(row) {
                continue;
            }
            let column_name = column_names.value(row);
            if !matches_filter(column_filter, column_name)? {
                continue;
            }
            schema
                .tables
                .last_mut()
                .expect("table was just inserted")
                .columns
                .push(ObjectColumn {
                    name: column_name.to_owned(),
                    ordinal: ordinals
                        .value(row)
                        .checked_add(1)
                        .ok_or_else(|| error("column ordinal overflows i32", Status::Internal))?,
                    remarks: (!remarks.is_null(row)).then(|| remarks.value(row).to_owned()),
                    type_name: type_names.value(row).to_owned(),
                    digits: digits.value(row),
                    scale: scales.value(row),
                    nullable: nullable.value(row),
                    default_value: (!defaults.is_null(row)).then(|| defaults.value(row).to_owned()),
                });
        }
    }
    load_constraints(
        connection,
        &mut schemas,
        schema_filter,
        table_filter,
        table_types,
        timeouts,
    )?;
    Ok(schemas)
}

fn load_schemas(
    connection: &Arc<Mutex<monetdb::Connection>>,
    schema_filter: Option<&str>,
    timeouts: Timeouts,
) -> Result<Vec<ObjectSchema>> {
    let predicate = like_predicate("name", schema_filter)?
        .map(|predicate| format!("WHERE {predicate}"))
        .unwrap_or_default();
    let query = format!("SELECT name FROM sys.schemas {predicate} ORDER BY name");
    let mut reader =
        query_reader_with_timeouts(connection, &query, super::DEFAULT_READ_BATCH_ROWS, timeouts)?;
    let mut schemas = Vec::new();
    for batch in &mut reader {
        let batch = batch.map_err(Error::from)?;
        let names = array_as::<StringArray>(&batch, 0)?;
        for row in 0..batch.num_rows() {
            schemas.push(ObjectSchema {
                name: names.value(row).to_owned(),
                tables: Vec::new(),
            });
        }
    }
    Ok(schemas)
}

fn load_tables(
    connection: &Arc<Mutex<monetdb::Connection>>,
    schema_filter: Option<&str>,
    table_filter: Option<&str>,
    table_types: Option<&[&str]>,
    timeouts: Timeouts,
) -> Result<Vec<ObjectSchema>> {
    let schema_where = like_predicate("s.name", schema_filter)?
        .map(|predicate| format!("WHERE {predicate}"))
        .unwrap_or_default();
    let table_join_filters = [
        like_predicate("t.name", table_filter)?,
        in_predicate("tt.table_type_name", table_types)?,
    ]
    .into_iter()
    .flatten()
    .map(|predicate| format!(" AND {predicate}"))
    .collect::<String>();
    let query = format!(
        "SELECT s.name, t.name, t.table_type_name \
         FROM sys.schemas AS s \
         LEFT OUTER JOIN (\
             SELECT t.*, tt.table_type_name \
             FROM sys.tables AS t \
             JOIN sys.table_types AS tt ON tt.table_type_id = t.type \
             WHERE TRUE {table_join_filters}\
         ) AS t ON t.schema_id = s.id \
         {schema_where} ORDER BY s.name, t.name"
    );
    let mut reader =
        query_reader_with_timeouts(connection, &query, super::DEFAULT_READ_BATCH_ROWS, timeouts)?;
    let mut schemas = Vec::<ObjectSchema>::new();
    for batch in &mut reader {
        let batch = batch.map_err(Error::from)?;
        let schema_names = array_as::<StringArray>(&batch, 0)?;
        let table_names = array_as::<StringArray>(&batch, 1)?;
        let table_type_names = array_as::<StringArray>(&batch, 2)?;
        for row in 0..batch.num_rows() {
            let schema_name = schema_names.value(row);
            if schemas.last().map(|schema| schema.name.as_str()) != Some(schema_name) {
                schemas.push(ObjectSchema {
                    name: schema_name.to_owned(),
                    tables: Vec::new(),
                });
            }
            if !table_names.is_null(row) {
                schemas
                    .last_mut()
                    .expect("schema was just inserted")
                    .tables
                    .push(ObjectTable {
                        name: table_names.value(row).to_owned(),
                        table_type: table_type_names.value(row).to_owned(),
                        columns: Vec::new(),
                        constraints: Vec::new(),
                    });
            }
        }
    }
    Ok(schemas)
}

pub(super) fn table_schema(
    connection: &Arc<Mutex<monetdb::Connection>>,
    schema_name: &str,
    table_name: &str,
    timeouts: Timeouts,
) -> Result<Schema> {
    let query = format!(
        r#"
        SELECT c.name, c.type, c.type_digits, c.type_scale, c."null"
          FROM sys.columns AS c
          JOIN sys.tables AS t ON t.id = c.table_id
          JOIN sys.schemas AS s ON s.id = t.schema_id
         WHERE s.name = {} AND t.name = {}
         ORDER BY c.number
        "#,
        raw_string_literal(schema_name)?,
        raw_string_literal(table_name)?,
    );
    let mut reader =
        query_reader_with_timeouts(connection, &query, super::DEFAULT_READ_BATCH_ROWS, timeouts)?;
    let mut fields = Vec::new();
    for batch in &mut reader {
        let batch = batch.map_err(Error::from)?;
        let names = array_as::<StringArray>(&batch, 0)?;
        let type_names = array_as::<StringArray>(&batch, 1)?;
        let digits = array_as::<Int32Array>(&batch, 2)?;
        let scales = array_as::<Int32Array>(&batch, 3)?;
        let nullable = array_as::<BooleanArray>(&batch, 4)?;
        for row in 0..batch.num_rows() {
            let data_type =
                prepared_monet_type(type_names.value(row), digits.value(row), scales.value(row))?;
            fields.push(
                prepared_arrow_field(names.value(row).to_owned(), &data_type)?
                    .with_nullable(nullable.value(row)),
            );
        }
    }
    if fields.is_empty() {
        return Err(error(
            format!("table '{schema_name}.{table_name}' does not exist"),
            Status::NotFound,
        ));
    }
    Ok(Schema::new(fields))
}

fn load_constraints(
    connection: &Arc<Mutex<monetdb::Connection>>,
    schemas: &mut [ObjectSchema],
    schema_filter: Option<&str>,
    table_filter: Option<&str>,
    table_types: Option<&[&str]>,
    timeouts: Timeouts,
) -> Result<()> {
    let table_indices = schemas
        .iter()
        .enumerate()
        .flat_map(|(schema_index, schema)| {
            schema
                .tables
                .iter()
                .enumerate()
                .map(move |(table_index, table)| {
                    (
                        (schema.name.clone(), table.name.clone()),
                        (schema_index, table_index),
                    )
                })
        })
        .collect::<HashMap<_, _>>();
    let predicates = [
        like_predicate("s.name", schema_filter)?,
        like_predicate("t.name", table_filter)?,
        in_predicate("tt.table_type_name", table_types)?,
    ]
    .into_iter()
    .flatten()
    .collect::<Vec<_>>();
    let where_clause = if predicates.is_empty() {
        String::new()
    } else {
        format!("WHERE {}", predicates.join(" AND "))
    };
    let query = format!(
        r#"
        SELECT s.name AS schema_name,
               t.name AS table_name,
               k.name AS constraint_name,
               k.type AS constraint_type,
               kc.name AS column_name,
               kc.nr AS column_number,
               ps.name AS usage_schema,
               pt.name AS usage_table,
               pc.name AS usage_column
          FROM sys.keys AS k
          JOIN sys.tables AS t ON t.id = k.table_id
          JOIN sys.schemas AS s ON s.id = t.schema_id
          JOIN sys.table_types AS tt ON tt.table_type_id = t.type
          LEFT OUTER JOIN sys.objects AS kc ON kc.id = k.id
          LEFT OUTER JOIN sys.keys AS pk ON pk.id = k.rkey
          LEFT OUTER JOIN sys.tables AS pt ON pt.id = pk.table_id
          LEFT OUTER JOIN sys.schemas AS ps ON ps.id = pt.schema_id
          LEFT OUTER JOIN sys.objects AS pc ON pc.id = pk.id AND pc.nr = kc.nr
         {where_clause}
         ORDER BY s.name, t.name, k.name, kc.nr
    "#
    );
    let mut reader =
        query_reader_with_timeouts(connection, &query, super::DEFAULT_READ_BATCH_ROWS, timeouts)?;
    for batch in &mut reader {
        let batch = batch.map_err(Error::from)?;
        let schema_names = array_as::<StringArray>(&batch, 0)?;
        let table_names = array_as::<StringArray>(&batch, 1)?;
        let constraint_names = array_as::<StringArray>(&batch, 2)?;
        let constraint_types = array_as::<Int32Array>(&batch, 3)?;
        let column_names = array_as::<StringArray>(&batch, 4)?;
        let usage_schemas = array_as::<StringArray>(&batch, 6)?;
        let usage_tables = array_as::<StringArray>(&batch, 7)?;
        let usage_columns = array_as::<StringArray>(&batch, 8)?;
        for row in 0..batch.num_rows() {
            let Some(&(schema_index, table_index)) = table_indices.get(&(
                schema_names.value(row).to_owned(),
                table_names.value(row).to_owned(),
            )) else {
                continue;
            };
            let table = &mut schemas[schema_index].tables[table_index];
            let name =
                (!constraint_names.is_null(row)).then(|| constraint_names.value(row).to_owned());
            let constraint_type = constraint_type_name(constraint_types.value(row));
            if table
                .constraints
                .last()
                .map(|constraint| (&constraint.name, constraint.constraint_type.as_str()))
                != Some((&name, constraint_type))
            {
                table.constraints.push(ObjectConstraint {
                    name: name.clone(),
                    constraint_type: constraint_type.to_owned(),
                    columns: Vec::new(),
                    usage: (constraint_type == "FOREIGN KEY").then(Vec::new),
                });
            }
            let constraint = table
                .constraints
                .last_mut()
                .expect("constraint was just inserted");
            if !column_names.is_null(row) {
                constraint.columns.push(column_names.value(row).to_owned());
            }
            if let Some(usage) = &mut constraint.usage
                && !usage_schemas.is_null(row)
                && !usage_tables.is_null(row)
                && !usage_columns.is_null(row)
            {
                usage.push(ConstraintUsage {
                    schema: usage_schemas.value(row).to_owned(),
                    table: usage_tables.value(row).to_owned(),
                    column: usage_columns.value(row).to_owned(),
                });
            }
        }
    }
    Ok(())
}

fn constraint_type_name(value: i32) -> &'static str {
    // MonetDB `sql/include/sql_catalog.h` defines the key-type codes used by
    // `sys.keys`: primary=0, unique=1, foreign=2, key=3, and check=4.
    match value {
        0 => "PRIMARY KEY",
        1 | 3 => "UNIQUE",
        2 => "FOREIGN KEY",
        4 => "CHECK",
        _ => "UNKNOWN",
    }
}

fn array_as<T: 'static>(batch: &RecordBatch, index: usize) -> Result<&T> {
    batch.column(index).as_any().downcast_ref().ok_or_else(|| {
        error(
            format!("metadata column {index} has an unexpected type"),
            Status::Internal,
        )
    })
}

fn matches_filter(filter: Option<&str>, value: &str) -> Result<bool> {
    filter
        .map(|pattern| like_pattern_matches(pattern, value))
        .unwrap_or(Ok(true))
}

fn like_predicate(column: &str, pattern: Option<&str>) -> Result<Option<String>> {
    pattern
        .map(|pattern| {
            validate_like_pattern(pattern)?;
            Ok(format!(
                "{column} LIKE {} ESCAPE R'\\'",
                raw_string_literal(pattern)?
            ))
        })
        .transpose()
}

fn in_predicate(column: &str, values: Option<&[&str]>) -> Result<Option<String>> {
    values
        .map(|values| {
            if values.is_empty() {
                return Ok("FALSE".to_owned());
            }
            let values = values
                .iter()
                .map(|value| raw_string_literal(value))
                .collect::<Result<Vec<_>>>()?;
            Ok(format!("{column} IN ({})", values.join(", ")))
        })
        .transpose()
}

pub(super) fn raw_string_literal(value: &str) -> Result<String> {
    if value.contains('\0') {
        return Err(error(
            "metadata filter contains a NUL byte",
            Status::InvalidArguments,
        ));
    }
    Ok(format!("R'{}'", value.replace('\'', "''")))
}

fn validate_like_pattern(pattern: &str) -> Result<()> {
    let mut chars = pattern.chars();
    while let Some(character) = chars.next() {
        if character != '\\' {
            continue;
        }
        match chars.next() {
            Some('%' | '_' | '\\') => {}
            Some(character) => {
                return Err(error(
                    format!("metadata LIKE pattern has invalid escape '\\{character}'"),
                    Status::InvalidArguments,
                ));
            }
            None => {
                return Err(error(
                    "metadata LIKE pattern ends with an escape character",
                    Status::InvalidArguments,
                ));
            }
        }
    }
    Ok(())
}

pub(super) fn like_pattern_matches(pattern: &str, value: &str) -> Result<bool> {
    validate_like_pattern(pattern)?;
    #[derive(Clone, Copy)]
    enum Token {
        Many,
        One,
        Literal(char),
    }
    let mut chars = pattern.chars();
    let mut tokens = Vec::new();
    while let Some(character) = chars.next() {
        match character {
            '%' => tokens.push(Token::Many),
            '_' => tokens.push(Token::One),
            '\\' => tokens.push(Token::Literal(
                chars.next().expect("pattern escapes were validated"),
            )),
            character => tokens.push(Token::Literal(character)),
        }
    }
    let value = value.chars().collect::<Vec<_>>();
    let (mut token, mut character) = (0, 0);
    let (mut star, mut retry) = (None, 0);
    while character < value.len() {
        match tokens.get(token) {
            Some(Token::Literal(expected)) if *expected == value[character] => {
                token += 1;
                character += 1;
            }
            Some(Token::One) => {
                token += 1;
                character += 1;
            }
            Some(Token::Many) => {
                star = Some(token);
                token += 1;
                retry = character;
            }
            _ => match star {
                Some(star) => {
                    token = star + 1;
                    retry += 1;
                    character = retry;
                }
                None => return Ok(false),
            },
        }
    }
    while matches!(tokens.get(token), Some(Token::Many)) {
        token += 1;
    }
    Ok(token == tokens.len())
}

pub(super) fn objects_batch(
    catalog: &str,
    include_catalog: bool,
    depth: ObjectDepth,
    schemas: &[ObjectSchema],
) -> Result<RecordBatch> {
    let mut catalog_names = StringBuilder::new();
    let mut schema_lists = make_builder(GET_OBJECTS_SCHEMA.field(1).data_type(), 1);
    if include_catalog {
        catalog_names.append_value(catalog);
        let schema_list = list_builder(&mut schema_lists)?;
        if depth == ObjectDepth::Catalogs {
            schema_list.append_null();
        } else {
            for schema in schemas {
                append_object_schema(schema_list, schema, catalog, depth)?;
            }
            schema_list.append(true);
        }
    }
    Ok(RecordBatch::try_new(
        GET_OBJECTS_SCHEMA.clone(),
        vec![Arc::new(catalog_names.finish()), schema_lists.finish()],
    )?)
}

fn append_object_schema(
    schemas: &mut ListBuilder<Box<dyn ArrayBuilder>>,
    schema: &ObjectSchema,
    catalog: &str,
    depth: ObjectDepth,
) -> Result<()> {
    let item = struct_values(schemas)?;
    item.field_builder::<StringBuilder>(0)
        .expect("canonical schema name builder")
        .append_value(&schema.name);
    let tables = item
        .field_builder::<ListBuilder<Box<dyn ArrayBuilder>>>(1)
        .expect("canonical schema tables builder");
    if depth == ObjectDepth::Schemas {
        tables.append_null();
    } else {
        for table in &schema.tables {
            append_object_table(tables, table, catalog, depth)?;
        }
        tables.append(true);
    }
    item.append(true);
    Ok(())
}

fn append_object_table(
    tables: &mut ListBuilder<Box<dyn ArrayBuilder>>,
    table: &ObjectTable,
    catalog: &str,
    depth: ObjectDepth,
) -> Result<()> {
    let item = struct_values(tables)?;
    item.field_builder::<StringBuilder>(0)
        .expect("canonical table name builder")
        .append_value(&table.name);
    item.field_builder::<StringBuilder>(1)
        .expect("canonical table type builder")
        .append_value(&table.table_type);
    let columns = item
        .field_builder::<ListBuilder<Box<dyn ArrayBuilder>>>(2)
        .expect("canonical table columns builder");
    if depth == ObjectDepth::Tables {
        columns.append_null();
    } else {
        for column in &table.columns {
            append_object_column(columns, column)?;
        }
        columns.append(true);
    }
    let constraints = item
        .field_builder::<ListBuilder<Box<dyn ArrayBuilder>>>(3)
        .expect("canonical constraints builder");
    if depth == ObjectDepth::Tables {
        constraints.append_null();
    } else {
        for constraint in &table.constraints {
            append_object_constraint(constraints, constraint, catalog)?;
        }
        constraints.append(true);
    }
    item.append(true);
    Ok(())
}

fn append_object_constraint(
    constraints: &mut ListBuilder<Box<dyn ArrayBuilder>>,
    constraint: &ObjectConstraint,
    catalog: &str,
) -> Result<()> {
    let item = struct_values(constraints)?;
    item.field_builder::<StringBuilder>(0)
        .expect("canonical constraint name builder")
        .append_option(constraint.name.as_deref());
    item.field_builder::<StringBuilder>(1)
        .expect("canonical constraint type builder")
        .append_value(&constraint.constraint_type);
    let columns = item
        .field_builder::<ListBuilder<Box<dyn ArrayBuilder>>>(2)
        .expect("canonical constraint columns builder");
    let column_values = columns
        .values()
        .as_any_mut()
        .downcast_mut::<StringBuilder>()
        .expect("canonical constraint column value builder");
    for column in &constraint.columns {
        column_values.append_value(column);
    }
    columns.append(true);

    let usage = item
        .field_builder::<ListBuilder<Box<dyn ArrayBuilder>>>(3)
        .expect("canonical constraint usage builder");
    if let Some(values) = &constraint.usage {
        for value in values {
            let usage_item = struct_values(usage)?;
            usage_item
                .field_builder::<StringBuilder>(0)
                .expect("canonical usage catalog builder")
                .append_value(catalog);
            usage_item
                .field_builder::<StringBuilder>(1)
                .expect("canonical usage schema builder")
                .append_value(&value.schema);
            usage_item
                .field_builder::<StringBuilder>(2)
                .expect("canonical usage table builder")
                .append_value(&value.table);
            usage_item
                .field_builder::<StringBuilder>(3)
                .expect("canonical usage column builder")
                .append_value(&value.column);
            usage_item.append(true);
        }
        usage.append(true);
    } else {
        usage.append_null();
    }
    item.append(true);
    Ok(())
}

fn append_object_column(
    columns: &mut ListBuilder<Box<dyn ArrayBuilder>>,
    column: &ObjectColumn,
) -> Result<()> {
    let item = struct_values(columns)?;
    item.field_builder::<StringBuilder>(0)
        .expect("canonical column name builder")
        .append_value(&column.name);
    item.field_builder::<Int32Builder>(1)
        .expect("canonical ordinal builder")
        .append_value(column.ordinal);
    item.field_builder::<StringBuilder>(2)
        .expect("canonical remarks builder")
        .append_option(column.remarks.as_deref());
    let xdbc = xdbc_type(column);
    item.field_builder::<Int16Builder>(3)
        .expect("canonical data type builder")
        .append_option(xdbc.data_type);
    item.field_builder::<StringBuilder>(4)
        .expect("canonical type name builder")
        .append_value(column.type_name.to_ascii_uppercase());
    item.field_builder::<Int32Builder>(5)
        .expect("canonical column size builder")
        .append_option(xdbc.size);
    item.field_builder::<Int16Builder>(6)
        .expect("canonical decimal digits builder")
        .append_option(xdbc.decimal_digits);
    item.field_builder::<Int16Builder>(7)
        .expect("canonical radix builder")
        .append_option(xdbc.radix);
    item.field_builder::<Int16Builder>(8)
        .expect("canonical nullable builder")
        .append_value(i16::from(column.nullable));
    item.field_builder::<StringBuilder>(9)
        .expect("canonical default builder")
        .append_option(column.default_value.as_deref());
    item.field_builder::<Int16Builder>(10)
        .expect("canonical SQL data type builder")
        .append_option(xdbc.sql_data_type);
    item.field_builder::<Int16Builder>(11)
        .expect("canonical datetime subtype builder")
        .append_option(xdbc.datetime_sub);
    item.field_builder::<Int32Builder>(12)
        .expect("canonical octet length builder")
        .append_option(xdbc.char_length);
    item.field_builder::<StringBuilder>(13)
        .expect("canonical nullable text builder")
        .append_value(if column.nullable { "YES" } else { "NO" });
    for index in 14..=16 {
        item.field_builder::<StringBuilder>(index)
            .expect("canonical scope builder")
            .append_null();
    }
    item.field_builder::<BooleanBuilder>(17)
        .expect("canonical autoincrement builder")
        .append_null();
    item.field_builder::<BooleanBuilder>(18)
        .expect("canonical generated column builder")
        .append_null();
    item.append(true);
    Ok(())
}

struct XdbcType {
    data_type: Option<i16>,
    sql_data_type: Option<i16>,
    datetime_sub: Option<i16>,
    size: Option<i32>,
    decimal_digits: Option<i16>,
    radix: Option<i16>,
    char_length: Option<i32>,
}

fn xdbc_type(column: &ObjectColumn) -> XdbcType {
    let name = column.type_name.to_ascii_lowercase();
    // These values mirror MonetDB's canonical ODBC metadata definitions in
    // `clients/odbc/driver/ODBCQueries.h` (DATA_TYPE, COLUMN_SIZE,
    // DECIMAL_DIGITS, NUM_PREC_RADIX, SQL_DATA_TYPE, SQL_DATETIME_SUB, and
    // CHAR_OCTET_LENGTH).
    let (data_type, sql_data_type, datetime_sub) = match name.as_str() {
        "boolean" => (Some(16), Some(16), None),
        "tinyint" => (Some(-6), Some(-6), None),
        "smallint" => (Some(5), Some(5), None),
        "int" | "integer" => (Some(4), Some(4), None),
        "bigint" => (Some(-5), Some(-5), None),
        "hugeint" => (Some(16_384), Some(16_384), None),
        "decimal" => (Some(3), Some(3), None),
        "real" => (Some(7), Some(7), None),
        "double" => (Some(8), Some(8), None),
        "date" => (Some(91), Some(9), Some(1)),
        "time" | "timetz" => (Some(92), Some(9), Some(2)),
        "timestamp" | "timestamptz" => (Some(93), Some(9), Some(3)),
        "blob" => (Some(-4), Some(-4), None),
        "char" => (Some(-8), Some(-8), None),
        "clob" => (Some(-10), Some(-10), None),
        "varchar" => (Some(-9), Some(-9), None),
        "uuid" => (None, Some(-11), None),
        "day_interval" => (None, Some(10), Some(3)),
        "month_interval" => match column.digits {
            1 => (Some(101), Some(10), Some(1)),
            2 => (Some(107), Some(10), Some(7)),
            3 => (Some(102), Some(10), Some(2)),
            _ => (None, Some(10), None),
        },
        "sec_interval" => match column.digits {
            4 => (Some(103), Some(10), Some(3)),
            5 => (Some(108), Some(10), Some(8)),
            6 => (Some(109), Some(10), Some(9)),
            7 => (Some(110), Some(10), Some(10)),
            8 => (Some(104), Some(10), Some(4)),
            9 => (Some(111), Some(10), Some(11)),
            10 => (Some(112), Some(10), Some(12)),
            11 => (Some(105), Some(10), Some(5)),
            12 => (Some(113), Some(10), Some(13)),
            13 => (Some(106), Some(10), Some(6)),
            _ => (None, Some(10), None),
        },
        _ => (None, None, None),
    };
    let size = match name.as_str() {
        "date" => Some(10),
        "day_interval" => Some(25),
        "month_interval" => match column.digits {
            1 => Some(26),
            2 => Some(38),
            3 => Some(27),
            _ => None,
        },
        "sec_interval" => match column.digits {
            4 => Some(25),
            5 => Some(36),
            6 => Some(41),
            7 => Some(47),
            8 => Some(26),
            9 => Some(39),
            10 => Some(45),
            11 => Some(28),
            12 => Some(44),
            13 => Some(30),
            _ => None,
        },
        "time" | "timetz" => Some(12),
        "timestamp" | "timestamptz" => Some(23),
        "uuid" => Some(36),
        _ => (column.digits > 0).then_some(column.digits),
    };
    let decimal_digits = match name.as_str() {
        "tinyint" | "smallint" | "int" | "integer" | "bigint" | "hugeint" | "day_interval"
        | "month_interval" | "sec_interval" => Some(0),
        "decimal" => i16::try_from(column.scale).ok(),
        "real" if column.digits == 24 && column.scale == 0 => Some(7),
        "double" if column.digits == 53 && column.scale == 0 => Some(15),
        "real" | "double" => i16::try_from(column.digits).ok(),
        "time" | "timetz" | "timestamp" | "timestamptz" => column
            .digits
            .checked_sub(1)
            .and_then(|value| i16::try_from(value).ok()),
        _ => None,
    };
    let radix = match name.as_str() {
        "tinyint" | "smallint" | "int" | "integer" | "bigint" | "hugeint" => Some(2),
        "decimal" => Some(10),
        "real" if column.digits == 24 && column.scale == 0 => Some(2),
        "double" if column.digits == 53 && column.scale == 0 => Some(2),
        "real" | "double" => Some(10),
        _ => None,
    };
    let char_length = match name.as_str() {
        "char" | "varchar" | "clob" | "json" | "url" | "xml" => column.digits.checked_mul(4),
        "blob" => Some(column.digits),
        _ => None,
    }
    .filter(|length| *length > 0);
    XdbcType {
        data_type,
        sql_data_type,
        datetime_sub,
        size,
        decimal_digits,
        radix,
        char_length,
    }
}

fn list_builder(
    builder: &mut Box<dyn ArrayBuilder>,
) -> Result<&mut ListBuilder<Box<dyn ArrayBuilder>>> {
    builder
        .as_any_mut()
        .downcast_mut()
        .ok_or_else(|| error("canonical object schema is not a list", Status::Internal))
}

fn struct_values(builder: &mut ListBuilder<Box<dyn ArrayBuilder>>) -> Result<&mut StructBuilder> {
    builder.values().as_any_mut().downcast_mut().ok_or_else(|| {
        error(
            "canonical object list does not contain structs",
            Status::Internal,
        )
    })
}

#[cfg(test)]
mod tests {
    use arrow_array::ListArray;

    use super::*;

    #[test]
    fn matches_adbc_like_patterns() {
        assert!(like_pattern_matches("pub%", "public").unwrap());
        assert!(like_pattern_matches("_ublic", "public").unwrap());
        assert!(like_pattern_matches(r"a\%b", "a%b").unwrap());
        assert!(!like_pattern_matches("sys", "public").unwrap());
        assert!(like_pattern_matches("bad\\", "bad").is_err());
        assert!(like_pattern_matches(r"bad\x", "badx").is_err());
        let adversarial = format!("{}x", "%".repeat(100_000));
        assert!(!like_pattern_matches(&adversarial, &"a".repeat(100_000)).unwrap());
        assert_eq!(raw_string_literal("a'b\\c").unwrap(), "R'a''b\\c'");
    }

    #[test]
    fn catalogs_depth_has_a_null_schema_list() {
        let batch = objects_batch("test", true, ObjectDepth::Catalogs, &[]).unwrap();
        let schemas = batch
            .column(1)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        assert!(schemas.is_null(0));
    }

    #[test]
    fn maps_canonical_monetdb_xdbc_metadata() {
        let column = |type_name: &str, digits: i32, scale: i32| ObjectColumn {
            name: "x".into(),
            ordinal: 1,
            remarks: None,
            type_name: type_name.into(),
            digits,
            scale,
            nullable: true,
            default_value: None,
        };
        let int = xdbc_type(&column("int", 31, 0));
        assert_eq!(
            (int.size, int.decimal_digits, int.radix),
            (Some(31), Some(0), Some(2))
        );
        let decimal = xdbc_type(&column("decimal", 12, 3));
        assert_eq!(
            (decimal.size, decimal.decimal_digits, decimal.radix),
            (Some(12), Some(3), Some(10))
        );
        let real = xdbc_type(&column("real", 24, 0));
        assert_eq!((real.decimal_digits, real.radix), (Some(7), Some(2)));
        let double = xdbc_type(&column("double", 53, 0));
        assert_eq!((double.decimal_digits, double.radix), (Some(15), Some(2)));
        let varchar = xdbc_type(&column("varchar", 9, 0));
        assert_eq!(
            (varchar.data_type, varchar.size, varchar.char_length),
            (Some(-9), Some(9), Some(36))
        );
        let blob = xdbc_type(&column("blob", 32, 0));
        assert_eq!((blob.data_type, blob.char_length), (Some(-4), Some(32)));
        let uuid = xdbc_type(&column("uuid", 0, 0));
        assert_eq!(
            (uuid.data_type, uuid.sql_data_type, uuid.size),
            (None, Some(-11), Some(36))
        );
        let timestamp = xdbc_type(&column("timestamp", 3, 0));
        assert_eq!(
            (
                timestamp.data_type,
                timestamp.sql_data_type,
                timestamp.datetime_sub,
                timestamp.size,
                timestamp.decimal_digits,
            ),
            (Some(93), Some(9), Some(3), Some(23), Some(2))
        );
        let interval = xdbc_type(&column("sec_interval", 12, 0));
        assert_eq!(
            (
                interval.data_type,
                interval.sql_data_type,
                interval.datetime_sub,
                interval.size
            ),
            (Some(113), Some(10), Some(13), Some(44))
        );
        let extension = xdbc_type(&column("json", 64, 0));
        assert_eq!(
            (extension.data_type, extension.char_length),
            (None, Some(256))
        );
    }
}
