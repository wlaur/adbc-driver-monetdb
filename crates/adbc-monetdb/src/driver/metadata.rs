use std::sync::{Arc, Mutex};

use adbc_core::error::{Error, Result, Status};
use adbc_core::options::ObjectDepth;
use adbc_core::schemas::GET_OBJECTS_SCHEMA;
use arrow_array::builder::{
    ArrayBuilder, BooleanBuilder, Int16Builder, Int32Builder, ListBuilder, StringBuilder,
    StructBuilder, make_builder,
};
use arrow_array::{Array, BooleanArray, Int32Array, RecordBatch, StringArray};

use super::{error, query_reader};

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
    schema_filter: Option<&str>,
    table_filter: Option<&str>,
    table_types: Option<&[&str]>,
    column_filter: Option<&str>,
) -> Result<Vec<ObjectSchema>> {
    let query = r#"
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
          LEFT OUTER JOIN sys.tables AS t ON t.schema_id = s.id
          LEFT OUTER JOIN sys.table_types AS tt ON tt.table_type_id = t.type
          LEFT OUTER JOIN sys.columns AS c ON c.table_id = t.id
          LEFT OUTER JOIN sys.comments AS cm ON cm.id = c.id
         ORDER BY s.name, t.name, c.number
    "#;
    let mut reader = query_reader(connection, query, super::DEFAULT_BATCH_ROWS)?;
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
            if !matches_filter(schema_filter, schema_name) {
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
            if !matches_filter(table_filter, table_name)
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
            if !matches_filter(column_filter, column_name) {
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
    load_constraints(connection, &mut schemas)?;
    Ok(schemas)
}

fn load_constraints(
    connection: &Arc<Mutex<monetdb::Connection>>,
    schemas: &mut [ObjectSchema],
) -> Result<()> {
    let query = r#"
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
          LEFT OUTER JOIN sys.objects AS kc ON kc.id = k.id
          LEFT OUTER JOIN sys.keys AS pk ON pk.id = k.rkey
          LEFT OUTER JOIN sys.tables AS pt ON pt.id = pk.table_id
          LEFT OUTER JOIN sys.schemas AS ps ON ps.id = pt.schema_id
          LEFT OUTER JOIN sys.objects AS pc ON pc.id = pk.id AND pc.nr = kc.nr
         ORDER BY s.name, t.name, k.name, kc.nr
    "#;
    let mut reader = query_reader(connection, query, super::DEFAULT_BATCH_ROWS)?;
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
            let Some(schema) = schemas
                .iter_mut()
                .find(|schema| schema.name == schema_names.value(row))
            else {
                continue;
            };
            let Some(table) = schema
                .tables
                .iter_mut()
                .find(|table| table.name == table_names.value(row))
            else {
                continue;
            };
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

fn matches_filter(filter: Option<&str>, value: &str) -> bool {
    filter
        .map(|pattern| like_pattern_matches(pattern, value))
        .unwrap_or(true)
}

pub(super) fn like_pattern_matches(pattern: &str, value: &str) -> bool {
    fn matches(pattern: &[char], value: &[char], p: usize, v: usize) -> bool {
        if p == pattern.len() {
            return v == value.len();
        }
        match pattern[p] {
            '%' => {
                matches(pattern, value, p + 1, v)
                    || (v < value.len() && matches(pattern, value, p, v + 1))
            }
            '_' => v < value.len() && matches(pattern, value, p + 1, v + 1),
            '\\' if p + 1 < pattern.len() => {
                v < value.len()
                    && pattern[p + 1] == value[v]
                    && matches(pattern, value, p + 2, v + 1)
            }
            character => {
                v < value.len() && character == value[v] && matches(pattern, value, p + 1, v + 1)
            }
        }
    }
    matches(
        &pattern.chars().collect::<Vec<_>>(),
        &value.chars().collect::<Vec<_>>(),
        0,
        0,
    )
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
        .append_option(xdbc.data_type);
    item.field_builder::<Int16Builder>(11)
        .expect("canonical datetime subtype builder")
        .append_null();
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
    size: Option<i32>,
    decimal_digits: Option<i16>,
    radix: Option<i16>,
    char_length: Option<i32>,
}

fn xdbc_type(column: &ObjectColumn) -> XdbcType {
    let name = column.type_name.to_ascii_lowercase();
    let data_type = match name.as_str() {
        "boolean" => Some(16),
        "tinyint" => Some(-6),
        "smallint" => Some(5),
        "int" | "integer" => Some(4),
        "bigint" | "hugeint" => Some(-5),
        "decimal" => Some(3),
        "real" => Some(7),
        "double" => Some(8),
        "date" => Some(91),
        "time" | "timetz" => Some(92),
        "timestamp" | "timestamptz" => Some(93),
        "blob" => Some(-4),
        "char" | "varchar" | "clob" | "string" | "json" | "url" | "inet" | "uuid" => Some(12),
        _ if name.contains("interval") => Some(110),
        _ => None,
    };
    let size = (column.digits > 0).then_some(column.digits);
    let decimal_digits = (name == "decimal")
        .then(|| i16::try_from(column.scale).ok())
        .flatten();
    let radix = matches!(
        name.as_str(),
        "tinyint" | "smallint" | "int" | "integer" | "bigint" | "hugeint" | "decimal"
    )
    .then_some(10);
    let char_length = matches!(
        name.as_str(),
        "char" | "varchar" | "clob" | "string" | "json" | "url" | "inet"
    )
    .then_some(column.digits)
    .filter(|length| *length > 0);
    XdbcType {
        data_type,
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
    use super::*;

    #[test]
    fn matches_adbc_like_patterns() {
        assert!(like_pattern_matches("pub%", "public"));
        assert!(like_pattern_matches("_ublic", "public"));
        assert!(like_pattern_matches(r"a\%b", "a%b"));
        assert!(!like_pattern_matches("sys", "public"));
    }
}
