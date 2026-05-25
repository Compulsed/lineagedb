//! Minimal emulation of the PostgreSQL system catalogs.
//!
//! GUI clients (TablePlus, DBeaver, psql's `\d`, ...) discover the schema by querying
//! `pg_catalog` / `information_schema` rather than by asking "what tables exist?" directly.
//! We don't have real catalogs, so we recognize the common introspection queries and return
//! canned rows. Since `person` is our only table, that's enough to make it show up in a
//! client's schema browser and to expand its columns.
//!
//! Detection is deliberately simple (substring matching on catalog identifiers). A genuine
//! `person` query never references these identifiers, so it falls through to normal planning;
//! the only false-positive risk is a person query that embeds a literal like `'pg_class'`,
//! which we accept as a fair trade for an educational server.

use std::sync::Arc;

use futures::{stream, StreamExt};

use pgwire::api::results::{DataRowEncoder, FieldFormat, FieldInfo, QueryResponse, Response};
use pgwire::api::Type;
use sqlparser::ast;
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser as SqlParser;

/// The single table this server exposes, and the schema it lives in.
const TABLE: &str = "person";
const SCHEMA: &str = "public";
/// (column name, SQL is_nullable) for the person table, in order.
const PERSON_COLUMNS: [(&str, &str); 3] =
    [("id", "NO"), ("full_name", "NO"), ("email", "YES")];

/// If `query` is a system-catalog / introspection query, returns a canned response so a GUI
/// client can browse the schema. Returns `None` for ordinary queries (handled normally).
pub fn intercept(query: &str) -> Option<Vec<Response>> {
    let lowered = query.trim().to_lowercase();

    // `SELECT version()` — clients use this to identify the server.
    if lowered.contains("version()") {
        return Some(vec![text_response(
            vec!["version".to_string()],
            vec![vec![Some("PostgreSQL 16.0 (lineagedb)".to_string())]],
        )]);
    }

    let references_catalog = [
        "pg_catalog",
        "pg_class",
        "pg_namespace",
        "pg_type",
        "pg_proc",
        "pg_attribute",
        "pg_matviews",
        "information_schema",
    ]
    .iter()
    .any(|marker| lowered.contains(marker));

    if !references_catalog {
        return None;
    }

    // The exact columns the client asked for, so our response lines up by name.
    let columns = projection_columns(query);

    let is_columns_list =
        lowered.contains("information_schema.columns") || lowered.contains("pg_attribute");
    let is_materialized_view =
        lowered.contains("relkind = 'm'") || lowered.contains("relkind='m'");
    let is_table_list = !is_materialized_view
        && (lowered.contains("information_schema.tables")
            || (lowered.contains("pg_class") && lowered.contains("relkind")));
    let is_schema_list = lowered.contains("pg_namespace")
        && lowered.contains("nspname")
        && !lowered.contains("pg_class")
        && !lowered.contains("pg_proc");

    let rows: Vec<Vec<Option<String>>> = if is_columns_list {
        person_column_rows(&columns)
    } else if is_table_list {
        vec![map_row(&columns, table_value)]
    } else if is_schema_list {
        vec![map_row(&columns, schema_value)]
    } else {
        // Recognized catalog query we have nothing for (pg_type, pg_proc, matviews, ...):
        // return the right column headers but no rows.
        Vec::new()
    };

    Some(vec![text_response(columns, rows)])
}

/// Builds a row by mapping each requested output column name to a value (NULL if unknown).
fn map_row(columns: &[String], value: fn(&str) -> Option<String>) -> Vec<Option<String>> {
    columns.iter().map(|column| value(column)).collect()
}

/// Values for a row describing the `person` table in a table-listing query.
fn table_value(column: &str) -> Option<String> {
    match column.to_lowercase().as_str() {
        "table_name" | "relname" | "tablename" => Some(TABLE.to_string()),
        "table_schema" | "schemaname" | "nspname" | "schema_name" => Some(SCHEMA.to_string()),
        "table_type" => Some("BASE TABLE".to_string()),
        "oid" => Some("16384".to_string()),
        _ => None,
    }
}

/// Values for a row describing our schema in a namespace-listing query.
fn schema_value(column: &str) -> Option<String> {
    match column.to_lowercase().as_str() {
        "nspname" | "schema_name" | "name" => Some(SCHEMA.to_string()),
        "oid" => Some("2200".to_string()),
        _ => None,
    }
}

/// One row per `person` column for a columns-listing query (`information_schema.columns`,
/// `pg_attribute`, ...).
fn person_column_rows(columns: &[String]) -> Vec<Vec<Option<String>>> {
    PERSON_COLUMNS
        .iter()
        .enumerate()
        .map(|(index, (name, nullable))| {
            columns
                .iter()
                .map(|column| match column.to_lowercase().as_str() {
                    "column_name" | "attname" => Some((*name).to_string()),
                    "data_type" | "udt_name" | "format_type" | "typname" => {
                        Some("text".to_string())
                    }
                    "is_nullable" => Some((*nullable).to_string()),
                    "table_name" | "relname" => Some(TABLE.to_string()),
                    "table_schema" | "nspname" | "schemaname" => Some(SCHEMA.to_string()),
                    "ordinal_position" | "attnum" => Some((index + 1).to_string()),
                    _ => None,
                })
                .collect()
        })
        .collect()
}

/// The output column names of a SELECT, derived from its projection (alias, identifier, or
/// function name). Returns an empty vec if the query doesn't parse as a simple SELECT.
fn projection_columns(query: &str) -> Vec<String> {
    let statements = match SqlParser::parse_sql(&PostgreSqlDialect {}, query) {
        Ok(statements) => statements,
        Err(_) => return Vec::new(),
    };

    let select = match statements.into_iter().next() {
        Some(ast::Statement::Query(query)) => match *query.body {
            ast::SetExpr::Select(select) => select,
            _ => return Vec::new(),
        },
        _ => return Vec::new(),
    };

    select
        .projection
        .iter()
        .enumerate()
        .map(|(index, item)| select_item_name(item, index))
        .collect()
}

fn select_item_name(item: &ast::SelectItem, index: usize) -> String {
    match item {
        ast::SelectItem::ExprWithAlias { alias, .. } => alias.value.clone(),
        ast::SelectItem::UnnamedExpr(expr) => {
            expr_name(expr).unwrap_or_else(|| format!("column{}", index + 1))
        }
        _ => format!("column{}", index + 1),
    }
}

fn expr_name(expr: &ast::Expr) -> Option<String> {
    match expr {
        ast::Expr::Identifier(ident) => Some(ident.value.clone()),
        ast::Expr::CompoundIdentifier(parts) => parts.last().map(|ident| ident.value.clone()),
        ast::Expr::Function(function) => function
            .name
            .0
            .last()
            .and_then(|part| part.as_ident())
            .map(|ident| ident.value.clone()),
        _ => None,
    }
}

/// Builds a query response of all-text columns from canned rows. Each row must have one cell
/// per column; `None` cells are sent as SQL NULL.
fn text_response(columns: Vec<String>, rows: Vec<Vec<Option<String>>>) -> Response {
    let fields: Vec<FieldInfo> = columns
        .iter()
        .map(|name| FieldInfo::new(name.clone(), None, None, Type::TEXT, FieldFormat::Text))
        .collect();
    let schema = Arc::new(fields);

    let mut encoder = DataRowEncoder::new(schema.clone());
    let row_stream = stream::iter(rows).map(move |row| {
        for cell in &row {
            encoder.encode_field(cell)?;
        }
        Ok(encoder.take_row())
    });

    Response::Query(QueryResponse::new(schema, row_stream))
}
