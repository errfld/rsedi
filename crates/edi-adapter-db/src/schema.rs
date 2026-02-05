//! Database schema mapping primitives.

use std::collections::{BTreeMap, HashMap};

use serde::{Deserialize, Serialize};

use crate::{Error, Result};

/// Database value used by reader/writer operations.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum DbValue {
    String(String),
    Integer(i64),
    Decimal(f64),
    Boolean(bool),
    Null,
}

/// Canonical row representation.
pub type Row = BTreeMap<String, DbValue>;

/// Supported column types for schema validation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ColumnType {
    String,
    Integer,
    Decimal,
    Boolean,
}

/// Foreign key description.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForeignKey {
    pub table: String,
    pub column: String,
}

/// Column definition in a table schema.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnDef {
    pub name: String,
    pub column_type: ColumnType,
    pub nullable: bool,
    pub primary_key: bool,
    pub foreign_key: Option<ForeignKey>,
}

impl ColumnDef {
    pub fn new(name: impl Into<String>, column_type: ColumnType) -> Self {
        Self {
            name: name.into(),
            column_type,
            nullable: false,
            primary_key: false,
            foreign_key: None,
        }
    }

    pub fn nullable(mut self, nullable: bool) -> Self {
        self.nullable = nullable;
        self
    }

    pub fn primary_key(mut self) -> Self {
        self.primary_key = true;
        self
    }

    pub fn foreign_key(mut self, table: impl Into<String>, column: impl Into<String>) -> Self {
        self.foreign_key = Some(ForeignKey {
            table: table.into(),
            column: column.into(),
        });
        self
    }
}

/// Table schema used for DB validation/mapping.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TableSchema {
    pub name: String,
    pub columns: Vec<ColumnDef>,
}

impl TableSchema {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            columns: Vec::new(),
        }
    }

    pub fn with_column(mut self, column: ColumnDef) -> Self {
        self.columns.push(column);
        self
    }

    pub fn column(&self, name: &str) -> Option<&ColumnDef> {
        self.columns.iter().find(|column| column.name == name)
    }

    pub fn primary_key(&self) -> Option<&ColumnDef> {
        self.columns.iter().find(|column| column.primary_key)
    }

    pub fn create_table_sql(&self) -> String {
        let columns: Vec<String> = self
            .columns
            .iter()
            .map(|column| column_definition_sql(column))
            .collect();

        format!(
            "CREATE TABLE IF NOT EXISTS {} ({})",
            quote_identifier(&self.name),
            columns.join(", ")
        )
    }

    pub fn validate_row(&self, row: &Row) -> Result<()> {
        for (column_name, value) in row {
            let column = self.column(column_name).ok_or_else(|| Error::Schema {
                details: format!("Unknown column '{column_name}' for table '{}'", self.name),
            })?;

            if matches!(value, DbValue::Null) {
                if !column.nullable {
                    return Err(Error::Schema {
                        details: format!(
                            "Column '{column_name}' in table '{}' cannot be null",
                            self.name
                        ),
                    });
                }
                continue;
            }

            if !value_matches_type(value, column.column_type) {
                return Err(Error::Schema {
                    details: format!(
                        "Type mismatch for '{}.{}': expected {:?}, found {:?}",
                        self.name, column_name, column.column_type, value
                    ),
                });
            }
        }

        for column in &self.columns {
            if !column.nullable && !row.contains_key(&column.name) {
                return Err(Error::Schema {
                    details: format!("Missing required column '{}.{}'", self.name, column.name),
                });
            }
        }

        Ok(())
    }
}

/// Collection of table schemas.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct SchemaMapping {
    tables: HashMap<String, TableSchema>,
}

impl SchemaMapping {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_table(&mut self, schema: TableSchema) {
        self.tables.insert(schema.name.clone(), schema);
    }

    pub fn table(&self, name: &str) -> Option<&TableSchema> {
        self.tables.get(name)
    }

    pub fn table_names(&self) -> Vec<&str> {
        self.tables.keys().map(String::as_str).collect()
    }

    pub fn tables(&self) -> impl Iterator<Item = &TableSchema> {
        self.tables.values()
    }

    pub fn validate_row(&self, table_name: &str, row: &Row) -> Result<()> {
        let schema = self.table(table_name).ok_or_else(|| Error::Schema {
            details: format!("Unknown table '{table_name}'"),
        })?;
        schema.validate_row(row)
    }
}

fn column_definition_sql(column: &ColumnDef) -> String {
    let mut parts = vec![
        quote_identifier(&column.name),
        column_type_sql(column.column_type).to_string(),
    ];

    if !column.nullable {
        parts.push("NOT NULL".to_string());
    }

    if column.primary_key {
        parts.push("PRIMARY KEY".to_string());
    }

    if let Some(foreign_key) = &column.foreign_key {
        parts.push(format!(
            "REFERENCES {}({})",
            quote_identifier(&foreign_key.table),
            quote_identifier(&foreign_key.column)
        ));
    }

    parts.join(" ")
}

fn column_type_sql(column_type: ColumnType) -> &'static str {
    match column_type {
        ColumnType::String => "TEXT",
        ColumnType::Integer => "INTEGER",
        ColumnType::Decimal => "REAL",
        ColumnType::Boolean => "BOOLEAN",
    }
}

fn quote_identifier(value: &str) -> String {
    let escaped = value.replace('\"', "\"\"");
    format!("\"{}\"", escaped)
}

fn value_matches_type(value: &DbValue, column_type: ColumnType) -> bool {
    matches!(
        (value, column_type),
        (DbValue::String(_), ColumnType::String)
            | (DbValue::Integer(_), ColumnType::Integer)
            | (DbValue::Decimal(_), ColumnType::Decimal)
            | (DbValue::Boolean(_), ColumnType::Boolean)
            | (DbValue::Null, _)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_orders_schema() -> TableSchema {
        TableSchema::new("orders")
            .with_column(ColumnDef::new("id", ColumnType::Integer).primary_key())
            .with_column(ColumnDef::new("order_no", ColumnType::String))
            .with_column(ColumnDef::new("total", ColumnType::Decimal).nullable(true))
            .with_column(ColumnDef::new("is_priority", ColumnType::Boolean))
    }

    #[test]
    fn test_db_schema_creation() {
        let schema = sample_orders_schema();
        assert_eq!(schema.name, "orders");
        assert_eq!(schema.columns.len(), 4);
    }

    #[test]
    fn test_table_mapping() {
        let mut mapping = SchemaMapping::new();
        mapping.add_table(sample_orders_schema());
        assert!(mapping.table("orders").is_some());
        assert!(mapping.table("missing").is_none());
    }

    #[test]
    fn test_column_types() {
        let schema = sample_orders_schema();
        assert_eq!(
            schema.column("id").unwrap().column_type,
            ColumnType::Integer
        );
        assert_eq!(
            schema.column("order_no").unwrap().column_type,
            ColumnType::String
        );
    }

    #[test]
    fn test_primary_key_handling() {
        let schema = sample_orders_schema();
        assert_eq!(schema.primary_key().unwrap().name, "id");
    }

    #[test]
    fn test_foreign_key_handling() {
        let line_schema = TableSchema::new("order_lines")
            .with_column(ColumnDef::new("line_id", ColumnType::Integer).primary_key())
            .with_column(
                ColumnDef::new("order_id", ColumnType::Integer).foreign_key("orders", "id"),
            );

        let fk = line_schema.column("order_id").unwrap().foreign_key.clone();
        assert_eq!(
            fk,
            Some(ForeignKey {
                table: "orders".to_string(),
                column: "id".to_string(),
            })
        );
    }

    #[test]
    fn test_schema_validation() {
        let schema = sample_orders_schema();
        let mut row = Row::new();
        row.insert("id".to_string(), DbValue::Integer(1));
        row.insert("order_no".to_string(), DbValue::String("PO-1".to_string()));
        row.insert("total".to_string(), DbValue::Decimal(42.5));
        row.insert("is_priority".to_string(), DbValue::Boolean(false));

        assert!(schema.validate_row(&row).is_ok());

        row.insert("total".to_string(), DbValue::String("wrong".to_string()));
        assert!(schema.validate_row(&row).is_err());
    }

    #[test]
    fn test_required_columns() {
        let schema = sample_orders_schema();
        let mut row = Row::new();
        row.insert("id".to_string(), DbValue::Integer(1));
        row.insert("is_priority".to_string(), DbValue::Boolean(true));
        assert!(schema.validate_row(&row).is_err());
    }
}
