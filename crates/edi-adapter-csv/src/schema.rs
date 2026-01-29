//! CSV schema definitions

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// CSV schema defining structure and types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CsvSchema {
    /// Column definitions in order
    pub columns: Vec<ColumnDef>,
    /// Whether the CSV has a header row
    pub has_header: bool,
    /// Field delimiter (default: comma)
    pub delimiter: char,
    /// Quote character (default: double quote)
    pub quote_char: char,
    /// Mapping from IR field names to column indices
    pub field_mappings: HashMap<String, usize>,
}

/// Definition of a CSV column
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnDef {
    /// Column name (used in header if has_header is true)
    pub name: String,
    /// Column data type
    pub column_type: ColumnType,
    /// Whether this column is required
    pub required: bool,
    /// Default value if empty
    pub default: Option<String>,
    /// IR field name mapping
    pub field_name: Option<String>,
}

/// Supported column types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ColumnType {
    /// String type
    String,
    /// Integer type
    Integer,
    /// Decimal/float type
    Decimal,
    /// Boolean type
    Boolean,
    /// Date type
    Date,
    /// DateTime type
    DateTime,
}

impl Default for CsvSchema {
    fn default() -> Self {
        Self {
            columns: Vec::new(),
            has_header: false,
            delimiter: ',',
            quote_char: '"',
            field_mappings: HashMap::new(),
        }
    }
}

impl CsvSchema {
    /// Create a new empty schema
    pub fn new() -> Self {
        Self::default()
    }

    /// Create schema with header
    pub fn with_header(mut self) -> Self {
        self.has_header = true;
        self
    }

    /// Set delimiter character
    pub fn with_delimiter(mut self, delimiter: char) -> Self {
        self.delimiter = delimiter;
        self
    }

    /// Add a column definition
    pub fn add_column(mut self, column: ColumnDef) -> Self {
        let index = self.columns.len();
        self.columns.push(column.clone());
        if let Some(field) = &column.field_name {
            self.field_mappings.insert(field.clone(), index);
        }
        self
    }

    /// Get column index by field name
    pub fn get_column_index(&self, field_name: &str) -> Option<usize> {
        self.field_mappings.get(field_name).copied()
    }

    /// Get column definition by name
    pub fn get_column(&self, name: &str) -> Option<&ColumnDef> {
        self.columns.iter().find(|c| c.name == name)
    }

    /// Get column definition by index
    pub fn get_column_by_index(&self, index: usize) -> Option<&ColumnDef> {
        self.columns.get(index)
    }

    /// Get header row values
    pub fn get_headers(&self) -> Vec<&str> {
        self.columns.iter().map(|c| c.name.as_str()).collect()
    }

    /// Validate row data against schema
    pub fn validate_row(&self, row: &[String]) -> Result<(), SchemaError> {
        for (idx, col) in self.columns.iter().enumerate() {
            let value = row.get(idx).map(|s| s.as_str()).unwrap_or("");

            if col.required && value.is_empty() {
                return Err(SchemaError::MissingRequiredField(col.name.clone()));
            }
        }
        Ok(())
    }
}

impl ColumnDef {
    /// Create a new column definition
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            column_type: ColumnType::String,
            required: false,
            default: None,
            field_name: None,
        }
    }

    /// Set column type
    pub fn with_type(mut self, column_type: ColumnType) -> Self {
        self.column_type = column_type;
        self
    }

    /// Mark column as required
    pub fn required(mut self) -> Self {
        self.required = true;
        self
    }

    /// Set default value
    pub fn with_default(mut self, default: impl Into<String>) -> Self {
        self.default = Some(default.into());
        self
    }

    /// Set IR field name mapping
    pub fn mapped_to(mut self, field_name: impl Into<String>) -> Self {
        let name = field_name.into();
        self.field_name = Some(name.clone());
        self
    }
}

/// Schema validation errors
#[derive(Debug, Clone, PartialEq)]
pub enum SchemaError {
    MissingRequiredField(String),
    TypeMismatch(String, ColumnType),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_csv_schema_creation() {
        let schema = CsvSchema::new()
            .with_header()
            .with_delimiter(';')
            .add_column(ColumnDef::new("name").required())
            .add_column(ColumnDef::new("age").with_type(ColumnType::Integer));

        assert!(schema.has_header);
        assert_eq!(schema.delimiter, ';');
        assert_eq!(schema.columns.len(), 2);
        assert_eq!(schema.columns[0].name, "name");
        assert!(schema.columns[0].required);
        assert_eq!(schema.columns[1].column_type, ColumnType::Integer);
    }

    #[test]
    fn test_column_mapping() {
        let schema = CsvSchema::new()
            .add_column(ColumnDef::new("csv_name").mapped_to("ir_field_name"))
            .add_column(ColumnDef::new("csv_age").mapped_to("ir_age"));

        assert_eq!(schema.get_column_index("ir_field_name"), Some(0));
        assert_eq!(schema.get_column_index("ir_age"), Some(1));
        assert_eq!(schema.get_column_index("nonexistent"), None);
    }

    #[test]
    fn test_type_definitions() {
        let column = ColumnDef::new("price")
            .with_type(ColumnType::Decimal)
            .required();

        assert_eq!(column.column_type, ColumnType::Decimal);
        assert!(column.required);

        let date_col = ColumnDef::new("created").with_type(ColumnType::Date);
        assert_eq!(date_col.column_type, ColumnType::Date);
    }

    #[test]
    fn test_required_columns() {
        let schema = CsvSchema::new()
            .add_column(ColumnDef::new("id").required())
            .add_column(ColumnDef::new("optional_field"));

        // Valid row with all required fields
        let valid_row = vec!["123".to_string(), "data".to_string()];
        assert!(schema.validate_row(&valid_row).is_ok());

        // Invalid row - missing required field
        let invalid_row = vec!["".to_string(), "data".to_string()];
        assert_eq!(
            schema.validate_row(&invalid_row),
            Err(SchemaError::MissingRequiredField("id".to_string()))
        );
    }

    #[test]
    fn test_default_values() {
        let column = ColumnDef::new("status").with_default("active");

        assert_eq!(column.default, Some("active".to_string()));

        let column_no_default = ColumnDef::new("name");
        assert_eq!(column_no_default.default, None);
    }

    #[test]
    fn test_get_headers() {
        let schema = CsvSchema::new()
            .add_column(ColumnDef::new("name"))
            .add_column(ColumnDef::new("age"))
            .add_column(ColumnDef::new("city"));

        let headers = schema.get_headers();
        assert_eq!(headers, vec!["name", "age", "city"]);
    }

    #[test]
    fn test_get_column() {
        let schema = CsvSchema::new()
            .add_column(ColumnDef::new("name").required())
            .add_column(ColumnDef::new("age").with_type(ColumnType::Integer));

        let name_col = schema.get_column("name").unwrap();
        assert!(name_col.required);

        let age_col = schema.get_column_by_index(1).unwrap();
        assert_eq!(age_col.column_type, ColumnType::Integer);

        assert!(schema.get_column("nonexistent").is_none());
    }
}
