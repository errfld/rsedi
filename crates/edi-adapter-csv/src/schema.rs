//! CSV schema definitions

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// CSV schema defining structure and types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CsvSchema {
    /// Schema name/identifier
    pub name: String,
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
    /// Null value representation
    pub null_value: Option<String>,
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
    /// Maximum length (for string types)
    pub max_length: Option<usize>,
    /// Minimum length (for string types)
    pub min_length: Option<usize>,
    /// Decimal precision (for decimal types)
    pub precision: Option<u8>,
    /// Decimal scale (for decimal types)
    pub scale: Option<u8>,
    /// Date/Time format string
    pub format: Option<String>,
}

/// Supported column types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ColumnType {
    /// String type
    String,
    /// Integer type (64-bit)
    Integer,
    /// Decimal/float type
    Decimal,
    /// Boolean type
    Boolean,
    /// Date type (YYYY-MM-DD)
    Date,
    /// DateTime type (ISO 8601)
    DateTime,
    /// Time type (HH:MM:SS)
    Time,
}

impl std::fmt::Display for ColumnType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ColumnType::String => write!(f, "string"),
            ColumnType::Integer => write!(f, "integer"),
            ColumnType::Decimal => write!(f, "decimal"),
            ColumnType::Boolean => write!(f, "boolean"),
            ColumnType::Date => write!(f, "date"),
            ColumnType::DateTime => write!(f, "datetime"),
            ColumnType::Time => write!(f, "time"),
        }
    }
}

impl Default for CsvSchema {
    fn default() -> Self {
        Self {
            name: String::new(),
            columns: Vec::new(),
            has_header: true,
            delimiter: ',',
            quote_char: '"',
            field_mappings: HashMap::new(),
            null_value: None,
        }
    }
}

impl CsvSchema {
    /// Create a new empty schema
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a schema with a name
    pub fn with_name(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            ..Default::default()
        }
    }

    /// Create schema with header
    pub fn with_header(mut self) -> Self {
        self.has_header = true;
        self
    }

    /// Create schema without header
    pub fn without_header(mut self) -> Self {
        self.has_header = false;
        self
    }

    /// Set delimiter character
    pub fn with_delimiter(mut self, delimiter: char) -> Self {
        self.delimiter = delimiter;
        self
    }

    /// Set quote character
    pub fn with_quote_char(mut self, quote_char: char) -> Self {
        self.quote_char = quote_char;
        self
    }

    /// Set null value representation
    pub fn with_null_value(mut self, null_value: impl Into<String>) -> Self {
        self.null_value = Some(null_value.into());
        self
    }

    /// Add a column definition
    pub fn add_column(mut self, column: ColumnDef) -> Self {
        let index = self.columns.len();
        self.columns.push(column.clone());
        if let Some(field) = &column.field_name {
            self.field_mappings.insert(field.clone(), index);
        }
        // Also map by column name
        self.field_mappings.insert(column.name.clone(), index);
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

    /// Get the number of columns
    pub fn column_count(&self) -> usize {
        self.columns.len()
    }

    /// Check if the schema has any columns
    pub fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }

    /// Validate row data against schema
    pub fn validate_row(&self, row: &[String]) -> Result<(), SchemaValidationError> {
        for (idx, col) in self.columns.iter().enumerate() {
            let value = row.get(idx).map(|s| s.as_str()).unwrap_or("");

            if col.required && value.is_empty() {
                return Err(SchemaValidationError::MissingRequiredField(
                    col.name.clone(),
                ));
            }

            // Check min/max length for strings
            if !value.is_empty() {
                if let Some(max) = col.max_length {
                    if value.len() > max {
                        return Err(SchemaValidationError::InvalidLength {
                            field: col.name.clone(),
                            min: col.min_length.unwrap_or(0),
                            max,
                            actual: value.len(),
                        });
                    }
                }
            }
        }
        Ok(())
    }

    /// Get the type for a column at the given index
    pub fn get_column_type(&self, index: usize) -> Option<ColumnType> {
        self.columns.get(index).map(|c| c.column_type)
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
            max_length: None,
            min_length: None,
            precision: None,
            scale: None,
            format: None,
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

    /// Set length constraints
    pub fn with_length(mut self, min: usize, max: usize) -> Self {
        self.min_length = Some(min);
        self.max_length = Some(max);
        self
    }

    /// Set maximum length
    pub fn with_max_length(mut self, max: usize) -> Self {
        self.max_length = Some(max);
        self
    }

    /// Set decimal precision and scale
    pub fn with_precision(mut self, precision: u8, scale: u8) -> Self {
        self.precision = Some(precision);
        self.scale = Some(scale);
        self
    }

    /// Set format string (for dates/times)
    pub fn with_format(mut self, format: impl Into<String>) -> Self {
        self.format = Some(format.into());
        self
    }
}

/// Schema validation errors (local to this module)
#[derive(Debug, Clone, PartialEq)]
pub enum SchemaValidationError {
    MissingRequiredField(String),
    InvalidLength {
        field: String,
        min: usize,
        max: usize,
        actual: usize,
    },
}

impl std::fmt::Display for SchemaValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SchemaValidationError::MissingRequiredField(field) => {
                write!(f, "Missing required field: {}", field)
            }
            SchemaValidationError::InvalidLength {
                field,
                min,
                max,
                actual,
            } => {
                write!(
                    f,
                    "Invalid length for field {}: expected {}-{}, got {}",
                    field, min, max, actual
                )
            }
        }
    }
}

impl std::error::Error for SchemaValidationError {}

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
        assert_eq!(schema.get_column_index("csv_name"), Some(0)); // Also by column name
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
        assert!(matches!(
            schema.validate_row(&invalid_row),
            Err(SchemaValidationError::MissingRequiredField(_))
        ));
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

    #[test]
    fn test_schema_with_name() {
        let schema = CsvSchema::with_name("orders_csv");
        assert_eq!(schema.name, "orders_csv");
    }

    #[test]
    fn test_column_type_display() {
        assert_eq!(ColumnType::String.to_string(), "string");
        assert_eq!(ColumnType::Integer.to_string(), "integer");
        assert_eq!(ColumnType::Decimal.to_string(), "decimal");
        assert_eq!(ColumnType::Boolean.to_string(), "boolean");
        assert_eq!(ColumnType::Date.to_string(), "date");
        assert_eq!(ColumnType::DateTime.to_string(), "datetime");
        assert_eq!(ColumnType::Time.to_string(), "time");
    }

    #[test]
    fn test_column_constraints() {
        let col = ColumnDef::new("code")
            .with_length(1, 10)
            .with_precision(10, 2)
            .with_format("YYYY-MM-DD");

        assert_eq!(col.min_length, Some(1));
        assert_eq!(col.max_length, Some(10));
        assert_eq!(col.precision, Some(10));
        assert_eq!(col.scale, Some(2));
        assert_eq!(col.format, Some("YYYY-MM-DD".to_string()));
    }

    #[test]
    fn test_length_validation() {
        let schema = CsvSchema::new().add_column(ColumnDef::new("code").with_max_length(5));

        let valid_row = vec!["ABC".to_string()];
        assert!(schema.validate_row(&valid_row).is_ok());

        let invalid_row = vec!["ABCDEF".to_string()];
        assert!(matches!(
            schema.validate_row(&invalid_row),
            Err(SchemaValidationError::InvalidLength { .. })
        ));
    }

    #[test]
    fn test_null_value() {
        let schema = CsvSchema::new().with_null_value("NULL");
        assert_eq!(schema.null_value, Some("NULL".to_string()));
    }
}
