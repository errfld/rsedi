//! Error types for CSV adapter with context

use crate::config::NullRepresentation;
use thiserror::Error;

/// Whether a row length mismatch is due to missing or extra columns.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowLengthMismatchKind {
    Missing,
    Extra,
}

impl std::fmt::Display for RowLengthMismatchKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Missing => write!(f, "missing values"),
            Self::Extra => write!(f, "extra values"),
        }
    }
}

/// Errors that can occur when working with CSV
#[derive(Error, Debug, Clone)]
pub enum CsvError {
    /// CSV read error with context
    #[error("CSV read error at line {line}: {message}")]
    Read { line: usize, message: String },

    /// CSV write error
    #[error("CSV write error: {0}")]
    Write(String),

    /// Schema validation error
    #[error("Schema error: {0}")]
    Schema(String),

    /// Type conversion error with context
    #[error("Conversion error at line {line}, column '{column}': {message}")]
    Conversion {
        line: usize,
        column: String,
        message: String,
    },

    /// I/O error
    #[error("IO error: {0}")]
    Io(String),

    /// Validation error with row context
    #[error("Validation error at line {line}: {message}")]
    Validation { line: usize, message: String },

    /// Row-length mismatch against expected header width
    #[error(
        "Row length mismatch at line {line}: expected {expected} columns, got {actual} ({kind})"
    )]
    RowLengthMismatch {
        line: usize,
        expected: usize,
        actual: usize,
        kind: RowLengthMismatchKind,
    },

    /// Configuration error
    #[error("Configuration error: {0}")]
    Config(String),
}

impl CsvError {
    /// Create a read error at a specific line
    pub fn read_at(line: usize, message: impl Into<String>) -> Self {
        Self::Read {
            line,
            message: message.into(),
        }
    }

    /// Create a read error without line number
    pub fn read(message: impl Into<String>) -> Self {
        Self::Read {
            line: 0,
            message: message.into(),
        }
    }

    /// Create a conversion error
    pub fn conversion(line: usize, column: impl Into<String>, message: impl Into<String>) -> Self {
        Self::Conversion {
            line,
            column: column.into(),
            message: message.into(),
        }
    }

    /// Create a validation error
    pub fn validation(line: usize, message: impl Into<String>) -> Self {
        Self::Validation {
            line,
            message: message.into(),
        }
    }

    /// Create a row-length mismatch error.
    pub fn row_length_mismatch(
        line: usize,
        expected: usize,
        actual: usize,
        kind: RowLengthMismatchKind,
    ) -> Self {
        Self::RowLengthMismatch {
            line,
            expected,
            actual,
            kind,
        }
    }

    /// Create a schema error
    pub fn schema(message: impl Into<String>) -> Self {
        Self::Schema(message.into())
    }

    /// Create a configuration error
    pub fn config(message: impl Into<String>) -> Self {
        Self::Config(message.into())
    }

    /// Create a write error
    pub fn write(message: impl Into<String>) -> Self {
        Self::Write(message.into())
    }

    /// Get the line number if available
    pub fn line_number(&self) -> Option<usize> {
        match self {
            Self::Read { line, .. } if *line > 0 => Some(*line),
            Self::Conversion { line, .. } => Some(*line),
            Self::Validation { line, .. } => Some(*line),
            Self::RowLengthMismatch { line, .. } => Some(*line),
            _ => None,
        }
    }
}

impl From<std::io::Error> for CsvError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e.to_string())
    }
}

/// Result type alias for CSV operations
pub type CsvResult<T> = std::result::Result<T, CsvError>;

/// Context for a CSV parsing operation
#[derive(Debug, Clone)]
pub struct ParseContext {
    /// Current line number (1-indexed)
    pub line: usize,
    /// Current column name or index
    pub column: String,
    /// Column index (0-indexed)
    pub column_index: usize,
}

impl ParseContext {
    /// Create a new parse context at the start of a file
    pub fn new() -> Self {
        Self {
            line: 1,
            column: String::new(),
            column_index: 0,
        }
    }

    /// Create context at a specific line
    pub fn at_line(line: usize) -> Self {
        Self {
            line,
            column: String::new(),
            column_index: 0,
        }
    }

    /// Update line number
    pub fn with_line(mut self, line: usize) -> Self {
        self.line = line;
        self
    }

    /// Update column
    pub fn set_column(&mut self, column: impl Into<String>, index: usize) {
        self.column = column.into();
        self.column_index = index;
    }

    /// Advance to next line
    pub fn next_line(&mut self) {
        self.line += 1;
        self.column_index = 0;
        self.column.clear();
    }

    /// Advance to next column
    pub fn next_column(&mut self) {
        self.column_index += 1;
    }
}

impl Default for ParseContext {
    fn default() -> Self {
        Self::new()
    }
}

/// Helper to convert null representation to string
pub fn null_to_string(null_rep: &NullRepresentation) -> String {
    match null_rep {
        NullRepresentation::EmptyString => String::new(),
        NullRepresentation::NullString => "NULL".to_string(),
        NullRepresentation::BackslashN => "\\N".to_string(),
        NullRepresentation::Custom(s) => s.clone(),
    }
}

/// Schema validation errors
#[derive(Debug, Clone, PartialEq)]
pub enum SchemaError {
    MissingRequiredField(String),
    TypeMismatch {
        field: String,
        expected: String,
        found: String,
    },
    InvalidLength {
        field: String,
        min: usize,
        max: usize,
        actual: usize,
    },
    UnknownField(String),
}

impl std::fmt::Display for SchemaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SchemaError::MissingRequiredField(field) => {
                write!(f, "Missing required field: {}", field)
            }
            SchemaError::TypeMismatch {
                field,
                expected,
                found,
            } => {
                write!(
                    f,
                    "Type mismatch for field {}: expected {}, found {}",
                    field, expected, found
                )
            }
            SchemaError::InvalidLength {
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
            SchemaError::UnknownField(field) => {
                write!(f, "Unknown field: {}", field)
            }
        }
    }
}

impl std::error::Error for SchemaError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_csv_error_read() {
        let err = CsvError::read_at(5, "invalid field");
        assert!(err.to_string().contains("line 5"));
        assert!(err.to_string().contains("invalid field"));
        assert_eq!(err.line_number(), Some(5));
    }

    #[test]
    fn test_csv_error_conversion() {
        let err = CsvError::conversion(10, "price", "invalid number");
        assert!(err.to_string().contains("line 10"));
        assert!(err.to_string().contains("column 'price'"));
        assert!(err.to_string().contains("invalid number"));
        assert_eq!(err.line_number(), Some(10));
    }

    #[test]
    fn test_csv_error_no_line() {
        let err = CsvError::write("disk full");
        assert_eq!(err.line_number(), None);
    }

    #[test]
    fn test_csv_error_row_length_mismatch() {
        let err = CsvError::row_length_mismatch(4, 3, 2, RowLengthMismatchKind::Missing);
        assert_eq!(err.line_number(), Some(4));
        assert!(
            err.to_string()
                .contains("expected 3 columns, got 2 (missing values)")
        );
    }

    #[test]
    fn test_parse_context() {
        let mut ctx = ParseContext::new();
        assert_eq!(ctx.line, 1);

        ctx.next_line();
        assert_eq!(ctx.line, 2);

        ctx.set_column("price", 3);
        assert_eq!(ctx.column, "price");
        assert_eq!(ctx.column_index, 3);
    }

    #[test]
    fn test_null_to_string() {
        assert_eq!(null_to_string(&NullRepresentation::EmptyString), "");
        assert_eq!(null_to_string(&NullRepresentation::NullString), "NULL");
        assert_eq!(null_to_string(&NullRepresentation::BackslashN), "\\N");
        assert_eq!(
            null_to_string(&NullRepresentation::Custom("N/A".to_string())),
            "N/A"
        );
    }

    #[test]
    fn test_schema_error_display() {
        let err = SchemaError::MissingRequiredField("id".to_string());
        assert_eq!(err.to_string(), "Missing required field: id");

        let err = SchemaError::TypeMismatch {
            field: "price".to_string(),
            expected: "decimal".to_string(),
            found: "string".to_string(),
        };
        assert!(err.to_string().contains("Type mismatch"));
        assert!(err.to_string().contains("price"));
    }
}
