//! Metadata for tracking source positions and validation state

use serde::{Deserialize, Serialize};

/// Source position information for error reporting
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Position {
    /// Line number (1-indexed)
    pub line: usize,

    /// Column number (1-indexed)
    pub column: usize,

    /// Byte offset from start of file
    pub offset: usize,

    /// Length in bytes
    pub length: usize,
}

/// Information about the source of the data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceInfo {
    /// Source file path or identifier
    pub source: String,

    /// Position within the source
    pub position: Position,

    /// Additional context (e.g., segment name, message type)
    pub context: Option<String>,
}

/// Validation state attached to nodes
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ValidationState {
    /// Whether this node has passed validation
    pub is_valid: bool,

    /// List of warnings
    pub warnings: Vec<ValidationMessage>,

    /// List of errors
    pub errors: Vec<ValidationMessage>,
}

/// A validation message (warning or error)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationMessage {
    /// Error/warning code
    pub code: String,

    /// Human-readable message
    pub message: String,

    /// Severity level
    pub severity: Severity,

    /// Path to the affected node
    pub path: String,

    /// Expected value (if applicable)
    pub expected: Option<String>,

    /// Actual value (if applicable)
    pub actual: Option<String>,
}

/// Severity level for validation messages
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Severity {
    Info,
    Warning,
    Error,
    Fatal,
}

impl Position {
    /// Create a new position
    pub fn new(line: usize, column: usize, offset: usize, length: usize) -> Self {
        Self {
            line,
            column,
            offset,
            length,
        }
    }
}

impl SourceInfo {
    /// Create new source info
    pub fn new(source: impl Into<String>, position: Position) -> Self {
        Self {
            source: source.into(),
            position,
            context: None,
        }
    }

    /// Add context
    pub fn with_context(mut self, context: impl Into<String>) -> Self {
        self.context = Some(context.into());
        self
    }
}

impl ValidationMessage {
    /// Create a new validation message
    pub fn new(
        code: impl Into<String>,
        message: impl Into<String>,
        severity: Severity,
        path: impl Into<String>,
    ) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
            severity,
            path: path.into(),
            expected: None,
            actual: None,
        }
    }

    /// Add expected/actual values
    pub fn with_values(mut self, expected: impl Into<String>, actual: impl Into<String>) -> Self {
        self.expected = Some(expected.into());
        self.actual = Some(actual.into());
        self
    }
}
