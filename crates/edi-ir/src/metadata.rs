//! Metadata for tracking source positions and validation state
#![allow(clippy::must_use_candidate)] // Constructor helpers are clear at call sites without #[must_use].
#![allow(clippy::return_self_not_must_use)] // Fluent setters are designed for chaining.

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
    #[must_use]
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
    #[must_use]
    pub fn new(source: impl Into<String>, position: Position) -> Self {
        Self {
            source: source.into(),
            position,
            context: None,
        }
    }

    /// Add context
    #[must_use]
    pub fn with_context(mut self, context: impl Into<String>) -> Self {
        self.context = Some(context.into());
        self
    }
}

impl ValidationMessage {
    /// Create a new validation message
    #[must_use]
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
    #[must_use]
    pub fn with_values(mut self, expected: impl Into<String>, actual: impl Into<String>) -> Self {
        self.expected = Some(expected.into());
        self.actual = Some(actual.into());
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_position_creation() {
        let pos = Position::new(10, 20, 100, 50);
        assert_eq!(pos.line, 10);
        assert_eq!(pos.column, 20);
        assert_eq!(pos.offset, 100);
        assert_eq!(pos.length, 50);
    }

    #[test]
    fn test_position_default() {
        let pos = Position::default();
        assert_eq!(pos.line, 0);
        assert_eq!(pos.column, 0);
        assert_eq!(pos.offset, 0);
        assert_eq!(pos.length, 0);
    }

    #[test]
    fn test_source_info_creation() {
        let pos = Position::new(5, 10, 50, 25);
        let source = SourceInfo::new("test.edi", pos);

        assert_eq!(source.source, "test.edi");
        assert_eq!(source.position.line, 5);
        assert_eq!(source.position.column, 10);
        assert_eq!(source.position.offset, 50);
        assert_eq!(source.position.length, 25);
        assert!(source.context.is_none());
    }

    #[test]
    fn test_source_info_with_context() {
        let pos = Position::new(1, 1, 0, 10);
        let source = SourceInfo::new("data.csv", pos).with_context("Header row");

        assert_eq!(source.source, "data.csv");
        assert_eq!(source.context, Some("Header row".to_string()));
    }

    #[test]
    fn test_validation_message_creation() {
        let msg =
            ValidationMessage::new("E001", "Invalid segment", Severity::Error, "ROOT/SEGMENT");

        assert_eq!(msg.code, "E001");
        assert_eq!(msg.message, "Invalid segment");
        assert_eq!(msg.severity, Severity::Error);
        assert_eq!(msg.path, "ROOT/SEGMENT");
        assert!(msg.expected.is_none());
        assert!(msg.actual.is_none());
    }

    #[test]
    fn test_validation_message_with_values() {
        let msg = ValidationMessage::new("W002", "Type mismatch", Severity::Warning, "ROOT/FIELD")
            .with_values("string", "integer");

        assert_eq!(msg.code, "W002");
        assert_eq!(msg.message, "Type mismatch");
        assert_eq!(msg.severity, Severity::Warning);
        assert_eq!(msg.path, "ROOT/FIELD");
        assert_eq!(msg.expected, Some("string".to_string()));
        assert_eq!(msg.actual, Some("integer".to_string()));
    }

    #[test]
    fn test_severity_variants() {
        let info = ValidationMessage::new("I001", "Info", Severity::Info, "");
        let warning = ValidationMessage::new("W001", "Warning", Severity::Warning, "");
        let error = ValidationMessage::new("E001", "Error", Severity::Error, "");
        let fatal = ValidationMessage::new("F001", "Fatal", Severity::Fatal, "");

        assert_eq!(info.severity, Severity::Info);
        assert_eq!(warning.severity, Severity::Warning);
        assert_eq!(error.severity, Severity::Error);
        assert_eq!(fatal.severity, Severity::Fatal);
    }

    #[test]
    fn test_validation_state_default() {
        let state = ValidationState::default();
        assert!(!state.is_valid);
        assert!(state.warnings.is_empty());
        assert!(state.errors.is_empty());
    }

    #[test]
    fn test_validation_state_with_messages() {
        let mut state = ValidationState {
            is_valid: true,
            ..Default::default()
        };

        let warning = ValidationMessage::new(
            "W001",
            "Optional field missing",
            Severity::Warning,
            "ROOT/FIELD",
        );

        let error = ValidationMessage::new(
            "E001",
            "Required field missing",
            Severity::Error,
            "ROOT/REQUIRED",
        );

        state.warnings.push(warning);
        state.errors.push(error);

        assert!(state.is_valid);
        assert_eq!(state.warnings.len(), 1);
        assert_eq!(state.errors.len(), 1);
        assert_eq!(state.warnings[0].severity, Severity::Warning);
        assert_eq!(state.errors[0].severity, Severity::Error);
    }

    #[test]
    fn test_source_info_string_types() {
        // Test with &str
        let pos = Position::new(1, 1, 0, 0);
        let source1 = SourceInfo::new("path/to/file", pos.clone());
        assert_eq!(source1.source, "path/to/file");

        // Test with String
        let path = String::from("another/path");
        let source2 = SourceInfo::new(path, pos);
        assert_eq!(source2.source, "another/path");
    }

    #[test]
    fn test_validation_message_string_types() {
        // Test with &str
        let msg1 = ValidationMessage::new("CODE", "Message", Severity::Info, "path");
        assert_eq!(msg1.code, "CODE");

        // Test with String
        let code = String::from("ERR001");
        let message = String::from("Error occurred");
        let path = String::from("/root/child");
        let msg2 = ValidationMessage::new(code, message, Severity::Error, path);
        assert_eq!(msg2.code, "ERR001");
        assert_eq!(msg2.message, "Error occurred");
        assert_eq!(msg2.path, "/root/child");
    }
}
