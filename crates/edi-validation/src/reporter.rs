//! Validation reporter

use std::collections::HashMap;
use std::fmt::Write as _;

/// Severity level for validation issues
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Severity {
    /// Error - validation failed
    Error,
    /// Warning - issue but not blocking
    Warning,
    /// Info - informational only
    Info,
}

impl std::fmt::Display for Severity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Severity::Error => write!(f, "ERROR"),
            Severity::Warning => write!(f, "WARNING"),
            Severity::Info => write!(f, "INFO"),
        }
    }
}

/// A single validation error or warning
#[derive(Debug, Clone)]
pub struct ValidationIssue {
    /// Severity level
    pub severity: Severity,
    /// Error message
    pub message: String,
    /// Path in the document where error occurred
    pub path: String,
    /// Line number (if available)
    pub line: Option<usize>,
    /// Column number (if available)
    pub column: Option<usize>,
    /// Error code
    pub code: Option<String>,
    /// Segment position
    pub segment_pos: Option<usize>,
    /// Element position within segment
    pub element_pos: Option<usize>,
    /// Component position within element
    pub component_pos: Option<usize>,
    /// Additional context
    pub context: Option<String>,
}

impl ValidationIssue {
    /// Create a new validation issue
    pub fn new(severity: Severity, message: impl Into<String>) -> Self {
        Self {
            severity,
            message: message.into(),
            path: String::new(),
            line: None,
            column: None,
            code: None,
            segment_pos: None,
            element_pos: None,
            component_pos: None,
            context: None,
        }
    }

    /// Set the path
    #[must_use]
    pub fn with_path(mut self, path: impl Into<String>) -> Self {
        self.path = path.into();
        self
    }

    /// Set position
    #[must_use]
    pub fn with_position(mut self, line: usize, column: usize) -> Self {
        self.line = Some(line);
        self.column = Some(column);
        self
    }

    /// Set error code
    #[must_use]
    pub fn with_code(mut self, code: impl Into<String>) -> Self {
        self.code = Some(code.into());
        self
    }

    /// Set segment element component positions
    #[must_use]
    pub fn with_positions(
        mut self,
        segment: usize,
        element: Option<usize>,
        component: Option<usize>,
    ) -> Self {
        self.segment_pos = Some(segment);
        self.element_pos = element;
        self.component_pos = component;
        self
    }

    /// Set context
    #[must_use]
    pub fn with_context(mut self, context: impl Into<String>) -> Self {
        self.context = Some(context.into());
        self
    }
}

/// Collection of validation issues
#[derive(Debug, Clone, Default)]
pub struct ValidationReport {
    issues: Vec<ValidationIssue>,
}

impl ValidationReport {
    /// Create a new empty report
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Add an issue
    pub fn add_issue(&mut self, issue: ValidationIssue) {
        self.issues.push(issue);
    }

    /// Create and add an error
    ///
    /// # Panics
    ///
    /// Panics if internal issue storage is unexpectedly empty after insertion.
    pub fn error(&mut self, message: impl Into<String>) -> &mut ValidationIssue {
        let issue = ValidationIssue::new(Severity::Error, message);
        self.issues.push(issue);
        self.issues.last_mut().unwrap()
    }

    /// Create and add a warning
    ///
    /// # Panics
    ///
    /// Panics if internal issue storage is unexpectedly empty after insertion.
    pub fn warning(&mut self, message: impl Into<String>) -> &mut ValidationIssue {
        let issue = ValidationIssue::new(Severity::Warning, message);
        self.issues.push(issue);
        self.issues.last_mut().unwrap()
    }

    /// Create and add an info message
    ///
    /// # Panics
    ///
    /// Panics if internal issue storage is unexpectedly empty after insertion.
    pub fn info(&mut self, message: impl Into<String>) -> &mut ValidationIssue {
        let issue = ValidationIssue::new(Severity::Info, message);
        self.issues.push(issue);
        self.issues.last_mut().unwrap()
    }

    /// Get all issues
    #[must_use]
    pub fn all_issues(&self) -> &[ValidationIssue] {
        &self.issues
    }

    /// Get errors only
    #[must_use]
    pub fn errors(&self) -> Vec<&ValidationIssue> {
        self.issues
            .iter()
            .filter(|i| i.severity == Severity::Error)
            .collect()
    }

    /// Get warnings only
    #[must_use]
    pub fn warnings(&self) -> Vec<&ValidationIssue> {
        self.issues
            .iter()
            .filter(|i| i.severity == Severity::Warning)
            .collect()
    }

    /// Get info messages only
    #[must_use]
    pub fn infos(&self) -> Vec<&ValidationIssue> {
        self.issues
            .iter()
            .filter(|i| i.severity == Severity::Info)
            .collect()
    }

    /// Get issues by severity
    #[must_use]
    pub fn by_severity(&self, severity: Severity) -> Vec<&ValidationIssue> {
        self.issues
            .iter()
            .filter(|i| i.severity == severity)
            .collect()
    }

    /// Check if report has any issues
    #[must_use]
    pub fn has_issues(&self) -> bool {
        !self.issues.is_empty()
    }

    /// Check if report has errors
    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.issues.iter().any(|i| i.severity == Severity::Error)
    }

    /// Get total count of issues
    #[must_use]
    pub fn count(&self) -> usize {
        self.issues.len()
    }

    /// Get count by severity
    #[must_use]
    pub fn count_by_severity(&self, severity: Severity) -> usize {
        self.issues
            .iter()
            .filter(|i| i.severity == severity)
            .count()
    }

    /// Clear all issues
    pub fn clear(&mut self) {
        self.issues.clear();
    }
}

/// Reports validation results
pub struct ValidationReporter {
    report: ValidationReport,
    /// Format options
    #[allow(dead_code)]
    format_options: FormatOptions,
}

impl ValidationReporter {
    /// Create a new validation reporter
    #[must_use]
    pub fn new() -> Self {
        Self {
            report: ValidationReport::new(),
            format_options: FormatOptions::default(),
        }
    }

    /// Create with format options
    #[must_use]
    pub fn with_options(format_options: FormatOptions) -> Self {
        Self {
            report: ValidationReport::new(),
            format_options,
        }
    }

    /// Report a single issue
    pub fn report_issue(&mut self, issue: ValidationIssue) {
        self.report.add_issue(issue);
    }

    /// Report an error
    pub fn report_error(&mut self, message: impl Into<String>) -> &mut ValidationIssue {
        self.report.error(message)
    }

    /// Report a warning
    pub fn report_warning(&mut self, message: impl Into<String>) -> &mut ValidationIssue {
        self.report.warning(message)
    }

    /// Get the report
    #[must_use]
    pub fn get_report(&self) -> &ValidationReport {
        &self.report
    }

    /// Take ownership of the report
    #[must_use]
    pub fn into_report(self) -> ValidationReport {
        self.report
    }

    /// Format all errors as a string
    #[must_use]
    pub fn format_errors(&self) -> String {
        let mut output = String::new();

        if self.report.issues.is_empty() {
            return "No validation issues found.".to_string();
        }

        // Group issues by severity
        let by_severity: HashMap<Severity, Vec<&ValidationIssue>> =
            self.report
                .issues
                .iter()
                .fold(HashMap::new(), |mut acc, issue| {
                    acc.entry(issue.severity).or_default().push(issue);
                    acc
                });

        // Format errors first
        if let Some(errors) = by_severity.get(&Severity::Error) {
            let _ = writeln!(output, "{} Error(s):", errors.len());
            for error in errors {
                output.push_str(&Self::format_issue(error));
                output.push('\n');
            }
            output.push('\n');
        }

        // Then warnings
        if let Some(warnings) = by_severity.get(&Severity::Warning) {
            let _ = writeln!(output, "{} Warning(s):", warnings.len());
            for warning in warnings {
                output.push_str(&Self::format_issue(warning));
                output.push('\n');
            }
            output.push('\n');
        }

        // Then info
        if let Some(infos) = by_severity.get(&Severity::Info) {
            let _ = writeln!(output, "{} Info message(s):", infos.len());
            for info in infos {
                output.push_str(&Self::format_issue(info));
                output.push('\n');
            }
        }

        output
    }

    /// Format a single issue
    fn format_issue(issue: &ValidationIssue) -> String {
        let mut parts = Vec::new();

        // Severity and code
        if let Some(ref code) = issue.code {
            parts.push(format!("[{} - {}]", issue.severity, code));
        } else {
            parts.push(format!("[{}]", issue.severity));
        }

        // Path
        if !issue.path.is_empty() {
            parts.push(format!("Path: {}", issue.path));
        }

        // Position
        let pos_parts: Vec<String> = [
            issue.line.map(|l| format!("Line {l}")),
            issue.column.map(|c| format!("Col {c}")),
            issue.segment_pos.map(|s| format!("Seg {s}")),
            issue.element_pos.map(|e| format!("Elem {e}")),
            issue.component_pos.map(|c| format!("Comp {c}")),
        ]
        .into_iter()
        .flatten()
        .collect();

        if !pos_parts.is_empty() {
            parts.push(format!("({})", pos_parts.join(", ")));
        }

        // Message
        parts.push(format!(": {}", issue.message));

        // Context
        if let Some(ref context) = issue.context {
            parts.push(format!("  Context: {context}"));
        }

        parts.join(" ")
    }
}

impl Default for ValidationReporter {
    fn default() -> Self {
        Self::new()
    }
}

/// Options for formatting error output
#[allow(clippy::exhaustive_enums)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DisplayOption {
    Show,
    Hide,
}

impl DisplayOption {
    #[must_use]
    pub const fn is_enabled(self) -> bool {
        matches!(self, Self::Show)
    }
}

/// Options for formatting error output
#[derive(Debug, Clone)]
pub struct FormatOptions {
    /// Include error codes in output
    pub show_codes: DisplayOption,
    /// Include path in output
    pub show_paths: DisplayOption,
    /// Include positions in output
    pub show_positions: DisplayOption,
    /// Include context in output
    pub show_context: DisplayOption,
}

impl Default for FormatOptions {
    fn default() -> Self {
        Self {
            show_codes: DisplayOption::Show,
            show_paths: DisplayOption::Show,
            show_positions: DisplayOption::Show,
            show_context: DisplayOption::Show,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_report_single_error() {
        let mut reporter = ValidationReporter::new();

        reporter.report_issue(
            ValidationIssue::new(Severity::Error, "Test error message")
                .with_path("/document/segment/element"),
        );

        let report = reporter.get_report();
        assert_eq!(report.count(), 1);
        assert!(report.has_errors());
        assert_eq!(report.errors().len(), 1);
    }

    #[test]
    fn test_report_multiple_errors() {
        let mut reporter = ValidationReporter::new();

        reporter.report_issue(
            ValidationIssue::new(Severity::Error, "First error")
                .with_path("/path/1")
                .with_code("E001"),
        );

        reporter.report_issue(
            ValidationIssue::new(Severity::Error, "Second error")
                .with_path("/path/2")
                .with_code("E002"),
        );

        reporter.report_issue(
            ValidationIssue::new(Severity::Warning, "A warning").with_path("/path/3"),
        );

        let report = reporter.get_report();
        assert_eq!(report.count(), 3);
        assert_eq!(report.errors().len(), 2);
        assert_eq!(report.warnings().len(), 1);
        assert!(report.has_errors());
    }

    #[test]
    fn test_error_position() {
        let mut reporter = ValidationReporter::new();

        reporter.report_issue(
            ValidationIssue::new(Severity::Error, "Positioned error")
                .with_path("/document/segment")
                .with_position(42, 15)
                .with_positions(3, Some(2), Some(1)),
        );

        let report = reporter.get_report();
        let issue = &report.all_issues()[0];

        assert_eq!(issue.line, Some(42));
        assert_eq!(issue.column, Some(15));
        assert_eq!(issue.segment_pos, Some(3));
        assert_eq!(issue.element_pos, Some(2));
        assert_eq!(issue.component_pos, Some(1));
    }

    #[test]
    fn test_error_path() {
        let mut reporter = ValidationReporter::new();

        reporter.report_issue(
            ValidationIssue::new(Severity::Error, "Path test")
                .with_path("/UNB/UNH/LIN[1]/C212/7140"),
        );

        let report = reporter.get_report();
        let issue = &report.all_issues()[0];
        assert_eq!(issue.path, "/UNB/UNH/LIN[1]/C212/7140");
    }

    #[test]
    fn test_error_severity() {
        let mut reporter = ValidationReporter::new();

        reporter.report_error("This is an error");
        reporter.report_warning("This is a warning");
        reporter.report.info("This is info");

        let report = reporter.get_report();

        assert_eq!(report.by_severity(Severity::Error).len(), 1);
        assert_eq!(report.by_severity(Severity::Warning).len(), 1);
        assert_eq!(report.by_severity(Severity::Info).len(), 1);

        assert_eq!(report.count_by_severity(Severity::Error), 1);
        assert_eq!(report.count_by_severity(Severity::Warning), 1);
        assert_eq!(report.count_by_severity(Severity::Info), 1);
    }

    #[test]
    fn test_format_errors() {
        let mut reporter = ValidationReporter::new();

        reporter.report_issue(
            ValidationIssue::new(Severity::Error, "Missing required field")
                .with_path("/UNH/Bgm1000")
                .with_position(5, 10)
                .with_code("REQUIRED"),
        );

        reporter.report_issue(
            ValidationIssue::new(Severity::Warning, "Field length exceeds recommended limit")
                .with_path("/LIN/C212/7140")
                .with_code("LENGTH"),
        );

        let formatted = reporter.format_errors();

        assert!(formatted.contains("Error(s)"));
        assert!(formatted.contains("Missing required field"));
        assert!(formatted.contains("Warning(s)"));
        assert!(formatted.contains("Field length exceeds recommended limit"));
        assert!(formatted.contains("/UNH/Bgm1000"));
        assert!(formatted.contains("/LIN/C212/7140"));
    }

    #[test]
    fn test_format_errors_empty() {
        let reporter = ValidationReporter::new();
        let formatted = reporter.format_errors();
        assert_eq!(formatted, "No validation issues found.");
    }

    #[test]
    fn test_format_errors_complex() {
        let mut reporter = ValidationReporter::new();

        reporter.report_issue(
            ValidationIssue::new(Severity::Error, "Complex error")
                .with_path("/complex/path")
                .with_position(10, 25)
                .with_positions(2, Some(3), Some(1))
                .with_code("COMPLEX")
                .with_context("Additional context here"),
        );

        let formatted = reporter.format_errors();

        assert!(formatted.contains("ERROR"));
        assert!(formatted.contains("Path:"));
        assert!(formatted.contains("complex/path"));
        assert!(formatted.contains("Line 10"));
        assert!(formatted.contains("Col 25"));
        assert!(formatted.contains("Seg 2"));
        assert!(formatted.contains("Elem 3"));
        assert!(formatted.contains("Comp 1"));
        assert!(formatted.contains("Context:"));
    }

    #[test]
    fn test_validation_issue_builder() {
        let issue = ValidationIssue::new(Severity::Error, "Test message")
            .with_path("/test/path")
            .with_position(1, 2)
            .with_code("TEST001")
            .with_positions(3, Some(4), Some(5))
            .with_context("Test context");

        assert_eq!(issue.severity, Severity::Error);
        assert_eq!(issue.message, "Test message");
        assert_eq!(issue.path, "/test/path");
        assert_eq!(issue.line, Some(1));
        assert_eq!(issue.column, Some(2));
        assert_eq!(issue.code, Some("TEST001".to_string()));
        assert_eq!(issue.segment_pos, Some(3));
        assert_eq!(issue.element_pos, Some(4));
        assert_eq!(issue.component_pos, Some(5));
        assert_eq!(issue.context, Some("Test context".to_string()));
    }

    #[test]
    fn test_report_helpers() {
        let mut reporter = ValidationReporter::new();

        // Test error helper
        reporter.report_error("Error 1");
        reporter.report_error("Error 2");

        // Test warning helper
        reporter.report_warning("Warning 1");
        reporter.report_warning("Warning 2");

        // Test info helper
        reporter.report.info("Info 1");

        let report = reporter.get_report();
        assert_eq!(report.errors().len(), 2);
        assert_eq!(report.warnings().len(), 2);
        assert_eq!(report.infos().len(), 1);
    }

    #[test]
    fn test_clear_report() {
        let mut report = ValidationReport::new();

        report.error("Error 1");
        report.warning("Warning 1");

        assert!(report.has_issues());
        assert_eq!(report.count(), 2);

        report.clear();

        assert!(!report.has_issues());
        assert_eq!(report.count(), 0);
        assert!(!report.has_errors());
    }

    #[test]
    fn test_all_issues_order() {
        let mut report = ValidationReport::new();

        report.error("First");
        report.warning("Second");
        report.error("Third");

        let issues = report.all_issues();
        assert_eq!(issues.len(), 3);
        assert_eq!(issues[0].message, "First");
        assert_eq!(issues[1].message, "Second");
        assert_eq!(issues[2].message, "Third");
    }

    #[test]
    fn test_severity_display() {
        assert_eq!(format!("{}", Severity::Error), "ERROR");
        assert_eq!(format!("{}", Severity::Warning), "WARNING");
        assert_eq!(format!("{}", Severity::Info), "INFO");
    }

    #[test]
    fn test_default_format_options() {
        let opts = FormatOptions::default();
        assert!(opts.show_codes.is_enabled());
        assert!(opts.show_paths.is_enabled());
        assert!(opts.show_positions.is_enabled());
        assert!(opts.show_context.is_enabled());
    }

    #[test]
    fn test_reporter_with_options() {
        let opts = FormatOptions {
            show_codes: DisplayOption::Hide,
            show_paths: DisplayOption::Show,
            show_positions: DisplayOption::Hide,
            show_context: DisplayOption::Show,
        };

        let reporter = ValidationReporter::with_options(opts);
        assert_eq!(reporter.report.count(), 0);
    }

    #[test]
    fn test_into_report() {
        let mut reporter = ValidationReporter::new();
        reporter.report_error("Test error");

        let report = reporter.into_report();
        assert_eq!(report.count(), 1);
    }

    #[test]
    fn test_empty_path() {
        let issue = ValidationIssue::new(Severity::Error, "No path");
        assert!(issue.path.is_empty());
    }

    #[test]
    fn test_partial_positions() {
        let issue = ValidationIssue::new(Severity::Error, "Partial").with_positions(1, None, None);

        assert_eq!(issue.segment_pos, Some(1));
        assert_eq!(issue.element_pos, None);
        assert_eq!(issue.component_pos, None);
    }

    #[test]
    fn test_format_with_different_severities() {
        let mut reporter = ValidationReporter::new();

        reporter.report_issue(ValidationIssue::new(Severity::Error, "Critical failure"));
        reporter.report_issue(ValidationIssue::new(Severity::Warning, "Minor issue"));
        reporter.report_issue(ValidationIssue::new(Severity::Info, "Just FYI"));

        let formatted = reporter.format_errors();

        // Check that all severity types are represented
        assert!(formatted.contains("Error(s)"));
        assert!(formatted.contains("Warning(s)"));
        assert!(formatted.contains("Info message(s)"));
    }
}
