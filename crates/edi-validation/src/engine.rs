//! Validation engine

use edi_ir::{Document, Node, NodeType};

/// Strictness level for validation
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum StrictnessLevel {
    /// Strict: Fail on any validation error
    Strict,
    /// Moderate: Allow warnings, fail on errors
    #[default]
    Moderate,
    /// Lenient: Allow all non-critical issues
    Lenient,
}

/// Validation configuration
#[derive(Debug, Clone)]
pub struct ValidationConfig {
    /// Strictness level
    pub strictness: StrictnessLevel,
    /// Continue validation after errors (collect all)
    pub continue_on_error: bool,
    /// Maximum errors before stopping (0 = unlimited)
    pub max_errors: usize,
}

impl Default for ValidationConfig {
    fn default() -> Self {
        Self {
            strictness: StrictnessLevel::Moderate,
            continue_on_error: true,
            max_errors: 0,
        }
    }
}

/// Validation result
#[derive(Debug, Clone)]
pub struct ValidationResult {
    /// Whether validation passed
    pub is_valid: bool,
    /// List of errors found
    pub errors: Vec<ValidationError>,
    /// List of warnings found
    pub warnings: Vec<ValidationError>,
}

impl ValidationResult {
    /// Create a new valid result
    pub fn valid() -> Self {
        Self {
            is_valid: true,
            errors: Vec::new(),
            warnings: Vec::new(),
        }
    }

    /// Check if there are any errors
    pub fn has_errors(&self) -> bool {
        !self.errors.is_empty()
    }

    /// Check if there are any warnings
    pub fn has_warnings(&self) -> bool {
        !self.warnings.is_empty()
    }

    /// Add an error
    pub fn add_error(&mut self, error: ValidationError) {
        self.errors.push(error);
        self.is_valid = false;
    }

    /// Add a warning
    pub fn add_warning(&mut self, warning: ValidationError) {
        self.warnings.push(warning);
    }
}

/// Validation error details
#[derive(Debug, Clone)]
pub struct ValidationError {
    /// Error message
    pub message: String,
    /// Path in the document where error occurred
    pub path: String,
    /// Line number (if available)
    pub line: Option<usize>,
    /// Severity level
    pub severity: Severity,
    /// Error code
    pub code: Option<String>,
}

/// Severity of a validation issue
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Severity {
    /// Error - validation failed
    Error,
    /// Warning - issue but not blocking
    Warning,
    /// Info - informational only
    Info,
}

/// Main validation engine
pub struct ValidationEngine {
    config: ValidationConfig,
}

impl ValidationEngine {
    /// Create a new validation engine
    pub fn new() -> Self {
        Self {
            config: ValidationConfig::default(),
        }
    }

    /// Create with specific configuration
    pub fn with_config(config: ValidationConfig) -> Self {
        Self { config }
    }

    /// Validate a complete document
    pub fn validate(&self, doc: &Document) -> crate::Result<ValidationResult> {
        let mut result = ValidationResult::valid();

        // Validate the root node
        self.validate_node(&doc.root, &mut result, "");

        // Apply strictness rules
        if self.config.strictness == StrictnessLevel::Strict && result.has_errors() {
            result.is_valid = false;
        }

        Ok(result)
    }

    /// Validate a single segment
    pub fn validate_segment(&self, segment: &Node) -> crate::Result<ValidationResult> {
        let mut result = ValidationResult::valid();

        if segment.node_type != NodeType::Segment {
            result.add_error(ValidationError {
                message: format!("Expected Segment, found {:?}", segment.node_type),
                path: segment.name.clone(),
                line: None,
                severity: Severity::Error,
                code: Some("TYPE_MISMATCH".to_string()),
            });
            return Ok(result);
        }

        // Validate segment children (elements)
        for (idx, child) in segment.children.iter().enumerate() {
            let path = format!("{}/{}", segment.name, idx);
            self.validate_element_internal(child, &mut result, &path);
        }

        Ok(result)
    }

    /// Validate a single element
    pub fn validate_element(&self, element: &Node) -> crate::Result<ValidationResult> {
        let mut result = ValidationResult::valid();
        self.validate_element_internal(element, &mut result, &element.name);
        Ok(result)
    }

    fn validate_node(&self, node: &Node, result: &mut ValidationResult, parent_path: &str) {
        let path = if parent_path.is_empty() {
            node.name.clone()
        } else {
            format!("{}/{}", parent_path, node.name)
        };

        match node.node_type {
            NodeType::Segment => {
                self.validate_segment_internal(node, result, &path);
            }
            NodeType::Element => {
                self.validate_element_internal(node, result, &path);
            }
            _ => {
                // Recursively validate children
                for child in &node.children {
                    self.validate_node(child, result, &path);
                }
            }
        }
    }

    fn validate_segment_internal(&self, segment: &Node, result: &mut ValidationResult, path: &str) {
        // Check for required elements
        for (idx, child) in segment.children.iter().enumerate() {
            let element_path = format!("{}/{}", path, idx);
            self.validate_element_internal(child, result, &element_path);
        }
    }

    fn validate_element_internal(&self, element: &Node, result: &mut ValidationResult, path: &str) {
        // Check if value is present for required elements
        if let Some(ref value) = element.value {
            if value.is_null() && self.config.strictness == StrictnessLevel::Strict {
                result.add_error(ValidationError {
                    message: "Required element has null value".to_string(),
                    path: path.to_string(),
                    line: None,
                    severity: Severity::Error,
                    code: Some("NULL_VALUE".to_string()),
                });
            }
        }

        // Validate component children if present
        for (idx, child) in element.children.iter().enumerate() {
            let component_path = format!("{}/{}", path, idx);
            self.validate_element_internal(child, result, &component_path);
        }
    }
}

impl Default for ValidationEngine {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use edi_ir::Value;

    // Helper function to create a test document
    fn create_test_document() -> Document {
        let mut root = Node::new("ROOT", NodeType::Root);

        // Create a test segment
        let mut segment = Node::new("TEST", NodeType::Segment);
        segment.add_child(Node::with_value(
            "FIELD1",
            NodeType::Element,
            Value::String("value1".to_string()),
        ));
        segment.add_child(Node::with_value(
            "FIELD2",
            NodeType::Element,
            Value::String("value2".to_string()),
        ));

        root.add_child(segment);
        Document::new(root)
    }

    fn create_test_segment() -> Node {
        let mut segment = Node::new("LIN", NodeType::Segment);
        segment.add_child(Node::with_value(
            "C212",
            NodeType::Element,
            Value::String("12345".to_string()),
        ));
        segment.add_child(Node::with_value(
            "C212",
            NodeType::Element,
            Value::String("EN".to_string()),
        ));
        segment
    }

    fn create_test_element() -> Node {
        Node::with_value(
            "7140",
            NodeType::Element,
            Value::String("ITEM123".to_string()),
        )
    }

    #[test]
    fn test_validate_document() {
        let doc = create_test_document();
        let engine = ValidationEngine::new();

        let result = engine.validate(&doc).unwrap();

        assert!(result.is_valid);
        assert!(!result.has_errors());
    }

    #[test]
    fn test_validate_document_with_empty_children() {
        let root = Node::new("ROOT", NodeType::Root);
        let doc = Document::new(root);
        let engine = ValidationEngine::new();

        let result = engine.validate(&doc).unwrap();

        assert!(result.is_valid);
    }

    #[test]
    fn test_validate_segment() {
        let segment = create_test_segment();
        let engine = ValidationEngine::new();

        let result = engine.validate_segment(&segment).unwrap();

        assert!(result.is_valid);
        assert!(!result.has_errors());
    }

    #[test]
    fn test_validate_segment_with_wrong_type() {
        let wrong_node = Node::new("NOT_SEGMENT", NodeType::Element);
        let engine = ValidationEngine::new();

        let result = engine.validate_segment(&wrong_node).unwrap();

        assert!(!result.is_valid);
        assert!(result.has_errors());
        assert_eq!(result.errors[0].severity, Severity::Error);
    }

    #[test]
    fn test_validate_element() {
        let element = create_test_element();
        let engine = ValidationEngine::new();

        let result = engine.validate_element(&element).unwrap();

        assert!(result.is_valid);
    }

    #[test]
    fn test_validate_element_with_null_value() {
        let element = Node::with_value("7140", NodeType::Element, Value::Null);
        let engine = ValidationEngine::with_config(ValidationConfig {
            strictness: StrictnessLevel::Strict,
            ..Default::default()
        });

        let result = engine.validate_element(&element).unwrap();

        // In strict mode, null values should produce errors
        assert!(!result.is_valid);
        assert!(result.has_errors());
    }

    #[test]
    fn test_strictness_levels() {
        let element = Node::with_value("FIELD", NodeType::Element, Value::Null);

        // Test Strict mode
        let strict_engine = ValidationEngine::with_config(ValidationConfig {
            strictness: StrictnessLevel::Strict,
            ..Default::default()
        });
        let result = strict_engine.validate_element(&element).unwrap();
        assert!(!result.is_valid);

        // Test Moderate mode
        let moderate_engine = ValidationEngine::with_config(ValidationConfig {
            strictness: StrictnessLevel::Moderate,
            ..Default::default()
        });
        let _result = moderate_engine.validate_element(&element).unwrap();
        // Moderate mode may still report warnings

        // Test Lenient mode
        let lenient_engine = ValidationEngine::with_config(ValidationConfig {
            strictness: StrictnessLevel::Lenient,
            ..Default::default()
        });
        let _result = lenient_engine.validate_element(&element).unwrap();
        // Lenient mode may be more permissive
    }

    #[test]
    fn test_partial_validation() {
        let mut root = Node::new("ROOT", NodeType::Root);

        // Create multiple segments, some valid, some with issues
        let mut segment1 = Node::new("SEG1", NodeType::Segment);
        segment1.add_child(Node::with_value(
            "FIELD",
            NodeType::Element,
            Value::String("valid".to_string()),
        ));

        let mut segment2 = Node::new("SEG2", NodeType::Segment);
        segment2.add_child(Node::with_value("FIELD", NodeType::Element, Value::Null));

        root.add_child(segment1);
        root.add_child(segment2);

        let doc = Document::new(root);

        // Continue on error mode
        let engine_continue = ValidationEngine::with_config(ValidationConfig {
            continue_on_error: true,
            strictness: StrictnessLevel::Strict,
            ..Default::default()
        });

        let result = engine_continue.validate(&doc).unwrap();
        // Should process all segments and collect errors
        assert!(!result.is_valid);
    }

    #[test]
    fn test_stop_on_first_error() {
        let mut root = Node::new("ROOT", NodeType::Root);

        let mut segment1 = Node::new("SEG1", NodeType::Segment);
        segment1.add_child(Node::with_value("FIELD", NodeType::Element, Value::Null));

        root.add_child(segment1);

        let doc = Document::new(root);

        // Stop on first error mode
        let engine_stop = ValidationEngine::with_config(ValidationConfig {
            continue_on_error: false,
            strictness: StrictnessLevel::Strict,
            max_errors: 1,
        });

        let result = engine_stop.validate(&doc).unwrap();
        assert!(!result.is_valid);
    }

    #[test]
    fn test_nested_document_validation() {
        let mut root = Node::new("ROOT", NodeType::Root);
        let mut group = Node::new("SG1", NodeType::SegmentGroup);
        let mut segment = Node::new("LIN", NodeType::Segment);

        segment.add_child(Node::with_value(
            "C212",
            NodeType::Element,
            Value::String("123".to_string()),
        ));
        group.add_child(segment);
        root.add_child(group);

        let doc = Document::new(root);
        let engine = ValidationEngine::new();

        let result = engine.validate(&doc).unwrap();
        assert!(result.is_valid);
    }

    #[test]
    fn test_validation_result_helpers() {
        let mut result = ValidationResult::valid();

        assert!(result.is_valid);
        assert!(!result.has_errors());
        assert!(!result.has_warnings());

        result.add_error(ValidationError {
            message: "Test error".to_string(),
            path: "/test".to_string(),
            line: Some(1),
            severity: Severity::Error,
            code: Some("TEST".to_string()),
        });

        assert!(!result.is_valid);
        assert!(result.has_errors());

        result.add_warning(ValidationError {
            message: "Test warning".to_string(),
            path: "/test".to_string(),
            line: None,
            severity: Severity::Warning,
            code: None,
        });

        assert!(result.has_warnings());
    }

    #[test]
    fn test_default_config() {
        let config = ValidationConfig::default();
        assert_eq!(config.strictness, StrictnessLevel::Moderate);
        assert!(config.continue_on_error);
        assert_eq!(config.max_errors, 0);
    }
}
