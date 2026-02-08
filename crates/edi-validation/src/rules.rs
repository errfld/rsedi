//! Validation rules

use edi_ir::{Node, NodeType};

/// Data types for validation
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataType {
    String,
    Integer,
    Decimal,
    Date,
    Time,
    Boolean,
    Binary,
}

/// Constraint for a field
#[derive(Debug, Clone, Default)]
pub struct Constraint {
    /// Whether field is required
    pub required: bool,
    /// Minimum length (if applicable)
    pub min_length: Option<usize>,
    /// Maximum length (if applicable)
    pub max_length: Option<usize>,
    /// Pattern to match (regex)
    pub pattern: Option<String>,
    /// Data type
    pub data_type: Option<DataType>,
    /// Minimum value (for numeric types)
    pub min_value: Option<f64>,
    /// Maximum value (for numeric types)
    pub max_value: Option<f64>,
}

impl Constraint {
    /// Create a new constraint
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Set required
    #[must_use]
    pub fn required(mut self) -> Self {
        self.required = true;
        self
    }

    /// Set min length
    #[must_use]
    pub fn min_length(mut self, len: usize) -> Self {
        self.min_length = Some(len);
        self
    }

    /// Set max length
    #[must_use]
    pub fn max_length(mut self, len: usize) -> Self {
        self.max_length = Some(len);
        self
    }

    /// Set pattern
    #[must_use]
    pub fn pattern(mut self, pattern: impl Into<String>) -> Self {
        self.pattern = Some(pattern.into());
        self
    }

    /// Set data type
    #[must_use]
    pub fn data_type(mut self, data_type: DataType) -> Self {
        self.data_type = Some(data_type);
        self
    }
}

/// Validation rule result
#[derive(Debug, Clone)]
pub struct RuleResult {
    pub is_valid: bool,
    pub message: Option<String>,
}

impl RuleResult {
    #[must_use]
    pub fn valid() -> Self {
        Self {
            is_valid: true,
            message: None,
        }
    }

    pub fn invalid(message: impl Into<String>) -> Self {
        Self {
            is_valid: false,
            message: Some(message.into()),
        }
    }
}

/// Validate required field
#[must_use]
pub fn validate_required(node: &Node) -> RuleResult {
    if node.value.as_ref().is_none_or(edi_ir::Value::is_null) {
        return RuleResult::invalid(format!("Field '{}' is required", node.name));
    }
    RuleResult::valid()
}

/// Validate length constraints
#[must_use]
pub fn validate_length(value: &str, constraint: &Constraint) -> RuleResult {
    let len = value.len();

    if let Some(min) = constraint.min_length {
        if len < min {
            return RuleResult::invalid(format!("Value length {len} is less than minimum {min}"));
        }
    }

    if let Some(max) = constraint.max_length {
        if len > max {
            return RuleResult::invalid(format!("Value length {len} exceeds maximum {max}"));
        }
    }

    RuleResult::valid()
}

/// Validate pattern matching using regex
#[must_use]
pub fn validate_pattern(value: &str, pattern: &str) -> RuleResult {
    use regex::Regex;

    // Empty strings should not match patterns that require at least one character
    if value.is_empty() {
        return RuleResult::invalid(format!("Empty value does not match pattern '{pattern}'"));
    }

    // Try to compile the regex pattern
    match Regex::new(pattern) {
        Ok(re) => {
            if re.is_match(value) {
                RuleResult::valid()
            } else {
                RuleResult::invalid(format!(
                    "Value '{value}' does not match pattern '{pattern}'"
                ))
            }
        }
        Err(e) => RuleResult::invalid(format!("Invalid regex pattern '{pattern}': {e}")),
    }
}

/// Validate data type
#[must_use]
pub fn validate_data_type(value: &str, data_type: DataType) -> RuleResult {
    match data_type {
        DataType::Integer => {
            if value.parse::<i64>().is_ok() {
                RuleResult::valid()
            } else {
                RuleResult::invalid(format!("Value '{value}' is not a valid integer"))
            }
        }
        DataType::Decimal => {
            if value.parse::<f64>().is_ok() {
                RuleResult::valid()
            } else {
                RuleResult::invalid(format!("Value '{value}' is not a valid decimal"))
            }
        }
        DataType::Boolean => {
            let lower = value.to_lowercase();
            if lower == "true"
                || lower == "false"
                || lower == "1"
                || lower == "0"
                || lower == "yes"
                || lower == "no"
            {
                RuleResult::valid()
            } else {
                RuleResult::invalid(format!("Value '{value}' is not a valid boolean"))
            }
        }
        DataType::Date => {
            // Simple date validation (YYYY-MM-DD format)
            if value.len() == 10
                && value.chars().nth(4) == Some('-')
                && value.chars().nth(7) == Some('-')
            {
                RuleResult::valid()
            } else {
                RuleResult::invalid(format!(
                    "Value '{value}' is not a valid date (expected YYYY-MM-DD)"
                ))
            }
        }
        DataType::Time => {
            // Simple time validation (HH:MM or HH:MM:SS)
            if (value.len() == 5 || value.len() == 8) && value.chars().nth(2) == Some(':') {
                RuleResult::valid()
            } else {
                RuleResult::invalid(format!("Value '{value}' is not a valid time"))
            }
        }
        DataType::String | DataType::Binary => RuleResult::valid(),
    }
}

/// Validate composite element
#[must_use]
pub fn validate_composite(node: &Node, constraints: &[Constraint]) -> RuleResult {
    if node.node_type != NodeType::Element && node.node_type != NodeType::Component {
        return RuleResult::invalid(format!(
            "Expected Element or Component, found {:?}",
            node.node_type
        ));
    }

    // Check if number of components matches expectations
    if node.children.len() > constraints.len() {
        return RuleResult::invalid(format!(
            "Composite has {} components but only {} constraints defined",
            node.children.len(),
            constraints.len()
        ));
    }

    // Validate each component
    for (idx, (child, constraint)) in node.children.iter().zip(constraints.iter()).enumerate() {
        if constraint.required && child.value.as_ref().is_none_or(edi_ir::Value::is_null) {
            return RuleResult::invalid(format!(
                "Component {} in composite '{}' is required",
                idx, node.name
            ));
        }
    }

    RuleResult::valid()
}

/// Segment order validation
pub struct SegmentOrderRule {
    pub segment_name: String,
    pub min_occurs: usize,
    pub max_occurs: Option<usize>,
}

/// Validate segment order in a group
#[must_use]
pub fn validate_segment_order(segments: &[&Node], rules: &[SegmentOrderRule]) -> RuleResult {
    for rule in rules {
        let count = segments
            .iter()
            .filter(|s| s.name == rule.segment_name)
            .count();

        if count < rule.min_occurs {
            return RuleResult::invalid(format!(
                "Segment '{}' occurs {} times, minimum required is {}",
                rule.segment_name, count, rule.min_occurs
            ));
        }

        if let Some(max) = rule.max_occurs {
            if count > max {
                return RuleResult::invalid(format!(
                    "Segment '{}' occurs {} times, maximum allowed is {}",
                    rule.segment_name, count, max
                ));
            }
        }
    }

    RuleResult::valid()
}

/// Conditional rule
#[derive(Debug, Clone)]
pub struct ConditionalRule {
    /// Field that triggers the condition
    pub trigger_field: String,
    /// Expected trigger value
    pub trigger_value: String,
    /// Fields that are required when condition is met
    pub required_fields: Vec<String>,
}

/// Validate conditional rules
#[must_use]
pub fn validate_conditional(nodes: &[&Node], rules: &[ConditionalRule]) -> RuleResult {
    for rule in rules {
        // Find trigger node
        let trigger = nodes.iter().find(|n| n.name == rule.trigger_field);

        if let Some(trigger_node) = trigger {
            let trigger_matches = trigger_node
                .value
                .as_ref()
                .and_then(edi_ir::Value::as_string)
                .is_some_and(|s| s == rule.trigger_value);

            if trigger_matches {
                // Check that all required fields are present
                for required_field in &rule.required_fields {
                    let found = nodes.iter().any(|n| {
                        n.name == *required_field && n.value.as_ref().is_some_and(|v| !v.is_null())
                    });

                    if !found {
                        return RuleResult::invalid(format!(
                            "Field '{}' is required when '{}' is '{}'",
                            required_field, rule.trigger_field, rule.trigger_value
                        ));
                    }
                }
            }
        }
    }

    RuleResult::valid()
}

/// Validate a value against a code list
#[must_use]
pub fn validate_code_list(value: &str, codes: &[String]) -> RuleResult {
    if codes.contains(&value.to_string()) {
        RuleResult::valid()
    } else {
        RuleResult::invalid(format!(
            "Value '{value}' is not in allowed codes: {codes:?}"
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use edi_ir::Value;

    #[test]
    fn test_required_field() {
        let node_with_value = Node::with_value(
            "FIELD",
            NodeType::Element,
            Value::String("test".to_string()),
        );
        let result = validate_required(&node_with_value);
        assert!(result.is_valid);

        let node_without_value = Node::new("FIELD", NodeType::Element);
        let result = validate_required(&node_without_value);
        assert!(!result.is_valid);

        let node_with_null = Node::with_value("FIELD", NodeType::Element, Value::Null);
        let result = validate_required(&node_with_null);
        assert!(!result.is_valid);
    }

    #[test]
    fn test_length_constraints() {
        let constraint = Constraint::new().min_length(3).max_length(10);

        // Valid length
        let result = validate_length("hello", &constraint);
        assert!(result.is_valid);

        // Too short
        let result = validate_length("ab", &constraint);
        assert!(!result.is_valid);

        // Too long
        let result = validate_length("this is way too long", &constraint);
        assert!(!result.is_valid);

        // At minimum boundary
        let result = validate_length("abc", &constraint);
        assert!(result.is_valid);

        // At maximum boundary
        let result = validate_length("0123456789", &constraint);
        assert!(result.is_valid);
    }

    #[test]
    fn test_length_only_min() {
        let constraint = Constraint::new().min_length(5);
        assert!(validate_length("hello world", &constraint).is_valid);
        assert!(!validate_length("hi", &constraint).is_valid);
    }

    #[test]
    fn test_length_only_max() {
        let constraint = Constraint::new().max_length(5);
        assert!(validate_length("hi", &constraint).is_valid);
        assert!(!validate_length("hello world", &constraint).is_valid);
    }

    #[test]
    fn test_length_empty_constraint() {
        let constraint = Constraint::new();
        assert!(validate_length("", &constraint).is_valid);
        assert!(validate_length("any length is fine", &constraint).is_valid);
    }

    #[test]
    fn test_pattern_matching() {
        // Numeric pattern
        let result = validate_pattern("12345", "^[0-9]+$");
        assert!(result.is_valid);

        let result = validate_pattern("123a", "^[0-9]+$");
        assert!(!result.is_valid);

        // Uppercase pattern
        let result = validate_pattern("HELLO", "^[A-Z]+$");
        assert!(result.is_valid);

        let result = validate_pattern("Hello", "^[A-Z]+$");
        assert!(!result.is_valid);

        // Alphanumeric pattern
        let result = validate_pattern("ABC123", "^[a-zA-Z0-9]+$");
        assert!(result.is_valid);

        let result = validate_pattern("ABC-123", "^[a-zA-Z0-9]+$");
        assert!(!result.is_valid);
    }

    #[test]
    fn test_pattern_empty_value() {
        let result = validate_pattern("", "^[0-9]+$");
        assert!(!result.is_valid);
    }

    #[test]
    fn test_data_type_validation() {
        // Integer
        assert!(validate_data_type("123", DataType::Integer).is_valid);
        assert!(validate_data_type("-456", DataType::Integer).is_valid);
        assert!(!validate_data_type("12.3", DataType::Integer).is_valid);
        assert!(!validate_data_type("abc", DataType::Integer).is_valid);

        // Decimal
        assert!(validate_data_type("123.45", DataType::Decimal).is_valid);
        assert!(validate_data_type("-67.89", DataType::Decimal).is_valid);
        assert!(validate_data_type("123", DataType::Decimal).is_valid);
        assert!(!validate_data_type("abc", DataType::Decimal).is_valid);

        // Boolean
        assert!(validate_data_type("true", DataType::Boolean).is_valid);
        assert!(validate_data_type("false", DataType::Boolean).is_valid);
        assert!(validate_data_type("1", DataType::Boolean).is_valid);
        assert!(validate_data_type("0", DataType::Boolean).is_valid);
        assert!(validate_data_type("YES", DataType::Boolean).is_valid);
        assert!(validate_data_type("no", DataType::Boolean).is_valid);
        assert!(!validate_data_type("maybe", DataType::Boolean).is_valid);

        // Date
        assert!(validate_data_type("2024-01-15", DataType::Date).is_valid);
        assert!(!validate_data_type("2024/01/15", DataType::Date).is_valid);
        assert!(!validate_data_type("01-15-2024", DataType::Date).is_valid);

        // Time
        assert!(validate_data_type("14:30", DataType::Time).is_valid);
        assert!(validate_data_type("14:30:00", DataType::Time).is_valid);
        assert!(!validate_data_type("2:30 PM", DataType::Time).is_valid);

        // String (always valid)
        assert!(validate_data_type("anything", DataType::String).is_valid);
        assert!(validate_data_type("", DataType::String).is_valid);

        // Binary (always valid for string input)
        assert!(validate_data_type("binary", DataType::Binary).is_valid);
    }

    #[test]
    fn test_composite_validation() {
        let constraints = vec![
            Constraint::new().required(),
            Constraint::new(),
            Constraint::new().required(),
        ];

        // Valid composite with all required fields
        let mut composite = Node::new("C212", NodeType::Element);
        composite.add_child(Node::with_value(
            "7140",
            NodeType::Component,
            Value::String("123".to_string()),
        ));
        composite.add_child(Node::with_value(
            "7143",
            NodeType::Component,
            Value::String("EN".to_string()),
        ));
        composite.add_child(Node::with_value(
            "1131",
            NodeType::Component,
            Value::String("6".to_string()),
        ));

        let result = validate_composite(&composite, &constraints);
        assert!(result.is_valid);

        // Invalid - missing required component
        let mut incomplete = Node::new("C212", NodeType::Element);
        incomplete.add_child(Node::with_value("7140", NodeType::Component, Value::Null));
        incomplete.add_child(Node::new("7143", NodeType::Component));
        incomplete.add_child(Node::with_value(
            "1131",
            NodeType::Component,
            Value::String("6".to_string()),
        ));

        let result = validate_composite(&incomplete, &constraints);
        assert!(!result.is_valid);

        // Too many components
        let mut too_many = Node::new("C212", NodeType::Element);
        too_many.add_child(Node::with_value(
            "1",
            NodeType::Component,
            Value::String("a".to_string()),
        ));
        too_many.add_child(Node::with_value(
            "2",
            NodeType::Component,
            Value::String("b".to_string()),
        ));
        too_many.add_child(Node::with_value(
            "3",
            NodeType::Component,
            Value::String("c".to_string()),
        ));
        too_many.add_child(Node::with_value(
            "4",
            NodeType::Component,
            Value::String("d".to_string()),
        ));

        let result = validate_composite(&too_many, &constraints);
        assert!(!result.is_valid);
    }

    #[test]
    fn test_composite_wrong_node_type() {
        let node = Node::new("SEGMENT", NodeType::Segment);
        let constraints = vec![Constraint::new()];

        let result = validate_composite(&node, &constraints);
        assert!(!result.is_valid);
    }

    #[test]
    fn test_segment_order() {
        // Create test nodes
        let unh = Node::new("UNH", NodeType::Segment);
        let lin = Node::new("LIN", NodeType::Segment);
        let qty = Node::new("QTY", NodeType::Segment);

        let segment_refs = vec![&unh, &lin, &qty, &lin]; // 2 LIN segments

        // Valid rules
        let rules = vec![
            SegmentOrderRule {
                segment_name: "UNH".to_string(),
                min_occurs: 1,
                max_occurs: Some(1),
            },
            SegmentOrderRule {
                segment_name: "LIN".to_string(),
                min_occurs: 0,
                max_occurs: Some(10),
            },
            SegmentOrderRule {
                segment_name: "QTY".to_string(),
                min_occurs: 0,
                max_occurs: None,
            },
        ];

        let result = validate_segment_order(&segment_refs, &rules);
        assert!(result.is_valid);

        // Missing required segment
        let rules_missing = vec![SegmentOrderRule {
            segment_name: "UNB".to_string(),
            min_occurs: 1,
            max_occurs: Some(1),
        }];

        let result = validate_segment_order(&segment_refs, &rules_missing);
        assert!(!result.is_valid);

        // Too many occurrences
        let rules_too_many = vec![SegmentOrderRule {
            segment_name: "LIN".to_string(),
            min_occurs: 0,
            max_occurs: Some(1),
        }];

        let result = validate_segment_order(&segment_refs, &rules_too_many);
        assert!(!result.is_valid);
    }

    #[test]
    fn test_conditional_rules() {
        // Create nodes where condition is met
        let currency =
            Node::with_value("C002", NodeType::Element, Value::String("USD".to_string()));
        let amount = Node::with_value(
            "C004",
            NodeType::Element,
            Value::String("100.00".to_string()),
        );
        let nodes = vec![&currency, &amount];

        let rules = vec![ConditionalRule {
            trigger_field: "C002".to_string(),
            trigger_value: "USD".to_string(),
            required_fields: vec!["C004".to_string()],
        }];

        let result = validate_conditional(&nodes, &rules);
        assert!(result.is_valid);

        // Condition met but required field missing
        let currency_only = vec![&currency];
        let result = validate_conditional(&currency_only, &rules);
        assert!(!result.is_valid);

        // Condition not met - required field can be absent
        let eur_currency =
            Node::with_value("C002", NodeType::Element, Value::String("EUR".to_string()));
        let eur_only = vec![&eur_currency];
        let result = validate_conditional(&eur_only, &rules);
        assert!(result.is_valid);

        // Trigger field not found
        let no_currency: Vec<&Node> = vec![];
        let result = validate_conditional(&no_currency, &rules);
        assert!(result.is_valid);
    }

    #[test]
    fn test_conditional_multiple_required_fields() {
        let trigger = Node::with_value("TYPE", NodeType::Element, Value::String("A".to_string()));
        let field1 = Node::with_value("F1", NodeType::Element, Value::String("v1".to_string()));
        let field2 = Node::with_value("F2", NodeType::Element, Value::String("v2".to_string()));

        let rules = vec![ConditionalRule {
            trigger_field: "TYPE".to_string(),
            trigger_value: "A".to_string(),
            required_fields: vec!["F1".to_string(), "F2".to_string()],
        }];

        // All required fields present
        let all_present = vec![&trigger, &field1, &field2];
        assert!(validate_conditional(&all_present, &rules).is_valid);

        // Missing one required field
        let missing_one = vec![&trigger, &field1];
        assert!(!validate_conditional(&missing_one, &rules).is_valid);
    }

    #[test]
    fn test_constraint_builder() {
        let constraint = Constraint::new()
            .required()
            .min_length(5)
            .max_length(10)
            .pattern("^[A-Z]+$")
            .data_type(DataType::String);

        assert!(constraint.required);
        assert_eq!(constraint.min_length, Some(5));
        assert_eq!(constraint.max_length, Some(10));
        assert_eq!(constraint.pattern, Some("^[A-Z]+$".to_string()));
        assert_eq!(constraint.data_type, Some(DataType::String));
    }

    #[test]
    fn test_rule_result_helpers() {
        let valid = RuleResult::valid();
        assert!(valid.is_valid);
        assert!(valid.message.is_none());

        let invalid = RuleResult::invalid("Test error message");
        assert!(!invalid.is_valid);
        assert_eq!(invalid.message, Some("Test error message".to_string()));
    }

    #[test]
    fn test_data_type_edge_cases() {
        // Integer edge cases
        assert!(validate_data_type("0", DataType::Integer).is_valid);
        assert!(validate_data_type("-0", DataType::Integer).is_valid);
        assert!(validate_data_type("+123", DataType::Integer).is_valid);
        assert!(!validate_data_type("12 3", DataType::Integer).is_valid);
        assert!(!validate_data_type("", DataType::Integer).is_valid);

        // Decimal edge cases
        assert!(validate_data_type("0.0", DataType::Decimal).is_valid);
        assert!(validate_data_type(".5", DataType::Decimal).is_valid);
        assert!(validate_data_type("1e10", DataType::Decimal).is_valid);
        assert!(!validate_data_type("1.2.3", DataType::Decimal).is_valid);

        // Date edge cases
        assert!(validate_data_type("2024-12-31", DataType::Date).is_valid);
        // Note: Current implementation requires exactly 10 characters (YYYY-MM-DD)
        // "24-01-01" has only 8 characters, so it's invalid
        assert!(!validate_data_type("24-01-01", DataType::Date).is_valid); // Short year format, invalid

        // Time edge cases
        assert!(validate_data_type("23:59:59", DataType::Time).is_valid);
        assert!(validate_data_type("00:00", DataType::Time).is_valid);
    }
}
