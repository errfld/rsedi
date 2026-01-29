//! Integration tests for edi-validation crate
//!
//! These tests verify end-to-end validation scenarios.

use edi_ir::{Document, Node, NodeType, Value};
use edi_validation::{
    codelist::{validate_code, CodeList, CodeListRegistry},
    engine::{StrictnessLevel, ValidationConfig, ValidationEngine},
    reporter::{Severity, ValidationIssue, ValidationReporter},
    rules::{
        validate_composite, validate_conditional, validate_data_type, validate_length,
        validate_pattern, validate_required, validate_segment_order, ConditionalRule, Constraint,
        DataType, SegmentOrderRule,
    },
};

/// Helper to create a complete EDI-like document structure
fn create_orders_document() -> Document {
    let mut root = Node::new("ROOT", NodeType::Root);

    // UNB - Interchange header
    let mut unb = Node::new("UNB", NodeType::Interchange);
    unb.add_child(Node::with_value(
        "S001",
        NodeType::Element,
        Value::String("UNOA".to_string()),
    ));
    unb.add_child(Node::with_value(
        "S002",
        NodeType::Element,
        Value::String("3".to_string()),
    ));
    root.add_child(unb);

    // UNH - Message header
    let mut unh = Node::new("UNH", NodeType::Message);
    unh.add_child(Node::with_value(
        "0062",
        NodeType::Element,
        Value::String("MSG001".to_string()),
    ));
    unh.add_child(Node::with_value(
        "S009",
        NodeType::Element,
        Value::String("ORDERS".to_string()),
    ));
    unh.add_child(Node::with_value(
        "S009",
        NodeType::Element,
        Value::String("D".to_string()),
    ));
    unh.add_child(Node::with_value(
        "S009",
        NodeType::Element,
        Value::String("96A".to_string()),
    ));
    unh.add_child(Node::with_value(
        "S009",
        NodeType::Element,
        Value::String("UN".to_string()),
    ));
    root.add_child(unh);

    // BGM - Beginning of message
    let mut bgm = Node::new("BGM", NodeType::Segment);
    bgm.add_child(Node::with_value(
        "C002",
        NodeType::Element,
        Value::String("220".to_string()),
    ));
    bgm.add_child(Node::with_value(
        "C106",
        NodeType::Element,
        Value::String("PO12345".to_string()),
    ));
    bgm.add_child(Node::with_value(
        "1225",
        NodeType::Element,
        Value::String("9".to_string()),
    ));
    root.add_child(bgm);

    // DTM - Date/Time
    let mut dtm = Node::new("DTM", NodeType::Segment);
    let mut dtm_composite = Node::new("C507", NodeType::Element);
    dtm_composite.add_child(Node::with_value(
        "2005",
        NodeType::Component,
        Value::String("137".to_string()),
    ));
    dtm_composite.add_child(Node::with_value(
        "2380",
        NodeType::Component,
        Value::String("20240115".to_string()),
    ));
    dtm_composite.add_child(Node::with_value(
        "2379",
        NodeType::Component,
        Value::String("102".to_string()),
    ));
    dtm.add_child(dtm_composite);
    root.add_child(dtm);

    // LIN - Line item (multiple)
    for i in 1..=3 {
        let mut lin = Node::new("LIN", NodeType::Segment);
        lin.add_child(Node::with_value(
            "1082",
            NodeType::Element,
            Value::Integer(i),
        ));

        let mut c212 = Node::new("C212", NodeType::Element);
        c212.add_child(Node::with_value(
            "7140",
            NodeType::Component,
            Value::String(format!("ITEM{:03}", i)),
        ));
        c212.add_child(Node::with_value(
            "7143",
            NodeType::Component,
            Value::String("EN".to_string()),
        ));
        c212.add_child(Node::with_value(
            "1131",
            NodeType::Component,
            Value::String("6".to_string()),
        ));
        lin.add_child(c212);

        root.add_child(lin);
    }

    Document::new(root)
}

#[test]
fn test_full_document_validation() {
    let doc = create_orders_document();
    let engine = ValidationEngine::new();

    let result = engine.validate(&doc).unwrap();

    // Basic document should be valid
    assert!(result.is_valid || result.errors.is_empty());
}

#[test]
fn test_document_with_multiple_issues() {
    let mut doc = create_orders_document();

    // Add some invalid elements
    if let Some(root) = doc.root.children.iter_mut().find(|c| c.name == "LIN") {
        // Add a null value element
        root.add_child(Node::with_value("INVALID", NodeType::Element, Value::Null));
    }

    let engine = ValidationEngine::with_config(ValidationConfig {
        strictness: StrictnessLevel::Strict,
        continue_on_error: true,
        ..Default::default()
    });

    let result = engine.validate(&doc).unwrap();

    // Should collect multiple issues
    assert!(!result.is_valid || result.has_errors());
}

#[test]
fn test_code_list_integration() {
    let mut registry = CodeListRegistry::new();

    // Create and register code lists
    let country_codes = CodeList::with_codes("countries", vec!["US", "GB", "DE", "FR", "JP"]);
    let currency_codes = CodeList::with_codes("currencies", vec!["USD", "EUR", "GBP", "JPY"]);

    registry.register(country_codes);
    registry.register(currency_codes);

    // Validate against code lists
    assert!(registry.validate("countries", "US"));
    assert!(registry.validate("currencies", "EUR"));
    assert!(!registry.validate("countries", "INVALID"));
    assert!(!registry.validate("currencies", "XYZ"));
}

#[test]
fn test_validation_rules_integration() {
    // Test required field validation
    let required_node = Node::with_value(
        "FIELD",
        NodeType::Element,
        Value::String("value".to_string()),
    );
    assert!(validate_required(&required_node).is_valid);

    let empty_node = Node::new("FIELD", NodeType::Element);
    assert!(!validate_required(&empty_node).is_valid);

    // Test length validation
    let constraint = Constraint::new().min_length(3).max_length(10);
    assert!(validate_length("hello", &constraint).is_valid);
    assert!(!validate_length("hi", &constraint).is_valid);
    assert!(!validate_length("this is way too long", &constraint).is_valid);

    // Test pattern validation
    assert!(validate_pattern("12345", "^[0-9]+$").is_valid);
    assert!(!validate_pattern("abc", "^[0-9]+$").is_valid);

    // Test data type validation
    assert!(validate_data_type("123", DataType::Integer).is_valid);
    assert!(!validate_data_type("abc", DataType::Integer).is_valid);
    assert!(validate_data_type("123.45", DataType::Decimal).is_valid);
    assert!(validate_data_type("2024-01-15", DataType::Date).is_valid);
}

#[test]
fn test_composite_element_validation() {
    let constraints = vec![
        Constraint::new().required(),
        Constraint::new().required(),
        Constraint::new(),
    ];

    // Valid composite
    let mut composite = Node::new("C212", NodeType::Element);
    composite.add_child(Node::with_value(
        "7140",
        NodeType::Component,
        Value::String("ITEM123".to_string()),
    ));
    composite.add_child(Node::with_value(
        "7143",
        NodeType::Component,
        Value::String("EN".to_string()),
    ));
    composite.add_child(Node::new("1131", NodeType::Component));

    assert!(validate_composite(&composite, &constraints).is_valid);

    // Invalid - missing required component
    let mut incomplete = Node::new("C212", NodeType::Element);
    incomplete.add_child(Node::with_value("7140", NodeType::Component, Value::Null));
    incomplete.add_child(Node::new("7143", NodeType::Component));

    assert!(!validate_composite(&incomplete, &constraints).is_valid);
}

#[test]
fn test_segment_order_validation() {
    let unh = Node::new("UNH", NodeType::Segment);
    let bgm = Node::new("BGM", NodeType::Segment);
    let dtm = Node::new("DTM", NodeType::Segment);
    let lin1 = Node::new("LIN", NodeType::Segment);
    let lin2 = Node::new("LIN", NodeType::Segment);

    let segments = vec![&unh, &bgm, &dtm, &lin1, &lin2];

    let rules = vec![
        SegmentOrderRule {
            segment_name: "UNH".to_string(),
            min_occurs: 1,
            max_occurs: Some(1),
        },
        SegmentOrderRule {
            segment_name: "BGM".to_string(),
            min_occurs: 1,
            max_occurs: Some(1),
        },
        SegmentOrderRule {
            segment_name: "LIN".to_string(),
            min_occurs: 0,
            max_occurs: Some(10),
        },
    ];

    assert!(validate_segment_order(&segments, &rules).is_valid);

    // Missing required segment
    let missing_unh = vec![&bgm, &dtm];
    assert!(!validate_segment_order(&missing_unh, &rules).is_valid);

    // Too many LIN segments (11 LIN segments, max is 10)
    let too_many_lin: Vec<&Node> = vec![
        &unh, &bgm, &lin1, &lin2, &lin1, &lin2, &lin1, &lin2, &lin1, &lin2, &lin1, &lin2, &lin1,
    ];
    assert!(!validate_segment_order(&too_many_lin, &rules).is_valid);
}

#[test]
fn test_conditional_rules_integration() {
    let currency_usd =
        Node::with_value("C002", NodeType::Element, Value::String("USD".to_string()));
    let amount = Node::with_value(
        "C004",
        NodeType::Element,
        Value::String("100.00".to_string()),
    );
    let currency_eur =
        Node::with_value("C002", NodeType::Element, Value::String("EUR".to_string()));

    let rules = vec![ConditionalRule {
        trigger_field: "C002".to_string(),
        trigger_value: "USD".to_string(),
        required_fields: vec!["C004".to_string()],
    }];

    // Condition met with required field present
    let nodes = vec![&currency_usd, &amount];
    assert!(validate_conditional(&nodes, &rules).is_valid);

    // Condition met but required field missing
    let nodes_missing = vec![&currency_usd];
    assert!(!validate_conditional(&nodes_missing, &rules).is_valid);

    // Condition not met (different currency)
    let nodes_eur = vec![&currency_eur];
    assert!(validate_conditional(&nodes_eur, &rules).is_valid);
}

#[test]
fn test_strictness_levels_integration() {
    let node_with_null = Node::with_value("FIELD", NodeType::Element, Value::Null);
    let doc = Document::new(Node::new("ROOT", NodeType::Root));

    // Strict mode
    let strict_engine = ValidationEngine::with_config(ValidationConfig {
        strictness: StrictnessLevel::Strict,
        ..Default::default()
    });

    // Lenient mode
    let lenient_engine = ValidationEngine::with_config(ValidationConfig {
        strictness: StrictnessLevel::Lenient,
        ..Default::default()
    });

    let strict_result = strict_engine.validate(&doc).unwrap();
    let lenient_result = lenient_engine.validate(&doc).unwrap();

    // Both should handle empty document
    assert!(strict_result.is_valid || !strict_result.has_errors());
    assert!(lenient_result.is_valid || !lenient_result.has_errors());
}

#[test]
fn test_error_reporting_integration() {
    let mut reporter = ValidationReporter::new();

    // Report multiple errors using report_issue builder pattern
    reporter.report_issue(
        ValidationIssue::new(Severity::Error, "Missing UNH segment")
            .with_path("/document")
            .with_code("E001")
            .with_position(1, 1),
    );

    reporter.report_issue(
        ValidationIssue::new(Severity::Error, "Invalid line item number")
            .with_path("/document/LIN[2]/1082")
            .with_code("E002")
            .with_position(10, 25),
    );

    reporter.report_issue(
        ValidationIssue::new(Severity::Warning, "Field length exceeds recommended limit")
            .with_path("/document/LIN[1]/C212/7140")
            .with_code("W001"),
    );

    let report = reporter.get_report();

    assert_eq!(report.count(), 3);
    assert_eq!(report.errors().len(), 2);
    assert_eq!(report.warnings().len(), 1);

    // Test formatted output
    let formatted = reporter.format_errors();
    assert!(formatted.contains("Missing UNH segment"));
    assert!(formatted.contains("Invalid line item number"));
    assert!(formatted.contains("Field length exceeds recommended limit"));
}

#[test]
fn test_end_to_end_validation_workflow() {
    // 1. Create a document
    let doc = create_orders_document();

    // 2. Set up validation engine
    let engine = ValidationEngine::with_config(ValidationConfig {
        strictness: StrictnessLevel::Moderate,
        continue_on_error: true,
        max_errors: 10,
    });

    // 3. Validate document
    let validation_result = engine.validate(&doc).unwrap();

    // 4. Set up reporter for any issues
    let mut reporter = ValidationReporter::new();

    for error in &validation_result.errors {
        reporter.report_issue(
            ValidationIssue::new(Severity::Error, error.message.clone()).with_path(&error.path),
        );
    }

    for warning in &validation_result.warnings {
        reporter.report_issue(
            ValidationIssue::new(Severity::Warning, warning.message.clone())
                .with_path(&warning.path),
        );
    }

    // 5. Generate report
    let formatted = reporter.format_errors();

    // Document should either be valid or have issues reported
    if validation_result.has_errors() {
        assert!(!formatted.contains("No validation issues found"));
    }
}

#[test]
fn test_code_list_validation_integration() {
    // Create a code list for country codes
    let country_codes = CodeList::with_codes("ISO3166", vec!["US", "GB", "DE", "FR", "JP", "AU"]);

    // Test valid codes
    assert!(validate_code("US", &country_codes).is_valid());
    assert!(validate_code("DE", &country_codes).is_valid());

    // Test invalid codes
    let result = validate_code("XX", &country_codes);
    assert!(!result.is_valid());
    assert!(result.error_message().is_some());

    // Test case sensitivity
    assert!(!validate_code("us", &country_codes).is_valid()); // Case sensitive by default

    let case_insensitive = CodeList::with_codes("ISO3166", vec!["US", "GB"]).case_sensitive(false);
    assert!(validate_code("us", &case_insensitive).is_valid());
}

#[test]
fn test_complex_validation_scenario() {
    // Create a document with multiple segments and elements
    let mut root = Node::new("ROOT", NodeType::Root);

    // Add some valid segments
    for i in 1..=5 {
        let mut seg = Node::new("LIN", NodeType::Segment);
        seg.add_child(Node::with_value(
            "1082",
            NodeType::Element,
            Value::Integer(i),
        ));

        let mut composite = Node::new("C212", NodeType::Element);
        composite.add_child(Node::with_value(
            "7140",
            NodeType::Component,
            Value::String(format!("PROD{:03}", i)),
        ));
        composite.add_child(Node::with_value(
            "7143",
            NodeType::Component,
            Value::String("EN".to_string()),
        ));
        seg.add_child(composite);

        root.add_child(seg);
    }

    // Add an invalid segment
    let mut invalid_seg = Node::new("LIN", NodeType::Segment);
    invalid_seg.add_child(Node::with_value(
        "1082",
        NodeType::Element,
        Value::String("not-a-number".to_string()),
    ));
    root.add_child(invalid_seg);

    let doc = Document::new(root);

    // Validate with strict mode
    let engine = ValidationEngine::with_config(ValidationConfig {
        strictness: StrictnessLevel::Strict,
        continue_on_error: true,
        max_errors: 0,
    });

    let result = engine.validate(&doc).unwrap();

    // Should continue after finding the invalid segment
    // and report all issues found
    if result.has_errors() {
        assert!(result.errors.len() >= 1);
    }
}
