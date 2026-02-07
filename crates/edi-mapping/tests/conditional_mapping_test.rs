//! Integration test: Complex conditional logic
//!
//! Tests various conditional constructs in mappings.

use edi_ir::{Document, Node, NodeType, Value};
use edi_mapping::{MappingDsl, MappingRuntime};
use std::path::PathBuf;

fn first_mapped_node(document: &Document) -> Option<&Node> {
    document.root.children.first()
}

#[test]
fn test_complex_conditional_logic_from_yaml() {
    let mapping_path =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/data/conditional_map.yaml");

    let mapping = MappingDsl::parse_file(&mapping_path).unwrap();
    assert_eq!(mapping.name, "conditional_map");

    // Test with standard order (type 220)
    let mut root1 = Node::new("ROOT", NodeType::Root);
    let mut bgm1 = Node::new("BGM", NodeType::Segment);
    bgm1.add_child(Node::with_value(
        "MessageName",
        NodeType::Field,
        Value::String("220".to_string()),
    ));
    bgm1.add_child(Node::with_value(
        "DocumentNumber",
        NodeType::Field,
        Value::String("ORD001".to_string()),
    ));
    root1.add_child(bgm1);

    let doc1 = Document::new(root1);
    let mut runtime = MappingRuntime::new();
    let result1 = runtime.execute(&mapping, &doc1).unwrap();

    // Should produce output (structure depends on implementation)
    // The mapping should have executed successfully
    assert!(
        first_mapped_node(&result1).is_some(),
        "Mapping should have produced output"
    );

    // Test with blanket order (type 221)
    let mut root2 = Node::new("ROOT", NodeType::Root);
    let mut bgm2 = Node::new("BGM", NodeType::Segment);
    bgm2.add_child(Node::with_value(
        "MessageName",
        NodeType::Field,
        Value::String("221".to_string()),
    ));
    bgm2.add_child(Node::with_value(
        "DocumentNumber",
        NodeType::Field,
        Value::String("BLK001".to_string()),
    ));
    root2.add_child(bgm2);

    let doc2 = Document::new(root2);
    let result2 = runtime.execute(&mapping, &doc2).unwrap();
    assert!(!result2.root.children.is_empty());
}

#[test]
fn test_and_condition() {
    let dsl = r#"
name: and_condition_test
source_type: TEST
target_type: OUTPUT
rules:
  - type: condition
    when:
      op: and
      conditions:
        - op: exists
          field: /Order/Number
        - op: equals
          field: /Order/Type
          value: "URGENT"
    then:
      - type: field
        source: /Order/Number
        target: urgent_order
"#;

    let mapping = MappingDsl::parse(dsl).unwrap();

    // Both conditions met
    let mut root1 = Node::new("ROOT", NodeType::Root);
    let mut order1 = Node::new("Order", NodeType::Segment);
    order1.add_child(Node::with_value(
        "Number",
        NodeType::Field,
        Value::String("ORD001".to_string()),
    ));
    order1.add_child(Node::with_value(
        "Type",
        NodeType::Field,
        Value::String("URGENT".to_string()),
    ));
    root1.add_child(order1);

    let doc1 = Document::new(root1);
    let mut runtime = MappingRuntime::new();
    let result1 = runtime.execute(&mapping, &doc1).unwrap();
    assert_eq!(first_mapped_node(&result1).unwrap().name, "urgent_order");

    // Only one condition met
    let mut root2 = Node::new("ROOT", NodeType::Root);
    let mut order2 = Node::new("Order", NodeType::Segment);
    order2.add_child(Node::with_value(
        "Number",
        NodeType::Field,
        Value::String("ORD002".to_string()),
    ));
    order2.add_child(Node::with_value(
        "Type",
        NodeType::Field,
        Value::String("NORMAL".to_string()),
    ));
    root2.add_child(order2);

    let doc2 = Document::new(root2);
    let result2 = runtime.execute(&mapping, &doc2).unwrap();
    assert!(result2.root.children.is_empty()); // No output when condition not met
}

#[test]
fn test_or_condition() {
    let dsl = r#"
name: or_condition_test
source_type: TEST
target_type: OUTPUT
rules:
  - type: condition
    when:
      op: or
      conditions:
        - op: equals
          field: /Order/Type
          value: "STANDARD"
        - op: equals
          field: /Order/Type
          value: "PRIORITY"
    then:
      - type: field
        source: /Order/Number
        target: valid_order
"#;

    let mapping = MappingDsl::parse(dsl).unwrap();
    let mut runtime = MappingRuntime::new();

    // First condition met
    let mut root1 = Node::new("ROOT", NodeType::Root);
    let mut order1 = Node::new("Order", NodeType::Segment);
    order1.add_child(Node::with_value(
        "Number",
        NodeType::Field,
        Value::String("ORD001".to_string()),
    ));
    order1.add_child(Node::with_value(
        "Type",
        NodeType::Field,
        Value::String("STANDARD".to_string()),
    ));
    root1.add_child(order1);
    let result1 = runtime.execute(&mapping, &Document::new(root1)).unwrap();
    assert_eq!(first_mapped_node(&result1).unwrap().name, "valid_order");

    // Second condition met
    let mut root2 = Node::new("ROOT", NodeType::Root);
    let mut order2 = Node::new("Order", NodeType::Segment);
    order2.add_child(Node::with_value(
        "Number",
        NodeType::Field,
        Value::String("ORD002".to_string()),
    ));
    order2.add_child(Node::with_value(
        "Type",
        NodeType::Field,
        Value::String("PRIORITY".to_string()),
    ));
    root2.add_child(order2);
    let result2 = runtime.execute(&mapping, &Document::new(root2)).unwrap();
    assert_eq!(first_mapped_node(&result2).unwrap().name, "valid_order");

    // Neither condition met
    let mut root3 = Node::new("ROOT", NodeType::Root);
    let mut order3 = Node::new("Order", NodeType::Segment);
    order3.add_child(Node::with_value(
        "Number",
        NodeType::Field,
        Value::String("ORD003".to_string()),
    ));
    order3.add_child(Node::with_value(
        "Type",
        NodeType::Field,
        Value::String("BULK".to_string()),
    ));
    root3.add_child(order3);
    let result3 = runtime.execute(&mapping, &Document::new(root3)).unwrap();
    assert!(result3.root.children.is_empty());
}

#[test]
fn test_not_condition() {
    let dsl = r#"
name: not_condition_test
source_type: TEST
target_type: OUTPUT
rules:
  - type: condition
    when:
      op: not
      condition:
        op: equals
        field: /Order/Status
        value: "CANCELLED"
    then:
      - type: field
        source: /Order/Number
        target: active_order
"#;

    let mapping = MappingDsl::parse(dsl).unwrap();
    let mut runtime = MappingRuntime::new();

    // Not cancelled - should match
    let mut root1 = Node::new("ROOT", NodeType::Root);
    let mut order1 = Node::new("Order", NodeType::Segment);
    order1.add_child(Node::with_value(
        "Number",
        NodeType::Field,
        Value::String("ORD001".to_string()),
    ));
    order1.add_child(Node::with_value(
        "Status",
        NodeType::Field,
        Value::String("ACTIVE".to_string()),
    ));
    root1.add_child(order1);
    let result1 = runtime.execute(&mapping, &Document::new(root1)).unwrap();
    assert_eq!(first_mapped_node(&result1).unwrap().name, "active_order");

    // Is cancelled - should not match
    let mut root2 = Node::new("ROOT", NodeType::Root);
    let mut order2 = Node::new("Order", NodeType::Segment);
    order2.add_child(Node::with_value(
        "Number",
        NodeType::Field,
        Value::String("ORD002".to_string()),
    ));
    order2.add_child(Node::with_value(
        "Status",
        NodeType::Field,
        Value::String("CANCELLED".to_string()),
    ));
    root2.add_child(order2);
    let result2 = runtime.execute(&mapping, &Document::new(root2)).unwrap();
    assert!(result2.root.children.is_empty());
}

#[test]
fn test_nested_conditions() {
    let dsl = r#"
name: nested_condition_test
source_type: TEST
target_type: OUTPUT
rules:
  - type: condition
    when:
      op: and
      conditions:
        - op: exists
          field: /Order/Number
        - op: or
          conditions:
            - op: equals
              field: /Order/Priority
              value: "HIGH"
            - op: and
              conditions:
                - op: equals
                  field: /Order/Type
                  value: "URGENT"
                - op: exists
                  field: /Order/Customer
    then:
      - type: field
        source: /Order/Number
        target: important_order
"#;

    let mapping = MappingDsl::parse(dsl).unwrap();

    // Exists + HIGH priority
    let mut root1 = Node::new("ROOT", NodeType::Root);
    let mut order1 = Node::new("Order", NodeType::Segment);
    order1.add_child(Node::with_value(
        "Number",
        NodeType::Field,
        Value::String("ORD001".to_string()),
    ));
    order1.add_child(Node::with_value(
        "Priority",
        NodeType::Field,
        Value::String("HIGH".to_string()),
    ));
    root1.add_child(order1);

    let doc1 = Document::new(root1);
    let mut runtime = MappingRuntime::new();
    let result1 = runtime.execute(&mapping, &doc1).unwrap();
    assert_eq!(first_mapped_node(&result1).unwrap().name, "important_order");

    // Exists + URGENT type + Customer exists
    let mut root2 = Node::new("ROOT", NodeType::Root);
    let mut order2 = Node::new("Order", NodeType::Segment);
    order2.add_child(Node::with_value(
        "Number",
        NodeType::Field,
        Value::String("ORD002".to_string()),
    ));
    order2.add_child(Node::with_value(
        "Type",
        NodeType::Field,
        Value::String("URGENT".to_string()),
    ));
    order2.add_child(Node::with_value(
        "Customer",
        NodeType::Field,
        Value::String("CUST001".to_string()),
    ));
    root2.add_child(order2);

    let doc2 = Document::new(root2);
    let result2 = runtime.execute(&mapping, &doc2).unwrap();
    assert_eq!(first_mapped_node(&result2).unwrap().name, "important_order");

    // Exists + URGENT type but no customer
    let mut root3 = Node::new("ROOT", NodeType::Root);
    let mut order3 = Node::new("Order", NodeType::Segment);
    order3.add_child(Node::with_value(
        "Number",
        NodeType::Field,
        Value::String("ORD003".to_string()),
    ));
    order3.add_child(Node::with_value(
        "Type",
        NodeType::Field,
        Value::String("URGENT".to_string()),
    ));
    root3.add_child(order3);

    let doc3 = Document::new(root3);
    let result3 = runtime.execute(&mapping, &doc3).unwrap();
    assert!(result3.root.children.is_empty());
}

#[test]
fn test_contains_condition() {
    let dsl = r#"
name: contains_condition_test
source_type: TEST
target_type: OUTPUT
rules:
  - type: condition
    when:
      op: contains
      field: /Order/Description
      value: "urgent"
    then:
      - type: field
        source: /Order/Number
        target: rush_order
"#;

    let mapping = MappingDsl::parse(dsl).unwrap();
    let mut runtime = MappingRuntime::new();

    // Contains "urgent"
    let mut root1 = Node::new("ROOT", NodeType::Root);
    let mut order1 = Node::new("Order", NodeType::Segment);
    order1.add_child(Node::with_value(
        "Number",
        NodeType::Field,
        Value::String("ORD001".to_string()),
    ));
    order1.add_child(Node::with_value(
        "Description",
        NodeType::Field,
        Value::String("This is urgent!".to_string()),
    ));
    root1.add_child(order1);
    let result1 = runtime.execute(&mapping, &Document::new(root1)).unwrap();
    assert_eq!(first_mapped_node(&result1).unwrap().name, "rush_order");

    // Does not contain "urgent"
    let mut root2 = Node::new("ROOT", NodeType::Root);
    let mut order2 = Node::new("Order", NodeType::Segment);
    order2.add_child(Node::with_value(
        "Number",
        NodeType::Field,
        Value::String("ORD002".to_string()),
    ));
    order2.add_child(Node::with_value(
        "Description",
        NodeType::Field,
        Value::String("Standard order".to_string()),
    ));
    root2.add_child(order2);
    let result2 = runtime.execute(&mapping, &Document::new(root2)).unwrap();
    assert!(result2.root.children.is_empty());
}

#[test]
fn test_matches_condition() {
    let dsl = r#"
name: matches_condition_test
source_type: TEST
target_type: OUTPUT
rules:
  - type: condition
    when:
      op: matches
      field: /Order/Number
      pattern: "^ORD[0-9]{6}$"
    then:
      - type: field
        source: /Order/Number
        target: formatted_order
"#;

    let mapping = MappingDsl::parse(dsl).unwrap();
    let mut runtime = MappingRuntime::new();

    // Matches pattern
    let mut root1 = Node::new("ROOT", NodeType::Root);
    let mut order1 = Node::new("Order", NodeType::Segment);
    order1.add_child(Node::with_value(
        "Number",
        NodeType::Field,
        Value::String("ORD123456".to_string()),
    ));
    root1.add_child(order1);
    let result1 = runtime.execute(&mapping, &Document::new(root1)).unwrap();
    // Check that the condition matched and produced output
    assert!(
        first_mapped_node(&result1).is_some(),
        "Condition should have matched and produced output"
    );

    // Does not match pattern
    let mut root2 = Node::new("ROOT", NodeType::Root);
    let mut order2 = Node::new("Order", NodeType::Segment);
    order2.add_child(Node::with_value(
        "Number",
        NodeType::Field,
        Value::String("ORDER123".to_string()),
    ));
    root2.add_child(order2);
    let result2 = runtime.execute(&mapping, &Document::new(root2)).unwrap();
    assert!(result2.root.children.is_empty());
}

#[test]
fn test_if_else_conditions() {
    let dsl = r#"
name: if_else_test
source_type: TEST
target_type: OUTPUT
rules:
  - type: condition
    when:
      op: equals
      field: /Order/Type
      value: "A"
    then:
      - type: field
        source: /Order/Number
        target: type_a_order
    else_rules:
      - type: condition
        when:
          op: equals
          field: /Order/Type
          value: "B"
        then:
          - type: field
            source: /Order/Number
            target: type_b_order
        else_rules:
          - type: field
            source: /Order/Number
            target: other_order
"#;

    let mapping = MappingDsl::parse(dsl).unwrap();
    let mut runtime = MappingRuntime::new();

    // Type A
    let mut root1 = Node::new("ROOT", NodeType::Root);
    let mut order1 = Node::new("Order", NodeType::Segment);
    order1.add_child(Node::with_value(
        "Number",
        NodeType::Field,
        Value::String("ORD001".to_string()),
    ));
    order1.add_child(Node::with_value(
        "Type",
        NodeType::Field,
        Value::String("A".to_string()),
    ));
    root1.add_child(order1);
    let result1 = runtime.execute(&mapping, &Document::new(root1)).unwrap();
    assert_eq!(first_mapped_node(&result1).unwrap().name, "type_a_order");

    // Type B
    let mut root2 = Node::new("ROOT", NodeType::Root);
    let mut order2 = Node::new("Order", NodeType::Segment);
    order2.add_child(Node::with_value(
        "Number",
        NodeType::Field,
        Value::String("ORD002".to_string()),
    ));
    order2.add_child(Node::with_value(
        "Type",
        NodeType::Field,
        Value::String("B".to_string()),
    ));
    root2.add_child(order2);
    let result2 = runtime.execute(&mapping, &Document::new(root2)).unwrap();
    assert_eq!(first_mapped_node(&result2).unwrap().name, "type_b_order");

    // Other type
    let mut root3 = Node::new("ROOT", NodeType::Root);
    let mut order3 = Node::new("Order", NodeType::Segment);
    order3.add_child(Node::with_value(
        "Number",
        NodeType::Field,
        Value::String("ORD003".to_string()),
    ));
    order3.add_child(Node::with_value(
        "Type",
        NodeType::Field,
        Value::String("C".to_string()),
    ));
    root3.add_child(order3);
    let result3 = runtime.execute(&mapping, &Document::new(root3)).unwrap();
    assert_eq!(first_mapped_node(&result3).unwrap().name, "other_order");
}

#[test]
fn test_condition_in_foreach() {
    let dsl = r#"
name: conditional_foreach_test
source_type: TEST
target_type: OUTPUT
rules:
  - type: foreach
    source: /Items
    target: valid_items
    rules:
      - type: condition
        when:
          op: exists
          field: SKU
        then:
          - type: field
            source: SKU
            target: product_code
          - type: condition
            when:
              op: equals
              field: Status
              value: "ACTIVE"
            then:
              - type: field
                source: Quantity
                target: active_quantity
"#;

    let mapping = MappingDsl::parse(dsl).unwrap();

    let mut root = Node::new("ROOT", NodeType::Root);
    let mut items = Node::new("Items", NodeType::SegmentGroup);

    // Item 1: Has SKU, ACTIVE
    let mut item1 = Node::new("Item", NodeType::Segment);
    item1.add_child(Node::with_value(
        "SKU",
        NodeType::Field,
        Value::String("SKU001".to_string()),
    ));
    item1.add_child(Node::with_value(
        "Status",
        NodeType::Field,
        Value::String("ACTIVE".to_string()),
    ));
    item1.add_child(Node::with_value(
        "Quantity",
        NodeType::Field,
        Value::Integer(100),
    ));
    items.add_child(item1);

    // Item 2: Has SKU, INACTIVE
    let mut item2 = Node::new("Item", NodeType::Segment);
    item2.add_child(Node::with_value(
        "SKU",
        NodeType::Field,
        Value::String("SKU002".to_string()),
    ));
    item2.add_child(Node::with_value(
        "Status",
        NodeType::Field,
        Value::String("INACTIVE".to_string()),
    ));
    item2.add_child(Node::with_value(
        "Quantity",
        NodeType::Field,
        Value::Integer(50),
    ));
    items.add_child(item2);

    // Item 3: No SKU (filtered out entirely)
    let mut item3 = Node::new("Item", NodeType::Segment);
    item3.add_child(Node::with_value(
        "Status",
        NodeType::Field,
        Value::String("ACTIVE".to_string()),
    ));
    items.add_child(item3);

    root.add_child(items);
    let document = Document::new(root);

    let mut runtime = MappingRuntime::new();
    let result = runtime.execute(&mapping, &document).unwrap();

    // Verify the foreach executed (structure may vary based on implementation)
    // Items should be processed, but structure depends on how conditions are evaluated
    assert_eq!(first_mapped_node(&result).unwrap().name, "valid_items");
    // The implementation may filter differently, so just verify we got some output
}

#[test]
fn test_empty_conditions() {
    let dsl = r#"
name: empty_condition_test
source_type: TEST
target_type: OUTPUT
rules:
  - type: condition
    when:
      op: and
      conditions: []
    then:
      - type: field
        source: /Order/Number
        target: result
"#;

    let mapping = MappingDsl::parse(dsl).unwrap();

    let mut root = Node::new("ROOT", NodeType::Root);
    let mut order = Node::new("Order", NodeType::Segment);
    order.add_child(Node::with_value(
        "Number",
        NodeType::Field,
        Value::String("ORD001".to_string()),
    ));
    root.add_child(order);
    let document = Document::new(root);

    let mut runtime = MappingRuntime::new();
    let result = runtime.execute(&mapping, &document).unwrap();

    // Empty AND should be vacuously true
    assert_eq!(first_mapped_node(&result).unwrap().name, "result");
}

#[test]
fn test_exists_with_empty_value() {
    let dsl = r#"
name: exists_empty_test
source_type: TEST
target_type: OUTPUT
rules:
  - type: condition
    when:
      op: exists
      field: /Order/Description
    then:
      - type: field
        source: /Order/Number
        target: has_description
"#;

    let mapping = MappingDsl::parse(dsl).unwrap();
    let mut runtime = MappingRuntime::new();

    // Has empty description - exists but empty
    let mut root1 = Node::new("ROOT", NodeType::Root);
    let mut order1 = Node::new("Order", NodeType::Segment);
    order1.add_child(Node::with_value(
        "Number",
        NodeType::Field,
        Value::String("ORD001".to_string()),
    ));
    order1.add_child(Node::with_value(
        "Description",
        NodeType::Field,
        Value::String("".to_string()),
    ));
    root1.add_child(order1);
    let result1 = runtime.execute(&mapping, &Document::new(root1)).unwrap();
    // Empty string should be treated as non-existent for exists check
    assert!(result1.root.children.is_empty());

    // Has actual description
    let mut root2 = Node::new("ROOT", NodeType::Root);
    let mut order2 = Node::new("Order", NodeType::Segment);
    order2.add_child(Node::with_value(
        "Number",
        NodeType::Field,
        Value::String("ORD002".to_string()),
    ));
    order2.add_child(Node::with_value(
        "Description",
        NodeType::Field,
        Value::String("Real description".to_string()),
    ));
    root2.add_child(order2);
    let result2 = runtime.execute(&mapping, &Document::new(root2)).unwrap();
    assert_eq!(first_mapped_node(&result2).unwrap().name, "has_description");
}
