use edi_ir::{Document, Node, NodeType, Value};
use edi_mapping::diagnostics::explain_mapping;
use edi_mapping::dsl::MappingDsl;
use edi_mapping::{MappingRuntime, lint_mapping};

fn field<'a>(node: &'a Node, name: &str) -> &'a Value {
    node.find_child(name)
        .and_then(|child| child.value.as_ref())
        .unwrap_or_else(|| panic!("missing field {name}"))
}

fn line(quantity: f64, net_amount: f64, sku: &str) -> Node {
    let mut line = Node::new("line", NodeType::Record);
    line.add_child(Node::with_value(
        "quantity",
        NodeType::Field,
        Value::Decimal(quantity),
    ));
    line.add_child(Node::with_value(
        "net_amount",
        NodeType::Field,
        Value::Decimal(net_amount),
    ));
    line.add_child(Node::with_value(
        "sku",
        NodeType::Field,
        Value::String(sku.to_string()),
    ));
    line
}

fn order_document() -> Document {
    let mut root = Node::new("order", NodeType::Root);
    let mut lines = Node::new("lines", NodeType::SegmentGroup);
    lines.add_child(line(2.0, 19.5, "SKU-1"));
    lines.add_child(line(3.0, 7.25, "SKU-2"));
    lines.add_child(line(4.0, 11.0, "SKU-1"));
    root.add_child(lines);
    Document::new(root)
}

#[test]
fn aggregate_rules_compute_order_totals_and_distinct_values() {
    let yaml = r#"
name: order_totals
source_type: order
target_type: order_summary
rules:
  - type: aggregate
    source: /lines/line/quantity
    target: total_quantity
    op: sum
  - type: aggregate
    source: /lines/line
    target: line_count
    op: count
  - type: aggregate
    source: /lines/line/net_amount
    target: first_amount
    op: first
  - type: aggregate
    source: /lines/line/net_amount
    target: last_amount
    op: last
  - type: aggregate
    source: /lines/line/sku
    target: distinct_skus
    op: distinct
"#;

    let mapping = MappingDsl::parse(yaml).expect("mapping parses");
    assert!(lint_mapping(&mapping).is_empty());
    assert!(
        explain_mapping(&mapping).contains("aggregate sum /lines/line/quantity -> total_quantity")
    );

    let mut runtime = MappingRuntime::new();
    let output = runtime
        .execute(&mapping, &order_document())
        .expect("mapping executes");

    let summary = output
        .root
        .find_child("total_quantity")
        .expect("first mapped node");
    assert_eq!(summary.name, "total_quantity");
    assert_eq!(summary.value, Some(Value::Decimal(9.0)));
    assert_eq!(field(summary, "line_count"), &Value::Integer(3));
    assert_eq!(field(summary, "first_amount"), &Value::Decimal(19.5));
    assert_eq!(field(summary, "last_amount"), &Value::Decimal(11.0));
    assert_eq!(
        field(summary, "distinct_skus"),
        &Value::String("SKU-1,SKU-2".to_string())
    );
}

#[test]
fn aggregate_rules_report_path_and_values_on_numeric_errors() {
    let yaml = r#"
name: bad_total
source_type: order
target_type: order_summary
rules:
  - type: aggregate
    source: /lines/line/sku
    target: total_sku
    op: sum
"#;
    let mapping = MappingDsl::parse(yaml).expect("mapping parses");
    let mut runtime = MappingRuntime::new();
    let err = runtime
        .execute(&mapping, &order_document())
        .expect_err("sum over strings should fail");
    let message = err.to_string();
    assert!(message.contains("aggregate rule"), "{message}");
    assert!(message.contains("/lines/line/sku"), "{message}");
    assert!(message.contains("SKU-1"), "{message}");
}
