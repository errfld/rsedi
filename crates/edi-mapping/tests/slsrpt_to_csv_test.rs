use std::path::PathBuf;

use edi_ir::{Document, Node, NodeType, Value};
use edi_mapping::{MappingDsl, MappingRuntime};

fn mapping_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../testdata/mappings/slsrpt_to_csv.yaml")
}

fn find_field_value<'a>(node: &'a Node, field_name: &str) -> Option<&'a Value> {
    let mut stack = vec![node];
    while let Some(current) = stack.pop() {
        if current.name == field_name {
            return current.value.as_ref();
        }
        for child in &current.children {
            stack.push(child);
        }
    }
    None
}

fn create_line_item(line_no: &str, item_id: &str, qty: &str, amount: &str) -> Node {
    let mut line_item = Node::new("LINE_ITEM", NodeType::SegmentGroup);

    let mut lin = Node::new("LIN", NodeType::Segment);
    lin.add_child(Node::with_value(
        "e1",
        NodeType::Element,
        Value::String(line_no.to_string()),
    ));
    let mut lin_e3 = Node::new("e3", NodeType::Element);
    lin_e3.add_child(Node::with_value(
        "c1",
        NodeType::Component,
        Value::String(item_id.to_string()),
    ));
    lin.add_child(lin_e3);
    line_item.add_child(lin);

    let mut qty_segment = Node::new("QTY", NodeType::Segment);
    let mut qty_e1 = Node::new("e1", NodeType::Element);
    qty_e1.add_child(Node::with_value(
        "c1",
        NodeType::Component,
        Value::String("153".to_string()),
    ));
    qty_e1.add_child(Node::with_value(
        "c2",
        NodeType::Component,
        Value::String(qty.to_string()),
    ));
    qty_e1.add_child(Node::with_value(
        "c3",
        NodeType::Component,
        Value::String("PCE".to_string()),
    ));
    qty_segment.add_child(qty_e1);
    line_item.add_child(qty_segment);

    let mut moa_segment = Node::new("MOA", NodeType::Segment);
    let mut moa_e1 = Node::new("e1", NodeType::Element);
    moa_e1.add_child(Node::with_value(
        "c1",
        NodeType::Component,
        Value::String("203".to_string()),
    ));
    moa_e1.add_child(Node::with_value(
        "c2",
        NodeType::Component,
        Value::String(amount.to_string()),
    ));
    moa_segment.add_child(moa_e1);
    line_item.add_child(moa_segment);

    line_item
}

fn create_slsrpt_document() -> Document {
    let mut root = Node::new("ROOT", NodeType::Root);

    let mut bgm = Node::new("BGM", NodeType::Segment);
    bgm.add_child(Node::with_value(
        "e2",
        NodeType::Element,
        Value::String("SLSRPT001".to_string()),
    ));
    root.add_child(bgm);

    root.add_child(create_line_item("1", "4006381333931", "120", "359.88"));
    root.add_child(create_line_item("2", "4006381333948", "80", "239.92"));

    Document::new(root)
}

#[test]
fn slsrpt_to_csv_maps_report_number_for_each_row() {
    let mapping = MappingDsl::parse_file(&mapping_path()).expect("mapping should parse");
    let document = create_slsrpt_document();
    let mut runtime = MappingRuntime::new();

    let result = runtime
        .execute(&mapping, &document)
        .expect("mapping execution should succeed");

    let rows = result
        .root
        .children
        .first()
        .expect("mapping should produce rows container");
    assert_eq!(rows.name, "rows");
    assert_eq!(rows.children.len(), 2);

    let report_numbers: Vec<String> = rows
        .children
        .iter()
        .map(|row| {
            find_field_value(row, "report_number")
                .and_then(Value::as_string)
                .expect("report_number should be present for each row")
        })
        .collect();
    assert_eq!(report_numbers, vec!["SLSRPT001", "SLSRPT001"]);

    let quantities: Vec<String> = rows
        .children
        .iter()
        .map(|row| {
            find_field_value(row, "quantity")
                .and_then(Value::as_string)
                .expect("quantity should be present for each row")
        })
        .collect();
    assert_eq!(quantities, vec!["120", "80"]);
}
