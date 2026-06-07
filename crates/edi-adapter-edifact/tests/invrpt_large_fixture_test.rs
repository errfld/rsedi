use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;

use edi_adapter_edifact::EdifactParser;
use edi_ir::{Node, NodeType};

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn count_segments(node: &Node, counts: &mut HashMap<String, usize>) {
    if matches!(
        node.node_type,
        NodeType::Interchange | NodeType::Message | NodeType::Segment
    ) {
        *counts.entry(node.name.clone()).or_default() += 1;
    }

    for child in &node.children {
        count_segments(child, counts);
    }
}

#[test]
fn large_invrpt_d96a_fixture_parses_with_deterministic_message_and_segment_counts() {
    let edi_path = repo_root().join("testdata/edi/valid_invrpt_d96a_large.edi");
    let data = fs::read(&edi_path).expect("large INVRPT fixture should load");

    let parser = EdifactParser::new();
    let outcome = parser
        .parse_with_warnings(&data, edi_path.to_string_lossy().as_ref())
        .expect("large INVRPT fixture should parse");

    assert_eq!(outcome.warnings.len(), 0, "valid fixture should not warn");
    assert_eq!(
        outcome.documents.len(),
        42,
        "expected one document per UNH/UNT message"
    );

    let mut segment_counts = HashMap::new();
    for document in &outcome.documents {
        count_segments(&document.root, &mut segment_counts);
    }

    assert_eq!(segment_counts.get("UNH"), Some(&42));
    assert_eq!(segment_counts.get("BGM"), Some(&42));
    assert_eq!(segment_counts.get("LIN"), Some(&12_993));
    assert_eq!(segment_counts.get("QTY"), Some(&12_993));
    assert_eq!(segment_counts.get("UNT"), Some(&42));
}
