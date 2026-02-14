use std::fs;
use std::path::PathBuf;

use edi_adapter_edifact::EdifactParser;
use edi_ir::{Document, NodeType};
use edi_schema::SchemaLoader;
use edi_validation::ValidationEngine;

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn normalize_document_for_validation(document: &Document) -> Document {
    let mut normalized = document.clone();
    if normalized.root.node_type == NodeType::Message {
        normalized.root.node_type = NodeType::Root;
        normalized.root.name = "ROOT".to_string();
    }
    normalized
}

fn validate_desadv_fixture(file_name: &str) {
    let root = repo_root();
    let schema_path = root.join("testdata/schemas/eancom_desadv_d96a.yaml");
    let edi_path = root.join(format!("testdata/edi/{file_name}"));

    let schema_loader = SchemaLoader::new(Vec::new());
    let schema = schema_loader
        .load_from_file(&schema_path)
        .expect("schema should load");

    let parser = EdifactParser::new();
    let data = fs::read(&edi_path).expect("edi fixture should load");
    let outcome = parser
        .parse_with_warnings(&data, edi_path.to_string_lossy().as_ref())
        .expect("edi should parse");

    assert!(
        !outcome.documents.is_empty(),
        "expected at least one message"
    );

    let engine = ValidationEngine::new();
    for document in &outcome.documents {
        let normalized = normalize_document_for_validation(document);
        let result = engine
            .validate_with_schema(&normalized, &schema)
            .expect("validation should run");
        assert!(!result.has_errors(), "unexpected validation errors");
    }
}

#[test]
fn desadv_minimal_fixture_validates_against_schema() {
    validate_desadv_fixture("valid_desadv_d96a_minimal.edi");
}

#[test]
fn desadv_full_fixture_validates_against_schema() {
    validate_desadv_fixture("valid_desadv_d96a_full.edi");
}
