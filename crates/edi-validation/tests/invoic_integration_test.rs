use std::fs;
use std::path::PathBuf;

use edi_adapter_edifact::EdifactParser;
use edi_ir::{Document, NodeType};
use edi_schema::SchemaLoader;
use edi_validation::{ValidationEngine, ValidationResult};

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

fn validate_invoic_fixture(file_name: &str) -> Vec<ValidationResult> {
    let root = repo_root();
    let schema_path = root.join("testdata/schemas/eancom_invoic_d96a.yaml");
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
    let mut results = Vec::new();

    for document in &outcome.documents {
        let normalized = normalize_document_for_validation(document);
        let result = engine
            .validate_with_schema(&normalized, &schema)
            .expect("validation should run");
        results.push(result);
    }

    results
}

#[test]
fn invoic_minimal_fixture_validates_against_schema() {
    let results = validate_invoic_fixture("valid_invoic_d96a_minimal.edi");
    assert!(
        results.iter().all(|result| !result.has_errors()),
        "unexpected validation errors"
    );
}

#[test]
fn invoic_full_fixture_validates_against_schema() {
    let results = validate_invoic_fixture("valid_invoic_d96a_full.edi");
    assert!(
        results.iter().all(|result| !result.has_errors()),
        "unexpected validation errors"
    );
}

#[test]
fn invoic_missing_bgm_fixture_reports_validation_errors() {
    let results = validate_invoic_fixture("invalid_invoic_d96a_missing_bgm.edi");
    assert!(
        results.iter().any(ValidationResult::has_errors),
        "expected validation errors for missing mandatory BGM segment"
    );
}

#[test]
fn invoic_invalid_fixture_reports_structured_diagnostics() {
    let results = validate_invoic_fixture("invalid_invoic_d96a_missing_element.edi");
    assert!(
        results.iter().any(ValidationResult::has_errors),
        "expected validation errors for missing mandatory element"
    );

    let issue = results
        .iter()
        .flat_map(|result| result.report.all_issues().iter())
        .find(|issue| issue.code.as_deref() == Some("MISSING_MANDATORY_ELEMENT"))
        .expect("expected missing mandatory element diagnostic");

    assert!(
        issue.path.contains("BGM") && issue.path.contains("e2"),
        "expected segment/element path context for missing element diagnostic"
    );
    assert!(
        issue.message.contains("Mandatory element"),
        "expected actionable missing element message"
    );
}
