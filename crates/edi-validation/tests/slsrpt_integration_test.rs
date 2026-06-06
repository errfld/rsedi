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

fn validate_slsrpt_fixture(file_name: &str) -> (Vec<ValidationResult>, usize) {
    let root = repo_root();
    let schema_path = root.join("testdata/schemas/eancom_slsrpt_d96a.yaml");
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

    let parse_warning_count = outcome.warnings.len();
    let engine = ValidationEngine::new();
    let mut results = Vec::new();

    for document in &outcome.documents {
        let normalized = normalize_document_for_validation(document);
        let result = engine
            .validate_with_schema(&normalized, &schema)
            .expect("validation should run");
        results.push(result);
    }

    (results, parse_warning_count)
}

#[test]
fn slsrpt_minimal_fixture_validates_against_schema() {
    let (results, parse_warning_count) = validate_slsrpt_fixture("valid_slsrpt_d96a_minimal.edi");
    assert_eq!(
        parse_warning_count, 0,
        "unexpected parse warnings for valid SLSRPT minimal fixture"
    );
    assert!(
        results
            .iter()
            .all(|result| !result.has_errors() && !result.has_warnings()),
        "unexpected validation errors or warnings"
    );
}

#[test]
fn slsrpt_full_fixture_validates_against_schema() {
    let (results, parse_warning_count) = validate_slsrpt_fixture("valid_slsrpt_d96a_full.edi");
    assert_eq!(
        parse_warning_count, 0,
        "unexpected parse warnings for valid SLSRPT full fixture"
    );
    assert!(
        results
            .iter()
            .all(|result| !result.has_errors() && !result.has_warnings()),
        "unexpected validation errors or warnings"
    );
}

#[test]
fn slsrpt_missing_bgm_fixture_reports_validation_errors() {
    let (results, _parse_warning_count) =
        validate_slsrpt_fixture("invalid_slsrpt_d96a_missing_bgm.edi");
    assert!(
        results.iter().any(ValidationResult::has_errors),
        "expected validation errors for missing mandatory BGM segment"
    );
    let has_missing_segment_error = results
        .iter()
        .flat_map(|result| result.report.all_issues())
        .any(|issue| issue.code.as_deref() == Some("MISSING_MANDATORY_SEGMENT"));
    assert!(
        has_missing_segment_error,
        "expected MISSING_MANDATORY_SEGMENT error code"
    );
}

#[test]
fn slsrpt_line_item_without_qty_reports_validation_errors() {
    let (results, _parse_warning_count) =
        validate_slsrpt_fixture("invalid_slsrpt_d96a_line_without_qty.edi");
    let missing_qty_issue = results
        .iter()
        .flat_map(|result| result.report.all_issues())
        .find(|issue| {
            issue.code.as_deref() == Some("MISSING_MANDATORY_SEGMENT")
                && issue.message.contains("QTY")
        });

    assert!(
        missing_qty_issue.is_some(),
        "expected missing mandatory QTY to be reported for the incomplete line item"
    );
}
