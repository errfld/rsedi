use std::borrow::Cow;
use std::fs;
use std::path::PathBuf;

use edi_adapter_edifact::EdifactParser;
use edi_ir::{Document, NodeType};
use edi_schema::SchemaLoader;
use edi_validation::{StrictnessLevel, ValidationConfig, ValidationEngine, ValidationResult};

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn normalize_document_for_validation<'a>(document: &'a Document) -> Cow<'a, Document> {
    if document.root.node_type == NodeType::Message {
        let mut normalized = document.clone();
        normalized.root.node_type = NodeType::Root;
        normalized.root.name = "ROOT".to_string();
        Cow::Owned(normalized)
    } else {
        Cow::Borrowed(document)
    }
}

fn validate_ordrsp_fixture(file_name: &str) -> (Vec<ValidationResult>, usize) {
    let root = repo_root();
    let schema_path = root.join("testdata/schemas/eancom_ordrsp_d96a.yaml");
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
            .validate_with_schema(normalized.as_ref(), &schema)
            .expect("validation should run");
        results.push(result);
    }

    (results, parse_warning_count)
}

#[test]
fn ordrsp_minimal_fixture_validates_against_schema() {
    let (results, parse_warning_count) = validate_ordrsp_fixture("valid_ordrsp_d96a_minimal.edi");
    assert_eq!(
        parse_warning_count, 0,
        "unexpected parse warnings for valid ORDRSP minimal fixture"
    );
    assert!(
        results
            .iter()
            .all(|result| !result.has_errors() && !result.has_warnings()),
        "unexpected validation errors or warnings"
    );
}

#[test]
fn ordrsp_full_fixture_validates_against_schema() {
    let (results, parse_warning_count) = validate_ordrsp_fixture("valid_ordrsp_d96a_full.edi");
    assert_eq!(
        parse_warning_count, 0,
        "unexpected parse warnings for valid ORDRSP full fixture"
    );
    assert!(
        results
            .iter()
            .all(|result| !result.has_errors() && !result.has_warnings()),
        "unexpected validation errors or warnings"
    );
}

#[test]
fn ordrsp_cnt_fixture_validates_against_schema() {
    let (results, parse_warning_count) = validate_ordrsp_fixture("valid_ordrsp_d96a_with_cnt.edi");
    assert_eq!(
        parse_warning_count, 0,
        "unexpected parse warnings for valid ORDRSP CNT fixture"
    );
    assert!(
        results
            .iter()
            .all(|result| !result.has_errors() && !result.has_warnings()),
        "valid ORDRSP CNT fixture should not report validation errors or warnings"
    );
}

#[test]
fn ordrsp_missing_bgm_fixture_honors_strictness_levels() {
    let root = repo_root();
    let schema_path = root.join("testdata/schemas/eancom_ordrsp_d96a.yaml");
    let edi_path = root.join("testdata/edi/invalid_ordrsp_d96a_missing_bgm.edi");

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
        "expected at least one parsed message"
    );

    let strict_engine = ValidationEngine::with_config(ValidationConfig {
        strictness: StrictnessLevel::Strict,
        ..ValidationConfig::default()
    });
    let lenient_engine = ValidationEngine::with_config(ValidationConfig {
        strictness: StrictnessLevel::Lenient,
        ..ValidationConfig::default()
    });

    for document in &outcome.documents {
        let normalized = normalize_document_for_validation(document);

        let strict_result = strict_engine
            .validate_with_schema(normalized.as_ref(), &schema)
            .expect("strict validation should run");
        assert!(
            strict_result.has_errors(),
            "strict mode should fail when mandatory BGM segment is missing"
        );

        let lenient_result = lenient_engine
            .validate_with_schema(normalized.as_ref(), &schema)
            .expect("lenient validation should run");
        assert!(
            !lenient_result.has_errors() && lenient_result.has_warnings(),
            "lenient mode should downgrade missing BGM to warning-only outcome"
        );
    }
}

#[test]
fn ordrsp_missing_bgm_fixture_reports_validation_errors() {
    let (results, _parse_warning_count) =
        validate_ordrsp_fixture("invalid_ordrsp_d96a_missing_bgm.edi");
    assert!(
        results.iter().any(ValidationResult::has_errors),
        "expected validation errors for missing mandatory BGM segment"
    );
    let missing_segment_issue = results
        .iter()
        .flat_map(|result| result.report.all_issues())
        .find(|issue| issue.code.as_deref() == Some("MISSING_MANDATORY_SEGMENT"));
    let issue = missing_segment_issue.expect("expected MISSING_MANDATORY_SEGMENT error code");
    assert!(
        issue.segment_pos.is_some() || !issue.path.is_empty() || issue.message.contains("BGM"),
        "missing segment error should include path/position context or mention BGM"
    );
}
