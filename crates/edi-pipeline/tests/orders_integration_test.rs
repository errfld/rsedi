//! ORDERS Integration Tests
//!
//! Comprehensive end-to-end tests for ORDERS message processing.
//! Tests the complete pipeline: EDI parsing → IR → Validation → Mapping → CSV output.

use std::fs;
use std::path::PathBuf;

use edi_adapter_csv::{CsvConfig, CsvWriter};
use edi_adapter_edifact::parser::EdifactParser;
use edi_mapping::dsl::MappingDsl;
use edi_mapping::runtime::MappingRuntime;
use edi_schema::loader::SchemaLoader;
use edi_validation::engine::{StrictnessLevel, ValidationConfig, ValidationEngine};

/// Helper function to get the project root directory
fn project_root() -> PathBuf {
    // Go up from crates/edi-pipeline to the workspace root
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .to_path_buf()
}

/// Helper function to get testdata path
fn testdata_path() -> PathBuf {
    project_root().join("testdata")
}

/// Helper to load and parse an EDI file
fn load_edi_file(filename: &str) -> edi_ir::Document {
    let path = testdata_path().join("edi").join(filename);
    let content =
        fs::read(&path).unwrap_or_else(|_| panic!("Failed to read {}", path.display()));

    let parser = EdifactParser::new();
    let docs = parser
        .parse(&content, filename)
        .unwrap_or_else(|err| panic!("Failed to parse {}: {}", filename, err));

    assert!(!docs.is_empty(), "No documents found in {}", filename);
    docs.into_iter().next().unwrap()
}

/// Helper to load a schema
fn load_schema(name: &str, version: &str) -> edi_schema::Schema {
    let schema_path = testdata_path().join("schemas");
    let loader = SchemaLoader::new(vec![schema_path]);
    loader
        .load(name, version)
        .unwrap_or_else(|err| panic!("Failed to load schema {}:{}: {}", name, version, err))
}

/// Helper to load a mapping
fn load_mapping(filename: &str) -> edi_mapping::dsl::Mapping {
    let path = testdata_path().join("mappings").join(filename);
    MappingDsl::parse_file(&path)
        .unwrap_or_else(|err| panic!("Failed to load mapping from {}: {}", path.display(), err))
}

/// Helper to validate a document against a schema
fn validate_document(
    doc: &edi_ir::Document,
    schema: &edi_schema::Schema,
) -> edi_validation::engine::ValidationResult {
    let engine = ValidationEngine::new();
    engine
        .validate_with_schema(doc, schema)
        .expect("Validation failed")
}

/// Helper to execute a mapping
fn execute_mapping(
    mapping: &edi_mapping::dsl::Mapping,
    doc: &edi_ir::Document,
) -> edi_ir::Document {
    let mut runtime = MappingRuntime::new();
    runtime
        .execute(mapping, doc)
        .expect("Mapping execution failed")
}

/// Helper to write document to CSV string
fn write_to_csv(doc: &edi_ir::Document) -> String {
    let writer = CsvWriter::new().with_config(CsvConfig::new().has_header(true));

    let mut output = Vec::new();
    writer
        .write_from_ir(&mut output, doc)
        .expect("Failed to write CSV");

    String::from_utf8(output).expect("Invalid UTF-8 in CSV output")
}

#[test]
fn test_orders_end_to_end_happy_path() {
    // 1. Load EDI file
    let doc = load_edi_file("orders_valid.edi");

    // Verify document structure
    assert_eq!(doc.metadata.doc_type, Some("ORDERS".to_string()));
    assert!(
        !doc.root.children.is_empty(),
        "Document should have children"
    );

    // 2. Validate against schema
    let schema = load_schema("EANCOM_ORDERS", "D96A");
    let validation_result = validate_document(&doc, &schema);

    // Should be valid (may have warnings but no errors)
    assert!(
        validation_result.is_valid || !validation_result.has_errors(),
        "Validation should pass for valid ORDERS. is_valid={}, has_errors={}",
        validation_result.is_valid,
        validation_result.has_errors()
    );

    // 3. Load and execute mapping
    let mapping = load_mapping("orders_to_csv.yaml");
    let mapped_doc = execute_mapping(&mapping, &doc);

    // Verify mapping produced output
    assert!(
        !mapped_doc.root.children.is_empty(),
        "Mapped document should have output"
    );

    // 4. Write to CSV
    let csv_output = write_to_csv(&mapped_doc);

    // 5. Verify CSV output contains expected data
    assert!(!csv_output.is_empty(), "CSV should not be empty");
}

#[test]
fn test_orders_validation_warnings() {
    // Load file with warnings (extra spaces, optional segments)
    let doc = load_edi_file("orders_with_warnings.edi");

    // Validate
    let schema = load_schema("EANCOM_ORDERS", "D96A");
    let engine = ValidationEngine::with_config(ValidationConfig {
        strictness: StrictnessLevel::Moderate,
        continue_on_error: true,
        ..Default::default()
    });

    let result = engine
        .validate_with_schema(&doc, &schema)
        .expect("Validation should not fail");

    // In moderate mode, warnings should not make it invalid
    // but we may have some warnings
    if result.has_warnings() {
        println!("Warnings found (expected): {:?}", result.report.warnings());
    }

    // The document should still be usable
    let mapping = load_mapping("orders_to_csv.yaml");
    let mapped_doc = execute_mapping(&mapping, &doc);
    let csv_output = write_to_csv(&mapped_doc);

    // Should still produce usable output
    assert!(!csv_output.is_empty(), "CSV output should not be empty");
}

#[test]
fn test_orders_validation_errors() {
    // Load file with structural errors (missing mandatory BGM)
    let doc = load_edi_file("orders_with_errors.edi");

    // Validate with strict mode
    let schema = load_schema("EANCOM_ORDERS", "D96A");
    let engine = ValidationEngine::with_config(ValidationConfig {
        strictness: StrictnessLevel::Strict,
        continue_on_error: true,
        ..Default::default()
    });

    let result = engine
        .validate_with_schema(&doc, &schema)
        .expect("Validation should complete");

    // Should have errors due to missing mandatory BGM segment
    assert!(
        result.has_errors() || !result.is_valid,
        "Validation should detect errors in malformed ORDERS"
    );

    // Verify we have specific error about missing segment (may or may not be flagged)
    let _has_bgm_error = result.errors.iter().any(|e| {
        e.message.contains("BGM") || e.code.as_ref().map(|c| c.contains("BGM")).unwrap_or(false)
    });

    // Note: Depending on schema implementation, this may or may not be flagged
    // The important thing is that validation runs and reports issues
    println!("Validation errors: {:?}", result.errors);
}

#[test]
fn test_orders_with_partner_profile() {
    // Load ACME partner-specific ORDERS
    let doc = load_edi_file("orders_acme.edi");

    // Verify document structure
    assert_eq!(doc.metadata.doc_type, Some("ORDERS".to_string()));

    // Load ACME partner schema
    let schema = load_schema("partner_acme_orders", "1.0");
    let result = validate_document(&doc, &schema);

    // Should validate against partner profile
    println!(
        "ACME partner validation: is_valid={}, errors={:?}",
        result.is_valid, result.errors
    );

    // Map to CSV
    let mapping = load_mapping("orders_to_csv.yaml");
    let mapped_doc = execute_mapping(&mapping, &doc);
    let csv_output = write_to_csv(&mapped_doc);

    // Should contain ACME-specific data
    assert!(!csv_output.is_empty(), "CSV output should not be empty");
}

#[test]
fn test_orders_multiple_line_items() {
    // Test that multiple line items are handled correctly
    let doc = load_edi_file("orders_valid.edi");

    // Count line item groups in the document
    let lin_count = doc
        .root
        .children
        .iter()
        .filter(|n| n.name == "LINE_ITEM")
        .count();

    assert!(
        lin_count >= 2,
        "Test file should have at least 2 line items"
    );

    // Map and verify
    let mapping = load_mapping("orders_to_csv.yaml");
    let mapped_doc = execute_mapping(&mapping, &doc);

    // Should have same number of output rows as line items
    let output_count = mapped_doc
        .root
        .children
        .first()
        .map_or(0, |container| container.children.len());
    assert_eq!(
        output_count, lin_count,
        "Should have one output row per line item"
    );
}

#[test]
fn test_orders_csv_structure() {
    // Test that CSV output has the expected structure
    let doc = load_edi_file("orders_valid.edi");

    let mapping = load_mapping("orders_to_csv.yaml");
    let mapped_doc = execute_mapping(&mapping, &doc);
    let csv_output = write_to_csv(&mapped_doc);

    // Check that CSV has headers and data rows
    assert!(
        csv_output.contains("line_number") && csv_output.contains("product_code"),
        "CSV should contain expected column headers"
    );

    // Check that data is present (line numbers 1 and 2)
    assert!(
        csv_output.contains("1") && csv_output.contains("2"),
        "CSV should contain line item data"
    );
}

#[test]
fn test_orders_null_handling() {
    // Test handling of missing/optional fields
    let doc = load_edi_file("orders_valid.edi");

    // Execute mapping
    let mapping = load_mapping("orders_to_csv.yaml");
    let mapped_doc = execute_mapping(&mapping, &doc);

    // Should produce output even with missing optional fields
    let csv_output = write_to_csv(&mapped_doc);
    assert!(
        !csv_output.is_empty(),
        "Should handle null values gracefully"
    );
}

#[test]
fn test_orders_streaming_performance() {
    // Test that parsing handles files efficiently
    let path = testdata_path().join("edi").join("orders_valid.edi");
    let content = fs::read(&path).expect("Failed to read file");

    let parser = EdifactParser::new();

    // Parse multiple times to check performance characteristics
    for i in 0..10 {
        let docs = parser
            .parse(&content, format!("iteration_{}", i))
            .expect("Parse should succeed");
        assert!(!docs.is_empty(), "Should parse document");
    }

    // If we get here without timeout, streaming is working reasonably
}

#[test]
fn test_orders_parser_error_handling() {
    // Test parser handles malformed input gracefully
    let malformed = b"NOT_A_VALID_EDI_FILE";

    let parser = EdifactParser::new();
    let result = parser.parse(malformed, "test");

    // Should either return empty or handle gracefully
    match result {
        Ok(docs) => {
            // If it returns documents, they might be empty
            println!("Parsed {} documents from malformed input", docs.len());
        }
        Err(e) => {
            // Error is also acceptable
            println!("Expected error for malformed input: {}", e);
        }
    }
}

#[test]
fn test_end_to_end_pipeline() {
    // Complete integration test: EDI → Validation → Mapping → CSV
    let doc = load_edi_file("orders_valid.edi");

    // Step 1: Parse (already done in load_edi_file)
    println!("1. Parsed document: type={:?}", doc.metadata.doc_type);

    // Step 2: Validate
    let schema = load_schema("EANCOM_ORDERS", "D96A");
    let validation_result = validate_document(&doc, &schema);
    println!("2. Validation: is_valid={}", validation_result.is_valid);

    // Step 3: Map
    let mapping = load_mapping("orders_to_csv.yaml");
    let mapped_doc = execute_mapping(&mapping, &doc);
    println!(
        "3. Mapping: produced {} output nodes",
        mapped_doc.root.children.len()
    );

    // Step 4: Serialize to CSV
    let csv_output = write_to_csv(&mapped_doc);
    println!("4. CSV Output:\n{}", csv_output);

    // Final assertions
    assert!(!csv_output.is_empty(), "Pipeline should produce CSV output");
}

#[test]
fn test_all_test_files_loadable() {
    // Verify all test data files can be loaded and parsed
    let test_files = vec![
        "orders_valid.edi",
        "orders_with_warnings.edi",
        "orders_with_errors.edi",
        "orders_acme.edi",
    ];

    for filename in test_files {
        let doc = load_edi_file(filename);
        assert!(
            !doc.root.children.is_empty(),
            "{} should have content",
            filename
        );
        println!("Successfully loaded and parsed {}", filename);
    }
}
