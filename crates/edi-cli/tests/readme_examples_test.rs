use std::env;
use std::fs;
use std::path::PathBuf;
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

fn cargo_bin() -> PathBuf {
    if let Ok(path) = env::var("CARGO_BIN_EXE_edi") {
        return PathBuf::from(path);
    }

    let target_dir = env::var("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| repo_root().join("target"));
    let executable_name = format!("edi{}", std::env::consts::EXE_SUFFIX);
    let fallback = target_dir.join("debug").join(executable_name);

    if fallback.exists() {
        return fallback;
    }

    panic!(
        "CARGO_BIN_EXE_edi is not set and fallback binary was not found at {}",
        fallback.display()
    );
}

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
}

fn testdata_path(path: &str) -> PathBuf {
    repo_root().join(path)
}

fn unique_temp_path(name: &str, extension: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time after epoch")
        .as_nanos();
    let filename = format!("edi-cli-{name}-{}-{nanos}.{extension}", std::process::id());
    env::temp_dir().join(filename)
}

fn write_temp_mapping(name: &str, yaml: &str) -> PathBuf {
    let path = unique_temp_path(name, "yaml");
    fs::write(&path, yaml).expect("mapping file should be writable");
    path
}

fn assert_exit_code(status: std::process::ExitStatus, expected: i32, context: &str) {
    let code = status.code().unwrap_or(-1);
    assert_eq!(code, expected, "{context} (exit code {code})");
}

#[test]
fn readme_validate_valid_orders_has_no_errors() {
    let binary = cargo_bin();
    let input = testdata_path("testdata/edi/valid_orders_d96a_minimal.edi");
    let schema = testdata_path("testdata/schemas/eancom_orders_d96a.yaml");

    let status = Command::new(binary)
        .args([
            "validate",
            input.to_string_lossy().as_ref(),
            "-s",
            schema.to_string_lossy().as_ref(),
        ])
        .status()
        .expect("run edi validate");

    let code = status.code().unwrap_or(-1);
    assert!(
        code == 0 || code == 1,
        "expected no validation errors (exit code {code})"
    );
}

#[test]
fn readme_validate_invalid_orders_reports_errors() {
    let binary = cargo_bin();
    let input = testdata_path("testdata/edi/invalid_orders_missing_bgm.edi");
    let schema = testdata_path("testdata/schemas/eancom_orders_d96a.yaml");

    let status = Command::new(binary)
        .args([
            "validate",
            input.to_string_lossy().as_ref(),
            "-s",
            schema.to_string_lossy().as_ref(),
        ])
        .status()
        .expect("run edi validate");

    assert_exit_code(status, 2, "expected validation errors for missing BGM");
}

#[test]
fn readme_transform_orders_to_json_writes_output() {
    let binary = cargo_bin();
    let input = testdata_path("testdata/edi/valid_orders_d96a_minimal.edi");
    let mapping = testdata_path("testdata/mappings/orders_to_json.yaml");
    let output = unique_temp_path("orders-transform", "json");

    let command_output = Command::new(binary)
        .args([
            "transform",
            input.to_string_lossy().as_ref(),
            output.to_string_lossy().as_ref(),
            "-m",
            mapping.to_string_lossy().as_ref(),
        ])
        .output()
        .expect("run edi transform");

    assert!(
        command_output.status.success(),
        "expected transform to succeed; stdout: {}; stderr: {}",
        String::from_utf8_lossy(&command_output.stdout),
        String::from_utf8_lossy(&command_output.stderr)
    );

    let bytes = fs::read(&output).expect("transform output should be readable");
    assert!(!bytes.is_empty(), "transform output should not be empty");

    let value: serde_json::Value =
        serde_json::from_slice(&bytes).expect("transform output should be valid JSON");
    assert!(!value.is_null(), "transform output should not be JSON null");

    let _ = fs::remove_file(&output);
}

#[test]
fn transform_csv_target_writes_csv_output() {
    let binary = cargo_bin();
    let input = testdata_path("testdata/edi/valid_orders_d96a_full.edi");
    let mapping = testdata_path("testdata/mappings/orders_to_csv.yaml");
    let output = unique_temp_path("orders-transform", "csv");

    let command_output = Command::new(binary)
        .args([
            "transform",
            input.to_string_lossy().as_ref(),
            output.to_string_lossy().as_ref(),
            "-m",
            mapping.to_string_lossy().as_ref(),
        ])
        .output()
        .expect("run edi transform");

    assert!(
        command_output.status.success(),
        "expected CSV transform to succeed; stdout: {}; stderr: {}",
        String::from_utf8_lossy(&command_output.stdout),
        String::from_utf8_lossy(&command_output.stderr)
    );

    let csv_output = fs::read_to_string(&output).expect("CSV output should be readable");
    assert!(
        csv_output.starts_with("document_type,line_number,product_code"),
        "unexpected CSV header: {csv_output}"
    );
    assert!(
        csv_output.contains("No description supplied"),
        "expected mapped field in CSV output: {csv_output}"
    );

    let _ = fs::remove_file(&output);
}

#[test]
fn transform_eancom_target_writes_edifact_output() {
    let binary = cargo_bin();
    let input = testdata_path("testdata/edi/valid_orders_d96a_minimal.edi");
    let mapping = write_temp_mapping(
        "orders-to-edifact",
        r#"
name: orders_to_edifact
source_type: EANCOM_D96A_ORDERS
target_type: EANCOM_D96A_ORDERS
rules:
  - type: field
    source: /UNH/e1
    target: UNH.e1
  - type: field
    source: /BGM/e1
    target: BGM.e1
  - type: field
    source: /BGM/e2
    target: BGM.e2
"#,
    );
    let output = unique_temp_path("orders-transform-edifact", "edi");

    let command_output = Command::new(binary)
        .args([
            "transform",
            input.to_string_lossy().as_ref(),
            output.to_string_lossy().as_ref(),
            "-m",
            mapping.to_string_lossy().as_ref(),
        ])
        .output()
        .expect("run edi transform");

    assert!(
        command_output.status.success(),
        "expected EDI transform to succeed; stdout: {}; stderr: {}",
        String::from_utf8_lossy(&command_output.stdout),
        String::from_utf8_lossy(&command_output.stderr)
    );

    let rendered = fs::read_to_string(&output).expect("EDI output should be readable");
    assert!(
        rendered.contains("UNH+1'"),
        "expected serialized UNH segment, got: {rendered}"
    );
    assert!(
        rendered.contains("BGM+220+ORDER123'"),
        "expected serialized BGM segment, got: {rendered}"
    );

    let _ = fs::remove_file(&output);
    let _ = fs::remove_file(&mapping);
}

#[test]
fn transform_eancom_target_with_invalid_shape_fails() {
    let binary = cargo_bin();
    let input = testdata_path("testdata/edi/valid_orders_d96a_minimal.edi");
    let mapping = write_temp_mapping(
        "orders-to-invalid-edifact-shape",
        r#"
name: invalid_edi_shape
source_type: EANCOM_D96A_ORDERS
target_type: EANCOM_D96A_ORDERS
rules:
  - type: field
    source: /BGM/e2
    target: order_number
"#,
    );
    let output = unique_temp_path("orders-transform-invalid-edifact", "edi");

    let command_output = Command::new(binary)
        .args([
            "transform",
            input.to_string_lossy().as_ref(),
            output.to_string_lossy().as_ref(),
            "-m",
            mapping.to_string_lossy().as_ref(),
        ])
        .output()
        .expect("run edi transform");

    assert_exit_code(
        command_output.status,
        2,
        "expected EDI transform with invalid shape to fail",
    );

    let stderr = String::from_utf8_lossy(&command_output.stderr);
    assert!(
        stderr.contains("No serializable EDIFACT segments found"),
        "expected actionable EDIFACT shape error, got stderr: {stderr}"
    );

    let _ = fs::remove_file(&output);
    let _ = fs::remove_file(&mapping);
}
