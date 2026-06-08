use std::env;
use std::fs;
use std::path::PathBuf;
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

static TEMP_FILE_COUNTER: AtomicU64 = AtomicU64::new(0);

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
    let counter = TEMP_FILE_COUNTER.fetch_add(1, Ordering::SeqCst);
    let filename = format!(
        "edi-cli-{name}-{}-{nanos}-{counter}.{extension}",
        std::process::id()
    );
    env::temp_dir().join(filename)
}

#[test]
fn mapping_lint_reports_unsupported_selector_before_transform_runtime() {
    let binary = cargo_bin();
    let mapping_path = unique_temp_path("mapping-lint-selector", "yaml");
    fs::write(
        &mapping_path,
        r#"
name: unsupported_selector
source_type: EANCOM_ORDERS
target_type: JSON_ORDERS
rules:
  - type: field
    source: /DTM[bad='137']/e2
    target: requested_date
"#,
    )
    .expect("write temp mapping");

    let output = Command::new(binary)
        .args(["mapping", "lint", mapping_path.to_string_lossy().as_ref()])
        .output()
        .expect("run edi mapping lint");

    let code = output.status.code().unwrap_or(-1);
    assert_eq!(code, 1, "lint with warnings should exit 1");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("unsupported selector key 'bad'"),
        "stdout: {stdout}"
    );
    assert!(stdout.contains("/DTM[bad='137']/e2"), "stdout: {stdout}");

    let _ = fs::remove_file(mapping_path);
}

#[test]
fn mapping_lint_with_schema_suggests_closest_segment_for_typo() {
    let binary = cargo_bin();
    let mapping_path = unique_temp_path("mapping-lint-schema-typo", "yaml");
    let schema = testdata_path("testdata/schemas/eancom_orders_d96a.yaml");
    fs::write(
        &mapping_path,
        r#"
name: path_typo
source_type: EANCOM_ORDERS
target_type: JSON_ORDERS
rules:
  - type: field
    source: /BGMN/e2
    target: order_number
"#,
    )
    .expect("write temp mapping");

    let output = Command::new(binary)
        .args([
            "mapping",
            "lint",
            mapping_path.to_string_lossy().as_ref(),
            "--schema",
            schema.to_string_lossy().as_ref(),
        ])
        .output()
        .expect("run edi mapping lint with schema");

    let code = output.status.code().unwrap_or(-1);
    assert_eq!(code, 1, "lint with warnings should exit 1");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("unknown segment 'BGMN'"),
        "stdout: {stdout}"
    );
    assert!(stdout.contains("did you mean 'BGM'"), "stdout: {stdout}");

    let _ = fs::remove_file(mapping_path);
}

#[test]
fn mapping_lint_with_schema_accepts_runtime_segment_groups() {
    let binary = cargo_bin();
    let mapping = testdata_path("testdata/mappings/orders_to_csv.yaml");
    let schema = testdata_path("testdata/schemas/eancom_orders_d96a.yaml");

    let output = Command::new(binary)
        .args([
            "mapping",
            "lint",
            mapping.to_string_lossy().as_ref(),
            "--schema",
            schema.to_string_lossy().as_ref(),
        ])
        .output()
        .expect("run edi mapping lint with schema");

    assert!(
        output.status.success(),
        "expected runtime group paths to lint cleanly; stdout: {}; stderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn mapping_explain_prints_readable_rule_tree() {
    let binary = cargo_bin();
    let mapping = testdata_path("testdata/mappings/orders_to_csv.yaml");

    let output = Command::new(binary)
        .args(["mapping", "explain", mapping.to_string_lossy().as_ref()])
        .output()
        .expect("run edi mapping explain");

    assert!(
        output.status.success(),
        "expected explain to succeed; stdout: {}; stderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Mapping orders_to_csv"), "stdout: {stdout}");
    assert!(
        stdout.contains("source_type: EANCOM_ORDERS"),
        "stdout: {stdout}"
    );
    assert!(
        stdout.contains("foreach LINE_ITEM -> orders"),
        "stdout: {stdout}"
    );
    assert!(
        stdout.contains("field /BGM/e2 -> order_number"),
        "stdout: {stdout}"
    );
}

#[test]
fn transform_dry_run_trace_mapping_outputs_machine_readable_rule_diagnostics_without_output_file() {
    let binary = cargo_bin();
    let input = testdata_path("testdata/edi/valid_orders_d96a_minimal.edi");
    let mapping = testdata_path("testdata/mappings/orders_to_json.yaml");
    let output_path = unique_temp_path("dry-run-trace", "json");

    let output = Command::new(binary)
        .args([
            "transform",
            input.to_string_lossy().as_ref(),
            output_path.to_string_lossy().as_ref(),
            "-m",
            mapping.to_string_lossy().as_ref(),
            "--dry-run",
            "--trace-mapping",
            "--trace-format",
            "json",
        ])
        .output()
        .expect("run edi transform dry-run trace");

    assert!(
        output.status.success(),
        "expected dry-run trace to succeed; stdout: {}; stderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        !output_path.exists(),
        "dry-run should not write the requested output file"
    );

    let trace: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("dry-run trace should be valid JSON");
    assert_eq!(trace["mapping"], "orders_to_json");
    let rules = trace["messages"][0]["rules"]
        .as_array()
        .expect("message trace should include rule array");
    assert!(
        rules.iter().any(|rule| {
            rule["rule_type"] == "field"
                && rule["source"] == "/BGM/e2"
                && rule["target"] == "order_number"
                && rule["resolved_node_count"] == 1
                && rule["output_value"] == "ORDER123"
        }),
        "trace did not include expected order_number field rule: {trace}"
    );

    let _ = fs::remove_file(output_path);
}

#[test]
fn transform_trace_mapping_keeps_stdout_machine_readable_when_output_is_stdout() {
    let binary = cargo_bin();
    let input = testdata_path("testdata/edi/valid_orders_d96a_minimal.edi");
    let mapping = testdata_path("testdata/mappings/orders_to_json.yaml");

    let output = Command::new(binary)
        .args([
            "transform",
            input.to_string_lossy().as_ref(),
            "--mapping",
            mapping.to_string_lossy().as_ref(),
            "--trace-mapping",
            "--trace-format",
            "json",
        ])
        .output()
        .expect("run traced transform to stdout");

    assert!(
        output.status.success(),
        "expected transform to succeed; stdout: {}; stderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let transformed: serde_json::Value = serde_json::from_slice(&output.stdout)
        .expect("stdout should contain only transformed JSON, not trace diagnostics");
    assert!(
        transformed.to_string().contains("ORDER123"),
        "stdout JSON should contain mapped order number: {transformed}"
    );

    let trace: serde_json::Value = serde_json::from_slice(&output.stderr)
        .expect("stderr should contain machine-readable JSON trace diagnostics");
    assert_eq!(trace["mapping"], "orders_to_json");
}
