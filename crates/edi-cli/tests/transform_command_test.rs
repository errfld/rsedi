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
fn transform_writes_json_to_stdout_when_output_is_omitted() {
    let binary = cargo_bin();
    let input = testdata_path("testdata/edi/valid_orders_d96a_minimal.edi");
    let mapping = testdata_path("testdata/mappings/orders_to_json.yaml");

    let output = Command::new(binary)
        .args([
            "transform",
            input.to_string_lossy().as_ref(),
            "-m",
            mapping.to_string_lossy().as_ref(),
        ])
        .output()
        .expect("run edi transform");

    assert!(
        output.status.success(),
        "expected transform to succeed; stdout: {}; stderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let payload = String::from_utf8(output.stdout).expect("stdout should be valid UTF-8");
    let parsed: serde_json::Value =
        serde_json::from_str(&payload).expect("stdout should contain valid JSON");

    assert!(
        parsed.get("root").is_some(),
        "expected mapped document JSON object"
    );
}

#[test]
fn transform_writes_csv_when_mapping_target_is_csv() {
    let binary = cargo_bin();
    let input = testdata_path("testdata/edi/valid_orders_d96a_minimal.edi");
    let mapping = testdata_path("testdata/mappings/orders_to_csv.yaml");
    let output_path = unique_temp_path("orders-transform-csv", "csv");

    let output = Command::new(binary)
        .args([
            "transform",
            input.to_string_lossy().as_ref(),
            output_path.to_string_lossy().as_ref(),
            "-m",
            mapping.to_string_lossy().as_ref(),
        ])
        .output()
        .expect("run edi transform");

    assert!(
        output.status.success(),
        "expected CSV transform to succeed; stdout: {}; stderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let csv = fs::read_to_string(&output_path).expect("CSV output should be readable");
    assert!(!csv.trim().is_empty(), "CSV output should not be empty");
    assert!(
        csv.contains("line_number"),
        "CSV output should include a header row with mapped fields"
    );

    let _ = fs::remove_file(&output_path);
}

#[test]
fn transform_requires_mapping_flag() {
    let binary = cargo_bin();
    let input = testdata_path("testdata/edi/valid_orders_d96a_minimal.edi");
    let output_path = unique_temp_path("missing-mapping", "json");

    let output = Command::new(binary)
        .args([
            "transform",
            input.to_string_lossy().as_ref(),
            output_path.to_string_lossy().as_ref(),
        ])
        .output()
        .expect("run edi transform");

    let code = output.status.code().unwrap_or(-1);
    assert_eq!(code, 2, "expected clap usage error exit code, got {code}");
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("--mapping"),
        "expected clap error message to reference required --mapping flag"
    );

    let _ = fs::remove_file(&output_path);
}

#[test]
fn transform_fails_for_unsupported_target_type() {
    let binary = cargo_bin();
    let input = testdata_path("testdata/edi/valid_orders_d96a_minimal.edi");
    let mapping_path = unique_temp_path("unsupported-target", "yaml");
    let output_path = unique_temp_path("unsupported-target", "txt");

    let mapping = r#"
name: unsupported_target
source_type: EANCOM_ORDERS
target_type: XML_ORDERS
rules:
  - type: field
    source: /BGM/e2
    target: order_number
"#;
    fs::write(&mapping_path, mapping).expect("write temp mapping");

    let output = Command::new(binary)
        .args([
            "transform",
            input.to_string_lossy().as_ref(),
            output_path.to_string_lossy().as_ref(),
            "-m",
            mapping_path.to_string_lossy().as_ref(),
        ])
        .output()
        .expect("run edi transform");

    let code = output.status.code().unwrap_or(-1);
    assert_eq!(code, 2, "expected transform failure exit code, got {code}");
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("Unsupported mapping target_type"),
        "expected unsupported target type error; stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let _ = fs::remove_file(&mapping_path);
    let _ = fs::remove_file(&output_path);
}

#[test]
fn transform_does_not_misclassify_credit_note_as_edi() {
    let binary = cargo_bin();
    let input = testdata_path("testdata/edi/valid_orders_d96a_minimal.edi");
    let mapping_path = unique_temp_path("credit-note-target", "yaml");
    let output_path = unique_temp_path("credit-note-target", "txt");

    let mapping = r#"
name: credit_note_target
source_type: EANCOM_ORDERS
target_type: CREDIT_NOTE
rules:
  - type: field
    source: /BGM/e2
    target: order_number
"#;
    fs::write(&mapping_path, mapping).expect("write temp mapping");

    let output = Command::new(binary)
        .args([
            "transform",
            input.to_string_lossy().as_ref(),
            output_path.to_string_lossy().as_ref(),
            "-m",
            mapping_path.to_string_lossy().as_ref(),
        ])
        .output()
        .expect("run edi transform");

    let code = output.status.code().unwrap_or(-1);
    assert_eq!(code, 2, "expected transform failure exit code, got {code}");
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("Unsupported mapping target_type"),
        "expected unsupported target type error; stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let _ = fs::remove_file(&mapping_path);
    let _ = fs::remove_file(&output_path);
}
