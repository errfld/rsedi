use std::env;
use std::fs;
use std::path::PathBuf;
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

fn cargo_bin() -> PathBuf {
    if let Ok(path) = env::var("CARGO_BIN_EXE_edi") {
        return PathBuf::from(path);
    }

    let fallback = repo_root().join("target").join("debug").join("edi");
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

    let status = Command::new(binary)
        .args([
            "transform",
            input.to_string_lossy().as_ref(),
            output.to_string_lossy().as_ref(),
            "-m",
            mapping.to_string_lossy().as_ref(),
        ])
        .status()
        .expect("run edi transform");

    assert!(status.success(), "expected transform to succeed");

    let bytes = fs::read(&output).expect("transform output should be readable");
    assert!(!bytes.is_empty(), "transform output should not be empty");

    let value: serde_json::Value =
        serde_json::from_slice(&bytes).expect("transform output should be valid JSON");
    assert!(!value.is_null(), "transform output should not be JSON null");

    let _ = fs::remove_file(&output);
}
