use std::env;
use std::fs;
use std::path::Path;
use std::path::PathBuf;
use std::process::{Command, Output};
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

fn write_temp_file(name: &str, extension: &str, content: &str) -> PathBuf {
    let path = unique_temp_path(name, extension);
    fs::write(&path, content).expect("temporary file should be writable");
    path
}

fn run_generate(args: &[&str]) -> Output {
    Command::new(cargo_bin())
        .args(args)
        .output()
        .expect("run edi generate")
}

fn assert_exit_code(output: &Output, expected: i32) {
    let actual = output.status.code().unwrap_or(-1);
    assert_eq!(
        actual,
        expected,
        "unexpected exit code; stdout: {}; stderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn remove_if_exists(path: &Path) {
    let _ = fs::remove_file(path);
}

#[test]
fn generate_csv_input_writes_edi_output_file() {
    let csv_input = write_temp_file(
        "generate-orders",
        "csv",
        "DOCUMENT_NUMBER,DOCUMENT_TYPE,LINE_NUMBER,GTIN,ORDERED_QUANTITY\nORDER123,220,1,4006381333931,10\nORDER123,220,2,0123456789012,5\n",
    );
    let mapping = write_temp_file(
        "generate-orders-mapping",
        "yaml",
        r#"
name: generate_orders_from_csv
source_type: CSV_ORDERS
target_type: EANCOM_D96A_ORDERS
rules:
  - type: field
    source: /rows/row/DOCUMENT_TYPE
    target: BGM.e1
  - type: field
    source: /rows/row/DOCUMENT_NUMBER
    target: BGM.e2
  - type: foreach
    source: /rows/row
    target: LIN
    rules:
      - type: field
        source: LINE_NUMBER
        target: e1
      - type: field
        source: GTIN
        target: e3.c1
      - type: field
        source: ORDERED_QUANTITY
        target: QTY21.e1
"#,
    );
    let output_path = unique_temp_path("generated-orders", "edi");

    let output = run_generate(&[
        "generate",
        csv_input.to_string_lossy().as_ref(),
        output_path.to_string_lossy().as_ref(),
        "-m",
        mapping.to_string_lossy().as_ref(),
        "--input-format",
        "csv",
    ]);

    assert!(
        output.status.success(),
        "expected generate to succeed; stdout: {}; stderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let edi = fs::read_to_string(&output_path).expect("generated EDI should be readable");
    assert!(edi.contains("BGM+220+ORDER123'"));
    assert!(edi.contains("LIN+1"));
    assert!(edi.contains("LIN+2"));
    assert!(edi.contains("QTY+21:10'"));

    remove_if_exists(&csv_input);
    remove_if_exists(&mapping);
    remove_if_exists(&output_path);
}

#[test]
fn generate_json_input_writes_to_stdout_when_output_is_omitted() {
    let json_input = write_temp_file(
        "generate-orders-json",
        "json",
        r#"{
  "rows": [
    {
      "document_type": "220",
      "document_number": "ORD-JSON-1",
      "line_number": 1,
      "gtin": "4012345678901",
      "ordered_quantity": 3
    }
  ]
}
"#,
    );
    let mapping = write_temp_file(
        "generate-orders-json-mapping",
        "yaml",
        r#"
name: generate_orders_from_json
source_type: JSON_ORDERS
target_type: EANCOM_D96A_ORDERS
rules:
  - type: field
    source: /rows/item/document_type
    target: BGM.e1
  - type: field
    source: /rows/item/document_number
    target: BGM.e2
  - type: foreach
    source: /rows/item
    target: LIN
    rules:
      - type: field
        source: line_number
        target: e1
      - type: field
        source: gtin
        target: e3.c1
      - type: field
        source: ordered_quantity
        target: QTY21.e1
"#,
    );

    let output = run_generate(&[
        "generate",
        json_input.to_string_lossy().as_ref(),
        "-m",
        mapping.to_string_lossy().as_ref(),
        "--input-format",
        "json",
    ]);

    assert!(
        output.status.success(),
        "expected generate to succeed; stdout: {}; stderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8(output.stdout).expect("stdout should be valid UTF-8");
    assert!(stdout.contains("BGM+220+ORD-JSON-1'"));
    assert!(stdout.contains("LIN+1"));
    assert!(stdout.contains("QTY+21:3'"));

    remove_if_exists(&json_input);
    remove_if_exists(&mapping);
}

#[test]
fn generate_fails_when_mapping_target_is_not_edi() {
    let json_input = write_temp_file(
        "generate-invalid-target",
        "json",
        "{\"order_number\":\"A1\"}",
    );
    let mapping = write_temp_file(
        "generate-invalid-target-mapping",
        "yaml",
        r#"
name: invalid_generate_target
source_type: JSON_ORDERS
target_type: JSON_ORDERS
rules:
  - type: field
    source: /order_number
    target: order_number
"#,
    );

    let output = run_generate(&[
        "generate",
        json_input.to_string_lossy().as_ref(),
        "-m",
        mapping.to_string_lossy().as_ref(),
        "--input-format",
        "json",
    ]);

    assert_exit_code(&output, 2);
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("must target EDI output"),
        "expected target format validation error; stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    remove_if_exists(&json_input);
    remove_if_exists(&mapping);
}

#[test]
fn generate_help_includes_mapping_and_input_format_flags() {
    let output = Command::new(cargo_bin())
        .args(["generate", "--help"])
        .output()
        .expect("run edi generate --help");

    assert!(output.status.success(), "generate --help should succeed");

    let stdout = String::from_utf8(output.stdout).expect("stdout should be UTF-8");
    assert!(stdout.contains("<INPUT>"));
    assert!(stdout.contains("[OUTPUT]"));
    assert!(stdout.contains("--mapping <MAPPING>"));
    assert!(stdout.contains("--input-format <INPUT_FORMAT>"));
}
