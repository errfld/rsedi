use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
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
        .expect("system time should be after UNIX_EPOCH")
        .as_nanos();
    env::temp_dir().join(format!(
        "edi-cli-{name}-{}-{nanos}.{extension}",
        std::process::id()
    ))
}

struct TempFile {
    path: PathBuf,
}

impl TempFile {
    fn create(name: &str, extension: &str, content: &str) -> Self {
        let path = unique_temp_path(name, extension);
        fs::write(&path, content).expect("temporary file should be created");
        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempFile {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

fn run_validate(input: &Path, schema: &Path) -> Output {
    run_validate_args(input, schema, &[])
}

fn run_validate_args(input: &Path, schema: &Path, extra_args: &[&str]) -> Output {
    let mut command = Command::new(cargo_bin());
    command.args([
        "validate",
        input.to_string_lossy().as_ref(),
        "-s",
        schema.to_string_lossy().as_ref(),
    ]);
    command.args(extra_args);
    command.output().expect("edi validate should execute")
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

#[test]
fn validate_returns_success_for_clean_orders_message() {
    let input = testdata_path("testdata/edi/valid_orders_d96a_minimal.edi");
    let schema = testdata_path("testdata/schemas/eancom_orders_d96a.yaml");
    let output = run_validate(&input, &schema);

    assert_exit_code(&output, 0);

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Errors: 0"));
    assert!(stdout.contains("Warnings: 0"));
    assert!(stdout.contains("Validation passed with no warnings."));
}

#[test]
fn validate_returns_warning_exit_code_for_schema_warnings() {
    let input = testdata_path("testdata/edi/orders_with_warnings.edi");
    let schema = testdata_path("testdata/schemas/eancom_orders_d96a.yaml");
    let output = run_validate(&input, &schema);

    assert_exit_code(&output, 1);

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Warnings:"));
    assert!(stdout.contains("[EXTRA_ELEMENT]"));
    assert!(stdout.contains("file="));
}

#[test]
fn validate_returns_error_exit_code_for_validation_errors() {
    let input = testdata_path("testdata/edi/invalid_orders_missing_bgm.edi");
    let schema = testdata_path("testdata/schemas/eancom_orders_d96a.yaml");
    let output = run_validate(&input, &schema);

    assert_exit_code(&output, 2);

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Errors: 1"));
    assert!(stdout.contains("[MISSING_MANDATORY_SEGMENT]"));
    assert!(stdout.contains("file="));
}

#[test]
fn validate_reports_parse_warning_with_line_context() {
    let partial_orders = TempFile::create(
        "partial-orders-missing-unt",
        "edi",
        "UNB+UNOA:3+SENDER+RECEIVER+200101:1200+12345'\n\
UNH+1+ORDERS:D:96A:UN'\n\
BGM+220+ORDER123+9'\n\
DTM+137:20200101:102'\n",
    );

    let permissive_schema = TempFile::create(
        "schema-without-unt",
        "yaml",
        r#"name: "TEST_ORDERS"
version: "D96A"
parent: null
segments:
  - tag: UNB
    is_mandatory: false
    max_repetitions: 1
    elements: []
  - tag: UNH
    is_mandatory: true
    max_repetitions: 1
    elements: []
  - tag: BGM
    is_mandatory: true
    max_repetitions: 1
    elements: []
  - tag: DTM
    is_mandatory: false
    max_repetitions: 5
    elements: []
"#,
    );

    let output = run_validate(partial_orders.path(), permissive_schema.path());

    assert_exit_code(&output, 1);

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Created partial message at EOF because UNT segment is missing"));
    assert!(stdout.contains("line=5"));
    assert!(stdout.contains("col=1"));
    assert!(stdout.contains("file="));
}

#[test]
fn validate_json_report_is_machine_readable_and_includes_actionable_fields() {
    let input = testdata_path("testdata/edi/invalid_orders_missing_bgm.edi");
    let schema = testdata_path("testdata/schemas/eancom_orders_d96a.yaml");
    let output = run_validate_args(&input, &schema, &["--report", "json"]);

    assert_exit_code(&output, 2);

    let report: serde_json::Value = serde_json::from_slice(&output.stdout)
        .expect("json report should be the only stdout payload");
    assert_eq!(report["source"], input.to_string_lossy().as_ref());
    assert_eq!(report["schema"], schema.to_string_lossy().as_ref());
    assert_eq!(report["summary"]["messages"], 1);
    assert_eq!(report["summary"]["errors"], 1);
    assert_eq!(report["issues"][0]["rule_id"], "MISSING_MANDATORY_SEGMENT");
    assert_eq!(report["issues"][0]["severity"], "error");
    assert_eq!(report["issues"][0]["message_index"], 1);
    assert!(report["issues"][0]["path"].is_string());
}

#[test]
fn validate_report_output_file_keeps_stdout_concise() {
    let input = testdata_path("testdata/edi/invalid_orders_missing_bgm.edi");
    let schema = testdata_path("testdata/schemas/eancom_orders_d96a.yaml");
    let report = unique_temp_path("validation-report", "sarif");

    let output = run_validate_args(
        &input,
        &schema,
        &[
            "--report",
            "sarif",
            "--output",
            report.to_string_lossy().as_ref(),
        ],
    );

    assert_exit_code(&output, 2);

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Validation report written to"));
    assert!(!stdout.contains("MISSING_MANDATORY_SEGMENT"));

    let report_body = fs::read_to_string(&report).expect("report file should be written");
    let sarif: serde_json::Value =
        serde_json::from_str(&report_body).expect("sarif should be JSON");
    assert_eq!(sarif["version"], "2.1.0");
    assert_eq!(
        sarif["runs"][0]["results"][0]["ruleId"],
        "MISSING_MANDATORY_SEGMENT"
    );

    let _ = fs::remove_file(report);
}

#[test]
fn validate_returns_error_when_schema_path_is_invalid() {
    let input = testdata_path("testdata/edi/valid_orders_d96a_minimal.edi");
    let missing_schema = testdata_path("testdata/schemas/does-not-exist.yaml");
    let output = run_validate(&input, &missing_schema);

    assert_exit_code(&output, 2);

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("Failed to load schema"));
}
