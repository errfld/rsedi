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

fn unique_temp_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after UNIX_EPOCH")
        .as_nanos();
    env::temp_dir().join(format!("edi-cli-{name}-{}-{nanos}", std::process::id()))
}

struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn create(name: &str) -> Self {
        let path = unique_temp_dir(name);
        fs::create_dir_all(&path).expect("temporary directory should be created");
        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

fn run_edi(args: &[&str]) -> Output {
    Command::new(cargo_bin())
        .args(args)
        .output()
        .expect("edi command should execute")
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
fn batch_validate_quarantines_bad_files_and_reports_json_summary() {
    let input_dir = TempDir::create("batch-input");
    let quarantine_dir = TempDir::create("batch-quarantine");
    fs::copy(
        testdata_path("testdata/edi/valid_orders_d96a_minimal.edi"),
        input_dir.path().join("good.edi"),
    )
    .expect("valid fixture should copy");
    fs::copy(
        testdata_path("testdata/edi/invalid_orders_missing_bgm.edi"),
        input_dir.path().join("bad.edi"),
    )
    .expect("invalid fixture should copy");

    let schema = testdata_path("testdata/schemas/eancom_orders_d96a.yaml");
    let output = run_edi(&[
        "batch",
        "validate",
        input_dir.path().to_string_lossy().as_ref(),
        "--schema",
        schema.to_string_lossy().as_ref(),
        "--quarantine-dir",
        quarantine_dir.path().to_string_lossy().as_ref(),
        "--format",
        "json",
    ]);

    assert_exit_code(&output, 2);
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("\"processed\":2"), "stdout: {stdout}");
    assert!(stdout.contains("\"failed\":1"), "stdout: {stdout}");
    assert!(stdout.contains("\"quarantined\":1"), "stdout: {stdout}");

    let list_output = run_edi(&[
        "quarantine",
        "list",
        quarantine_dir.path().to_string_lossy().as_ref(),
        "--format",
        "json",
    ]);
    assert_exit_code(&list_output, 0);
    let list_stdout = String::from_utf8_lossy(&list_output.stdout);
    assert!(list_stdout.contains("bad.edi"), "stdout: {list_stdout}");
    assert!(list_stdout.contains("validation"), "stdout: {list_stdout}");
}

#[test]
fn batch_transform_writes_one_output_per_input_file() {
    let input_dir = TempDir::create("batch-transform-input");
    let output_dir = TempDir::create("batch-transform-output");
    fs::copy(
        testdata_path("testdata/edi/valid_orders_d96a_minimal.edi"),
        input_dir.path().join("good.edi"),
    )
    .expect("valid fixture should copy");

    let mapping = testdata_path("testdata/mappings/orders_to_json.yaml");
    let output = run_edi(&[
        "batch",
        "transform",
        input_dir.path().to_string_lossy().as_ref(),
        output_dir.path().to_string_lossy().as_ref(),
        "--mapping",
        mapping.to_string_lossy().as_ref(),
        "--format",
        "json",
    ]);

    assert_exit_code(&output, 0);
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("\"processed\":1"), "stdout: {stdout}");
    assert!(
        output_dir.path().join("good.json").exists(),
        "batch transform should create output file"
    );
}
