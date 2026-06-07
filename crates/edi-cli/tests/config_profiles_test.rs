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

struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn new(name: &str) -> Self {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be after UNIX_EPOCH")
            .as_nanos();
        let path = env::temp_dir().join(format!("edi-cli-{name}-{}-{nanos}", std::process::id()));
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

const EXIT_SUCCESS: i32 = 0;
const EXIT_ERRORS: i32 = 2;
const EXIT_CONFIG_ERROR: i32 = 3;

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
fn init_creates_starter_config_and_directories() {
    let temp = TempDir::new("init-profile");

    let output = Command::new(cargo_bin())
        .current_dir(temp.path())
        .args(["init", "--profile", "orders"])
        .output()
        .expect("edi init should execute");

    assert_exit_code(&output, EXIT_SUCCESS);
    assert!(temp.path().join("rsedi.yaml").exists());
    assert!(temp.path().join("schemas").is_dir());
    assert!(temp.path().join("mappings").is_dir());
    assert!(temp.path().join("input").is_dir());
    assert!(temp.path().join("output").is_dir());
    assert!(temp.path().join("quarantine").is_dir());

    let config = fs::read_to_string(temp.path().join("rsedi.yaml"))
        .expect("generated config should be readable");
    assert!(config.contains("profiles:"));
    assert!(config.contains("orders:"));
    assert!(config.contains("schema:"));
    assert!(config.contains("mapping:"));
}

#[test]
fn init_rejects_profile_names_that_would_break_yaml() {
    let temp = TempDir::new("init-invalid-profile");

    let output = Command::new(cargo_bin())
        .current_dir(temp.path())
        .args(["init", "--profile", "bad: name"])
        .output()
        .expect("edi init should execute");

    assert_exit_code(&output, EXIT_ERRORS);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("Invalid profile name"));
    assert!(!temp.path().join("rsedi.yaml").exists());
}

#[test]
fn config_check_validates_profile_referenced_files() {
    let temp = TempDir::new("config-check-valid");
    fs::create_dir_all(temp.path().join("schemas")).expect("schema dir");
    fs::create_dir_all(temp.path().join("mappings")).expect("mapping dir");
    fs::create_dir_all(temp.path().join("input")).expect("input dir");
    fs::create_dir_all(temp.path().join("output")).expect("output dir");
    fs::create_dir_all(temp.path().join("quarantine")).expect("quarantine dir");
    fs::write(
        temp.path().join("schemas/orders.yaml"),
        "name: ORDERS\nversion: D96A\nsegments: []\n",
    )
    .expect("schema file");
    fs::write(
        temp.path().join("mappings/orders.yaml"),
        "source_type: EDI\ntarget_type: JSON\nrules: []\n",
    )
    .expect("mapping file");
    fs::write(
        temp.path().join("input/orders.edi"),
        "UNH+1+ORDERS:D:96A:UN'",
    )
    .expect("input file");
    fs::write(temp.path().join("output/orders.json"), "{}").expect("output file");
    fs::write(
        temp.path().join("rsedi.yaml"),
        r#"profiles:
  orders:
    input: input/orders.edi
    output: output/orders.json
    schema: schemas/orders.yaml
    mapping: mappings/orders.yaml
    quarantine: quarantine
    output_format: json
"#,
    )
    .expect("config file");

    let output = Command::new(cargo_bin())
        .current_dir(temp.path())
        .args(["config", "check", "--profile", "orders"])
        .output()
        .expect("edi config check should execute");

    assert_exit_code(&output, EXIT_SUCCESS);
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Config OK"));
    assert!(stdout.contains("orders"));
}

#[test]
fn config_check_reports_unknown_keys_and_missing_files() {
    let temp = TempDir::new("config-check-invalid");
    fs::write(
        temp.path().join("rsedi.yaml"),
        r#"profiles:
  orders:
    schema: schemas/missing.yaml
    mapping: mappings/missing.yaml
    typo_path: nope
"#,
    )
    .expect("config file");

    let output = Command::new(cargo_bin())
        .current_dir(temp.path())
        .args(["config", "check", "--profile", "orders"])
        .output()
        .expect("edi config check should execute");

    assert_exit_code(&output, EXIT_CONFIG_ERROR);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("unknown field"));
    assert!(stderr.contains("typo_path"));
}

#[test]
fn config_check_reports_missing_referenced_files() {
    let temp = TempDir::new("config-check-missing-files");
    fs::write(
        temp.path().join("rsedi.yaml"),
        r#"profiles:
  orders:
    input: input/missing.edi
    output: output/missing.json
    schema: schemas/missing.yaml
    mapping: mappings/missing.yaml
    quarantine: quarantine
"#,
    )
    .expect("config file");

    let output = Command::new(cargo_bin())
        .current_dir(temp.path())
        .args(["config", "check", "--profile", "orders"])
        .output()
        .expect("edi config check should execute");

    assert_exit_code(&output, EXIT_ERRORS);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("Missing:"));
    assert!(stderr.contains("schemas/missing.yaml"));
    assert!(stderr.contains("mappings/missing.yaml"));
    assert!(stderr.contains("input/missing.edi"));
    assert!(stderr.contains("output/missing.json"));
    assert!(stderr.contains("quarantine"));
}

#[test]
fn validate_uses_profile_paths_from_default_config() {
    let temp = TempDir::new("validate-profile");
    fs::create_dir_all(temp.path().join("schemas")).expect("schema dir");
    fs::create_dir_all(temp.path().join("input")).expect("input dir");
    fs::copy(
        testdata_path("testdata/schemas/eancom_orders_d96a.yaml"),
        temp.path().join("schemas/orders.yaml"),
    )
    .expect("schema copy");
    fs::copy(
        testdata_path("testdata/edi/valid_orders_d96a_minimal.edi"),
        temp.path().join("input/orders.edi"),
    )
    .expect("input copy");
    fs::write(
        temp.path().join("rsedi.yaml"),
        r#"profiles:
  orders:
    input: input/orders.edi
    schema: schemas/orders.yaml
"#,
    )
    .expect("config file");

    let output = Command::new(cargo_bin())
        .current_dir(temp.path())
        .args(["--profile", "orders", "validate"])
        .output()
        .expect("edi validate should execute");

    assert_exit_code(&output, EXIT_SUCCESS);
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Validation passed with no warnings."));
}

#[test]
fn transform_uses_profile_paths_for_input_mapping_and_output() {
    let temp = TempDir::new("transform-profile");
    fs::create_dir_all(temp.path().join("input")).expect("input dir");
    fs::create_dir_all(temp.path().join("mappings")).expect("mapping dir");
    fs::create_dir_all(temp.path().join("output")).expect("output dir");
    fs::copy(
        testdata_path("testdata/edi/valid_orders_d96a_minimal.edi"),
        temp.path().join("input/orders.edi"),
    )
    .expect("input copy");
    fs::copy(
        testdata_path("testdata/mappings/orders_to_json.yaml"),
        temp.path().join("mappings/orders_to_json.yaml"),
    )
    .expect("mapping copy");
    fs::write(
        temp.path().join("rsedi.yaml"),
        r#"profiles:
  orders:
    input: input/orders.edi
    output: output/orders.json
    mapping: mappings/orders_to_json.yaml
"#,
    )
    .expect("config file");

    let output = Command::new(cargo_bin())
        .current_dir(temp.path())
        .args(["--profile", "orders", "transform"])
        .output()
        .expect("edi transform should execute");

    assert_exit_code(&output, EXIT_SUCCESS);
    let json = fs::read_to_string(temp.path().join("output/orders.json"))
        .expect("profile output should be written");
    assert!(json.contains("ORDER123"));
}
