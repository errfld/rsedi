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

    for profile in ["debug", "release"] {
        let candidate = target_dir.join(profile).join(&executable_name);
        if candidate.exists() {
            return candidate;
        }
    }

    panic!(
        "cargo_bin() could not find {executable_name} under {} debug or release directories",
        target_dir.display()
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

const UNSUPPORTED_INVRPT_PAYLOAD: &str = "UNH+1+INVRPT:D:96A:UN'BGM+35+REPORT1+9'UNT+3+1'";

fn temp_workspace(name: &str) -> PathBuf {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock after epoch")
        .as_nanos();
    let path = env::temp_dir().join(format!("rsedi-{name}-{unique}"));
    fs::create_dir_all(&path).expect("create temp workspace");
    path
}

#[test]
fn schema_list_and_inspect_show_built_in_message_packs() {
    let list = Command::new(cargo_bin())
        .args(["schema", "list"])
        .output()
        .expect("run edi schema list");

    assert!(
        list.status.success(),
        "expected list to succeed; stdout: {}; stderr: {}",
        String::from_utf8_lossy(&list.stdout),
        String::from_utf8_lossy(&list.stderr)
    );
    let stdout = String::from_utf8(list.stdout).expect("stdout is UTF-8");
    for pack in [
        "eancom:d96a:orders",
        "eancom:d96a:slsrpt",
        "eancom:d96a:ordrsp",
    ] {
        assert!(stdout.contains(pack), "missing pack {pack} in {stdout}");
    }

    let inspect = Command::new(cargo_bin())
        .args(["schema", "inspect", "eancom:d96a:orders"])
        .output()
        .expect("run edi schema inspect");
    assert!(
        inspect.status.success(),
        "expected inspect to succeed; stdout: {}; stderr: {}",
        String::from_utf8_lossy(&inspect.stdout),
        String::from_utf8_lossy(&inspect.stderr)
    );
    let stdout = String::from_utf8(inspect.stdout).expect("stdout is UTF-8");
    assert!(stdout.contains("message_type: ORDERS"), "stdout: {stdout}");
    assert!(stdout.contains("version: D96A"), "stdout: {stdout}");
}

#[test]
fn schema_install_copies_pack_and_records_project_config() {
    let workspace = temp_workspace("schema-install");

    let install = Command::new(cargo_bin())
        .current_dir(&workspace)
        .args(["schema", "install", "eancom:d96a:orders"])
        .output()
        .expect("run edi schema install");

    assert!(
        install.status.success(),
        "expected install to succeed; stdout: {}; stderr: {}",
        String::from_utf8_lossy(&install.stdout),
        String::from_utf8_lossy(&install.stderr)
    );

    let installed_schema = workspace.join("schemas/eancom/d96a/orders.yaml");
    assert!(installed_schema.exists(), "schema was not installed");
    let config = fs::read_to_string(workspace.join("rsedi.yaml")).expect("read generated config");
    assert!(
        config.contains("eancom:d96a:orders"),
        "installed pack was not recorded in config: {config}"
    );
}

#[test]
fn validate_auto_schema_detects_orders_pack_without_schema_path() {
    let input = testdata_path("testdata/edi/valid_orders_d96a_minimal.edi");

    let output = Command::new(cargo_bin())
        .current_dir(repo_root())
        .args([
            "validate",
            input.to_string_lossy().as_ref(),
            "--auto-schema",
        ])
        .output()
        .expect("run edi validate --auto-schema");

    assert!(
        output.status.success(),
        "expected auto-schema validation to pass; stdout: {}; stderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8(output.stdout).expect("stdout is UTF-8");
    assert!(
        stdout.contains("eancom:d96a:orders"),
        "auto-schema selection did not name the selected pack: {stdout}"
    );
    assert!(stdout.contains("Validation passed"), "stdout: {stdout}");
}

#[test]
fn validate_auto_schema_reports_install_command_for_missing_pack() {
    let workspace = temp_workspace("missing-pack");
    let input = workspace.join("unsupported.edi");
    fs::write(&input, UNSUPPORTED_INVRPT_PAYLOAD).expect("write unsupported EDI");

    let output = Command::new(cargo_bin())
        .current_dir(&workspace)
        .args([
            "validate",
            input.to_string_lossy().as_ref(),
            "--auto-schema",
        ])
        .output()
        .expect("run edi validate --auto-schema");

    assert_eq!(output.status.code(), Some(2));
    let stderr = String::from_utf8(output.stderr).expect("stderr is UTF-8");
    assert!(
        stderr.contains("No schema pack installed or built in"),
        "stderr: {stderr}"
    );
    assert!(
        stderr.contains("edi schema install eancom:d96a:invrpt"),
        "stderr should include install command: {stderr}"
    );
}
