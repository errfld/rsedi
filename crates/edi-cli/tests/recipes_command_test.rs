use std::env;
use std::path::PathBuf;
use std::process::Command;

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

#[test]
fn recipes_list_includes_curated_common_flows() {
    let output = Command::new(cargo_bin())
        .args(["recipes", "list"])
        .output()
        .expect("run edi recipes list");

    assert!(
        output.status.success(),
        "expected recipes list to succeed; stdout: {}; stderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8(output.stdout).expect("stdout is UTF-8");
    for recipe in [
        "validate-edifact",
        "orders-to-json",
        "orders-to-csv",
        "csv-to-orders",
        "batch-validate-directory",
    ] {
        assert!(
            stdout.contains(recipe),
            "missing recipe {recipe} in {stdout}"
        );
    }
}

#[test]
fn recipe_run_dry_run_prints_copyable_command() {
    let input = testdata_path("testdata/edi/valid_orders_d96a_minimal.edi");
    let mapping = testdata_path("testdata/mappings/orders_to_json.yaml");
    let output = Command::new(cargo_bin())
        .args([
            "recipes",
            "run",
            "orders-to-json",
            "--input",
            input.to_string_lossy().as_ref(),
            "--mapping",
            mapping.to_string_lossy().as_ref(),
            "--dry-run",
        ])
        .output()
        .expect("run edi recipes run --dry-run");

    assert!(
        output.status.success(),
        "expected dry run to succeed; stdout: {}; stderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8(output.stdout).expect("stdout is UTF-8");
    assert!(stdout.contains("Planned command:"), "stdout: {stdout}");
    assert!(stdout.contains("edi transform"), "stdout: {stdout}");
    assert!(stdout.contains("--mapping"), "stdout: {stdout}");
    assert!(
        stdout.contains(input.to_string_lossy().as_ref()),
        "stdout: {stdout}"
    );
}

#[test]
fn wizard_non_tty_does_not_block_and_points_to_recipes() {
    let output = Command::new(cargo_bin())
        .arg("wizard")
        .output()
        .expect("run edi wizard");

    let code = output.status.code().unwrap_or(-1);
    assert_eq!(code, 2, "expected non-TTY wizard usage error, got {code}");

    let stderr = String::from_utf8(output.stderr).expect("stderr is UTF-8");
    assert!(stderr.contains("Non-interactive"), "stderr: {stderr}");
    assert!(stderr.contains("recipes run"), "stderr: {stderr}");
}

#[test]
fn wizard_dry_run_autodetects_testdata_like_workspace() {
    let output = Command::new(cargo_bin())
        .current_dir(repo_root())
        .args(["wizard", "--dry-run"])
        .output()
        .expect("run edi wizard --dry-run");

    assert!(
        output.status.success(),
        "expected wizard dry run to succeed; stdout: {}; stderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8(output.stdout).expect("stdout is UTF-8");
    assert!(stdout.contains("Planned command:"), "stdout: {stdout}");
    assert!(stdout.contains("edi transform"), "stdout: {stdout}");
    assert!(stdout.contains("valid_orders_d96a"), "stdout: {stdout}");
}
