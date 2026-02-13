use std::env;
use std::fs;
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

fn run_edi(args: &[&str]) -> Output {
    Command::new(cargo_bin())
        .args(args)
        .output()
        .expect("run edi")
}

#[test]
fn parse_command_outputs_json_to_stdout() {
    let edi_input = write_temp_file(
        "parse-orders",
        "edi",
        "UNA:+.? '\nUNB+UNOA:3+SENDER+RECEIVER+240101:1200+1'\nUNH+1+ORDERS:D:96A:UN'\nBGM+220+PO123+9'\nUNS+S'\nUNT+4+1'\nUNZ+1+1'\n",
    );

    let output = run_edi(&["parse", edi_input.to_string_lossy().as_ref(), "--pretty"]);

    assert!(
        output.status.success(),
        "expected parse to succeed; stdout: {}; stderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8(output.stdout).expect("stdout should be UTF-8");
    let parsed: serde_json::Value =
        serde_json::from_str(&stdout).expect("stdout should contain valid JSON");
    let documents = parsed
        .as_array()
        .expect("parse output should be a JSON array");
    assert!(
        documents.iter().any(|document| document.get("root").is_some()),
        "at least one parsed document should include a 'root' field"
    );

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("Parse summary: messages=1, warnings=0"));

    let _ = fs::remove_file(edi_input);
}

#[test]
fn invalid_config_returns_fatal_exit_code() {
    let bad_config = write_temp_file("bad-cli-config", "yaml", "color: neon");
    let edi_input = write_temp_file(
        "parse-with-bad-config",
        "edi",
        "UNA:+.? '\nUNB+UNOA:3+SENDER+RECEIVER+240101:1200+1'\nUNH+1+ORDERS:D:96A:UN'\nUNT+2+1'\nUNZ+1+1'\n",
    );

    let output = run_edi(&[
        "--config",
        bad_config.to_string_lossy().as_ref(),
        "parse",
        edi_input.to_string_lossy().as_ref(),
    ]);

    assert_eq!(output.status.code(), Some(3));
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("ERROR:"),
        "expected colored/plain error prefix; stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let _ = fs::remove_file(bad_config);
    let _ = fs::remove_file(edi_input);
}
