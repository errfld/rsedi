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
    let filename = format!("edi-cli-{name}-{}-{nanos}.{extension}", std::process::id());
    env::temp_dir().join(filename)
}

fn write_temp_mapping(name: &str, yaml: &str) -> PathBuf {
    let path = unique_temp_path(name, "yaml");
    fs::write(&path, yaml).expect("mapping file should be writable");
    path
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

    let command_output = Command::new(binary)
        .args([
            "transform",
            input.to_string_lossy().as_ref(),
            output.to_string_lossy().as_ref(),
            "-m",
            mapping.to_string_lossy().as_ref(),
        ])
        .output()
        .expect("run edi transform");

    assert!(
        command_output.status.success(),
        "expected transform to succeed; stdout: {}; stderr: {}",
        String::from_utf8_lossy(&command_output.stdout),
        String::from_utf8_lossy(&command_output.stderr)
    );

    let bytes = fs::read(&output).expect("transform output should be readable");
    assert!(!bytes.is_empty(), "transform output should not be empty");

    let value: serde_json::Value =
        serde_json::from_slice(&bytes).expect("transform output should be valid JSON");
    assert!(!value.is_null(), "transform output should not be JSON null");

    let _ = fs::remove_file(&output);
}

#[test]
fn transform_target_outputs_table_driven() {
    enum MappingSource {
        Fixture(&'static str),
        Inline(&'static str),
    }

    enum Expectation {
        Success {
            output_substrings: Vec<&'static str>,
        },
        Failure {
            exit_code: i32,
            stderr_substrings: Vec<&'static str>,
        },
    }

    struct Case {
        name: &'static str,
        input_path: &'static str,
        mapping_source: MappingSource,
        output_extension: &'static str,
        expectation: Expectation,
    }

    let cases = vec![
        Case {
            name: "csv_output",
            input_path: "testdata/edi/valid_orders_d96a_full.edi",
            mapping_source: MappingSource::Fixture("testdata/mappings/orders_to_csv.yaml"),
            output_extension: "csv",
            expectation: Expectation::Success {
                output_substrings: vec![
                    "document_type,line_number,product_code",
                    "No description supplied",
                ],
            },
        },
        Case {
            name: "edi_output",
            input_path: "testdata/edi/valid_orders_d96a_minimal.edi",
            mapping_source: MappingSource::Inline(
                r#"
name: orders_to_edifact
source_type: EANCOM_D96A_ORDERS
target_type: EANCOM_D96A_ORDERS
rules:
  - type: field
    source: /UNH/e1
    target: UNH.e1
  - type: field
    source: /BGM/e1
    target: BGM.e1
  - type: field
    source: /BGM/e2
    target: BGM.e2
"#,
            ),
            output_extension: "edi",
            expectation: Expectation::Success {
                output_substrings: vec!["UNH+1'", "BGM+220+ORDER123'"],
            },
        },
        Case {
            name: "edi_invalid_shape",
            input_path: "testdata/edi/valid_orders_d96a_minimal.edi",
            mapping_source: MappingSource::Inline(
                r#"
name: invalid_edi_shape
source_type: EANCOM_D96A_ORDERS
target_type: EANCOM_D96A_ORDERS
rules:
  - type: field
    source: /BGM/e2
    target: order_number
"#,
            ),
            output_extension: "edi",
            expectation: Expectation::Failure {
                exit_code: 2,
                stderr_substrings: vec!["No serializable EDIFACT segments found"],
            },
        },
    ];

    let binary = cargo_bin();

    for case in cases {
        let input = testdata_path(case.input_path);
        let (mapping_path, remove_mapping_file) = match &case.mapping_source {
            MappingSource::Fixture(path) => (testdata_path(path), false),
            MappingSource::Inline(yaml) => (write_temp_mapping(case.name, yaml), true),
        };
        let output = unique_temp_path(case.name, case.output_extension);

        let command_output = Command::new(&binary)
            .args([
                "transform",
                input.to_string_lossy().as_ref(),
                output.to_string_lossy().as_ref(),
                "-m",
                mapping_path.to_string_lossy().as_ref(),
            ])
            .output()
            .expect("run edi transform");

        match &case.expectation {
            Expectation::Success { output_substrings } => {
                assert!(
                    command_output.status.success(),
                    "expected transform case '{}' to succeed; stdout: {}; stderr: {}",
                    case.name,
                    String::from_utf8_lossy(&command_output.stdout),
                    String::from_utf8_lossy(&command_output.stderr)
                );

                let rendered = fs::read_to_string(&output)
                    .expect("transform output should be readable for success case");
                for substring in output_substrings {
                    assert!(
                        rendered.contains(substring),
                        "expected output for case '{}' to contain '{}', got: {}",
                        case.name,
                        substring,
                        rendered
                    );
                }
            }
            Expectation::Failure {
                exit_code,
                stderr_substrings,
            } => {
                assert_exit_code(
                    command_output.status,
                    *exit_code,
                    &format!("expected transform case '{}' to fail", case.name),
                );

                let stderr = String::from_utf8_lossy(&command_output.stderr);
                for substring in stderr_substrings {
                    assert!(
                        stderr.contains(substring),
                        "expected stderr for case '{}' to contain '{}', got: {}",
                        case.name,
                        substring,
                        stderr
                    );
                }
            }
        }

        let _ = fs::remove_file(&output);
        if remove_mapping_file {
            let _ = fs::remove_file(&mapping_path);
        }
    }
}
