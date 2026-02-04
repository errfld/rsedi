//! # edi-cli
//!
//! CLI application and configuration for EDI Integration Engine.
//!
//! This crate provides the command-line interface for running
//! EDI transformations and managing configurations.

use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::process::ExitCode;

use anyhow::{Context, anyhow, bail};
use clap::{Parser, Subcommand};
use edi_adapter_edifact::EdifactParser;
use edi_adapter_edifact::parser::ParseWarning;
use edi_ir::NodeType;
use edi_mapping::{MappingDsl, MappingRuntime};
use edi_schema::SchemaLoader;
use edi_validation::{Severity, ValidationEngine, ValidationIssue};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CliExitCode {
    Success = 0,
    Warnings = 1,
    Errors = 2,
}

impl CliExitCode {
    fn as_exit_code(self) -> ExitCode {
        ExitCode::from(self as u8)
    }
}

#[derive(Parser)]
#[command(name = "edi")]
#[command(about = "EDI Integration Engine CLI")]
#[command(version)]
struct Cli {
    /// Path to configuration file
    #[arg(short, long)]
    config: Option<String>,

    /// Subcommand to execute
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Transform an EDI file
    Transform {
        /// Input file path
        input: String,

        /// Output file path
        output: String,

        /// Mapping file path
        #[arg(short, long)]
        mapping: String,

        /// Schema file path
        #[arg(short, long)]
        schema: Option<String>,
    },

    /// Validate an EDI file against a schema
    Validate {
        /// Input file path
        input: String,

        /// Schema file path
        #[arg(short, long)]
        schema: String,
    },

    /// Generate a sample EDI file
    Generate {
        /// Output file path
        output: String,

        /// Message type (e.g., ORDERS, DESADV)
        #[arg(short, long)]
        message_type: String,

        /// Schema version (e.g., D96A)
        #[arg(short, long)]
        version: String,
    },
}

fn main() -> ExitCode {
    tracing_subscriber::fmt::init();

    match run() {
        Ok(code) => code.as_exit_code(),
        Err(error) => {
            eprintln!("Error: {error:#}");
            CliExitCode::Errors.as_exit_code()
        }
    }
}

fn run() -> anyhow::Result<CliExitCode> {
    let cli = Cli::parse();

    if let Some(config_path) = cli.config.as_deref() {
        tracing::info!(config = %config_path, "Configuration file argument provided");
    }

    match cli.command {
        Commands::Transform {
            input,
            output,
            mapping,
            schema,
        } => {
            transform(&input, &output, &mapping, schema.as_deref())?;
            Ok(CliExitCode::Success)
        }
        Commands::Validate { input, schema } => validate(&input, &schema),
        Commands::Generate {
            output,
            message_type,
            version,
        } => {
            tracing::info!(%output, %message_type, %version, "Generate command requested");
            Err(anyhow!(
                "The 'generate' command is not implemented in this MVP"
            ))
        }
    }
}

fn transform(
    input_path: &str,
    output_path: &str,
    mapping_path: &str,
    schema_path: Option<&str>,
) -> anyhow::Result<()> {
    tracing::info!(input = %input_path, output = %output_path, mapping = %mapping_path, "Starting transform command");

    if let Some(schema) = schema_path {
        tracing::info!(schema = %schema, "Schema argument provided for transform (not used in MVP flow)");
    }

    let input_bytes = std::fs::read(input_path)
        .with_context(|| format!("Failed to read input file '{}'", input_path))?;

    let parser = EdifactParser::new();
    let parsed = parser
        .parse_with_warnings(&input_bytes, input_path)
        .with_context(|| format!("Failed to parse EDIFACT input '{}'", input_path))?;

    if parsed.documents.is_empty() {
        bail!("No EDIFACT messages were found in '{}'", input_path);
    }

    let mapping = MappingDsl::parse_file(Path::new(mapping_path))
        .map_err(|error| anyhow!("Failed to parse mapping '{}': {}", mapping_path, error))?;

    let mut runtime = MappingRuntime::new();
    let mut mapped_documents = Vec::with_capacity(parsed.documents.len());

    for (index, document) in parsed.documents.iter().enumerate() {
        let mapped = runtime.execute(&mapping, document).with_context(|| {
            format!(
                "Failed to apply mapping '{}' to message {}",
                mapping_path,
                index + 1
            )
        })?;
        mapped_documents.push(mapped);
    }

    let mut output_file = File::create(output_path)
        .with_context(|| format!("Failed to create output file '{}'", output_path))?;

    match mapped_documents.as_slice() {
        [single_document] => serde_json::to_writer_pretty(&mut output_file, single_document)
            .with_context(|| {
                format!(
                    "Failed to serialize transformed output to '{}'",
                    output_path
                )
            })?,
        _ => serde_json::to_writer_pretty(&mut output_file, &mapped_documents).with_context(
            || {
                format!(
                    "Failed to serialize transformed output to '{}'",
                    output_path
                )
            },
        )?,
    }

    output_file
        .write_all(b"\n")
        .with_context(|| format!("Failed to finalize output file '{}'", output_path))?;

    for warning in &parsed.warnings {
        eprintln!("WARNING: {}", format_parse_warning(warning));
    }

    println!(
        "Transform complete: {} message(s) written to {} ({} parse warning(s)).",
        mapped_documents.len(),
        output_path,
        parsed.warnings.len()
    );

    Ok(())
}

fn validate(input_path: &str, schema_path: &str) -> anyhow::Result<CliExitCode> {
    tracing::info!(input = %input_path, schema = %schema_path, "Starting validate command");

    let input_bytes = std::fs::read(input_path)
        .with_context(|| format!("Failed to read input file '{}'", input_path))?;

    let parser = EdifactParser::new();
    let parsed = parser
        .parse_with_warnings(&input_bytes, input_path)
        .with_context(|| format!("Failed to parse EDIFACT input '{}'", input_path))?;

    if parsed.documents.is_empty() {
        eprintln!(
            "Validation summary: no EDIFACT messages found in '{}'",
            input_path
        );
        return Ok(CliExitCode::Errors);
    }

    let schema_loader = SchemaLoader::new(Vec::new());
    let schema = schema_loader
        .load_from_file(Path::new(schema_path))
        .with_context(|| format!("Failed to load schema '{}'", schema_path))?;

    let validator = ValidationEngine::new();

    let mut error_lines: Vec<String> = Vec::new();
    let mut warning_lines: Vec<String> = parsed.warnings.iter().map(format_parse_warning).collect();

    let mut error_count = 0usize;
    let mut warning_count = warning_lines.len();

    for (index, document) in parsed.documents.iter().enumerate() {
        let message_number = index + 1;
        let normalized_document = normalize_document_for_validation(document);
        let result = validator
            .validate_with_schema(&normalized_document, &schema)
            .with_context(|| {
                format!(
                    "Validation failed while processing message {}",
                    message_number
                )
            })?;

        for issue in result.report.all_issues() {
            match issue.severity {
                Severity::Error => {
                    error_count += 1;
                    error_lines.push(format_validation_issue(message_number, issue));
                }
                Severity::Warning => {
                    warning_count += 1;
                    warning_lines.push(format_validation_issue(message_number, issue));
                }
                Severity::Info => {}
            }
        }
    }

    println!(
        "Validation summary for '{}' against '{}':",
        input_path, schema_path
    );
    println!("  Messages: {}", parsed.documents.len());
    println!("  Errors: {}", error_count);
    println!("  Warnings: {}", warning_count);

    if !error_lines.is_empty() {
        println!("\nErrors:");
        for line in &error_lines {
            println!("  - {}", line);
        }
    }

    if !warning_lines.is_empty() {
        println!("\nWarnings:");
        for line in &warning_lines {
            println!("  - {}", line);
        }
    }

    if error_count > 0 {
        Ok(CliExitCode::Errors)
    } else if warning_count > 0 {
        Ok(CliExitCode::Warnings)
    } else {
        println!("\nValidation passed with no warnings.");
        Ok(CliExitCode::Success)
    }
}

fn normalize_document_for_validation(document: &edi_ir::Document) -> edi_ir::Document {
    let mut normalized = document.clone();

    if normalized.root.node_type == NodeType::Message {
        normalized.root.node_type = NodeType::Root;
        normalized.root.name = "ROOT".to_string();
    }

    normalized
}

fn format_parse_warning(warning: &ParseWarning) -> String {
    let message_ref = warning
        .message_ref
        .as_deref()
        .unwrap_or("unknown_message_ref");

    format!(
        "message_ref={} line={} col={}: {}",
        message_ref, warning.position.line, warning.position.column, warning.message
    )
}

fn format_validation_issue(message_number: usize, issue: &ValidationIssue) -> String {
    let code = issue.code.as_deref().unwrap_or("UNKNOWN");
    let mut location_parts = Vec::new();

    if !issue.path.is_empty() {
        location_parts.push(format!("path={}", issue.path));
    }
    if let Some(line) = issue.line {
        location_parts.push(format!("line={}", line));
    }
    if let Some(segment_pos) = issue.segment_pos {
        location_parts.push(format!("segment={}", segment_pos));
    }
    if let Some(element_pos) = issue.element_pos {
        location_parts.push(format!("element={}", element_pos));
    }
    if let Some(component_pos) = issue.component_pos {
        location_parts.push(format!("component={}", component_pos));
    }

    let location = if location_parts.is_empty() {
        "location=unknown".to_string()
    } else {
        location_parts.join(", ")
    };

    format!(
        "message #{} [{}] {} ({})",
        message_number, code, issue.message, location
    )
}
