//! # edi-cli
//!
//! CLI application and configuration for EDI Integration Engine.
//!
//! This crate provides the command-line interface for running
//! EDI transformations and managing configurations.

use std::borrow::Cow;
use std::env;
use std::fs::File;
use std::io::{IsTerminal, Write};
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use anyhow::{Context, anyhow, bail};
use clap::{Parser, Subcommand, ValueEnum};
use edi_adapter_csv::{ColumnDef, CsvConfig, CsvSchema, CsvWriter};
use edi_adapter_edifact::parser::ParseWarning;
use edi_adapter_edifact::{EdifactParser, EdifactSerializer};
use edi_ir::Document;
use edi_ir::NodeType;
use edi_ir::Value;
use edi_mapping::{MappingDsl, MappingRuntime};
use edi_schema::SchemaLoader;
use edi_validation::{Severity, ValidationEngine, ValidationIssue};
use serde::Deserialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CliExitCode {
    Success = 0,
    Warnings = 1,
    Errors = 2,
    Fatal = 3,
}

impl CliExitCode {
    fn as_exit_code(self) -> ExitCode {
        ExitCode::from(self as u8)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
enum ColorMode {
    #[default]
    Auto,
    Always,
    Never,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct CliConfig {
    progress: bool,
    progress_threshold_bytes: u64,
    color: ColorMode,
}

impl Default for CliConfig {
    fn default() -> Self {
        Self {
            progress: true,
            progress_threshold_bytes: 1024 * 1024,
            color: ColorMode::Auto,
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct RuntimeOptions {
    progress: bool,
    progress_threshold_bytes: u64,
    color: ColorMode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TransformOutputFormat {
    Json,
    Csv,
    Edi,
}

impl TransformOutputFormat {
    fn from_target_type(target_type: &str) -> anyhow::Result<Self> {
        let normalized = target_type.trim().to_ascii_uppercase();
        let tokens: Vec<&str> = normalized
            .split(|c: char| !c.is_ascii_alphanumeric())
            .filter(|token| !token.is_empty())
            .collect();

        if tokens.contains(&"JSON") {
            Ok(Self::Json)
        } else if tokens.contains(&"CSV") {
            Ok(Self::Csv)
        } else if tokens
            .iter()
            .any(|token| matches!(*token, "EDI" | "EDIFACT" | "EANCOM"))
        {
            Ok(Self::Edi)
        } else {
            bail!(
                "Unsupported mapping target_type '{}'; expected one containing JSON, CSV, or EDI",
                target_type
            );
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Json => "JSON",
            Self::Csv => "CSV",
            Self::Edi => "EDI",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum GenerateInputFormat {
    Csv,
    Json,
}

impl GenerateInputFormat {
    fn from_source_type(source_type: &str) -> anyhow::Result<Self> {
        let normalized = source_type.trim().to_ascii_uppercase();
        let tokens: Vec<&str> = normalized
            .split(|c: char| !c.is_ascii_alphanumeric())
            .filter(|token| !token.is_empty())
            .collect();

        if tokens.contains(&"CSV") {
            Ok(Self::Csv)
        } else if tokens.contains(&"JSON") {
            Ok(Self::Json)
        } else {
            bail!(
                "Unsupported mapping source_type '{}'; expected one containing CSV or JSON",
                source_type
            );
        }
    }

    fn from_input_path(path: &str) -> Option<Self> {
        let extension = Path::new(path).extension()?.to_str()?.to_ascii_lowercase();
        match extension.as_str() {
            "csv" => Some(Self::Csv),
            "json" => Some(Self::Json),
            _ => None,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Csv => "CSV",
            Self::Json => "JSON",
        }
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

        /// Output file path (writes to stdout when omitted)
        output: Option<String>,

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

    /// Parse an EDI file and output JSON IR
    Parse {
        /// Input file path
        input: String,

        /// Output file path (writes to stdout when omitted)
        output: Option<String>,

        /// Pretty-print JSON output
        #[arg(long, default_value_t = false)]
        pretty: bool,
    },

    /// Generate EDI output from CSV/JSON input using a mapping
    Generate {
        /// Input file path (CSV or JSON)
        input: String,

        /// Output file path (writes to stdout when omitted)
        output: Option<String>,

        /// Mapping file path
        #[arg(short, long)]
        mapping: String,

        /// Source input format (inferred from file extension or mapping source_type when omitted)
        #[arg(long, value_enum)]
        input_format: Option<GenerateInputFormat>,
    },
}

fn main() -> ExitCode {
    tracing_subscriber::fmt::init();

    match run() {
        Ok(code) => code.as_exit_code(),
        Err(error) => {
            print_error(ColorMode::Auto, &format!("{error:#}"));
            CliExitCode::Fatal.as_exit_code()
        }
    }
}

fn run() -> anyhow::Result<CliExitCode> {
    let cli = Cli::parse();
    let config = load_cli_config(cli.config.as_deref())?;
    let runtime = RuntimeOptions {
        progress: config.progress,
        progress_threshold_bytes: config.progress_threshold_bytes,
        color: config.color,
    };

    if let Some(config_path) = cli.config.as_deref() {
        tracing::info!(config = %config_path, "Loaded explicit CLI config");
    }

    let command_result = match cli.command {
        Commands::Transform {
            input,
            output,
            mapping,
            schema,
        } => transform(
            &input,
            output.as_deref(),
            &mapping,
            schema.as_deref(),
            runtime,
        ),
        Commands::Validate { input, schema } => validate(&input, &schema, runtime),
        Commands::Parse {
            input,
            output,
            pretty,
        } => parse(&input, output.as_deref(), pretty, runtime),
        Commands::Generate {
            input,
            output,
            mapping,
            input_format,
        } => generate(&input, output.as_deref(), &mapping, input_format, runtime),
    };

    match command_result {
        Ok(code) => Ok(code),
        Err(error) => {
            print_error(runtime.color, &format!("{error:#}"));
            Ok(CliExitCode::Errors)
        }
    }
}

fn load_cli_config(explicit_path: Option<&str>) -> anyhow::Result<CliConfig> {
    if let Some(path) = explicit_path {
        let path = PathBuf::from(path);
        return read_cli_config_file(&path);
    }

    for path in default_config_paths() {
        if path.exists() {
            return read_cli_config_file(&path);
        }
    }

    Ok(CliConfig::default())
}

fn read_cli_config_file(path: &Path) -> anyhow::Result<CliConfig> {
    let bytes = std::fs::read(path)
        .with_context(|| format!("Failed to read CLI config '{}'", path.display()))?;
    serde_yaml::from_slice(&bytes)
        .with_context(|| format!("Failed to parse CLI config '{}'", path.display()))
}

fn default_config_paths() -> Vec<PathBuf> {
    let mut paths = Vec::new();

    if let Ok(current_dir) = env::current_dir() {
        paths.push(current_dir.join("edi-cli.yaml"));
        paths.push(current_dir.join(".edi-cli.yaml"));
    }

    if let Some(config_home) = env::var_os("XDG_CONFIG_HOME") {
        paths.push(PathBuf::from(config_home).join("edi/cli.yaml"));
    } else if let Some(appdata) = env::var_os("APPDATA") {
        paths.push(PathBuf::from(appdata).join("edi/cli.yaml"));
    } else if let Some(home) = env::var_os("HOME").or_else(|| env::var_os("USERPROFILE")) {
        paths.push(PathBuf::from(home).join(".config/edi/cli.yaml"));
    }

    paths
}

fn use_color(mode: ColorMode) -> bool {
    match mode {
        ColorMode::Always => true,
        ColorMode::Never => false,
        ColorMode::Auto => std::io::stderr().is_terminal() && env::var_os("NO_COLOR").is_none(),
    }
}

fn print_error(color_mode: ColorMode, message: &str) {
    if use_color(color_mode) {
        eprintln!("\x1b[31mERROR\x1b[0m: {message}");
    } else {
        eprintln!("ERROR: {message}");
    }
}

fn print_warning(color_mode: ColorMode, message: &str) {
    if use_color(color_mode) {
        eprintln!("\x1b[33mWARNING\x1b[0m: {message}");
    } else {
        eprintln!("WARNING: {message}");
    }
}

fn emit_progress(runtime: RuntimeOptions, input_path: &str, stage: &str) {
    if !runtime.progress {
        return;
    }

    let should_emit = std::fs::metadata(input_path)
        .map(|metadata| metadata.len() >= runtime.progress_threshold_bytes)
        .unwrap_or(false);
    if !should_emit {
        return;
    }

    if use_color(runtime.color) {
        eprintln!("\x1b[36mPROGRESS\x1b[0m: {stage}");
    } else {
        eprintln!("PROGRESS: {stage}");
    }
}

fn transform(
    input_path: &str,
    output_path: Option<&str>,
    mapping_path: &str,
    schema_path: Option<&str>,
    runtime: RuntimeOptions,
) -> anyhow::Result<CliExitCode> {
    tracing::info!(
        input = %input_path,
        output = output_path.unwrap_or("stdout"),
        mapping = %mapping_path,
        "Starting transform command"
    );

    if let Some(schema) = schema_path {
        tracing::warn!(schema = %schema, "Transform schema argument is not implemented in this MVP and will be ignored");
        print_warning(
            runtime.color,
            &format!(
                "Transform schema argument is not implemented in this MVP; ignoring '{}'.",
                schema
            ),
        );
    }

    emit_progress(runtime, input_path, "reading EDIFACT input");
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
        .with_context(|| format!("Failed to parse mapping '{}'", mapping_path))?;
    let output_format = TransformOutputFormat::from_target_type(&mapping.target_type)
        .with_context(|| format!("Mapping '{}' has invalid target_type", mapping_path))?;

    let mut mapping_runtime = MappingRuntime::new();
    let mut mapped_documents = Vec::with_capacity(parsed.documents.len());

    for (index, document) in parsed.documents.iter().enumerate() {
        let mapped = mapping_runtime
            .execute(&mapping, document)
            .with_context(|| {
                format!(
                    "Failed to apply mapping '{}' to message {}",
                    mapping_path,
                    index + 1
                )
            })?;
        mapped_documents.push(mapped);
    }

    emit_progress(runtime, input_path, "serializing transformed output");
    write_transformed_output(mapped_documents.as_slice(), output_format, output_path)?;

    for warning in &parsed.warnings {
        print_warning(runtime.color, &format_parse_warning(warning, input_path));
    }

    let destination = output_path.unwrap_or("stdout");
    tracing::info!(
        message_count = mapped_documents.len(),
        destination = destination,
        format = output_format.as_str(),
        warning_count = parsed.warnings.len(),
        "Transform complete"
    );

    if parsed.warnings.is_empty() {
        Ok(CliExitCode::Success)
    } else {
        Ok(CliExitCode::Warnings)
    }
}

fn parse(
    input_path: &str,
    output_path: Option<&str>,
    pretty: bool,
    runtime: RuntimeOptions,
) -> anyhow::Result<CliExitCode> {
    tracing::info!(
        input = %input_path,
        output = output_path.unwrap_or("stdout"),
        pretty,
        "Starting parse command"
    );

    emit_progress(runtime, input_path, "reading EDIFACT input");
    let input_bytes = std::fs::read(input_path)
        .with_context(|| format!("Failed to read input file '{}'", input_path))?;

    let parser = EdifactParser::new();
    let parsed = parser
        .parse_with_warnings(&input_bytes, input_path)
        .with_context(|| format!("Failed to parse EDIFACT input '{}'", input_path))?;

    if parsed.documents.is_empty() {
        print_error(
            runtime.color,
            &format!("No EDIFACT messages were found in '{}'", input_path),
        );
        return Ok(CliExitCode::Errors);
    }

    match output_path {
        Some(path) => {
            let output_file =
                File::create(path).with_context(|| format!("Failed to create '{}'", path))?;
            if pretty {
                serde_json::to_writer_pretty(output_file, &parsed.documents)
                    .with_context(|| format!("Failed to write parsed JSON to '{}'", path))?;
            } else {
                serde_json::to_writer(output_file, &parsed.documents)
                    .with_context(|| format!("Failed to write parsed JSON to '{}'", path))?;
            }
        }
        None => {
            let stdout = std::io::stdout();
            let mut handle = stdout.lock();
            if pretty {
                serde_json::to_writer_pretty(&mut handle, &parsed.documents)
                    .context("Failed to write parsed JSON to stdout")?;
            } else {
                serde_json::to_writer(&mut handle, &parsed.documents)
                    .context("Failed to write parsed JSON to stdout")?;
            }
            handle
                .write_all(b"\n")
                .context("Failed to finalize parse output on stdout")?;
        }
    }

    for warning in &parsed.warnings {
        print_warning(runtime.color, &format_parse_warning(warning, input_path));
    }

    eprintln!(
        "Parse summary: messages={}, warnings={}",
        parsed.documents.len(),
        parsed.warnings.len()
    );

    if parsed.warnings.is_empty() {
        Ok(CliExitCode::Success)
    } else {
        Ok(CliExitCode::Warnings)
    }
}

fn generate(
    input_path: &str,
    output_path: Option<&str>,
    mapping_path: &str,
    input_format: Option<GenerateInputFormat>,
    runtime: RuntimeOptions,
) -> anyhow::Result<CliExitCode> {
    tracing::info!(
        input = %input_path,
        output = output_path.unwrap_or("stdout"),
        mapping = %mapping_path,
        requested_input_format = input_format.map(GenerateInputFormat::as_str),
        "Starting generate command"
    );

    emit_progress(runtime, input_path, "loading mapping and source document");
    let mapping = MappingDsl::parse_file(Path::new(mapping_path))
        .with_context(|| format!("Failed to parse mapping '{}'", mapping_path))?;
    let resolved_input_format =
        resolve_generate_input_format(input_path, input_format, &mapping.source_type)
            .with_context(|| {
                format!(
                    "Failed to determine input format for '{}'. Use --input-format csv|json to override inference",
                    input_path
                )
            })?;

    let source_document = load_generate_source_document(input_path, resolved_input_format)
        .with_context(|| {
            format!(
                "Failed to load {} input '{}'",
                resolved_input_format.as_str(),
                input_path
            )
        })?;

    let output_format = TransformOutputFormat::from_target_type(&mapping.target_type)
        .with_context(|| format!("Mapping '{}' has invalid target_type", mapping_path))?;
    if output_format != TransformOutputFormat::Edi {
        print_error(
            runtime.color,
            &format!(
                "Generate mapping '{}' must target EDI output, but target_type '{}' resolves to {}",
                mapping_path,
                mapping.target_type,
                output_format.as_str()
            ),
        );
        return Ok(CliExitCode::Errors);
    }

    let mut mapping_runtime = MappingRuntime::new();
    let mapped_document = mapping_runtime
        .execute(&mapping, &source_document)
        .with_context(|| {
            format!(
                "Failed to apply mapping '{}' to generate input",
                mapping_path
            )
        })?;

    emit_progress(runtime, input_path, "writing generated EDI output");
    write_transformed_output(
        std::slice::from_ref(&mapped_document),
        TransformOutputFormat::Edi,
        output_path,
    )?;

    let destination = output_path.unwrap_or("stdout");
    tracing::info!(
        destination = destination,
        source_format = resolved_input_format.as_str(),
        "Generate complete"
    );

    Ok(CliExitCode::Success)
}

fn resolve_generate_input_format(
    input_path: &str,
    input_format: Option<GenerateInputFormat>,
    source_type: &str,
) -> anyhow::Result<GenerateInputFormat> {
    if let Some(explicit) = input_format {
        return Ok(explicit);
    }

    if let Some(from_path) = GenerateInputFormat::from_input_path(input_path) {
        return Ok(from_path);
    }

    GenerateInputFormat::from_source_type(source_type)
}

fn load_generate_source_document(
    input_path: &str,
    input_format: GenerateInputFormat,
) -> anyhow::Result<Document> {
    match input_format {
        GenerateInputFormat::Csv => load_csv_source_document(input_path),
        GenerateInputFormat::Json => load_json_source_document(input_path),
    }
}

fn load_csv_source_document(input_path: &str) -> anyhow::Result<Document> {
    let input_file =
        File::open(input_path).with_context(|| format!("Failed to open '{}'", input_path))?;
    let reader = edi_adapter_csv::CsvReader::new();
    let csv_document = reader
        .read_to_ir(input_file)
        .map_err(|error| anyhow!("Failed to parse CSV input: {error}"))?;

    Ok(normalize_csv_source_document(csv_document))
}

fn normalize_csv_source_document(document: Document) -> Document {
    let edi_ir::Document {
        root,
        metadata,
        schema_ref,
    } = document;

    let mut rows = edi_ir::Node::new("rows", NodeType::SegmentGroup);
    for mut record in root.children {
        record.name = "row".to_string();
        rows.add_child(record);
    }

    let mut normalized_root = edi_ir::Node::new("ROOT", NodeType::Root);
    normalized_root.add_child(rows);

    let mut normalized = Document::with_metadata(normalized_root, metadata);
    normalized.schema_ref = schema_ref;
    normalized
}

fn load_json_source_document(input_path: &str) -> anyhow::Result<Document> {
    let input_bytes = std::fs::read(input_path)
        .with_context(|| format!("Failed to read input file '{}'", input_path))?;

    if let Ok(document) = serde_json::from_slice::<Document>(&input_bytes) {
        return Ok(document);
    }

    let json_value: serde_json::Value = serde_json::from_slice(&input_bytes)
        .with_context(|| format!("Failed to parse JSON payload '{}'", input_path))?;
    Ok(document_from_json_value(&json_value))
}

fn document_from_json_value(value: &serde_json::Value) -> Document {
    let mut root = edi_ir::Node::new("ROOT", NodeType::Root);
    match value {
        serde_json::Value::Object(properties) => {
            for (name, property) in properties {
                root.add_child(json_property_to_node(name, property));
            }
        }
        serde_json::Value::Array(items) => {
            root.add_child(json_array_to_group("rows", "item", items));
        }
        _ => {
            root.add_child(edi_ir::Node::with_value(
                "value",
                NodeType::Field,
                json_scalar_to_ir_value(value),
            ));
        }
    }
    Document::new(root)
}

fn json_property_to_node(name: &str, value: &serde_json::Value) -> edi_ir::Node {
    match value {
        serde_json::Value::Object(properties) => {
            let mut node = edi_ir::Node::new(name, NodeType::SegmentGroup);
            for (property_name, property_value) in properties {
                node.add_child(json_property_to_node(property_name, property_value));
            }
            node
        }
        serde_json::Value::Array(items) => json_array_to_group(name, "item", items),
        _ => edi_ir::Node::with_value(name, NodeType::Field, json_scalar_to_ir_value(value)),
    }
}

fn json_array_to_group(
    group_name: &str,
    item_name: &str,
    items: &[serde_json::Value],
) -> edi_ir::Node {
    let mut group = edi_ir::Node::new(group_name, NodeType::SegmentGroup);
    for item in items {
        group.add_child(json_array_item_to_node(item_name, item));
    }
    group
}

fn json_array_item_to_node(item_name: &str, value: &serde_json::Value) -> edi_ir::Node {
    match value {
        serde_json::Value::Object(properties) => {
            let mut node = edi_ir::Node::new(item_name, NodeType::Record);
            for (name, property) in properties {
                node.add_child(json_property_to_node(name, property));
            }
            node
        }
        serde_json::Value::Array(items) => json_array_to_group(item_name, "item", items),
        _ => edi_ir::Node::with_value(item_name, NodeType::Field, json_scalar_to_ir_value(value)),
    }
}

fn json_scalar_to_ir_value(value: &serde_json::Value) -> Value {
    match value {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(boolean) => Value::Boolean(*boolean),
        serde_json::Value::Number(number) => {
            if let Some(integer) = number.as_i64() {
                Value::Integer(integer)
            } else if let Some(unsigned) = number.as_u64() {
                if let Ok(integer) = i64::try_from(unsigned) {
                    Value::Integer(integer)
                } else {
                    Value::String(number.to_string())
                }
            } else if let Some(decimal) = number.as_f64() {
                Value::Decimal(decimal)
            } else {
                Value::String(number.to_string())
            }
        }
        serde_json::Value::String(text) => Value::String(text.clone()),
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => Value::Null,
    }
}

fn write_transformed_output(
    mapped_documents: &[Document],
    output_format: TransformOutputFormat,
    output_path: Option<&str>,
) -> anyhow::Result<()> {
    match output_path {
        Some(path) => {
            let mut output_file = File::create(path)
                .with_context(|| format!("Failed to create output file '{}'", path))?;
            serialize_transformed_documents(mapped_documents, output_format, &mut output_file)
                .with_context(|| {
                    format!(
                        "Failed to serialize transformed {} output to '{}'",
                        output_format.as_str(),
                        path
                    )
                })?;
            if output_format != TransformOutputFormat::Csv {
                output_file
                    .write_all(b"\n")
                    .with_context(|| format!("Failed to finalize output file '{}'", path))?;
            }
        }
        None => {
            let stdout = std::io::stdout();
            let mut stdout_handle = stdout.lock();
            serialize_transformed_documents(mapped_documents, output_format, &mut stdout_handle)
                .with_context(|| {
                    format!(
                        "Failed to serialize transformed {} output to stdout",
                        output_format.as_str()
                    )
                })?;
            if output_format != TransformOutputFormat::Csv {
                stdout_handle
                    .write_all(b"\n")
                    .context("Failed to finalize transformed output on stdout")?;
            }
        }
    }

    Ok(())
}

fn serialize_transformed_documents<W: Write>(
    mapped_documents: &[Document],
    output_format: TransformOutputFormat,
    writer: &mut W,
) -> anyhow::Result<()> {
    match output_format {
        TransformOutputFormat::Json => match mapped_documents {
            [single_document] => serde_json::to_writer_pretty(writer, single_document)
                .context("Failed to serialize mapped document as JSON")?,
            _ => serde_json::to_writer_pretty(writer, mapped_documents)
                .context("Failed to serialize mapped documents as JSON")?,
        },
        TransformOutputFormat::Csv => serialize_documents_as_csv(mapped_documents, writer)?,
        TransformOutputFormat::Edi => serialize_documents_as_edi(mapped_documents, writer)?,
    }

    Ok(())
}

fn serialize_documents_as_edi<W: Write>(
    mapped_documents: &[Document],
    writer: &mut W,
) -> anyhow::Result<()> {
    let serializer = EdifactSerializer::new();

    for (index, document) in mapped_documents.iter().enumerate() {
        let payload = serializer.serialize_document(document).map_err(|error| {
            anyhow!(
                "message {} does not match EDIFACT output shape requirements: {}",
                index + 1,
                error
            )
        })?;

        writer
            .write_all(payload.as_bytes())
            .with_context(|| format!("failed to write EDI payload for message {}", index + 1))?;

        if index + 1 < mapped_documents.len() {
            writer.write_all(b"\n").with_context(|| {
                format!(
                    "failed to separate EDI payloads between messages {} and {}",
                    index + 1,
                    index + 2
                )
            })?;
        }
    }

    Ok(())
}

fn serialize_documents_as_csv<W: Write>(
    mapped_documents: &[Document],
    writer: &mut W,
) -> anyhow::Result<()> {
    let csv_schema = mapped_documents
        .first()
        .and_then(infer_csv_schema_from_document);

    for (index, document) in mapped_documents.iter().enumerate() {
        let mut csv_writer = CsvWriter::new();
        if let Some(schema) = &csv_schema {
            csv_writer = csv_writer.with_schema(schema.clone());
        }
        csv_writer = csv_writer.with_config(CsvConfig::new().has_header(index == 0));

        csv_writer
            .write_from_ir(&mut *writer, document)
            .map_err(|err| {
                anyhow!(
                    "failed to serialize mapped document {} as CSV: {err}",
                    index + 1
                )
            })?;
    }
    Ok(())
}

fn infer_csv_schema_from_document(document: &Document) -> Option<CsvSchema> {
    let first_record = csv_records_from_document(document).first()?;

    let schema = first_record.children.iter().fold(
        CsvSchema::with_name("transform_output").with_header(),
        |schema, child| schema.add_column(ColumnDef::new(child.name.clone())),
    );

    Some(schema)
}

fn csv_records_from_document(document: &Document) -> &[edi_ir::Node] {
    let root = &document.root;
    if root.children.len() == 1
        && root.children[0].node_type == NodeType::SegmentGroup
        && !root.children[0].children.is_empty()
    {
        &root.children[0].children
    } else {
        &root.children
    }
}

fn validate(
    input_path: &str,
    schema_path: &str,
    runtime: RuntimeOptions,
) -> anyhow::Result<CliExitCode> {
    tracing::info!(input = %input_path, schema = %schema_path, "Starting validate command");

    emit_progress(runtime, input_path, "reading EDIFACT input");
    let input_bytes = std::fs::read(input_path)
        .with_context(|| format!("Failed to read input file '{}'", input_path))?;

    let parser = EdifactParser::new();
    let parsed = parser
        .parse_with_warnings(&input_bytes, input_path)
        .with_context(|| format!("Failed to parse EDIFACT input '{}'", input_path))?;

    if parsed.documents.is_empty() {
        print_error(
            runtime.color,
            &format!(
                "Validation summary: no EDIFACT messages found in '{}'",
                input_path
            ),
        );
        return Ok(CliExitCode::Errors);
    }

    let schema_loader = SchemaLoader::new(Vec::new());
    let schema = schema_loader
        .load_from_file(Path::new(schema_path))
        .with_context(|| format!("Failed to load schema '{}'", schema_path))?;

    let validator = ValidationEngine::new();

    let mut error_lines: Vec<String> = Vec::new();
    let mut warning_lines: Vec<String> = parsed
        .warnings
        .iter()
        .map(|warning| format_parse_warning(warning, input_path))
        .collect();
    let mut info_lines: Vec<String> = Vec::new();

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
                    error_lines.push(format_validation_issue(message_number, input_path, issue));
                }
                Severity::Warning => {
                    warning_count += 1;
                    warning_lines.push(format_validation_issue(message_number, input_path, issue));
                }
                Severity::Info => {
                    let formatted = format_validation_issue(message_number, input_path, issue);
                    tracing::debug!(issue = %formatted, "Validation info issue");
                    info_lines.push(formatted);
                }
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

    if !info_lines.is_empty() {
        println!("\nInfo:");
        for line in &info_lines {
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

/// `normalize_document_for_validation` adapts parser output to the validation engine contract:
/// validation expects a `NodeType::Root` entry node, while EDIFACT parsing can produce a
/// `NodeType::Message` root. When that happens we clone and remap `NodeType::Message` to
/// `NodeType::Root` named `ROOT`; otherwise we borrow the original document without cloning.
fn normalize_document_for_validation<'a>(
    document: &'a edi_ir::Document,
) -> Cow<'a, edi_ir::Document> {
    if document.root.node_type == NodeType::Message {
        let mut normalized = document.clone();
        normalized.root.node_type = NodeType::Root;
        normalized.root.name = "ROOT".to_string();
        Cow::Owned(normalized)
    } else {
        Cow::Borrowed(document)
    }
}

fn format_parse_warning(warning: &ParseWarning, source_path: &str) -> String {
    let message_ref = warning
        .message_ref
        .as_deref()
        .unwrap_or("unknown_message_ref");

    format!(
        "file={} message_ref={} line={} col={}: {}",
        source_path, message_ref, warning.position.line, warning.position.column, warning.message
    )
}

fn format_validation_issue(
    message_number: usize,
    source_path: &str,
    issue: &ValidationIssue,
) -> String {
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
        "file={} message #{} [{}] {} ({})",
        source_path, message_number, code, issue.message, location
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use edi_ir::{Node, Value};

    #[test]
    fn infer_transform_output_format_recognizes_edi_family_tokens() {
        assert_eq!(
            TransformOutputFormat::from_target_type("EANCOM_D96A_ORDERS").expect("eancom"),
            TransformOutputFormat::Edi
        );
        assert_eq!(
            TransformOutputFormat::from_target_type("EDIFACT_ORDERS").expect("edifact"),
            TransformOutputFormat::Edi
        );
        assert_eq!(
            TransformOutputFormat::from_target_type("EDI_ORDERS").expect("edi"),
            TransformOutputFormat::Edi
        );
    }

    #[test]
    fn infer_transform_output_format_does_not_match_edi_substrings() {
        assert!(TransformOutputFormat::from_target_type("CREDIT_NOTE").is_err());
        assert!(TransformOutputFormat::from_target_type("MEDICAL_ORDER").is_err());
    }

    #[test]
    fn infer_transform_output_format_handles_json_and_csv() {
        assert_eq!(
            TransformOutputFormat::from_target_type("JSON_ORDERS").expect("json"),
            TransformOutputFormat::Json
        );
        assert_eq!(
            TransformOutputFormat::from_target_type("CSV_ORDERS").expect("csv"),
            TransformOutputFormat::Csv
        );
    }

    #[test]
    fn serialize_documents_as_csv_reuses_first_document_schema() {
        let first_document = csv_document(&[("order_number", "A100"), ("line_number", "1")]);
        let second_document = csv_document(&[
            ("line_number", "2"),
            ("order_number", "A100"),
            ("unexpected_extra_field", "ignored"),
        ]);

        let mut output = Vec::new();
        serialize_documents_as_csv(&[first_document, second_document], &mut output)
            .expect("serialize documents as CSV");

        let csv = String::from_utf8(output).expect("CSV output should be valid UTF-8");
        let mut lines = csv.lines();

        assert_eq!(lines.next(), Some("order_number,line_number"));
        assert_eq!(lines.next(), Some("A100,1"));
        assert_eq!(lines.next(), Some("A100,2"));
        assert_eq!(lines.next(), None);
    }

    #[test]
    fn infer_generate_input_format_from_source_type() {
        assert_eq!(
            GenerateInputFormat::from_source_type("CSV_ORDERS").expect("csv"),
            GenerateInputFormat::Csv
        );
        assert_eq!(
            GenerateInputFormat::from_source_type("ORDERS_JSON").expect("json"),
            GenerateInputFormat::Json
        );
        assert!(GenerateInputFormat::from_source_type("EANCOM_ORDERS").is_err());
    }

    #[test]
    fn infer_generate_input_format_from_file_extension() {
        assert_eq!(
            GenerateInputFormat::from_input_path("/tmp/orders.csv"),
            Some(GenerateInputFormat::Csv)
        );
        assert_eq!(
            GenerateInputFormat::from_input_path("/tmp/orders.JSON"),
            Some(GenerateInputFormat::Json)
        );
        assert_eq!(GenerateInputFormat::from_input_path("/tmp/orders"), None);
    }

    #[test]
    fn document_from_json_value_normalizes_root_array_to_rows() {
        let input: serde_json::Value = serde_json::json!([
            { "DOCUMENT_NUMBER": "ORD-1", "LINE_NUMBER": 1 },
            { "DOCUMENT_NUMBER": "ORD-1", "LINE_NUMBER": 2 }
        ]);

        let document = document_from_json_value(&input);

        assert_eq!(document.root.name, "ROOT");
        let rows = document
            .root
            .find_child("rows")
            .expect("rows group should exist");
        assert_eq!(rows.children.len(), 2);
        assert_eq!(rows.children[0].name, "item");
        assert_eq!(
            rows.children[0]
                .find_child("DOCUMENT_NUMBER")
                .and_then(|field| field.value.as_ref())
                .and_then(Value::as_string)
                .as_deref(),
            Some("ORD-1")
        );
    }

    #[test]
    fn json_scalar_to_ir_value_keeps_large_unsigned_numbers_exact() {
        let value: serde_json::Value = serde_json::json!(18446744073709551615u64);
        assert_eq!(
            json_scalar_to_ir_value(&value),
            Value::String("18446744073709551615".to_string())
        );
    }

    fn csv_document(fields: &[(&str, &str)]) -> Document {
        let mut record = Node::new("record", NodeType::Record);
        for (name, value) in fields {
            record.add_child(Node::with_value(
                *name,
                NodeType::Field,
                Value::String((*value).to_string()),
            ));
        }

        let mut root = Node::new("ROOT", NodeType::Root);
        root.add_child(record);

        Document::new(root)
    }
}
