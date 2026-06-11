//! # edi-cli
//!
//! CLI application and configuration for EDI Integration Engine.
//!
//! This crate provides the command-line interface for running
//! EDI transformations and managing configurations.

use std::borrow::Cow;
use std::collections::HashMap;
use std::env;
use std::fmt::Write as _;
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
use edi_mapping::{
    MappingDsl, MappingRuntime, MappingTrace, MessageMappingTrace, explain_mapping, lint_mapping,
    lint_mapping_with_schema,
};
use edi_schema::SchemaLoader;
use edi_validation::{Severity, ValidationEngine, ValidationIssue};
use serde::{Deserialize, Serialize};

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
    profiles: HashMap<String, ProfileConfig>,

    #[serde(skip)]
    source_path: Option<PathBuf>,
}

impl Default for CliConfig {
    fn default() -> Self {
        Self {
            progress: true,
            progress_threshold_bytes: 1024 * 1024,
            color: ColorMode::Auto,
            profiles: HashMap::new(),
            source_path: None,
        }
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct ProfileConfig {
    input: Option<PathBuf>,
    output: Option<PathBuf>,
    schema: Option<PathBuf>,
    mapping: Option<PathBuf>,
    quarantine: Option<PathBuf>,
    output_format: Option<String>,
    color: Option<ColorMode>,
    progress: Option<bool>,
    progress_threshold_bytes: Option<u64>,
}

#[derive(Debug, Clone, Copy)]
struct RuntimeOptions {
    progress: bool,
    progress_threshold_bytes: u64,
    color: ColorMode,
}

#[derive(Debug, Clone, Copy)]
struct TransformCommandOptions {
    dry_run: bool,
    trace_mapping: bool,
    trace_format: TraceFormat,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum RecipeName {
    ValidateEdifact,
    OrdersToJson,
    OrdersToCsv,
    CsvToOrders,
    BatchValidateDirectory,
}

impl RecipeName {
    fn as_str(self) -> &'static str {
        match self {
            Self::ValidateEdifact => "validate-edifact",
            Self::OrdersToJson => "orders-to-json",
            Self::OrdersToCsv => "orders-to-csv",
            Self::CsvToOrders => "csv-to-orders",
            Self::BatchValidateDirectory => "batch-validate-directory",
        }
    }

    fn description(self) -> &'static str {
        match self {
            Self::ValidateEdifact => "Validate an EDIFACT/EANCOM file against a schema",
            Self::OrdersToJson => "Transform an EANCOM ORDERS file to JSON",
            Self::OrdersToCsv => "Transform an EANCOM ORDERS file to CSV",
            Self::CsvToOrders => "Generate EANCOM ORDERS EDI from CSV input",
            Self::BatchValidateDirectory => "Validate every .edi file in a directory",
        }
    }

    fn all() -> &'static [Self] {
        &[
            Self::ValidateEdifact,
            Self::OrdersToJson,
            Self::OrdersToCsv,
            Self::CsvToOrders,
            Self::BatchValidateDirectory,
        ]
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum TraceFormat {
    Text,
    Json,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum ValidationReportFormat {
    Text,
    Json,
    Html,
    Sarif,
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

    /// Named project profile to apply from the config file
    #[arg(long, global = true)]
    profile: Option<String>,

    /// Subcommand to execute
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Create a starter rsedi.yaml and workspace directories
    Init {
        /// Profile name to create in the starter config
        #[arg(long, default_value = "default")]
        profile: String,

        /// Overwrite an existing rsedi.yaml
        #[arg(long, default_value_t = false)]
        force: bool,
    },

    /// Manage CLI configuration
    Config {
        #[command(subcommand)]
        command: ConfigCommands,
    },

    /// Discover and run guided workflow recipes
    Recipes {
        #[command(subcommand)]
        command: RecipeCommands,
    },

    /// Interactively select a common EDI workflow, or print an auto-detected plan with --dry-run
    Wizard {
        /// Print the planned command without prompting or running it
        #[arg(long, default_value_t = false)]
        dry_run: bool,
    },

    /// Transform an EDI file
    Transform {
        /// Input file path (uses profile input when omitted)
        input: Option<String>,

        /// Output file path (writes to stdout when omitted unless profile output is set)
        output: Option<String>,

        /// Mapping file path
        #[arg(short, long)]
        mapping: Option<String>,

        /// Schema file path
        #[arg(short, long)]
        schema: Option<String>,

        /// Evaluate mapping and diagnostics without writing transformed output
        #[arg(long, default_value_t = false)]
        dry_run: bool,

        /// Emit rule-by-rule mapping diagnostics
        #[arg(long, default_value_t = false)]
        trace_mapping: bool,

        /// Mapping trace output format
        #[arg(long, value_enum, default_value = "text")]
        trace_format: TraceFormat,
    },

    /// Inspect mapping files
    Mapping {
        #[command(subcommand)]
        command: MappingCommands,
    },

    /// Validate an EDI file against a schema
    Validate {
        /// Input file path (uses profile input when omitted)
        input: Option<String>,

        /// Schema file path
        #[arg(short, long)]
        schema: Option<String>,

        /// Validation report format
        #[arg(long, value_enum, default_value = "text")]
        report: ValidationReportFormat,

        /// Persist the validation report to this path instead of printing details to stdout
        #[arg(short, long)]
        output: Option<String>,
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
        /// Input file path (CSV or JSON; uses profile input when omitted)
        input: Option<String>,

        /// Output file path (writes to stdout when omitted unless profile output is set)
        output: Option<String>,

        /// Mapping file path
        #[arg(short, long)]
        mapping: Option<String>,

        /// Source input format (inferred from file extension or mapping source_type when omitted)
        #[arg(long, value_enum)]
        input_format: Option<GenerateInputFormat>,
    },
}

#[derive(Subcommand)]
enum ConfigCommands {
    /// Validate project configuration and referenced files
    Check {
        /// Profile to check (defaults to --profile or all profiles)
        #[arg(long)]
        profile: Option<String>,
    },
}

#[derive(Subcommand)]
enum RecipeCommands {
    /// List available guided recipes
    List,
    /// Run a guided recipe, or print the command with --dry-run
    Run {
        /// Recipe name
        #[arg(value_enum)]
        name: RecipeName,

        /// Input file or directory, depending on the recipe
        #[arg(long)]
        input: Option<String>,

        /// Output file path for recipes that write one artifact
        #[arg(long)]
        output: Option<String>,

        /// Schema file path for validation recipes
        #[arg(long)]
        schema: Option<String>,

        /// Mapping file path for transform/generate recipes
        #[arg(long)]
        mapping: Option<String>,

        /// Print the exact command without running it
        #[arg(long, default_value_t = false)]
        dry_run: bool,
    },
}

#[derive(Subcommand)]
enum MappingCommands {
    /// Validate mapping DSL structure and runtime-supported selectors
    Lint {
        /// Mapping file path
        mapping: String,

        /// Optional schema file path for schema-aware path diagnostics
        #[arg(short, long)]
        schema: Option<String>,
    },
    /// Print a readable mapping rule tree
    Explain {
        /// Mapping file path
        mapping: String,
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
    let base_runtime = runtime_options(&config, None);

    if let Some(config_path) = cli.config.as_deref() {
        tracing::info!(config = %config_path, "Loaded explicit CLI config");
    } else if let Some(config_path) = &config.source_path {
        tracing::info!(config = %config_path.display(), "Loaded discovered CLI config");
    }

    let command_result = (|| -> anyhow::Result<CliExitCode> {
        match cli.command {
            Commands::Init { profile, force } => {
                init_project(cli.profile.as_deref().unwrap_or(&profile), force)
            }
            Commands::Config { command } => match command {
                ConfigCommands::Check { profile } => {
                    config_check(&config, profile.as_deref().or(cli.profile.as_deref()))
                }
            },
            Commands::Recipes { command } => match command {
                RecipeCommands::List => recipes_list(),
                RecipeCommands::Run {
                    name,
                    input,
                    output,
                    schema,
                    mapping,
                    dry_run,
                } => recipes_run(
                    name,
                    input.as_deref(),
                    output.as_deref(),
                    schema.as_deref(),
                    mapping.as_deref(),
                    dry_run,
                    base_runtime,
                ),
            },
            Commands::Wizard { dry_run } => wizard(dry_run, base_runtime),
            Commands::Transform {
                input,
                output,
                mapping,
                schema,
                dry_run,
                trace_mapping,
                trace_format,
            } => {
                let profile = resolve_selected_profile(&config, cli.profile.as_deref())?;
                let runtime = runtime_options(&config, profile);
                let input =
                    resolve_profile_value(input, profile.and_then(|p| p.input.as_ref()), "input")?;
                let output =
                    resolve_optional_profile_value(output, profile.and_then(|p| p.output.as_ref()));
                let mapping = resolve_profile_value(
                    mapping,
                    profile.and_then(|p| p.mapping.as_ref()),
                    "mapping",
                )?;
                let schema =
                    resolve_optional_profile_value(schema, profile.and_then(|p| p.schema.as_ref()));
                let options = TransformCommandOptions {
                    dry_run,
                    trace_mapping,
                    trace_format,
                };
                transform(
                    &input,
                    output.as_deref(),
                    &mapping,
                    schema.as_deref(),
                    runtime,
                    options,
                )
            }
            Commands::Mapping { command } => match command {
                MappingCommands::Lint { mapping, schema } => {
                    mapping_lint(&mapping, schema.as_deref())
                }
                MappingCommands::Explain { mapping } => mapping_explain(&mapping),
            },
            Commands::Validate {
                input,
                schema,
                report,
                output,
            } => {
                let profile = resolve_selected_profile(&config, cli.profile.as_deref())?;
                let runtime = runtime_options(&config, profile);
                let input =
                    resolve_profile_value(input, profile.and_then(|p| p.input.as_ref()), "input")?;
                let schema = resolve_profile_value(
                    schema,
                    profile.and_then(|p| p.schema.as_ref()),
                    "schema",
                )?;
                validate(&input, &schema, runtime, report, output.as_deref())
            }
            Commands::Parse {
                input,
                output,
                pretty,
            } => parse(&input, output.as_deref(), pretty, base_runtime),
            Commands::Generate {
                input,
                output,
                mapping,
                input_format,
            } => {
                let profile = resolve_selected_profile(&config, cli.profile.as_deref())?;
                let runtime = runtime_options(&config, profile);
                let input =
                    resolve_profile_value(input, profile.and_then(|p| p.input.as_ref()), "input")?;
                let output =
                    resolve_optional_profile_value(output, profile.and_then(|p| p.output.as_ref()));
                let mapping = resolve_profile_value(
                    mapping,
                    profile.and_then(|p| p.mapping.as_ref()),
                    "mapping",
                )?;
                generate(&input, output.as_deref(), &mapping, input_format, runtime)
            }
        }
    })();

    match command_result {
        Ok(code) => Ok(code),
        Err(error) => {
            print_error(base_runtime.color, &format!("{error:#}"));
            Ok(CliExitCode::Errors)
        }
    }
}

fn runtime_options(config: &CliConfig, profile: Option<&ProfileConfig>) -> RuntimeOptions {
    RuntimeOptions {
        progress: profile.and_then(|p| p.progress).unwrap_or(config.progress),
        progress_threshold_bytes: profile
            .and_then(|p| p.progress_threshold_bytes)
            .unwrap_or(config.progress_threshold_bytes),
        color: profile.and_then(|p| p.color).unwrap_or(config.color),
    }
}

fn resolve_selected_profile<'a>(
    config: &'a CliConfig,
    profile_name: Option<&str>,
) -> anyhow::Result<Option<&'a ProfileConfig>> {
    match profile_name {
        Some(name) => config
            .profiles
            .get(name)
            .map(Some)
            .ok_or_else(|| anyhow!("Profile '{}' was not found in the CLI config", name)),
        None => Ok(None),
    }
}

fn resolve_profile_value(
    explicit: Option<String>,
    profile_value: Option<&PathBuf>,
    field: &str,
) -> anyhow::Result<String> {
    explicit
        .or_else(|| profile_value.map(|path| path.to_string_lossy().into_owned()))
        .ok_or_else(|| {
            let flag_hint = match field {
                "mapping" => "--mapping",
                "schema" => "--schema",
                _ => field,
            };
            anyhow!("Missing {field}; pass {flag_hint} or select a profile that defines {field}")
        })
}

fn resolve_optional_profile_value(
    explicit: Option<String>,
    profile_value: Option<&PathBuf>,
) -> Option<String> {
    explicit.or_else(|| profile_value.map(|path| path.to_string_lossy().into_owned()))
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
    let mut config: CliConfig = serde_yaml::from_slice(&bytes)
        .with_context(|| format!("Failed to parse CLI config '{}'", path.display()))?;
    config.source_path = Some(path.to_path_buf());
    if let Some(base_dir) = path.parent() {
        for profile in config.profiles.values_mut() {
            resolve_profile_paths(base_dir, profile);
        }
    }
    Ok(config)
}

fn resolve_profile_paths(base_dir: &Path, profile: &mut ProfileConfig) {
    absolutize_profile_path(base_dir, &mut profile.input);
    absolutize_profile_path(base_dir, &mut profile.output);
    absolutize_profile_path(base_dir, &mut profile.schema);
    absolutize_profile_path(base_dir, &mut profile.mapping);
    absolutize_profile_path(base_dir, &mut profile.quarantine);
}

fn absolutize_profile_path(base_dir: &Path, value: &mut Option<PathBuf>) {
    if let Some(path) = value {
        if path.is_relative() {
            *path = base_dir.join(&path);
        }
    }
}

fn default_config_paths() -> Vec<PathBuf> {
    let mut paths = Vec::new();

    if let Ok(current_dir) = env::current_dir() {
        paths.push(current_dir.join("rsedi.yaml"));
        paths.push(current_dir.join("edi.yaml"));
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

fn init_project(profile: &str, force: bool) -> anyhow::Result<CliExitCode> {
    ensure_valid_profile_name(profile)?;

    let config_path = Path::new("rsedi.yaml");
    if config_path.exists() && !force {
        bail!(
            "Config '{}' already exists; pass --force to overwrite it",
            config_path.display()
        );
    }

    for dir in ["schemas", "mappings", "input", "output", "quarantine"] {
        std::fs::create_dir_all(dir)
            .with_context(|| format!("Failed to create directory '{dir}'"))?;
    }

    let config = format!(
        r#"# rsedi project configuration
# Use --profile {profile} to apply these defaults to validate/transform/generate.
progress: true
progress_threshold_bytes: 1048576
color: auto
profiles:
  {profile}:
    input: input/example.edi
    output: output/result.json
    schema: schemas/example.yaml
    mapping: mappings/example.yaml
    quarantine: quarantine
    output_format: json
"#
    );
    std::fs::write(config_path, config)
        .with_context(|| format!("Failed to write '{}'", config_path.display()))?;

    println!(
        "Created {} with profile '{}'.",
        config_path.display(),
        profile
    );
    println!("Created starter directories: schemas, mappings, input, output, quarantine.");
    Ok(CliExitCode::Success)
}

fn ensure_valid_profile_name(profile: &str) -> anyhow::Result<()> {
    if profile.is_empty()
        || !profile
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '.'))
    {
        bail!(
            "Invalid profile name '{}'; use only ASCII letters, digits, '.', '_' or '-'",
            profile
        );
    }

    Ok(())
}

fn config_check(config: &CliConfig, profile_name: Option<&str>) -> anyhow::Result<CliExitCode> {
    if config.source_path.is_none() {
        bail!("No config file found. Run 'edi init' or pass --config <path>.");
    }

    let profiles: Vec<(&str, &ProfileConfig)> = if let Some(name) = profile_name {
        vec![
            config
                .profiles
                .get_key_value(name)
                .map(|(key, profile)| (key.as_str(), profile))
                .ok_or_else(|| anyhow!("Profile '{}' was not found in the CLI config", name))?,
        ]
    } else {
        config
            .profiles
            .iter()
            .map(|(name, profile)| (name.as_str(), profile))
            .collect()
    };

    if profiles.is_empty() {
        bail!("Config contains no profiles.");
    }

    let mut missing = Vec::new();
    for (name, profile) in &profiles {
        check_profile_path(name, "input", profile.input.as_ref(), &mut missing);
        check_profile_path(name, "output", profile.output.as_ref(), &mut missing);
        check_profile_path(name, "schema", profile.schema.as_ref(), &mut missing);
        check_profile_path(name, "mapping", profile.mapping.as_ref(), &mut missing);
        check_profile_path(
            name,
            "quarantine",
            profile.quarantine.as_ref(),
            &mut missing,
        );
    }

    if !missing.is_empty() {
        for line in &missing {
            eprintln!("Missing: {line}");
        }
        return Ok(CliExitCode::Errors);
    }

    let checked = profiles
        .iter()
        .map(|(name, _)| *name)
        .collect::<Vec<_>>()
        .join(", ");
    println!("Config OK: checked profile(s): {checked}");
    Ok(CliExitCode::Success)
}

fn check_profile_path(
    profile_name: &str,
    field: &str,
    path: Option<&PathBuf>,
    missing: &mut Vec<String>,
) {
    if let Some(path) = path {
        if !path.exists() {
            missing.push(format!(
                "profile '{profile_name}' {field} path '{}' does not exist",
                path.display()
            ));
        }
    }
}

fn mapping_lint(mapping_path: &str, schema_path: Option<&str>) -> anyhow::Result<CliExitCode> {
    let mapping = MappingDsl::parse_file(Path::new(mapping_path))
        .with_context(|| format!("Failed to parse mapping '{}'", mapping_path))?;
    let diagnostics = if let Some(schema_path) = schema_path {
        let schema_loader = SchemaLoader::new(Vec::new());
        let schema = schema_loader
            .load_from_file(Path::new(schema_path))
            .with_context(|| format!("Failed to load schema '{}'", schema_path))?;
        lint_mapping_with_schema(&mapping, &schema)
    } else {
        lint_mapping(&mapping)
    };
    if diagnostics.is_empty() {
        println!("Mapping OK: {mapping_path}");
        return Ok(CliExitCode::Success);
    }

    for diagnostic in &diagnostics {
        println!(
            "{}: {}: {} ({})",
            diagnostic.severity.as_str(),
            diagnostic.rule_path,
            diagnostic.message,
            diagnostic.source_path
        );
    }
    Ok(CliExitCode::Warnings)
}

fn mapping_explain(mapping_path: &str) -> anyhow::Result<CliExitCode> {
    let mapping = MappingDsl::parse_file(Path::new(mapping_path))
        .with_context(|| format!("Failed to parse mapping '{}'", mapping_path))?;
    print!("{}", explain_mapping(&mapping));
    Ok(CliExitCode::Success)
}

fn recipes_list() -> anyhow::Result<CliExitCode> {
    println!("Available recipes:");
    for recipe in RecipeName::all() {
        println!("  {:<24} {}", recipe.as_str(), recipe.description());
    }
    println!();
    println!("Run 'edi recipes run <name> --dry-run ...' to print the copyable command.");
    Ok(CliExitCode::Success)
}

fn recipes_run(
    name: RecipeName,
    input: Option<&str>,
    output: Option<&str>,
    schema: Option<&str>,
    mapping: Option<&str>,
    dry_run: bool,
    runtime: RuntimeOptions,
) -> anyhow::Result<CliExitCode> {
    let command = recipe_command(name, input, output, schema, mapping)?;
    if dry_run {
        print_planned_command(&command);
        return Ok(CliExitCode::Success);
    }

    match name {
        RecipeName::ValidateEdifact => validate(
            required_recipe_arg(name, "input", input)?,
            required_recipe_arg(name, "schema", schema)?,
            runtime,
            ValidationReportFormat::Text,
            output,
        ),
        RecipeName::OrdersToJson | RecipeName::OrdersToCsv => transform(
            required_recipe_arg(name, "input", input)?,
            output,
            required_recipe_arg(name, "mapping", mapping)?,
            schema,
            runtime,
            TransformCommandOptions {
                dry_run: false,
                trace_mapping: false,
                trace_format: TraceFormat::Text,
            },
        ),
        RecipeName::CsvToOrders => generate(
            required_recipe_arg(name, "input", input)?,
            output,
            required_recipe_arg(name, "mapping", mapping)?,
            Some(GenerateInputFormat::Csv),
            runtime,
        ),
        RecipeName::BatchValidateDirectory => batch_validate_directory(
            required_recipe_arg(name, "input", input)?,
            required_recipe_arg(name, "schema", schema)?,
            runtime,
        ),
    }
}

fn recipe_command(
    name: RecipeName,
    input: Option<&str>,
    output: Option<&str>,
    schema: Option<&str>,
    mapping: Option<&str>,
) -> anyhow::Result<Vec<String>> {
    let mut command = vec!["edi".to_string()];
    match name {
        RecipeName::ValidateEdifact => {
            command.push("validate".to_string());
            command.push(required_recipe_arg(name, "input", input)?.to_string());
            command.push("--schema".to_string());
            command.push(required_recipe_arg(name, "schema", schema)?.to_string());
            if let Some(output) = output {
                command.push("--output".to_string());
                command.push(output.to_string());
            }
        }
        RecipeName::OrdersToJson | RecipeName::OrdersToCsv => {
            command.push("transform".to_string());
            command.push(required_recipe_arg(name, "input", input)?.to_string());
            if let Some(output) = output {
                command.push(output.to_string());
            }
            command.push("--mapping".to_string());
            command.push(required_recipe_arg(name, "mapping", mapping)?.to_string());
            if let Some(schema) = schema {
                command.push("--schema".to_string());
                command.push(schema.to_string());
            }
        }
        RecipeName::CsvToOrders => {
            command.push("generate".to_string());
            command.push(required_recipe_arg(name, "input", input)?.to_string());
            if let Some(output) = output {
                command.push(output.to_string());
            }
            command.push("--mapping".to_string());
            command.push(required_recipe_arg(name, "mapping", mapping)?.to_string());
            command.push("--input-format".to_string());
            command.push("csv".to_string());
        }
        RecipeName::BatchValidateDirectory => {
            command.push("recipes".to_string());
            command.push("run".to_string());
            command.push(name.as_str().to_string());
            command.push("--input".to_string());
            command.push(required_recipe_arg(name, "input", input)?.to_string());
            command.push("--schema".to_string());
            command.push(required_recipe_arg(name, "schema", schema)?.to_string());
        }
    }
    Ok(command)
}

fn required_recipe_arg<'a>(
    recipe: RecipeName,
    argument_name: &'static str,
    value: Option<&'a str>,
) -> anyhow::Result<&'a str> {
    value.ok_or_else(|| {
        anyhow!(
            "Recipe '{}' requires --{}; rerun with --dry-run to preview the exact command once all required paths are provided",
            recipe.as_str(),
            argument_name
        )
    })
}

fn print_planned_command(command: &[String]) {
    println!("Planned command:");
    println!("{}", shell_join(command));
}

fn shell_join(args: &[String]) -> String {
    args.iter()
        .map(|arg| shell_quote(arg))
        .collect::<Vec<_>>()
        .join(" ")
}

fn shell_quote(arg: &str) -> String {
    if !arg.is_empty()
        && arg
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '/' | '.' | '_' | '-' | ':' | '='))
    {
        return arg.to_string();
    }

    format!("'{}'", arg.replace('\'', "'\\''"))
}

fn batch_validate_directory(
    input_dir: &str,
    schema_path: &str,
    runtime: RuntimeOptions,
) -> anyhow::Result<CliExitCode> {
    let mut edi_files = std::fs::read_dir(input_dir)
        .with_context(|| format!("Failed to read input directory '{}'", input_dir))?
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .filter(|path| path.extension().and_then(|ext| ext.to_str()) == Some("edi"))
        .collect::<Vec<_>>();
    edi_files.sort();

    if edi_files.is_empty() {
        bail!("No .edi files found in '{}'", input_dir);
    }

    let mut worst = CliExitCode::Success;
    for path in edi_files {
        let path = path.to_string_lossy();
        let code = validate(
            path.as_ref(),
            schema_path,
            runtime,
            ValidationReportFormat::Text,
            None,
        )?;
        worst = max_exit_code(worst, code);
    }
    Ok(worst)
}

fn max_exit_code(left: CliExitCode, right: CliExitCode) -> CliExitCode {
    if (left as u8) >= (right as u8) {
        left
    } else {
        right
    }
}

fn wizard(dry_run: bool, runtime: RuntimeOptions) -> anyhow::Result<CliExitCode> {
    if dry_run {
        let plan = autodetect_wizard_plan()?;
        print_planned_command(&plan);
        return Ok(CliExitCode::Success);
    }

    if !std::io::stdin().is_terminal() {
        bail!(
            "Non-interactive wizard cannot prompt. Use 'edi recipes list' and 'edi recipes run <name> --dry-run' instead."
        );
    }

    let plan = autodetect_wizard_plan()?;
    print_planned_command(&plan);
    println!("Run the command above, or use 'edi recipes run' with explicit paths for automation.");
    let _ = runtime;
    Ok(CliExitCode::Success)
}

fn autodetect_wizard_plan() -> anyhow::Result<Vec<String>> {
    let cwd = env::current_dir().context("Failed to read current directory")?;
    let input = find_first_matching_file(&cwd, &["edi"], Some("valid_orders_d96a"))
        .or_else(|| find_first_matching_file(&cwd, &["edi"], Some("orders")))
        .or_else(|| find_first_matching_file(&cwd, &["edi"], None))
        .ok_or_else(|| {
            anyhow!("Wizard could not find an .edi input file in the current workspace")
        })?;
    let mapping = find_first_matching_file(&cwd, &["yaml", "yml"], Some("orders_to_json"))
        .or_else(|| find_first_matching_file(&cwd, &["yaml", "yml"], Some("json")))
        .ok_or_else(|| {
            anyhow!("Wizard could not find an orders-to-JSON mapping file in the current workspace")
        })?;

    recipe_command(
        RecipeName::OrdersToJson,
        Some(&input.to_string_lossy()),
        None,
        None,
        Some(&mapping.to_string_lossy()),
    )
}

fn find_first_matching_file(
    root: &Path,
    extensions: &[&str],
    name_contains: Option<&str>,
) -> Option<PathBuf> {
    let mut matches = Vec::new();
    collect_matching_files(root, extensions, name_contains, 0, &mut matches);
    matches.sort();
    matches.into_iter().next()
}

fn collect_matching_files(
    dir: &Path,
    extensions: &[&str],
    name_contains: Option<&str>,
    depth: usize,
    matches: &mut Vec<PathBuf>,
) {
    if depth > 4 {
        return;
    }
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };

    for entry in entries.flatten() {
        let path = entry.path();
        let file_name = path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or_default();
        if path.is_dir() {
            if matches!(file_name, "target" | ".git") {
                continue;
            }
            collect_matching_files(&path, extensions, name_contains, depth + 1, matches);
        } else if path
            .extension()
            .and_then(|extension| extension.to_str())
            .map(|extension| extensions.contains(&extension))
            .unwrap_or(false)
            && name_contains
                .map(|needle| file_name.to_ascii_lowercase().contains(needle))
                .unwrap_or(true)
        {
            matches.push(path);
        }
    }
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
    options: TransformCommandOptions,
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
    let mut trace_messages = Vec::new();

    for (index, document) in parsed.documents.iter().enumerate() {
        let mapped = if options.trace_mapping {
            let (mapped, rules) = mapping_runtime
                .execute_with_trace(&mapping, document)
                .with_context(|| {
                    format!(
                        "Failed to apply mapping '{}' to message {}",
                        mapping_path,
                        index + 1
                    )
                })?;
            trace_messages.push(MessageMappingTrace {
                message_index: index + 1,
                rules,
            });
            mapped
        } else {
            mapping_runtime
                .execute(&mapping, document)
                .with_context(|| {
                    format!(
                        "Failed to apply mapping '{}' to message {}",
                        mapping_path,
                        index + 1
                    )
                })?
        };
        mapped_documents.push(mapped);
    }

    if options.trace_mapping {
        let trace = MappingTrace {
            mapping: mapping.name.clone(),
            source_type: mapping.source_type.clone(),
            target_type: mapping.target_type.clone(),
            messages: trace_messages,
        };
        if options.dry_run || output_path.is_some() {
            let stdout = std::io::stdout();
            let mut handle = stdout.lock();
            write_mapping_trace(&mut handle, &trace, options.trace_format)?;
        } else {
            let stderr = std::io::stderr();
            let mut handle = stderr.lock();
            write_mapping_trace(&mut handle, &trace, options.trace_format)?;
        }
        if options.dry_run {
            return Ok(if parsed.warnings.is_empty() {
                CliExitCode::Success
            } else {
                CliExitCode::Warnings
            });
        }
    }

    if options.dry_run {
        return Ok(if parsed.warnings.is_empty() {
            CliExitCode::Success
        } else {
            CliExitCode::Warnings
        });
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

fn write_mapping_trace<W: Write>(
    writer: &mut W,
    trace: &MappingTrace,
    format: TraceFormat,
) -> anyhow::Result<()> {
    match format {
        TraceFormat::Json => {
            serde_json::to_writer_pretty(&mut *writer, trace)
                .context("Failed to write mapping trace JSON")?;
            writer
                .write_all(b"\n")
                .context("Failed to finalize mapping trace output")?;
        }
        TraceFormat::Text => {
            writeln!(writer, "Mapping trace: {}", trace.mapping)
                .context("Failed to write mapping trace header")?;
            for message in &trace.messages {
                writeln!(writer, "message {}:", message.message_index)
                    .context("Failed to write mapping trace message")?;
                for rule in &message.rules {
                    let source = rule.source.as_deref().unwrap_or("-");
                    let target = rule.target.as_deref().unwrap_or("-");
                    writeln!(
                        writer,
                        "  {} source={} target={} resolved={}",
                        rule.rule_type, source, target, rule.resolved_node_count
                    )
                    .context("Failed to write mapping trace rule")?;
                }
            }
        }
    }
    Ok(())
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
    report_format: ValidationReportFormat,
    report_output_path: Option<&str>,
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

    let snippets = SourceSnippets::from_bytes(&input_bytes);
    let mut report_issues: Vec<ValidationReportIssue> = Vec::new();
    let mut error_lines: Vec<String> = Vec::new();
    let mut warning_lines: Vec<String> = parsed
        .warnings
        .iter()
        .map(|warning| {
            report_issues.push(ValidationReportIssue::from_parse_warning(
                input_path, warning,
            ));
            format_parse_warning(warning, input_path)
        })
        .collect();
    let mut info_lines: Vec<String> = Vec::new();

    let mut error_count = 0usize;
    let mut warning_count = warning_lines.len();

    for (index, document) in parsed.documents.iter().enumerate() {
        let message_number = index + 1;
        let normalized_document = normalize_document_for_validation(document);
        let message_ref = find_message_ref(&normalized_document);
        let result = validator
            .validate_with_schema(&normalized_document, &schema)
            .with_context(|| {
                format!(
                    "Validation failed while processing message {}",
                    message_number
                )
            })?;

        for issue in result.report.all_issues() {
            let report_issue = ValidationReportIssue::from_validation_issue(
                message_number,
                message_ref.as_deref(),
                input_path,
                issue,
                &snippets,
            );
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
            report_issues.push(report_issue);
        }
    }

    let report = ValidationCliReport {
        source: input_path.to_string(),
        schema: schema_path.to_string(),
        summary: ValidationReportSummary {
            messages: parsed.documents.len(),
            errors: error_count,
            warnings: warning_count,
            infos: info_lines.len(),
        },
        issues: report_issues,
    };

    write_validation_report(
        &report,
        report_format,
        report_output_path,
        &error_lines,
        &warning_lines,
        &info_lines,
    )?;

    if error_count > 0 {
        Ok(CliExitCode::Errors)
    } else if warning_count > 0 {
        Ok(CliExitCode::Warnings)
    } else {
        if report_format == ValidationReportFormat::Text && report_output_path.is_none() {
            println!("\nValidation passed with no warnings.");
        }
        Ok(CliExitCode::Success)
    }
}

#[derive(Debug, Serialize)]
struct ValidationCliReport {
    source: String,
    schema: String,
    summary: ValidationReportSummary,
    issues: Vec<ValidationReportIssue>,
}

#[derive(Debug, Serialize)]
struct ValidationReportSummary {
    messages: usize,
    errors: usize,
    warnings: usize,
    infos: usize,
}

#[derive(Debug, Serialize)]
struct ValidationReportIssue {
    severity: &'static str,
    rule_id: String,
    message: String,
    source: String,
    message_index: Option<usize>,
    message_ref: Option<String>,
    path: Option<String>,
    segment_index: Option<usize>,
    element_index: Option<usize>,
    component_index: Option<usize>,
    line: Option<usize>,
    column: Option<usize>,
    snippet: Option<String>,
    context: Option<String>,
}

impl ValidationReportIssue {
    fn from_validation_issue(
        message_index: usize,
        message_ref: Option<&str>,
        source_path: &str,
        issue: &ValidationIssue,
        snippets: &SourceSnippets,
    ) -> Self {
        Self {
            severity: severity_name(issue.severity),
            rule_id: issue.code.clone().unwrap_or_else(|| "UNKNOWN".to_string()),
            message: issue.message.clone(),
            source: source_path.to_string(),
            message_index: Some(message_index),
            message_ref: message_ref.map(ToOwned::to_owned),
            path: Some(if issue.path.is_empty() {
                "$".to_string()
            } else {
                issue.path.clone()
            }),
            segment_index: issue.segment_pos,
            element_index: issue.element_pos,
            component_index: issue.component_pos,
            line: issue.line,
            column: issue.column,
            snippet: issue.segment_pos.and_then(|pos| snippets.segment(pos)),
            context: issue.context.clone(),
        }
    }

    fn from_parse_warning(source_path: &str, warning: &ParseWarning) -> Self {
        Self {
            severity: "warning",
            rule_id: "PARSE_WARNING".to_string(),
            message: warning.message.clone(),
            source: source_path.to_string(),
            message_index: None,
            message_ref: warning.message_ref.clone(),
            path: None,
            segment_index: None,
            element_index: None,
            component_index: None,
            line: Some(warning.position.line),
            column: Some(warning.position.column),
            snippet: None,
            context: None,
        }
    }
}

struct SourceSnippets {
    segments: Vec<String>,
}

impl SourceSnippets {
    fn from_bytes(input: &[u8]) -> Self {
        let text = String::from_utf8_lossy(input);
        let segments = text
            .split('\'')
            .map(str::trim)
            .filter(|segment| !segment.is_empty())
            .map(|segment| format!("{}'", segment.replace(['\r', '\n'], " ")))
            .collect();
        Self { segments }
    }

    fn segment(&self, zero_based_index: usize) -> Option<String> {
        self.segments.get(zero_based_index).cloned()
    }
}

fn severity_name(severity: Severity) -> &'static str {
    match severity {
        Severity::Error => "error",
        Severity::Warning => "warning",
        Severity::Info => "info",
    }
}

fn write_validation_report(
    report: &ValidationCliReport,
    format: ValidationReportFormat,
    output_path: Option<&str>,
    error_lines: &[String],
    warning_lines: &[String],
    info_lines: &[String],
) -> anyhow::Result<()> {
    let rendered =
        render_validation_report(report, format, error_lines, warning_lines, info_lines)?;

    if let Some(path) = output_path {
        std::fs::write(path, rendered)
            .with_context(|| format!("Failed to write validation report '{}'", path))?;
        println!(
            "Validation report written to {} (messages={}, errors={}, warnings={})",
            path, report.summary.messages, report.summary.errors, report.summary.warnings
        );
        return Ok(());
    }

    print!("{rendered}");
    Ok(())
}

fn render_validation_report(
    report: &ValidationCliReport,
    format: ValidationReportFormat,
    error_lines: &[String],
    warning_lines: &[String],
    info_lines: &[String],
) -> anyhow::Result<String> {
    match format {
        ValidationReportFormat::Text => Ok(render_validation_text_report(
            report,
            error_lines,
            warning_lines,
            info_lines,
        )),
        ValidationReportFormat::Json => {
            serde_json::to_string_pretty(report).context("Failed to render JSON validation report")
        }
        ValidationReportFormat::Html => Ok(render_validation_html_report(report)),
        ValidationReportFormat::Sarif => {
            serde_json::to_string_pretty(&validation_report_to_sarif(report))
                .context("Failed to render SARIF validation report")
        }
    }
    .map(|mut rendered| {
        rendered.push('\n');
        rendered
    })
}

fn render_validation_text_report(
    report: &ValidationCliReport,
    error_lines: &[String],
    warning_lines: &[String],
    info_lines: &[String],
) -> String {
    let mut output = String::new();
    let _ = writeln!(
        output,
        "Validation summary for '{}' against '{}':",
        report.source, report.schema
    );
    let _ = writeln!(output, "  Messages: {}", report.summary.messages);
    let _ = writeln!(output, "  Errors: {}", report.summary.errors);
    let _ = writeln!(output, "  Warnings: {}", report.summary.warnings);

    if !error_lines.is_empty() {
        output.push_str("\nErrors:\n");
        for line in error_lines {
            let _ = writeln!(output, "  - {line}");
        }
    }

    if !warning_lines.is_empty() {
        output.push_str("\nWarnings:\n");
        for line in warning_lines {
            let _ = writeln!(output, "  - {line}");
        }
    }

    if !info_lines.is_empty() {
        output.push_str("\nInfo:\n");
        for line in info_lines {
            let _ = writeln!(output, "  - {line}");
        }
    }

    output
}

fn render_validation_html_report(report: &ValidationCliReport) -> String {
    let mut rows = String::new();
    for issue in &report.issues {
        let _ = writeln!(
            rows,
            "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>",
            escape_html(issue.severity),
            escape_html(&issue.rule_id),
            issue
                .message_index
                .map_or_else(String::new, |idx| idx.to_string()),
            escape_html(issue.path.as_deref().unwrap_or("")),
            escape_html(&issue.message)
        );
    }

    format!(
        "<!doctype html><html><head><meta charset=\"utf-8\"><title>rsedi validation report</title></head><body><h1>Validation report</h1><p>Source: {}</p><p>Schema: {}</p><p>Messages: {} Errors: {} Warnings: {}</p><table><thead><tr><th>Severity</th><th>Rule</th><th>Msg #</th><th>Path</th><th>Details</th></tr></thead><tbody>{}</tbody></table></body></html>",
        escape_html(&report.source),
        escape_html(&report.schema),
        report.summary.messages,
        report.summary.errors,
        report.summary.warnings,
        rows
    )
}

fn validation_report_to_sarif(report: &ValidationCliReport) -> serde_json::Value {
    let results: Vec<serde_json::Value> = report
        .issues
        .iter()
        .map(|issue| {
            let mut location = serde_json::json!({
                "physicalLocation": {
                    "artifactLocation": { "uri": issue.source },
                }
            });
            if let Some(line) = issue.line {
                location["physicalLocation"]["region"] = serde_json::json!({
                    "startLine": line,
                    "startColumn": issue.column.unwrap_or(1)
                });
            }
            serde_json::json!({
                "ruleId": issue.rule_id,
                "level": issue.severity,
                "message": { "text": issue.message },
                "locations": [location],
                "properties": {
                    "messageIndex": issue.message_index,
                    "messageRef": issue.message_ref,
                    "path": issue.path,
                    "segmentIndex": issue.segment_index,
                    "elementIndex": issue.element_index,
                    "componentIndex": issue.component_index,
                    "snippet": issue.snippet,
                    "context": issue.context,
                }
            })
        })
        .collect();

    serde_json::json!({
        "version": "2.1.0",
        "$schema": "https://json.schemastore.org/sarif-2.1.0.json",
        "runs": [{
            "tool": {
                "driver": {
                    "name": "rsedi",
                    "informationUri": "https://github.com/errfld/rsedi",
                    "rules": []
                }
            },
            "results": results
        }]
    })
}

fn escape_html(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
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

fn find_message_ref(document: &edi_ir::Document) -> Option<String> {
    find_segment(&document.root, "UNH")
        .and_then(|unh| unh.children.first())
        .and_then(|element| element.value.as_ref())
        .and_then(Value::as_string)
}

fn find_segment<'a>(node: &'a edi_ir::Node, tag: &str) -> Option<&'a edi_ir::Node> {
    if node.node_type == NodeType::Segment && node.name == tag {
        return Some(node);
    }

    node.children
        .iter()
        .find_map(|child| find_segment(child, tag))
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
