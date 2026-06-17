use std::path::{Path, PathBuf};

use anyhow::{Context, anyhow, bail};
use edi_adapter_edifact::EdifactParser;
use edi_ir::{Document, Value};

use crate::CliExitCode;
use crate::config::CliConfig;

#[derive(Debug, Clone, Copy)]
pub(crate) struct SchemaPack {
    id: &'static str,
    standard: &'static str,
    version: &'static str,
    message_type: &'static str,
    schema_rel_path: &'static str,
    description: &'static str,
}

#[derive(Debug, Clone)]
pub(crate) struct ResolvedSchema {
    pub(crate) path: String,
    pub(crate) label: Option<String>,
}

const BUILT_IN_SCHEMA_PACKS: &[SchemaPack] = &[
    SchemaPack {
        id: "eancom:d96a:orders",
        standard: "EANCOM",
        version: "D96A",
        message_type: "ORDERS",
        schema_rel_path: "testdata/schemas/eancom_orders_d96a.yaml",
        description: "EANCOM D96A purchase order message",
    },
    SchemaPack {
        id: "eancom:d96a:slsrpt",
        standard: "EANCOM",
        version: "D96A",
        message_type: "SLSRPT",
        schema_rel_path: "testdata/schemas/eancom_slsrpt_d96a.yaml",
        description: "EANCOM D96A sales data report message",
    },
    SchemaPack {
        id: "eancom:d96a:ordrsp",
        standard: "EANCOM",
        version: "D96A",
        message_type: "ORDRSP",
        schema_rel_path: "testdata/schemas/eancom_ordrsp_d96a.yaml",
        description: "EANCOM D96A purchase order response message",
    },
    SchemaPack {
        id: "eancom:d96a:desadv",
        standard: "EANCOM",
        version: "D96A",
        message_type: "DESADV",
        schema_rel_path: "testdata/schemas/eancom_desadv_d96a.yaml",
        description: "EANCOM D96A despatch advice message",
    },
    SchemaPack {
        id: "eancom:d96a:invoic",
        standard: "EANCOM",
        version: "D96A",
        message_type: "INVOIC",
        schema_rel_path: "testdata/schemas/eancom_invoic_d96a.yaml",
        description: "EANCOM D96A invoice message",
    },
];

pub(crate) fn schema_list(config: &CliConfig) -> anyhow::Result<CliExitCode> {
    println!("Schema/message packs:");
    for pack in BUILT_IN_SCHEMA_PACKS {
        let installed = installed_schema_path(pack).exists()
            || config.schema_packs.iter().any(|id| id == pack.id);
        let status = if installed { "installed" } else { "built-in" };
        println!(
            "  {:<22} {:<9} {} {} ({status}) - {}",
            pack.id, pack.standard, pack.version, pack.message_type, pack.description
        );
    }
    Ok(CliExitCode::Success)
}

pub(crate) fn schema_install(pack_id: &str) -> anyhow::Result<CliExitCode> {
    let pack = find_built_in_pack(pack_id).ok_or_else(|| unknown_pack_error(pack_id))?;
    let source = built_in_schema_source_path(pack);
    if !source.exists() {
        bail!(
            "Built-in schema source '{}' for pack '{}' was not found",
            source.display(),
            pack.id
        );
    }

    let destination = installed_schema_path(pack);
    if let Some(parent) = destination.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("Failed to create schema directory '{}'", parent.display()))?;
    }
    std::fs::copy(&source, &destination).with_context(|| {
        format!(
            "Failed to install schema pack '{}' from '{}' to '{}'",
            pack.id,
            source.display(),
            destination.display()
        )
    })?;
    record_installed_schema_pack(pack.id)?;
    println!(
        "Installed {} to {} and recorded it in rsedi.yaml.",
        pack.id,
        destination.display()
    );
    Ok(CliExitCode::Success)
}

pub(crate) fn schema_inspect(config: &CliConfig, pack_id: &str) -> anyhow::Result<CliExitCode> {
    let pack = find_built_in_pack(pack_id).ok_or_else(|| unknown_pack_error(pack_id))?;
    println!("id: {}", pack.id);
    println!("standard: {}", pack.standard);
    println!("version: {}", pack.version);
    println!("message_type: {}", pack.message_type);
    println!("description: {}", pack.description);
    println!(
        "built_in_schema: {}",
        built_in_schema_source_path(pack).display()
    );
    let installed_path = installed_schema_path(pack);
    println!("installed_schema: {}", installed_path.display());
    println!(
        "installed: {}",
        installed_path.exists() || config.schema_packs.iter().any(|id| id == pack.id)
    );
    Ok(CliExitCode::Success)
}

pub(crate) fn schema_doctor(config: &CliConfig) -> anyhow::Result<CliExitCode> {
    let mut errors = 0usize;
    for pack_id in &config.schema_packs {
        match find_built_in_pack(pack_id) {
            Some(pack) => {
                let installed_path = installed_schema_path(pack);
                let built_in_path = built_in_schema_source_path(pack);
                if installed_path.exists() || built_in_path.exists() {
                    println!("OK: {}", pack.id);
                } else {
                    errors += 1;
                    println!("Missing: {} (run: edi schema install {})", pack.id, pack.id);
                }
            }
            None => {
                errors += 1;
                println!("Unknown configured schema pack: {pack_id}");
            }
        }
    }
    if config.schema_packs.is_empty() {
        println!("No installed schema packs recorded in config.");
    }
    Ok(if errors == 0 {
        CliExitCode::Success
    } else {
        CliExitCode::Errors
    })
}

pub(crate) fn resolve_auto_schema(
    input_path: &str,
    explicit_schema: Option<String>,
    profile_schema: Option<&PathBuf>,
) -> anyhow::Result<ResolvedSchema> {
    if let Some(path) = explicit_schema {
        return Ok(ResolvedSchema { path, label: None });
    }
    if let Some(path) = profile_schema {
        return Ok(ResolvedSchema {
            path: path.to_string_lossy().into_owned(),
            label: None,
        });
    }

    let message = detect_message_type(input_path)?;
    let Some(pack) = find_pack_for_message(&message) else {
        let candidate = message.pack_id();
        bail!(
            "No schema pack installed or built in for {} {}. Install it with: edi schema install {}",
            message.message_type,
            message.version,
            candidate
        );
    };

    let path = resolve_pack_schema_path(pack)?;
    println!("Auto-selected schema pack {} ({})", pack.id, path.display());
    Ok(ResolvedSchema {
        path: path.to_string_lossy().into_owned(),
        label: Some(pack.id.to_string()),
    })
}

#[derive(Debug, Clone)]
struct DetectedMessageType {
    message_type: String,
    version: String,
}

impl DetectedMessageType {
    fn pack_id(&self) -> String {
        format!(
            "eancom:{}:{}",
            self.version.to_ascii_lowercase(),
            self.message_type.to_ascii_lowercase()
        )
    }
}

fn detect_message_type(input_path: &str) -> anyhow::Result<DetectedMessageType> {
    let input_bytes = std::fs::read(input_path)
        .with_context(|| format!("Failed to read input file '{}'", input_path))?;
    let parser = EdifactParser::new();
    let parsed = parser
        .parse_with_warnings(&input_bytes, input_path)
        .with_context(|| format!("Failed to parse EDIFACT input '{}'", input_path))?;
    let document = parsed
        .documents
        .first()
        .ok_or_else(|| anyhow!("No EDIFACT messages were found in '{}'", input_path))?;
    extract_message_type(document).ok_or_else(|| {
        anyhow!(
            "Could not auto-detect schema because the first message has no UNH message type/version"
        )
    })
}

fn extract_message_type(document: &Document) -> Option<DetectedMessageType> {
    let unh = document.root.find_child("UNH")?;
    let message_identifier = unh.children.get(1)?;
    let message_type = node_value(message_identifier.children.first()?)?;
    let release = node_value(message_identifier.children.get(1)?)?;
    let version = node_value(message_identifier.children.get(2)?)?;
    Some(DetectedMessageType {
        message_type: message_type.to_ascii_uppercase(),
        version: format!("{release}{version}").to_ascii_uppercase(),
    })
}

fn node_value(node: &edi_ir::Node) -> Option<String> {
    node.value.as_ref().and_then(Value::as_string)
}

fn find_pack_for_message(message: &DetectedMessageType) -> Option<&'static SchemaPack> {
    BUILT_IN_SCHEMA_PACKS.iter().find(|pack| {
        pack.message_type
            .eq_ignore_ascii_case(&message.message_type)
            && pack.version.eq_ignore_ascii_case(&message.version)
    })
}

fn find_built_in_pack(pack_id: &str) -> Option<&'static SchemaPack> {
    BUILT_IN_SCHEMA_PACKS
        .iter()
        .find(|pack| pack.id.eq_ignore_ascii_case(pack_id))
}

fn unknown_pack_error(pack_id: &str) -> anyhow::Error {
    anyhow!(
        "Unknown schema pack '{}'. Run 'edi schema list' to see available packs.",
        pack_id
    )
}

fn resolve_pack_schema_path(pack: &SchemaPack) -> anyhow::Result<PathBuf> {
    let installed = installed_schema_path(pack);
    if installed.exists() {
        return Ok(installed);
    }
    let built_in = built_in_schema_source_path(pack);
    if built_in.exists() {
        return Ok(built_in);
    }
    bail!(
        "No schema pack installed or built in for {} {}. Install it with: edi schema install {}",
        pack.message_type,
        pack.version,
        pack.id
    );
}

fn installed_schema_path(pack: &SchemaPack) -> PathBuf {
    let mut parts = pack.id.split(':');
    let standard = parts.next().unwrap_or("schema");
    let version = parts.next().unwrap_or("version");
    let message = parts.next().unwrap_or("message");
    Path::new("schemas")
        .join(standard)
        .join(version)
        .join(format!("{message}.yaml"))
}

fn built_in_schema_source_path(pack: &SchemaPack) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join(pack.schema_rel_path)
}

fn record_installed_schema_pack(pack_id: &str) -> anyhow::Result<()> {
    let config_path = Path::new("rsedi.yaml");
    let mut config = if config_path.exists() {
        let config_text = std::fs::read_to_string(config_path)
            .with_context(|| format!("Failed to read '{}'", config_path.display()))?;
        serde_yaml::from_str::<serde_yaml::Value>(&config_text)
            .with_context(|| format!("Failed to parse '{}'", config_path.display()))?
    } else {
        serde_yaml::Value::Mapping(serde_yaml::Mapping::new())
    };

    let mapping = config.as_mapping_mut().ok_or_else(|| {
        anyhow!(
            "Failed to update '{}': root YAML value must be a mapping",
            config_path.display()
        )
    })?;
    let key = serde_yaml::Value::String("schema_packs".to_string());
    let packs = mapping
        .entry(key)
        .or_insert_with(|| serde_yaml::Value::Sequence(Vec::new()));
    let sequence = packs.as_sequence_mut().ok_or_else(|| {
        anyhow!(
            "Failed to update '{}': schema_packs must be a YAML list",
            config_path.display()
        )
    })?;

    let already_recorded = sequence.iter().any(|value| value.as_str() == Some(pack_id));
    if !already_recorded {
        sequence.push(serde_yaml::Value::String(pack_id.to_string()));
    }

    let rendered = serde_yaml::to_string(&config)
        .with_context(|| format!("Failed to render '{}'", config_path.display()))?;
    std::fs::write(config_path, rendered)
        .with_context(|| format!("Failed to update '{}'", config_path.display()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn finds_schema_pack_case_insensitively() {
        let pack = find_built_in_pack("EANCOM:D96A:ORDERS").expect("orders pack exists");
        assert_eq!(pack.id, "eancom:d96a:orders");
        assert_eq!(pack.message_type, "ORDERS");
    }

    #[test]
    fn installed_schema_path_uses_pack_segments() {
        let pack = find_built_in_pack("eancom:d96a:desadv").expect("desadv pack exists");
        assert_eq!(
            installed_schema_path(pack),
            Path::new("schemas")
                .join("eancom")
                .join("d96a")
                .join("desadv.yaml")
        );
    }

    #[test]
    fn detected_message_type_builds_schema_pack_id() {
        let detected = DetectedMessageType {
            message_type: "ORDERS".to_string(),
            version: "D96A".to_string(),
        };
        assert_eq!(detected.pack_id(), "eancom:d96a:orders");
    }
}
