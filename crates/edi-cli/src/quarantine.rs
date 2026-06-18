use std::path::Path;

use anyhow::{Context, bail};
use serde::{Deserialize, Serialize};

use crate::{BatchOutputFormat, CliExitCode};

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct QuarantineMetadata {
    id: String,
    source: String,
    category: String,
    reason: String,
    error: String,
    payload: String,
    created_unix_seconds: u64,
}

pub(crate) fn write_quarantine_item(
    quarantine_dir: &str,
    source_path: &Path,
    category: &str,
    reason: &str,
) -> anyhow::Result<String> {
    let dir = Path::new(quarantine_dir);
    std::fs::create_dir_all(dir)
        .with_context(|| format!("Failed to create quarantine directory '{}'", quarantine_dir))?;
    let stem = source_path
        .file_stem()
        .and_then(|stem| stem.to_str())
        .unwrap_or("message");
    let mut id = sanitize_quarantine_id(stem);
    let mut counter = 1usize;
    while dir.join(format!("{id}.quarantine.json")).exists() {
        counter += 1;
        id = format!("{}-{counter}", sanitize_quarantine_id(stem));
    }
    let payload_name = format!("{id}.edi");
    let payload_path = dir.join(&payload_name);
    std::fs::copy(source_path, &payload_path)
        .with_context(|| format!("Failed to copy '{}' into quarantine", source_path.display()))?;
    let created_unix_seconds = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let metadata = QuarantineMetadata {
        id: id.clone(),
        source: source_path.to_string_lossy().to_string(),
        category: category.to_string(),
        reason: reason.to_string(),
        error: reason.to_string(),
        payload: payload_name,
        created_unix_seconds,
    };
    let metadata_path = dir.join(format!("{id}.quarantine.json"));
    let metadata_bytes = serde_json::to_vec_pretty(&metadata)?;
    std::fs::write(&metadata_path, metadata_bytes).with_context(|| {
        format!(
            "Failed to write quarantine metadata '{}'",
            metadata_path.display()
        )
    })?;
    Ok(id)
}

pub(crate) fn read_quarantine_metadata(dir: &str, id: &str) -> anyhow::Result<QuarantineMetadata> {
    let path = Path::new(dir).join(format!("{id}.quarantine.json"));
    let bytes = std::fs::read(&path)
        .with_context(|| format!("Failed to read quarantine metadata '{}'", path.display()))?;
    let metadata: QuarantineMetadata = serde_json::from_slice(&bytes)
        .with_context(|| format!("Failed to parse quarantine metadata '{}'", path.display()))?;
    validate_quarantine_metadata(&metadata)
        .with_context(|| format!("Invalid quarantine metadata '{}'", path.display()))?;
    Ok(metadata)
}

pub(crate) fn quarantine_list(dir: &str, format: BatchOutputFormat) -> anyhow::Result<CliExitCode> {
    let entries = read_all_quarantine_metadata(dir)?;
    match format {
        BatchOutputFormat::Json => println!("{}", serde_json::to_string(&entries)?),
        BatchOutputFormat::Text => {
            for entry in entries {
                println!("{}\t{}\t{}", entry.id, entry.category, entry.source);
            }
        }
    }
    Ok(CliExitCode::Success)
}

pub(crate) fn quarantine_show(
    dir: &str,
    id: &str,
    format: BatchOutputFormat,
) -> anyhow::Result<CliExitCode> {
    let metadata = read_quarantine_metadata(dir, id)?;
    match format {
        BatchOutputFormat::Json => println!("{}", serde_json::to_string(&metadata)?),
        BatchOutputFormat::Text => {
            let payload_path = Path::new(dir).join(&metadata.payload);
            let payload_result = std::fs::read_to_string(&payload_path);
            println!("id: {}", metadata.id);
            println!("source: {}", metadata.source);
            println!("category: {}", metadata.category);
            println!("error: {}", metadata.error);
            match payload_result {
                Ok(payload) => {
                    println!("snippet: {}", payload.chars().take(240).collect::<String>());
                }
                Err(error) => {
                    println!(
                        "snippet_error: failed to read '{}': {error}",
                        payload_path.display()
                    );
                    println!("snippet: ");
                }
            }
        }
    }
    Ok(CliExitCode::Success)
}

pub(crate) fn quarantine_payload_path(dir: &str, id: &str) -> anyhow::Result<std::path::PathBuf> {
    let metadata = read_quarantine_metadata(dir, id)?;
    Ok(Path::new(dir).join(&metadata.payload))
}

pub(crate) fn quarantine_export(dir: &str, id: &str, output: &str) -> anyhow::Result<CliExitCode> {
    let payload_path = quarantine_payload_path(dir, id)?;
    std::fs::copy(&payload_path, output).with_context(|| {
        format!(
            "Failed to export quarantine payload '{}' to '{}'",
            payload_path.display(),
            output
        )
    })?;
    println!("Exported quarantine item '{id}' to '{output}'.");
    Ok(CliExitCode::Success)
}

fn sanitize_quarantine_id(value: &str) -> String {
    let sanitized: String = value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
                ch
            } else {
                '-'
            }
        })
        .collect();
    if sanitized.is_empty() {
        "message".to_string()
    } else {
        sanitized
    }
}

fn validate_quarantine_metadata(metadata: &QuarantineMetadata) -> anyhow::Result<()> {
    if metadata.id.trim().is_empty() {
        bail!("quarantine metadata id is empty");
    }
    if metadata.payload.trim().is_empty() {
        bail!("quarantine metadata payload is empty");
    }
    if metadata.category.trim().is_empty() {
        bail!("quarantine metadata category is empty");
    }
    Ok(())
}

fn read_all_quarantine_metadata(dir: &str) -> anyhow::Result<Vec<QuarantineMetadata>> {
    let mut entries = Vec::new();
    for entry in std::fs::read_dir(dir)
        .with_context(|| format!("Failed to read quarantine directory '{}'", dir))?
    {
        let path = entry?.path();
        let file_name = path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or_default();
        if file_name.ends_with(".quarantine.json") {
            let bytes = std::fs::read(&path).with_context(|| {
                format!("Failed to read quarantine metadata '{}'", path.display())
            })?;
            let metadata: QuarantineMetadata =
                serde_json::from_slice(&bytes).with_context(|| {
                    format!("Failed to parse quarantine metadata '{}'", path.display())
                })?;
            validate_quarantine_metadata(&metadata)
                .with_context(|| format!("Invalid quarantine metadata '{}'", path.display()))?;
            entries.push(metadata);
        }
    }
    entries.sort_by(|left: &QuarantineMetadata, right| left.id.cmp(&right.id));
    Ok(entries)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitize_quarantine_id_preserves_safe_characters_only() {
        assert_eq!(sanitize_quarantine_id("INV/2026:06"), "INV-2026-06");
        assert_eq!(sanitize_quarantine_id(""), "message");
    }

    #[test]
    fn metadata_validation_rejects_missing_payload_boundary() {
        let metadata = QuarantineMetadata {
            id: "bad".to_string(),
            source: "source.edi".to_string(),
            category: "validation".to_string(),
            reason: "invalid".to_string(),
            error: "invalid".to_string(),
            payload: " ".to_string(),
            created_unix_seconds: 0,
        };

        let error = validate_quarantine_metadata(&metadata).expect_err("missing payload fails");
        assert!(
            error.to_string().contains("payload is empty"),
            "unexpected error: {error:#}"
        );
    }
}
