use std::collections::HashMap;
use std::env;
use std::path::{Path, PathBuf};

use anyhow::Context;
use serde::Deserialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub(crate) enum ColorMode {
    #[default]
    Auto,
    Always,
    Never,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct CliConfig {
    pub(crate) progress: bool,
    pub(crate) progress_threshold_bytes: u64,
    pub(crate) color: ColorMode,
    pub(crate) schema_packs: Vec<String>,
    pub(crate) profiles: HashMap<String, ProfileConfig>,

    #[serde(skip)]
    pub(crate) source_path: Option<PathBuf>,
}

impl Default for CliConfig {
    fn default() -> Self {
        Self {
            progress: true,
            progress_threshold_bytes: 1024 * 1024,
            color: ColorMode::Auto,
            schema_packs: Vec::new(),
            profiles: HashMap::new(),
            source_path: None,
        }
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct ProfileConfig {
    pub(crate) input: Option<PathBuf>,
    pub(crate) output: Option<PathBuf>,
    pub(crate) schema: Option<PathBuf>,
    pub(crate) mapping: Option<PathBuf>,
    pub(crate) quarantine: Option<PathBuf>,
    pub(crate) output_format: Option<String>,
    pub(crate) color: Option<ColorMode>,
    pub(crate) progress: Option<bool>,
    pub(crate) progress_threshold_bytes: Option<u64>,
}

pub(crate) fn load_cli_config(explicit_path: Option<&str>) -> anyhow::Result<CliConfig> {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn profile_paths_are_resolved_relative_to_config_file() {
        let base_dir = Path::new("/tmp/rsedi-config");
        let mut profile = ProfileConfig {
            input: Some(PathBuf::from("input")),
            output: Some(PathBuf::from("out/result.json")),
            schema: Some(PathBuf::from("/absolute/schema.yaml")),
            mapping: None,
            quarantine: Some(PathBuf::from("quarantine")),
            output_format: None,
            color: Some(ColorMode::Never),
            progress: Some(false),
            progress_threshold_bytes: Some(42),
        };

        resolve_profile_paths(base_dir, &mut profile);

        assert_eq!(
            profile.input.as_deref(),
            Some(base_dir.join("input").as_path())
        );
        assert_eq!(
            profile.output.as_deref(),
            Some(base_dir.join("out/result.json").as_path())
        );
        assert_eq!(
            profile.schema.as_deref(),
            Some(Path::new("/absolute/schema.yaml"))
        );
        assert_eq!(
            profile.quarantine.as_deref(),
            Some(base_dir.join("quarantine").as_path())
        );
    }
}
