//! Schema loader

use crate::model::{ElementDefinition, Schema, SegmentDefinition};
use crate::{Error, Result};
use serde::Deserialize;
use std::path::Path;

/// Serializable schema format for loading
#[derive(Debug, Deserialize)]
struct SchemaFile {
    name: String,
    version: String,
    #[serde(default)]
    segments: Vec<SegmentFile>,
}

#[derive(Debug, Deserialize)]
struct SegmentFile {
    tag: String,
    #[serde(default)]
    elements: Vec<ElementFile>,
    #[serde(default)]
    is_mandatory: bool,
    #[serde(default)]
    max_repetitions: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct ElementFile {
    id: String,
    name: String,
    data_type: String,
    #[serde(default)]
    min_length: usize,
    #[serde(default = "default_max_length")]
    max_length: usize,
    #[serde(default)]
    is_mandatory: bool,
}

fn default_max_length() -> usize {
    35
}

/// Loads schemas from various sources
pub struct SchemaLoader;

impl SchemaLoader {
    /// Create a new schema loader
    pub fn new() -> Self {
        Self
    }

    /// Load a schema from a file
    pub fn load_from_file(&self, path: &Path) -> Result<Schema> {
        let content = std::fs::read_to_string(path)?;

        if path
            .extension()
            .map(|e| e == "yaml" || e == "yml")
            .unwrap_or(false)
        {
            self.load_from_yaml(&content)
        } else {
            self.load_from_json(&content)
        }
    }

    /// Load a schema from JSON
    pub fn load_from_json(&self, json: &str) -> Result<Schema> {
        let schema_file: SchemaFile = serde_json::from_str(json)
            .map_err(|e| Error::InvalidFormat(format!("JSON parse error: {}", e)))?;

        Ok(Schema {
            name: schema_file.name,
            version: schema_file.version,
            segments: schema_file
                .segments
                .into_iter()
                .map(|s| SegmentDefinition {
                    tag: s.tag,
                    elements: s
                        .elements
                        .into_iter()
                        .map(|e| ElementDefinition {
                            id: e.id,
                            name: e.name,
                            data_type: e.data_type,
                            min_length: e.min_length,
                            max_length: e.max_length,
                            is_mandatory: e.is_mandatory,
                        })
                        .collect(),
                    is_mandatory: s.is_mandatory,
                    max_repetitions: s.max_repetitions,
                })
                .collect(),
        })
    }

    /// Load a schema from YAML
    pub fn load_from_yaml(&self, yaml: &str) -> Result<Schema> {
        let schema_file: SchemaFile = serde_yaml::from_str(yaml)
            .map_err(|e| Error::InvalidFormat(format!("YAML parse error: {}", e)))?;

        Ok(Schema {
            name: schema_file.name,
            version: schema_file.version,
            segments: schema_file
                .segments
                .into_iter()
                .map(|s| SegmentDefinition {
                    tag: s.tag,
                    elements: s
                        .elements
                        .into_iter()
                        .map(|e| ElementDefinition {
                            id: e.id,
                            name: e.name,
                            data_type: e.data_type,
                            min_length: e.min_length,
                            max_length: e.max_length,
                            is_mandatory: e.is_mandatory,
                        })
                        .collect(),
                    is_mandatory: s.is_mandatory,
                    max_repetitions: s.max_repetitions,
                })
                .collect(),
        })
    }

    /// Parse version string into components
    pub fn parse_version(&self, version: &str) -> Result<(String, u32, Option<u32>)> {
        let parts: Vec<&str> = version.split('.').collect();
        if parts.is_empty() {
            return Err(Error::InvalidFormat("Empty version string".to_string()));
        }

        let major = parts[0]
            .parse::<u32>()
            .map_err(|_| Error::InvalidFormat(format!("Invalid major version: {}", parts[0])))?;

        let minor = if parts.len() > 1 {
            Some(parts[1].parse::<u32>().map_err(|_| {
                Error::InvalidFormat(format!("Invalid minor version: {}", parts[1]))
            })?)
        } else {
            None
        };

        Ok((version.to_string(), major, minor))
    }
}

impl Default for SchemaLoader {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_load_schema_from_file_json() {
        let loader = SchemaLoader::new();
        let path = Path::new("tests/data/minimal_schema.json");
        let result = loader.load_from_file(path);
        assert!(result.is_ok());
        let schema = result.unwrap();
        assert_eq!(schema.name, "minimal");
        assert_eq!(schema.version, "1.0");
    }

    #[test]
    fn test_load_schema_from_file_yaml() {
        let loader = SchemaLoader::new();
        let path = Path::new("tests/data/minimal_schema.yaml");
        let result = loader.load_from_file(path);
        assert!(result.is_ok());
        let schema = result.unwrap();
        assert_eq!(schema.name, "minimal");
        assert_eq!(schema.version, "1.0");
    }

    #[test]
    fn test_load_eancom_d96a() {
        let loader = SchemaLoader::new();
        let path = Path::new("tests/data/eancom_d96a_orders.json");
        let result = loader.load_from_file(path);
        assert!(result.is_ok());
        let schema = result.unwrap();
        assert_eq!(schema.name, "ORDERS");
        assert_eq!(schema.version, "D96A");
        assert_eq!(schema.segments.len(), 4);

        let unh = &schema.segments[0];
        assert_eq!(unh.tag, "UNH");
        assert!(unh.is_mandatory);
        assert_eq!(unh.elements.len(), 1);
        assert_eq!(unh.elements[0].id, "0062");
    }

    #[test]
    fn test_load_schema_not_found() {
        let loader = SchemaLoader::new();
        let path = Path::new("tests/data/nonexistent.json");
        let result = loader.load_from_file(path);
        assert!(result.is_err());
        match result.unwrap_err() {
            Error::Io(_) => (), // Expected
            e => panic!("Expected Io error, got {:?}", e),
        }
    }

    #[test]
    fn test_load_schema_invalid_format() {
        let loader = SchemaLoader::new();
        let path = Path::new("tests/data/invalid_schema.json");
        let result = loader.load_from_file(path);
        assert!(result.is_err());
        match result.unwrap_err() {
            Error::InvalidFormat(_) => (), // Expected
            e => panic!("Expected InvalidFormat error, got {:?}", e),
        }
    }

    #[test]
    fn test_load_from_json() {
        let loader = SchemaLoader::new();
        let json = r#"{"name": "TEST", "version": "1.0", "segments": []}"#;
        let result = loader.load_from_json(json);
        assert!(result.is_ok());
        let schema = result.unwrap();
        assert_eq!(schema.name, "TEST");
        assert_eq!(schema.version, "1.0");
    }

    #[test]
    fn test_load_from_json_with_segments() {
        let loader = SchemaLoader::new();
        let json = r#"
        {
            "name": "TEST",
            "version": "1.0",
            "segments": [
                {
                    "tag": "UNH",
                    "elements": [
                        {
                            "id": "0062",
                            "name": "Reference",
                            "data_type": "an",
                            "min_length": 1,
                            "max_length": 14,
                            "is_mandatory": true
                        }
                    ],
                    "is_mandatory": true,
                    "max_repetitions": 1
                }
            ]
        }
        "#;
        let result = loader.load_from_json(json);
        assert!(result.is_ok());
        let schema = result.unwrap();
        assert_eq!(schema.segments.len(), 1);
        assert_eq!(schema.segments[0].elements[0].name, "Reference");
    }

    #[test]
    fn test_load_from_json_invalid() {
        let loader = SchemaLoader::new();
        let json = "not valid json";
        let result = loader.load_from_json(json);
        assert!(result.is_err());
    }

    #[test]
    fn test_load_from_yaml() {
        let loader = SchemaLoader::new();
        let yaml = "name: TEST\nversion: '1.0'\nsegments: []";
        let result = loader.load_from_yaml(yaml);
        assert!(result.is_ok());
        let schema = result.unwrap();
        assert_eq!(schema.name, "TEST");
    }

    #[test]
    fn test_load_from_yaml_invalid() {
        let loader = SchemaLoader::new();
        let yaml = "name: TEST\nversion: ["; // Invalid YAML
        let result = loader.load_from_yaml(yaml);
        assert!(result.is_err());
    }

    #[test]
    fn test_schema_version_parsing_simple() {
        let loader = SchemaLoader::new();
        let result = loader.parse_version("1");
        assert!(result.is_ok());
        let (full, major, minor) = result.unwrap();
        assert_eq!(full, "1");
        assert_eq!(major, 1);
        assert_eq!(minor, None);
    }

    #[test]
    fn test_schema_version_parsing_with_minor() {
        let loader = SchemaLoader::new();
        let result = loader.parse_version("2.5");
        assert!(result.is_ok());
        let (full, major, minor) = result.unwrap();
        assert_eq!(full, "2.5");
        assert_eq!(major, 2);
        assert_eq!(minor, Some(5));
    }

    #[test]
    fn test_schema_version_parsing_invalid() {
        let loader = SchemaLoader::new();
        let result = loader.parse_version("abc");
        assert!(result.is_err());
    }

    #[test]
    fn test_schema_version_parsing_empty() {
        let loader = SchemaLoader::new();
        let result = loader.parse_version("");
        assert!(result.is_err());
    }
}
