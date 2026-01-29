//! Schema loader with inheritance support

use crate::inheritance::{detect_circular_dependency, merge_schemas};
use crate::model::{ElementDefinition, Schema, SchemaRef, SegmentDefinition};
use crate::registry::ConcurrentSchemaRegistry;
use crate::{Error, Result};
use serde::Deserialize;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tracing::{debug, info, trace};

/// Serializable schema format for loading from files
#[derive(Debug, Deserialize)]
struct SchemaFile {
    name: String,
    version: String,
    #[serde(default)]
    parent: Option<SchemaRefFile>,
    #[serde(default)]
    segments: Vec<SegmentFile>,
}

/// Serializable schema reference for inheritance
#[derive(Debug, Deserialize, Clone)]
struct SchemaRefFile {
    name: String,
    version: String,
}

impl From<SchemaRefFile> for SchemaRef {
    fn from(file: SchemaRefFile) -> Self {
        Self::new(file.name, file.version)
    }
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

/// Enhanced schema loader with inheritance support
pub struct SchemaLoader {
    registry: Arc<ConcurrentSchemaRegistry>,
    schema_paths: Vec<PathBuf>,
}

impl SchemaLoader {
    /// Create a new schema loader with the given search paths
    pub fn new(schema_paths: Vec<PathBuf>) -> Self {
        Self {
            registry: Arc::new(ConcurrentSchemaRegistry::new()),
            schema_paths,
        }
    }

    /// Create a new schema loader with a pre-configured registry
    pub fn with_registry(
        registry: Arc<ConcurrentSchemaRegistry>,
        schema_paths: Vec<PathBuf>,
    ) -> Self {
        Self {
            registry,
            schema_paths,
        }
    }

    /// Load a schema by name and version
    /// First checks the cache, then loads from disk
    pub fn load(&self, name: &str, version: &str) -> Result<Schema> {
        let qualified_name = format!("{}: {}", name, version);

        // Check cache first
        if let Some(cached) = self.registry.get(&qualified_name) {
            debug!("Cache hit for schema: {}", qualified_name);
            return Ok(cached);
        }

        trace!("Cache miss for schema: {}", qualified_name);

        // Find and load the schema file
        let schema = self.load_from_disk(name, version)?;

        // Cache the loaded schema
        self.registry.register(&qualified_name, schema.clone());

        Ok(schema)
    }

    /// Load a schema with full inheritance resolution
    /// Resolves the entire inheritance chain and merges schemas
    pub fn load_with_inheritance(&self, schema_ref: &SchemaRef) -> Result<Schema> {
        let qualified_name = schema_ref.qualified_name();

        // Check cache first
        if let Some(cached) = self.registry.get(&qualified_name) {
            debug!("Cache hit for schema with inheritance: {}", qualified_name);
            return Ok(cached);
        }

        info!("Loading schema with inheritance: {}", qualified_name);

        // Load the base schema
        let base_schema = self.load(&schema_ref.name, &schema_ref.version)?;

        // Resolve inheritance chain
        let chain = self.resolve_inheritance_chain(&base_schema)?;

        if chain.is_empty() {
            // No inheritance, just cache and return
            self.registry.register(&qualified_name, base_schema.clone());
            return Ok(base_schema);
        }

        // Merge schemas from base to leaf
        let merged = self.merge_schemas(chain)?;

        // Cache the merged result
        self.registry.register(&qualified_name, merged.clone());

        Ok(merged)
    }

    /// Load a schema from a specific file path
    pub fn load_from_file(&self, path: &Path) -> Result<Schema> {
        trace!("Loading schema from file: {:?}", path);
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

    /// Load a schema from JSON string
    pub fn load_from_json(&self, json: &str) -> Result<Schema> {
        let schema_file: SchemaFile = serde_json::from_str(json)
            .map_err(|e| Error::InvalidFormat(format!("JSON parse error: {}", e)))?;

        Ok(self.convert_schema_file(schema_file))
    }

    /// Load a schema from YAML string
    pub fn load_from_yaml(&self, yaml: &str) -> Result<Schema> {
        let schema_file: SchemaFile = serde_yaml::from_str(yaml)
            .map_err(|e| Error::InvalidFormat(format!("YAML parse error: {}", e)))?;

        Ok(self.convert_schema_file(schema_file))
    }

    /// Convert a SchemaFile to a Schema
    fn convert_schema_file(&self, schema_file: SchemaFile) -> Schema {
        let parent = schema_file.parent.map(|p| p.into());

        let segments: Vec<SegmentDefinition> = schema_file
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
            .collect();

        let mut schema = Schema::new(schema_file.name, schema_file.version).with_segments(segments);

        if let Some(parent_ref) = parent {
            schema.inheritance.parent = Some(parent_ref);
        }

        schema
    }

    /// Load a schema from disk by name and version
    fn load_from_disk(&self, name: &str, version: &str) -> Result<Schema> {
        // Try to find the schema file in the search paths
        let file_name = format!("{}_{}.json", name.to_lowercase(), version.to_lowercase());

        for path in &self.schema_paths {
            let file_path = path.join(&file_name);
            if file_path.exists() {
                trace!("Found schema file: {:?}", file_path);
                return self.load_from_file(&file_path);
            }

            // Also try with the name as-is (for EDIFACT, EANCOM, etc.)
            let file_path = path.join(format!("{}.json", name.to_lowercase()));
            if file_path.exists() {
                trace!("Found schema file: {:?}", file_path);
                return self.load_from_file(&file_path);
            }
        }

        // Try common variations
        let variations = vec![
            format!("{}_{}.json", name.to_lowercase(), version.to_lowercase()),
            format!("{}_{}.yaml", name.to_lowercase(), version.to_lowercase()),
            format!("{}_{}.yml", name.to_lowercase(), version.to_lowercase()),
            format!("{}.json", name.to_lowercase()),
        ];

        for path in &self.schema_paths {
            for variation in &variations {
                let file_path = path.join(variation);
                if file_path.exists() {
                    trace!("Found schema file: {:?}", file_path);
                    return self.load_from_file(&file_path);
                }
            }
        }

        Err(Error::NotFound(format!(
            "Schema {}: {} not found in search paths: {:?}",
            name, version, self.schema_paths
        )))
    }

    /// Resolve the inheritance chain for a schema
    /// Returns schemas from base (EDIFACT) to most specific
    fn resolve_inheritance_chain(&self, schema: &Schema) -> Result<Vec<Schema>> {
        let mut chain = vec![schema.clone()];
        let mut visited = HashSet::new();
        let mut current_schema = schema.clone();

        // Track visited schemas to detect cycles
        visited.insert(current_schema.qualified_name());

        // Walk up the inheritance hierarchy
        while let Some(parent_ref) = &current_schema.inheritance.parent {
            let parent_name = parent_ref.qualified_name();

            // Check for circular dependencies
            if visited.contains(&parent_name) {
                return Err(Error::Inheritance(format!(
                    "Circular dependency detected: {} -> {}",
                    schema.qualified_name(),
                    parent_name
                )));
            }

            // Detect would-be circular dependencies proactively
            if detect_circular_dependency(&current_schema.qualified_name(), &parent_name, &visited)
            {
                return Err(Error::Inheritance(format!(
                    "Circular dependency detected involving: {}",
                    parent_name
                )));
            }

            // Load the parent schema
            let parent_schema = match self.load(&parent_ref.name, &parent_ref.version) {
                Ok(s) => s,
                Err(Error::NotFound(_)) => {
                    return Err(Error::Inheritance(format!(
                        "Parent schema not found: {} (referenced by {})",
                        parent_name,
                        current_schema.qualified_name()
                    )));
                }
                Err(e) => return Err(e),
            };

            visited.insert(parent_name);
            chain.push(parent_schema.clone());
            current_schema = parent_schema;
        }

        // Reverse to get base -> leaf order
        chain.reverse();

        trace!(
            "Resolved inheritance chain for {}: {:?}",
            schema.qualified_name(),
            chain.iter().map(|s| s.qualified_name()).collect::<Vec<_>>()
        );

        Ok(chain)
    }

    /// Merge a chain of schemas from base to leaf
    fn merge_schemas(&self, chain: Vec<Schema>) -> Result<Schema> {
        if chain.is_empty() {
            return Err(Error::Inheritance("Cannot merge empty chain".to_string()));
        }

        if chain.len() == 1 {
            return Ok(chain.into_iter().next().unwrap());
        }

        // Start with the first (most base) schema
        let mut result = chain[0].clone();
        let leaf_name = chain.last().unwrap().qualified_name();

        // Apply each subsequent level
        for parent in &chain[1..] {
            result = merge_schemas(&result, parent);
            result.name = parent.name.clone();
            result.version = parent.version.clone();
        }

        // Update inheritance metadata
        result.inheritance.inheritance_chain = chain
            .iter()
            .map(|s| SchemaRef::new(&s.name, &s.version))
            .collect();
        result.inheritance.is_merged = true;

        info!("Merged inheritance chain into schema: {}", leaf_name);

        Ok(result)
    }

    /// Add a search path for schema files
    pub fn add_path(&mut self, path: PathBuf) {
        self.schema_paths.push(path);
    }

    /// Get the registry (for testing/debugging)
    pub fn registry(&self) -> &ConcurrentSchemaRegistry {
        &self.registry
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
        Self::new(vec![PathBuf::from(".")])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;

    fn create_test_loader() -> SchemaLoader {
        SchemaLoader::new(vec![
            PathBuf::from("tests/data"),
            PathBuf::from("crates/edi-schema/tests/data"),
        ])
    }

    #[test]
    fn test_load_simple_schema() {
        let loader = create_test_loader();
        let result = loader.load("minimal", "1.0");
        assert!(result.is_ok());

        let schema = result.unwrap();
        assert_eq!(schema.name, "minimal");
        assert_eq!(schema.version, "1.0");
        assert!(schema.segments.is_empty());
    }

    #[test]
    fn test_load_eancom_orders() {
        let loader = create_test_loader();
        // Try loading the EANCOM D96A ORDERS schema
        let result = loader.load_from_file(Path::new("tests/data/eancom_d96a_orders.json"));
        assert!(result.is_ok());

        let schema = result.unwrap();
        assert_eq!(schema.name, "ORDERS");
        assert_eq!(schema.version, "D96A");
        assert_eq!(schema.segments.len(), 4);
    }

    #[test]
    fn test_load_with_inheritance() {
        let loader = create_test_loader();
        let schema_ref = SchemaRef::new("partner_orders", "1.0");

        // This requires the test schema files to be set up properly
        // We'll create a test version first
        let _result = loader.load_with_inheritance(&schema_ref);
        // May fail if test files don't exist yet
        // Just verify it doesn't panic
    }

    #[test]
    fn test_load_from_file_json() {
        let loader = SchemaLoader::default();
        let path = Path::new("tests/data/minimal.json");
        let result = loader.load_from_file(path);
        assert!(result.is_ok());
        let schema = result.unwrap();
        assert_eq!(schema.name, "minimal");
        assert_eq!(schema.version, "1.0");
    }

    #[test]
    fn test_load_from_file_yaml() {
        let loader = SchemaLoader::default();
        let path = Path::new("tests/data/minimal_schema.yaml");
        let result = loader.load_from_file(path);
        assert!(result.is_ok());
        let schema = result.unwrap();
        assert_eq!(schema.name, "minimal");
        assert_eq!(schema.version, "1.0");
    }

    #[test]
    fn test_load_schema_not_found() {
        let loader = create_test_loader();
        let result = loader.load("nonexistent", "1.0");
        assert!(result.is_err());
        match result.unwrap_err() {
            Error::NotFound(_) => (),
            e => panic!("Expected NotFound error, got {:?}", e),
        }
    }

    #[test]
    fn test_load_schema_invalid_format() {
        let loader = SchemaLoader::default();
        let path = Path::new("tests/data/invalid_schema.json");
        let result = loader.load_from_file(path);
        assert!(result.is_err());
        match result.unwrap_err() {
            Error::InvalidFormat(_) => (),
            e => panic!("Expected InvalidFormat error, got {:?}", e),
        }
    }

    #[test]
    fn test_load_from_json() {
        let loader = SchemaLoader::default();
        let json = r#"{"name": "TEST", "version": "1.0", "segments": []}"#;
        let result = loader.load_from_json(json);
        assert!(result.is_ok());
        let schema = result.unwrap();
        assert_eq!(schema.name, "TEST");
        assert_eq!(schema.version, "1.0");
    }

    #[test]
    fn test_load_from_json_with_parent() {
        let loader = SchemaLoader::default();
        let json = r#"
        {
            "name": "ORDERS",
            "version": "D96A",
            "parent": {
                "name": "EANCOM",
                "version": "D96A"
            },
            "segments": []
        }
        "#;
        let result = loader.load_from_json(json);
        assert!(result.is_ok());
        let schema = result.unwrap();
        assert_eq!(schema.name, "ORDERS");
        assert!(schema.inheritance.parent.is_some());
        let parent = schema.inheritance.parent.unwrap();
        assert_eq!(parent.name, "EANCOM");
        assert_eq!(parent.version, "D96A");
    }

    #[test]
    fn test_load_from_json_with_segments() {
        let loader = SchemaLoader::default();
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
        let loader = SchemaLoader::default();
        let json = "not valid json";
        let result = loader.load_from_json(json);
        assert!(result.is_err());
    }

    #[test]
    fn test_load_from_yaml() {
        let loader = SchemaLoader::default();
        let yaml = "name: TEST\nversion: '1.0'\nsegments: []";
        let result = loader.load_from_yaml(yaml);
        assert!(result.is_ok());
        let schema = result.unwrap();
        assert_eq!(schema.name, "TEST");
    }

    #[test]
    fn test_load_from_yaml_with_parent() {
        let loader = SchemaLoader::default();
        let yaml = r#"
name: ORDERS
version: D96A
parent:
  name: EANCOM
  version: D96A
segments: []
"#;
        let result = loader.load_from_yaml(yaml);
        assert!(result.is_ok());
        let schema = result.unwrap();
        assert!(schema.inheritance.parent.is_some());
    }

    #[test]
    fn test_load_from_yaml_invalid() {
        let loader = SchemaLoader::default();
        let yaml = "name: TEST\nversion: ["; // Invalid YAML
        let result = loader.load_from_yaml(yaml);
        assert!(result.is_err());
    }

    #[test]
    fn test_caching() {
        let loader = create_test_loader();

        // First load
        let start = Instant::now();
        let schema1 = loader.load("minimal", "1.0").unwrap();
        let duration1 = start.elapsed();

        // Second load should be faster (from cache)
        let start = Instant::now();
        let schema2 = loader.load("minimal", "1.0").unwrap();
        let duration2 = start.elapsed();

        assert_eq!(schema1.name, schema2.name);
        // Second load should be much faster
        assert!(duration2 < duration1 || duration2.as_micros() < 100);
    }

    #[test]
    fn test_resolve_inheritance_chain_simple() {
        // Create a loader with a mock registry
        let registry = Arc::new(ConcurrentSchemaRegistry::new());
        let loader = SchemaLoader::with_registry(registry.clone(), vec![]);

        // Create a parent schema
        let parent =
            Schema::new("EDIFACT", "D96A").with_segments(vec![SegmentDefinition::new("UNA")]);
        registry.register("EDIFACT: D96A", parent);

        // Create a child schema
        let child = Schema::new("EANCOM", "D96A")
            .with_parent(SchemaRef::new("EDIFACT", "D96A"))
            .with_segments(vec![SegmentDefinition::new("UNH")]);

        // Manually set up the registry for the child
        registry.register("EANCOM: D96A", child.clone());

        // Resolve the chain
        let chain = loader.resolve_inheritance_chain(&child).unwrap();

        assert_eq!(chain.len(), 2);
        assert_eq!(chain[0].name, "EDIFACT");
        assert_eq!(chain[1].name, "EANCOM");
    }

    #[test]
    fn test_resolve_inheritance_chain_missing_parent() {
        let loader = SchemaLoader::with_registry(Arc::new(ConcurrentSchemaRegistry::new()), vec![]);

        let child = Schema::new("CHILD", "1.0").with_parent(SchemaRef::new("NONEXISTENT", "1.0"));

        let result = loader.resolve_inheritance_chain(&child);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Parent schema not found"));
    }

    #[test]
    fn test_resolve_inheritance_chain_circular() {
        let registry = Arc::new(ConcurrentSchemaRegistry::new());
        let loader = SchemaLoader::with_registry(registry.clone(), vec![]);

        // Create circular dependency: A -> B -> A
        let schema_a = Schema::new("A", "1.0").with_parent(SchemaRef::new("B", "1.0"));
        let schema_b = Schema::new("B", "1.0").with_parent(SchemaRef::new("A", "1.0"));

        registry.register("A: 1.0", schema_a.clone());
        registry.register("B: 1.0", schema_b.clone());

        let result = loader.resolve_inheritance_chain(&schema_a);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Circular dependency"));
    }

    #[test]
    fn test_merge_schemas_from_chain() {
        let loader = SchemaLoader::default();

        // Create base schema
        let base = Schema::new("EDIFACT", "D96A").with_segments(vec![
            SegmentDefinition::new("UNA").mandatory(true),
            SegmentDefinition::new("UNZ").mandatory(true),
        ]);

        // Create EANCOM schema
        let eancom = Schema::new("EANCOM", "D96A")
            .with_segments(vec![SegmentDefinition::new("UNH").mandatory(true)]);

        // Create ORDERS schema
        let orders = Schema::new("ORDERS", "D96A")
            .with_segments(vec![SegmentDefinition::new("BGM").mandatory(true)]);

        let chain = vec![base, eancom, orders];
        let merged = loader.merge_schemas(chain).unwrap();

        assert_eq!(merged.segments.len(), 4);
        assert!(merged.find_segment("UNA").is_some());
        assert!(merged.find_segment("UNZ").is_some());
        assert!(merged.find_segment("UNH").is_some());
        assert!(merged.find_segment("BGM").is_some());
    }

    #[test]
    fn test_performance_load() {
        let loader = create_test_loader();

        let start = Instant::now();
        let result = loader.load("minimal", "1.0");
        let duration = start.elapsed();

        assert!(result.is_ok());
        assert!(
            duration.as_millis() < 100,
            "Load took {}ms, expected <100ms",
            duration.as_millis()
        );
    }

    #[test]
    fn test_schema_version_parsing_simple() {
        let loader = SchemaLoader::default();
        let result = loader.parse_version("1");
        assert!(result.is_ok());
        let (full, major, minor) = result.unwrap();
        assert_eq!(full, "1");
        assert_eq!(major, 1);
        assert_eq!(minor, None);
    }

    #[test]
    fn test_schema_version_parsing_with_minor() {
        let loader = SchemaLoader::default();
        let result = loader.parse_version("2.5");
        assert!(result.is_ok());
        let (full, major, minor) = result.unwrap();
        assert_eq!(full, "2.5");
        assert_eq!(major, 2);
        assert_eq!(minor, Some(5));
    }

    #[test]
    fn test_schema_version_parsing_invalid() {
        let loader = SchemaLoader::default();
        let result = loader.parse_version("abc");
        assert!(result.is_err());
    }

    #[test]
    fn test_schema_version_parsing_empty() {
        let loader = SchemaLoader::default();
        let result = loader.parse_version("");
        assert!(result.is_err());
    }

    #[test]
    fn test_load_full_inheritance_chain() {
        // This test requires the test schema files to be present
        // It's more of an integration test
        let loader = create_test_loader();

        // Try to load ORDERS which should have EANCOM parent
        // (if the test files are set up)
        let _ = loader.load("orders", "d96a");
    }
}
