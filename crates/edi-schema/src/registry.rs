//! Schema registry with inheritance support

use crate::model::Schema;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// Registry for managing schemas with hierarchical inheritance
pub struct SchemaRegistry {
    schemas: HashMap<String, Schema>,
}

/// Thread-safe schema registry for concurrent access
pub struct ConcurrentSchemaRegistry {
    schemas: Arc<RwLock<HashMap<String, Schema>>>,
}

impl SchemaRegistry {
    /// Create a new empty registry
    pub fn new() -> Self {
        Self {
            schemas: HashMap::new(),
        }
    }

    /// Register a schema
    pub fn register(&mut self, name: impl Into<String>, schema: Schema) {
        self.schemas.insert(name.into(), schema);
    }

    /// Get a schema by name
    pub fn get(&self, name: &str) -> Option<&Schema> {
        self.schemas.get(name)
    }

    /// Check if a schema exists
    pub fn contains(&self, name: &str) -> bool {
        self.schemas.contains_key(name)
    }

    /// Get the number of schemas in the registry
    pub fn len(&self) -> usize {
        self.schemas.len()
    }

    /// Check if the registry is empty
    pub fn is_empty(&self) -> bool {
        self.schemas.is_empty()
    }

    /// Remove a schema from the registry
    pub fn remove(&mut self, name: &str) -> Option<Schema> {
        self.schemas.remove(name)
    }

    /// Get all schema names
    pub fn names(&self) -> Vec<&String> {
        self.schemas.keys().collect()
    }

    /// Register multiple schemas at once
    pub fn register_many(&mut self, schemas: Vec<(String, Schema)>) {
        for (name, schema) in schemas {
            self.schemas.insert(name, schema);
        }
    }
}

impl Default for SchemaRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl ConcurrentSchemaRegistry {
    /// Create a new empty concurrent registry
    pub fn new() -> Self {
        Self {
            schemas: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Register a schema (thread-safe)
    pub fn register(&self, name: impl Into<String>, schema: Schema) {
        let mut schemas = self.schemas.write().unwrap();
        schemas.insert(name.into(), schema);
    }

    /// Get a schema by name (thread-safe)
    pub fn get(&self, name: &str) -> Option<Schema> {
        let schemas = self.schemas.read().unwrap();
        schemas.get(name).cloned()
    }

    /// Check if a schema exists (thread-safe)
    pub fn contains(&self, name: &str) -> bool {
        let schemas = self.schemas.read().unwrap();
        schemas.contains_key(name)
    }

    /// Get the number of schemas in the registry (thread-safe)
    pub fn len(&self) -> usize {
        let schemas = self.schemas.read().unwrap();
        schemas.len()
    }

    /// Check if the registry is empty (thread-safe)
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Default for ConcurrentSchemaRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for ConcurrentSchemaRegistry {
    fn clone(&self) -> Self {
        Self {
            schemas: Arc::clone(&self.schemas),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_schema(name: &str) -> Schema {
        Schema {
            name: name.to_string(),
            version: "1.0".to_string(),
            segments: vec![],
            inheritance: Default::default(),
        }
    }

    #[test]
    fn test_registry_creation() {
        let registry = SchemaRegistry::new();
        assert!(registry.is_empty());
        assert_eq!(registry.len(), 0);
    }

    #[test]
    fn test_register_schema() {
        let mut registry = SchemaRegistry::new();
        let schema = create_test_schema("ORDERS");

        registry.register("orders", schema);

        assert!(!registry.is_empty());
        assert_eq!(registry.len(), 1);
        assert!(registry.contains("orders"));
    }

    #[test]
    fn test_get_schema() {
        let mut registry = SchemaRegistry::new();
        let schema = create_test_schema("ORDERS");

        registry.register("orders", schema);

        let retrieved = registry.get("orders");
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().name, "ORDERS");
    }

    #[test]
    fn test_get_schema_not_found() {
        let registry = SchemaRegistry::new();

        let retrieved = registry.get("nonexistent");
        assert!(retrieved.is_none());
    }

    #[test]
    fn test_registry_caching() {
        let mut registry = SchemaRegistry::new();
        let schema1 = create_test_schema("ORDERS");
        let schema2 = create_test_schema("DESADV");

        registry.register("orders", schema1);
        registry.register("desadv", schema2);

        // Retrieve multiple times - should return the same schema
        let first = registry.get("orders");
        let second = registry.get("orders");

        assert!(first.is_some());
        assert!(second.is_some());
        assert_eq!(first.unwrap().name, second.unwrap().name);
        assert_eq!(registry.len(), 2);
    }

    #[test]
    fn test_register_multiple_schemas() {
        let mut registry = SchemaRegistry::new();

        let schemas = vec![
            ("orders".to_string(), create_test_schema("ORDERS")),
            ("desadv".to_string(), create_test_schema("DESADV")),
            ("invoic".to_string(), create_test_schema("INVOIC")),
        ];

        registry.register_many(schemas);

        assert_eq!(registry.len(), 3);
        assert!(registry.contains("orders"));
        assert!(registry.contains("desadv"));
        assert!(registry.contains("invoic"));
    }

    #[test]
    fn test_registry_names() {
        let mut registry = SchemaRegistry::new();

        registry.register("schema_a", create_test_schema("A"));
        registry.register("schema_b", create_test_schema("B"));

        let names = registry.names();
        assert_eq!(names.len(), 2);
        assert!(names.contains(&&"schema_a".to_string()));
        assert!(names.contains(&&"schema_b".to_string()));
    }

    #[test]
    fn test_registry_remove() {
        let mut registry = SchemaRegistry::new();

        registry.register("temp", create_test_schema("TEMP"));
        assert!(registry.contains("temp"));

        let removed = registry.remove("temp");
        assert!(removed.is_some());
        assert!(!registry.contains("temp"));
        assert!(registry.is_empty());
    }

    #[test]
    fn test_concurrent_registry_creation() {
        let registry = ConcurrentSchemaRegistry::new();
        assert_eq!(registry.len(), 0);
    }

    #[test]
    fn test_concurrent_register_and_get() {
        let registry = ConcurrentSchemaRegistry::new();
        let schema = create_test_schema("TEST");

        registry.register("test", schema);

        let retrieved = registry.get("test");
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().name, "TEST");
    }

    #[test]
    fn test_concurrent_contains() {
        let registry = ConcurrentSchemaRegistry::new();

        assert!(!registry.contains("missing"));

        registry.register("present", create_test_schema("PRESENT"));
        assert!(registry.contains("present"));
    }

    #[test]
    fn test_concurrent_clone() {
        let registry = ConcurrentSchemaRegistry::new();
        registry.register("schema1", create_test_schema("SCHEMA1"));

        let cloned = registry.clone();

        // Both should see the same data
        assert!(registry.contains("schema1"));
        assert!(cloned.contains("schema1"));

        // Adding to one should be visible in the other
        registry.register("schema2", create_test_schema("SCHEMA2"));
        assert!(cloned.contains("schema2"));
    }

    #[test]
    fn test_concurrent_access() {
        use std::thread;

        let registry = ConcurrentSchemaRegistry::new();
        let registry_clone = registry.clone();

        // Spawn a thread that registers schemas
        let handle = thread::spawn(move || {
            registry_clone.register("from_thread", create_test_schema("THREAD_SCHEMA"));
        });

        // Register from main thread
        registry.register("from_main", create_test_schema("MAIN_SCHEMA"));

        // Wait for thread to complete
        handle.join().unwrap();

        // Both schemas should be present
        assert!(registry.contains("from_main"));
        assert!(registry.contains("from_thread"));
        assert_eq!(registry.len(), 2);
    }

    #[test]
    fn test_concurrent_multiple_readers() {
        use std::thread;

        let registry = ConcurrentSchemaRegistry::new();
        registry.register("shared", create_test_schema("SHARED"));

        let mut handles = vec![];

        // Spawn multiple reader threads
        for i in 0..5 {
            let registry_clone = registry.clone();
            handles.push(thread::spawn(move || {
                let schema = registry_clone.get("shared");
                assert!(schema.is_some());
                assert_eq!(schema.unwrap().name, "SHARED");
                i
            }));
        }

        // Wait for all threads
        for handle in handles {
            handle.join().unwrap();
        }
    }

    #[test]
    fn test_default_impl() {
        let registry: SchemaRegistry = Default::default();
        assert!(registry.is_empty());

        let concurrent: ConcurrentSchemaRegistry = Default::default();
        assert_eq!(concurrent.len(), 0);
    }
}
