//! Database read operations.

use std::fmt;

use edi_ir::{Document, Node, NodeType, Value};

use crate::Result;
use crate::connection::DbConnection;
use crate::schema::{DbValue, Row, SchemaMapping};

/// Query options for read operations.
#[derive(Debug, Clone, Default)]
pub struct QueryOptions {
    pub filter: Option<Row>,
    pub offset: usize,
    pub limit: Option<usize>,
}

impl QueryOptions {
    pub fn with_filter(mut self, filter: Row) -> Self {
        self.filter = Some(filter);
        self
    }

    pub fn with_offset(mut self, offset: usize) -> Self {
        self.offset = offset;
        self
    }

    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }
}

/// Reader facade.
#[derive(Clone)]
pub struct DbReader {
    connection: DbConnection,
}

impl fmt::Debug for DbReader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DbReader").finish_non_exhaustive()
    }
}

impl DbReader {
    pub fn new(connection: DbConnection) -> Self {
        Self { connection }
    }

    pub async fn read_table(&self, table: &str) -> Result<Vec<Row>> {
        self.connection.select_rows(table, None, 0, None).await
    }

    pub async fn read_single(&self, table: &str, filter: &Row) -> Result<Option<Row>> {
        let rows = self
            .connection
            .select_rows(table, Some(filter), 0, Some(1))
            .await?;
        Ok(rows.into_iter().next())
    }

    pub async fn read_with_options(&self, table: &str, options: &QueryOptions) -> Result<Vec<Row>> {
        self.connection
            .select_rows(
                table,
                options.filter.as_ref(),
                options.offset,
                options.limit,
            )
            .await
    }

    pub async fn read_with_schema(
        &self,
        table: &str,
        options: &QueryOptions,
        schema_mapping: &SchemaMapping,
    ) -> Result<Vec<Row>> {
        let schema = schema_mapping
            .table(table)
            .ok_or_else(|| crate::Error::Schema {
                details: format!("Unknown table '{table}'"),
            })?;
        let rows = self
            .connection
            .select_rows_with_schema(
                table,
                options.filter.as_ref(),
                options.offset,
                options.limit,
                Some(schema),
            )
            .await?;
        for row in &rows {
            schema_mapping.validate_row(table, row)?;
        }
        Ok(rows)
    }

    pub async fn read_to_ir(&self, table: &str, options: &QueryOptions) -> Result<Document> {
        let rows = self.read_with_options(table, options).await?;
        let mut root = Node::new("DB", NodeType::Root);

        for row in rows {
            let mut record = Node::new(table, NodeType::Record);
            for (column, value) in row {
                record.add_child(Node::with_value(
                    column,
                    NodeType::Field,
                    db_to_ir_value(value),
                ));
            }
            root.add_child(record);
        }

        Ok(Document::new(root))
    }
}

fn db_to_ir_value(value: DbValue) -> Value {
    match value {
        DbValue::String(value) => Value::String(value),
        DbValue::Blob(value) => Value::Binary(value),
        DbValue::Integer(value) => Value::Integer(value),
        DbValue::Decimal(value) => Value::Decimal(value),
        DbValue::Boolean(value) => Value::Boolean(value),
        DbValue::Null => Value::Null,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connection::DbConnection;
    use crate::schema::{ColumnDef, ColumnType, DbValue, SchemaMapping, TableSchema};

    fn order_row(id: i64, order_no: &str) -> Row {
        let mut row = Row::new();
        row.insert("id".to_string(), DbValue::Integer(id));
        row.insert(
            "order_no".to_string(),
            DbValue::String(order_no.to_string()),
        );
        row
    }

    fn sample_schema() -> SchemaMapping {
        let table = TableSchema::new("orders")
            .with_column(ColumnDef::new("id", ColumnType::Integer).primary_key())
            .with_column(ColumnDef::new("order_no", ColumnType::String));
        let mut schema = SchemaMapping::new();
        schema.add_table(table);
        schema
    }

    async fn setup_reader() -> (DbConnection, DbReader) {
        let connection = DbConnection::new();
        connection.connect().await.unwrap();
        connection.apply_schema(&sample_schema()).await.unwrap();
        connection
            .insert_row("orders", order_row(1, "PO-1"))
            .await
            .unwrap();
        connection
            .insert_row("orders", order_row(2, "PO-2"))
            .await
            .unwrap();
        connection
            .insert_row("orders", order_row(3, "PO-3"))
            .await
            .unwrap();

        let reader = DbReader::new(connection.clone());
        (connection, reader)
    }

    #[tokio::test]
    async fn test_read_single_row() {
        let (_, reader) = setup_reader().await;

        let mut filter = Row::new();
        filter.insert("id".to_string(), DbValue::Integer(2));

        let row = reader
            .read_single("orders", &filter)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            row.get("order_no"),
            Some(&DbValue::String("PO-2".to_string()))
        );
    }

    #[tokio::test]
    async fn test_read_multiple_rows() {
        let (_, reader) = setup_reader().await;
        let rows = reader.read_table("orders").await.unwrap();
        assert_eq!(rows.len(), 3);
    }

    #[tokio::test]
    async fn test_read_with_filter() {
        let (_, reader) = setup_reader().await;

        let mut filter = Row::new();
        filter.insert("id".to_string(), DbValue::Integer(3));
        let options = QueryOptions::default().with_filter(filter);

        let rows = reader.read_with_options("orders", &options).await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].get("order_no"),
            Some(&DbValue::String("PO-3".to_string()))
        );
    }

    #[tokio::test]
    async fn test_read_pagination() {
        let (_, reader) = setup_reader().await;

        let options = QueryOptions::default().with_offset(1).with_limit(1);
        let rows = reader.read_with_options("orders", &options).await.unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get("id"), Some(&DbValue::Integer(2)));
    }

    #[tokio::test]
    async fn test_read_empty_result() {
        let (_, reader) = setup_reader().await;

        let mut filter = Row::new();
        filter.insert("id".to_string(), DbValue::Integer(99));
        let options = QueryOptions::default().with_filter(filter);

        let rows = reader.read_with_options("orders", &options).await.unwrap();
        assert!(rows.is_empty());
    }

    #[tokio::test]
    async fn test_read_to_ir() {
        let (_, reader) = setup_reader().await;

        let document = reader
            .read_to_ir("orders", &QueryOptions::default())
            .await
            .unwrap();

        assert_eq!(document.root.name, "DB");
        assert_eq!(document.root.children.len(), 3);
        assert_eq!(document.root.children[0].node_type, NodeType::Record);
    }

    #[tokio::test]
    async fn test_read_with_schema() {
        let (_, reader) = setup_reader().await;

        let table = TableSchema::new("orders")
            .with_column(ColumnDef::new("id", ColumnType::Integer))
            .with_column(ColumnDef::new("order_no", ColumnType::String));

        let mut mapping = SchemaMapping::new();
        mapping.add_table(table);

        let rows = reader
            .read_with_schema("orders", &QueryOptions::default(), &mapping)
            .await
            .unwrap();
        assert_eq!(rows.len(), 3);
    }

    #[tokio::test]
    async fn test_read_async() {
        let (connection, reader) = setup_reader().await;
        let reader2 = DbReader::new(connection);
        let options = QueryOptions::default().with_limit(2);

        let (rows1, rows2) = tokio::join!(
            reader.read_table("orders"),
            reader2.read_with_options("orders", &options)
        );

        assert_eq!(rows1.unwrap().len(), 3);
        assert_eq!(rows2.unwrap().len(), 2);
    }
}
