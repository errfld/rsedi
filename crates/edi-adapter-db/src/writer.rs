//! Database write operations.

use edi_ir::{Document, Node, Value};

use crate::Result;
use crate::connection::DbConnection;
use crate::schema::{DbValue, Row, SchemaMapping};

/// Writer facade.
#[derive(Clone)]
pub struct DbWriter {
    connection: DbConnection,
}

impl DbWriter {
    pub fn new(connection: DbConnection) -> Self {
        Self { connection }
    }

    pub async fn insert(&self, table: &str, row: Row) -> Result<()> {
        self.connection.insert_row(table, row).await
    }

    pub async fn insert_batch(&self, table: &str, rows: &[Row]) -> Result<usize> {
        for row in rows {
            self.connection.insert_row(table, row.clone()).await?;
        }
        Ok(rows.len())
    }

    pub async fn update(&self, table: &str, filter: &Row, updates: &Row) -> Result<usize> {
        self.connection.update_rows(table, filter, updates).await
    }

    pub async fn upsert(&self, table: &str, key_column: &str, row: Row) -> Result<()> {
        self.connection.upsert_row(table, key_column, row).await
    }

    pub async fn insert_with_schema(
        &self,
        table: &str,
        row: Row,
        schema_mapping: &SchemaMapping,
    ) -> Result<()> {
        schema_mapping.validate_row(table, &row)?;
        self.insert(table, row).await
    }

    pub async fn write_with_transaction(&self, table: &str, rows: &[Row]) -> Result<usize> {
        let mut tx = self.connection.begin_transaction().await?;
        for row in rows {
            tx.insert_row(table, row.clone()).await?;
        }
        tx.commit().await?;
        Ok(rows.len())
    }

    pub async fn write_from_ir(&self, table: &str, document: &Document) -> Result<usize> {
        let records = collect_records(&document.root);
        let mut written = 0usize;

        for record in records {
            let mut row = Row::new();
            for field in &record.children {
                row.insert(
                    field.name.clone(),
                    ir_value_to_db(field.value.clone().unwrap_or(Value::Null)),
                );
            }

            self.insert(table, row).await?;
            written += 1;
        }

        Ok(written)
    }
}

fn collect_records(root: &Node) -> Vec<&Node> {
    let records: Vec<&Node> = root
        .children
        .iter()
        .filter(|node| node.node_type == edi_ir::NodeType::Record)
        .collect();

    if records.is_empty() {
        root.children.iter().collect()
    } else {
        records
    }
}

fn ir_value_to_db(value: Value) -> DbValue {
    match value {
        Value::String(value) | Value::Date(value) | Value::Time(value) | Value::DateTime(value) => {
            DbValue::String(value)
        }
        Value::Integer(value) => DbValue::Integer(value),
        Value::Decimal(value) => DbValue::Decimal(value),
        Value::Boolean(value) => DbValue::Boolean(value),
        Value::Binary(value) => DbValue::String(String::from_utf8_lossy(&value).into_owned()),
        Value::Null => DbValue::Null,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Error;
    use crate::connection::DbConnection;
    use crate::schema::{ColumnDef, ColumnType, SchemaMapping, TableSchema};
    use edi_ir::{Node, NodeType};

    fn sample_row(id: i64, order_no: &str) -> Row {
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

    async fn setup_writer() -> (DbConnection, DbWriter) {
        let connection = DbConnection::new();
        connection.connect().await.unwrap();
        connection.apply_schema(&sample_schema()).await.unwrap();
        let writer = DbWriter::new(connection.clone());
        (connection, writer)
    }

    #[tokio::test]
    async fn test_write_single_row() {
        let (connection, writer) = setup_writer().await;
        writer
            .insert("orders", sample_row(1, "PO-1"))
            .await
            .unwrap();

        assert_eq!(connection.table_row_count("orders").await.unwrap(), 1);
    }

    #[tokio::test]
    async fn test_write_batch() {
        let (connection, writer) = setup_writer().await;
        let rows = vec![sample_row(1, "PO-1"), sample_row(2, "PO-2")];

        let written = writer.insert_batch("orders", &rows).await.unwrap();
        assert_eq!(written, 2);
        assert_eq!(connection.table_row_count("orders").await.unwrap(), 2);
    }

    #[tokio::test]
    async fn test_update_existing() {
        let (connection, writer) = setup_writer().await;
        writer
            .insert("orders", sample_row(1, "PO-1"))
            .await
            .unwrap();

        let mut filter = Row::new();
        filter.insert("id".to_string(), DbValue::Integer(1));

        let mut updates = Row::new();
        updates.insert(
            "order_no".to_string(),
            DbValue::String("PO-1-UPDATED".to_string()),
        );

        let updated = writer.update("orders", &filter, &updates).await.unwrap();
        assert_eq!(updated, 1);

        let rows = connection
            .select_rows("orders", None, 0, None)
            .await
            .unwrap();
        assert_eq!(
            rows[0].get("order_no"),
            Some(&DbValue::String("PO-1-UPDATED".to_string()))
        );
    }

    #[tokio::test]
    async fn test_upsert() {
        let (connection, writer) = setup_writer().await;

        writer
            .upsert("orders", "id", sample_row(1, "PO-1"))
            .await
            .unwrap();
        writer
            .upsert("orders", "id", sample_row(1, "PO-1-REPLACED"))
            .await
            .unwrap();

        let rows = connection
            .select_rows("orders", None, 0, None)
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].get("order_no"),
            Some(&DbValue::String("PO-1-REPLACED".to_string()))
        );
    }

    #[tokio::test]
    async fn test_write_with_transaction() {
        let (connection, writer) = setup_writer().await;
        let rows = vec![sample_row(1, "PO-1"), sample_row(2, "PO-2")];

        writer
            .write_with_transaction("orders", &rows)
            .await
            .unwrap();
        assert_eq!(connection.table_row_count("orders").await.unwrap(), 2);
    }

    #[tokio::test]
    async fn test_write_from_ir() {
        let (connection, writer) = setup_writer().await;

        let mut root = Node::new("ROOT", NodeType::Root);
        let mut record = Node::new("orders", NodeType::Record);
        record.add_child(Node::with_value("id", NodeType::Field, Value::Integer(1)));
        record.add_child(Node::with_value(
            "order_no",
            NodeType::Field,
            Value::String("PO-IR".to_string()),
        ));
        root.add_child(record);

        let document = Document::new(root);
        let written = writer.write_from_ir("orders", &document).await.unwrap();

        assert_eq!(written, 1);
        assert_eq!(connection.table_row_count("orders").await.unwrap(), 1);
    }

    #[tokio::test]
    async fn test_insert_with_schema_validation() {
        let (_, writer) = setup_writer().await;

        let table = TableSchema::new("orders")
            .with_column(ColumnDef::new("id", ColumnType::Integer))
            .with_column(ColumnDef::new("order_no", ColumnType::String));
        let mut schema = SchemaMapping::new();
        schema.add_table(table);

        let result = writer
            .insert_with_schema("orders", sample_row(1, "PO-1"), &schema)
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_write_async() {
        let (connection, writer) = setup_writer().await;
        let writer2 = DbWriter::new(connection.clone());

        let (result1, result2) = tokio::join!(
            writer.insert("orders", sample_row(1, "PO-1")),
            writer2.insert("orders", sample_row(2, "PO-2"))
        );

        result1.unwrap();
        result2.unwrap();
        assert_eq!(connection.table_row_count("orders").await.unwrap(), 2);
    }

    #[tokio::test]
    async fn test_upsert_requires_key_column() {
        let (_, writer) = setup_writer().await;

        let mut row = Row::new();
        row.insert("order_no".to_string(), DbValue::String("PO-1".to_string()));

        let err = writer.upsert("orders", "id", row).await.unwrap_err();
        assert!(matches!(err, Error::Query { .. }));
    }
}
