//! Database write operations.

use std::fmt;

use edi_ir::{Document, Node, Value};

use crate::Error;
use crate::Result;
use crate::connection::{DbConnection, DbTransaction};
use crate::schema::{DbValue, Row, SchemaMapping};

/// IR write strategy when persisting records to a table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WriteMode {
    Insert,
    Update { filter_columns: Vec<String> },
    Upsert { key_column: String },
}

/// Batch and transaction behavior for IR write operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteOptions {
    pub mode: WriteMode,
    pub batch_size: usize,
    pub transactional: bool,
}

impl Default for WriteOptions {
    fn default() -> Self {
        Self {
            mode: WriteMode::Insert,
            batch_size: 500,
            transactional: true,
        }
    }
}

impl WriteOptions {
    pub fn with_mode(mut self, mode: WriteMode) -> Self {
        self.mode = mode;
        self
    }

    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    pub fn transactional(mut self, enabled: bool) -> Self {
        self.transactional = enabled;
        self
    }
}

/// Writer facade.
#[derive(Clone)]
pub struct DbWriter {
    connection: DbConnection,
}

impl fmt::Debug for DbWriter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DbWriter").finish_non_exhaustive()
    }
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
        self.write_from_ir_with_options(table, document, &WriteOptions::default())
            .await
    }

    pub async fn write_from_ir_with_options(
        &self,
        table: &str,
        document: &Document,
        options: &WriteOptions,
    ) -> Result<usize> {
        let rows = collect_rows(&document.root);
        self.write_rows_with_options(table, &rows, options).await
    }

    pub async fn write_from_ir_with_schema(
        &self,
        table: &str,
        document: &Document,
        schema_mapping: &SchemaMapping,
        options: &WriteOptions,
    ) -> Result<usize> {
        let rows = collect_rows(&document.root);
        for row in &rows {
            schema_mapping.validate_row(table, row)?;
        }
        self.write_rows_with_options(table, &rows, options).await
    }

    async fn write_rows_with_options(
        &self,
        table: &str,
        rows: &[Row],
        options: &WriteOptions,
    ) -> Result<usize> {
        if options.batch_size == 0 {
            return Err(Error::Query {
                table: table.to_string(),
                details: "batch_size must be greater than zero".to_string(),
            });
        }

        let mut affected = 0usize;
        for chunk in rows.chunks(options.batch_size) {
            if options.transactional {
                let mut tx = self.connection.begin_transaction().await?;
                for row in chunk {
                    affected +=
                        apply_mode_with_transaction(&mut tx, table, row, &options.mode).await?;
                }
                tx.commit().await?;
            } else {
                for row in chunk {
                    affected +=
                        apply_mode_with_connection(&self.connection, table, row, &options.mode)
                            .await?;
                }
            }
        }

        Ok(affected)
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

fn collect_rows(root: &Node) -> Vec<Row> {
    collect_records(root)
        .into_iter()
        .map(|record| {
            let mut row = Row::new();
            for field in &record.children {
                row.insert(
                    field.name.clone(),
                    ir_value_to_db(field.value.clone().unwrap_or(Value::Null)),
                );
            }
            row
        })
        .collect()
}

fn build_filter_row(table: &str, row: &Row, filter_columns: &[String]) -> Result<Row> {
    if filter_columns.is_empty() {
        return Err(Error::Query {
            table: table.to_string(),
            details: "Update mode requires at least one filter column".to_string(),
        });
    }

    let mut filter = Row::new();
    for column in filter_columns {
        let value = row.get(column).cloned().ok_or_else(|| Error::Query {
            table: table.to_string(),
            details: format!("Update filter column '{column}' is missing from row"),
        })?;
        filter.insert(column.clone(), value);
    }

    Ok(filter)
}

async fn apply_mode_with_connection(
    connection: &DbConnection,
    table: &str,
    row: &Row,
    mode: &WriteMode,
) -> Result<usize> {
    match mode {
        WriteMode::Insert => {
            connection.insert_row(table, row.clone()).await?;
            Ok(1)
        }
        WriteMode::Update { filter_columns } => {
            let filter = build_filter_row(table, row, filter_columns)?;
            connection.update_rows(table, &filter, row).await
        }
        WriteMode::Upsert { key_column } => {
            connection
                .upsert_row(table, key_column, row.clone())
                .await?;
            Ok(1)
        }
    }
}

async fn apply_mode_with_transaction(
    tx: &mut DbTransaction,
    table: &str,
    row: &Row,
    mode: &WriteMode,
) -> Result<usize> {
    match mode {
        WriteMode::Insert => {
            tx.insert_row(table, row.clone()).await?;
            Ok(1)
        }
        WriteMode::Update { filter_columns } => {
            let filter = build_filter_row(table, row, filter_columns)?;
            tx.update_rows(table, &filter, row).await
        }
        WriteMode::Upsert { key_column } => {
            tx.upsert_row(table, key_column, row.clone()).await?;
            Ok(1)
        }
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
        Value::Binary(value) => DbValue::Blob(value),
        Value::Null => DbValue::Null,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
    async fn test_write_from_ir_update_mode() {
        let (connection, writer) = setup_writer().await;
        writer
            .insert("orders", sample_row(1, "PO-1"))
            .await
            .unwrap();

        let mut root = Node::new("ROOT", NodeType::Root);
        let mut record = Node::new("orders", NodeType::Record);
        record.add_child(Node::with_value("id", NodeType::Field, Value::Integer(1)));
        record.add_child(Node::with_value(
            "order_no",
            NodeType::Field,
            Value::String("PO-1-UPDATED".to_string()),
        ));
        root.add_child(record);
        let document = Document::new(root);

        let options = WriteOptions::default().with_mode(WriteMode::Update {
            filter_columns: vec!["id".to_string()],
        });
        let updated = writer
            .write_from_ir_with_options("orders", &document, &options)
            .await
            .unwrap();

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
    async fn test_write_from_ir_upsert_mode() {
        let (connection, writer) = setup_writer().await;

        let mut root = Node::new("ROOT", NodeType::Root);
        let mut first = Node::new("orders", NodeType::Record);
        first.add_child(Node::with_value("id", NodeType::Field, Value::Integer(1)));
        first.add_child(Node::with_value(
            "order_no",
            NodeType::Field,
            Value::String("PO-1".to_string()),
        ));
        root.add_child(first);

        let mut second = Node::new("orders", NodeType::Record);
        second.add_child(Node::with_value("id", NodeType::Field, Value::Integer(1)));
        second.add_child(Node::with_value(
            "order_no",
            NodeType::Field,
            Value::String("PO-1-NEW".to_string()),
        ));
        root.add_child(second);

        let options = WriteOptions::default().with_mode(WriteMode::Upsert {
            key_column: "id".to_string(),
        });
        let written = writer
            .write_from_ir_with_options("orders", &Document::new(root), &options)
            .await
            .unwrap();
        assert_eq!(written, 2);

        let rows = connection
            .select_rows("orders", None, 0, None)
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].get("order_no"),
            Some(&DbValue::String("PO-1-NEW".to_string()))
        );
    }

    #[tokio::test]
    async fn test_write_from_ir_with_batching() {
        let (connection, writer) = setup_writer().await;
        let mut root = Node::new("ROOT", NodeType::Root);
        for idx in 1..=3 {
            let mut record = Node::new("orders", NodeType::Record);
            record.add_child(Node::with_value("id", NodeType::Field, Value::Integer(idx)));
            record.add_child(Node::with_value(
                "order_no",
                NodeType::Field,
                Value::String(format!("PO-{idx}")),
            ));
            root.add_child(record);
        }

        let options = WriteOptions::default()
            .with_batch_size(2)
            .transactional(true);
        let written = writer
            .write_from_ir_with_options("orders", &Document::new(root), &options)
            .await
            .unwrap();
        assert_eq!(written, 3);
        assert_eq!(connection.table_row_count("orders").await.unwrap(), 3);
    }

    #[tokio::test]
    async fn test_write_from_ir_update_mode_requires_filter_column() {
        let (_, writer) = setup_writer().await;
        let mut root = Node::new("ROOT", NodeType::Root);
        let mut record = Node::new("orders", NodeType::Record);
        record.add_child(Node::with_value(
            "order_no",
            NodeType::Field,
            Value::String("PO-1".to_string()),
        ));
        root.add_child(record);

        let options = WriteOptions::default().with_mode(WriteMode::Update {
            filter_columns: vec!["id".to_string()],
        });
        let err = writer
            .write_from_ir_with_options("orders", &Document::new(root), &options)
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Query { .. }));
    }

    #[tokio::test]
    async fn test_write_from_ir_rejects_zero_batch_size() {
        let (_, writer) = setup_writer().await;
        let mut root = Node::new("ROOT", NodeType::Root);
        let mut record = Node::new("orders", NodeType::Record);
        record.add_child(Node::with_value("id", NodeType::Field, Value::Integer(1)));
        record.add_child(Node::with_value(
            "order_no",
            NodeType::Field,
            Value::String("PO-1".to_string()),
        ));
        root.add_child(record);
        let document = Document::new(root);

        let options = WriteOptions::default().with_batch_size(0);
        let err = writer
            .write_from_ir_with_options("orders", &document, &options)
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Query { .. }));
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
