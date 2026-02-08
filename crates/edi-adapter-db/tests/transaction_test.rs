use edi_adapter_db::{
    ColumnDef, ColumnType, DbConnection, DbValue, DbWriter, Row, SchemaMapping, TableSchema,
};

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

#[tokio::test]
async fn test_transaction_commit_persists_changes() {
    let connection = DbConnection::new();
    connection.connect().await.unwrap();
    connection.apply_schema(&sample_schema()).await.unwrap();

    let mut tx = connection.begin_transaction().await.unwrap();
    tx.insert_row("orders", order_row(1, "PO-1")).await.unwrap();
    tx.insert_row("orders", order_row(2, "PO-2")).await.unwrap();
    tx.commit().await.unwrap();

    assert_eq!(connection.table_row_count("orders").await.unwrap(), 2);
}

#[tokio::test]
async fn test_transaction_rollback_discards_changes() {
    let connection = DbConnection::new();
    connection.connect().await.unwrap();
    connection.apply_schema(&sample_schema()).await.unwrap();

    let mut tx = connection.begin_transaction().await.unwrap();
    tx.insert_row("orders", order_row(1, "PO-1")).await.unwrap();
    tx.rollback().await.unwrap();

    assert_eq!(connection.table_row_count("orders").await.unwrap(), 0);
}

#[tokio::test]
async fn test_writer_transactional_batch() {
    let connection = DbConnection::new();
    connection.connect().await.unwrap();
    connection.apply_schema(&sample_schema()).await.unwrap();

    let writer = DbWriter::new(connection.clone());
    let rows = vec![order_row(1, "PO-1"), order_row(2, "PO-2")];

    let written = writer
        .write_with_transaction("orders", &rows)
        .await
        .unwrap();
    assert_eq!(written, 2);
    assert_eq!(connection.table_row_count("orders").await.unwrap(), 2);
}
