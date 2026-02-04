use edi_adapter_db::{DbConnection, DbValue, DbWriter, Row};

fn order_row(id: i64, order_no: &str) -> Row {
    let mut row = Row::new();
    row.insert("id".to_string(), DbValue::Integer(id));
    row.insert(
        "order_no".to_string(),
        DbValue::String(order_no.to_string()),
    );
    row
}

#[tokio::test]
async fn test_transaction_commit_persists_changes() {
    let connection = DbConnection::new();
    connection.connect().await.unwrap();

    let mut tx = connection.begin_transaction().await.unwrap();
    tx.insert_row("orders", order_row(1, "PO-1")).unwrap();
    tx.insert_row("orders", order_row(2, "PO-2")).unwrap();
    tx.commit().await.unwrap();

    assert_eq!(connection.table_row_count("orders").await.unwrap(), 2);
}

#[tokio::test]
async fn test_transaction_rollback_discards_changes() {
    let connection = DbConnection::new();
    connection.connect().await.unwrap();

    let mut tx = connection.begin_transaction().await.unwrap();
    tx.insert_row("orders", order_row(1, "PO-1")).unwrap();
    tx.rollback().await.unwrap();

    assert_eq!(connection.table_row_count("orders").await.unwrap(), 0);
}

#[tokio::test]
async fn test_writer_transactional_batch() {
    let connection = DbConnection::new();
    connection.connect().await.unwrap();

    let writer = DbWriter::new(connection.clone());
    let rows = vec![order_row(1, "PO-1"), order_row(2, "PO-2")];

    let written = writer
        .write_with_transaction("orders", &rows)
        .await
        .unwrap();
    assert_eq!(written, 2);
    assert_eq!(connection.table_row_count("orders").await.unwrap(), 2);
}
