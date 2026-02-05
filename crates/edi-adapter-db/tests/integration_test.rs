use edi_adapter_db::{DbConnection, DbReader, DbValue, DbWriter, QueryOptions, Row};

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
async fn test_full_read_write_cycle() {
    let connection = DbConnection::new();
    connection.connect().await.unwrap();

    let writer = DbWriter::new(connection.clone());
    let reader = DbReader::new(connection.clone());

    writer.insert("orders", order_row(1, "PO-1")).await.unwrap();
    writer.insert("orders", order_row(2, "PO-2")).await.unwrap();

    let rows = reader.read_table("orders").await.unwrap();
    assert_eq!(rows.len(), 2);

    let document = reader
        .read_to_ir("orders", &QueryOptions::default())
        .await
        .unwrap();
    assert_eq!(document.root.children.len(), 2);
}

#[tokio::test]
async fn test_write_from_ir_and_read_back() {
    use edi_ir::{Document, Node, NodeType, Value};

    let connection = DbConnection::new();
    connection.connect().await.unwrap();

    let writer = DbWriter::new(connection.clone());
    let reader = DbReader::new(connection);

    let mut root = Node::new("ROOT", NodeType::Root);
    for i in 1..=3 {
        let mut record = Node::new("orders", NodeType::Record);
        record.add_child(Node::with_value("id", NodeType::Field, Value::Integer(i)));
        record.add_child(Node::with_value(
            "order_no",
            NodeType::Field,
            Value::String(format!("PO-{i}")),
        ));
        root.add_child(record);
    }

    let document = Document::new(root);
    let written = writer.write_from_ir("orders", &document).await.unwrap();
    assert_eq!(written, 3);

    let rows = reader.read_table("orders").await.unwrap();
    assert_eq!(rows.len(), 3);
}
