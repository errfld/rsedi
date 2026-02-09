use edi_adapter_db::{
    ColumnDef, ColumnType, DbConnection, DbReader, DbValue, DbWriter, QueryOptions, Row,
    SchemaMapping, TableSchema, WriteMode, WriteOptions,
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
async fn test_full_read_write_cycle() {
    let connection = DbConnection::new();
    connection.connect().await.unwrap();
    connection.apply_schema(&sample_schema()).await.unwrap();

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
    connection.apply_schema(&sample_schema()).await.unwrap();

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

#[tokio::test]
async fn test_read_with_schema_typed_values() {
    let connection = DbConnection::new();
    connection.connect().await.unwrap();

    let table = TableSchema::new("flags")
        .with_column(ColumnDef::new("id", ColumnType::Integer).primary_key())
        .with_column(ColumnDef::new("is_priority", ColumnType::Boolean))
        .with_column(ColumnDef::new("total", ColumnType::Decimal));
    let mut schema = SchemaMapping::new();
    schema.add_table(table);

    connection.apply_schema(&schema).await.unwrap();

    let writer = DbWriter::new(connection.clone());
    let reader = DbReader::new(connection);

    let mut row = Row::new();
    row.insert("id".to_string(), DbValue::Integer(1));
    row.insert("is_priority".to_string(), DbValue::Boolean(true));
    row.insert("total".to_string(), DbValue::Decimal(12.5));
    writer.insert("flags", row).await.unwrap();

    let rows = reader
        .read_with_schema("flags", &QueryOptions::default(), &schema)
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("is_priority"), Some(&DbValue::Boolean(true)));
    assert_eq!(rows[0].get("total"), Some(&DbValue::Decimal(12.5)));
}

#[tokio::test]
async fn test_read_to_ir_with_schema() {
    use edi_ir::Value;

    let connection = DbConnection::new();
    connection.connect().await.unwrap();
    connection.apply_schema(&sample_schema()).await.unwrap();

    let writer = DbWriter::new(connection.clone());
    let reader = DbReader::new(connection);

    writer.insert("orders", order_row(1, "PO-1")).await.unwrap();

    let document = reader
        .read_to_ir_with_schema("orders", &QueryOptions::default(), &sample_schema())
        .await
        .unwrap();
    assert_eq!(document.root.children.len(), 1);
    assert!(matches!(
        document.root.children[0].children[1].value,
        Some(Value::String(_))
    ));
}

#[tokio::test]
async fn test_write_from_ir_with_upsert_mode() {
    use edi_ir::{Document, Node, NodeType, Value};

    let connection = DbConnection::new();
    connection.connect().await.unwrap();
    connection.apply_schema(&sample_schema()).await.unwrap();

    let writer = DbWriter::new(connection.clone());
    let reader = DbReader::new(connection);

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
        Value::String("PO-1-UPDATED".to_string()),
    ));
    root.add_child(second);

    let options = WriteOptions::default()
        .with_mode(WriteMode::Upsert {
            key_column: "id".to_string(),
        })
        .with_batch_size(1);
    let affected = writer
        .write_from_ir_with_options("orders", &Document::new(root), &options)
        .await
        .unwrap();
    assert_eq!(affected, 2);

    let rows = reader.read_table("orders").await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get("order_no"),
        Some(&DbValue::String("PO-1-UPDATED".to_string()))
    );
}
