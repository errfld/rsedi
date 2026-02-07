use edi_adapter_db::{
    ColumnDef, ColumnType, DbConnection, DbReader, DbValue, DbWriter, QueryOptions, Row,
    SchemaMapping, TableSchema,
};

fn invalid_row() -> Row {
    let mut row = Row::new();
    row.insert("id".to_string(), DbValue::String("wrong-type".to_string()));
    row
}

#[tokio::test]
async fn test_write_without_connect_fails() {
    let connection = DbConnection::new();
    let writer = DbWriter::new(connection);

    let mut row = Row::new();
    row.insert("id".to_string(), DbValue::Integer(1));

    let err = writer.insert("orders", row).await.unwrap_err();
    assert!(err.to_string().contains("not connected"));
}

#[tokio::test]
async fn test_read_missing_table_fails() {
    let connection = DbConnection::new();
    connection.connect().await.unwrap();

    let reader = DbReader::new(connection);
    let err = reader
        .read_with_options("missing", &QueryOptions::default())
        .await
        .unwrap_err();

    assert!(matches!(err, edi_adapter_db::Error::Sql { .. }));
}

#[tokio::test]
async fn test_schema_validation_error_is_reported() {
    let connection = DbConnection::new();
    connection.connect().await.unwrap();

    let writer = DbWriter::new(connection);

    let table = TableSchema::new("orders").with_column(ColumnDef::new("id", ColumnType::Integer));
    let mut mapping = SchemaMapping::new();
    mapping.add_table(table);

    let err = writer
        .insert_with_schema("orders", invalid_row(), &mapping)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("Type mismatch"));
}
