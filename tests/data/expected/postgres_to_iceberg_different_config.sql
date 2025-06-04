CREATE SOURCE postgres_mydb_source
WITH (
    connector = 'postgres-cdc',
    hostname = 'db.example.com',
    port = '5433',
    username = 'myuser',
    password = 'mypass',
    database.name = 'mydb',
    slot.name = 'rw_replication_slot',
    publication.name = 'rw_publication',
    publication.create.enable = 'True',
    schema.name = 'public'
)
FORMAT PLAIN ENCODE JSON;

CREATE TABLE products (*) 
FROM postgres_mydb_source
TABLE 'public.products';

CREATE TABLE customers (*) 
FROM postgres_mydb_source
TABLE 'public.customers';

CREATE CONNECTION iceberg_connection
WITH (
    type = 'iceberg',
    catalog.type = 'rest',
    warehouse.path = 's3://my-bucket/',
    catalog.uri = 'thrift://metastore:9083'
);

CREATE SINK products_sink
FROM products
WITH (
    connector = 'iceberg',
    connection = iceberg_connection,
    type = 'append-only',
    database.name = 'analytics',
    table.name = 'products',
    primary_key = 'id'
);

CREATE SINK customers_sink
FROM customers
WITH (
    connector = 'iceberg',
    connection = iceberg_connection,
    type = 'append-only',
    database.name = 'analytics',
    table.name = 'customers',
    primary_key = 'id'
);