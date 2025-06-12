
CREATE SOURCE postgres_mydb_source
WITH (
    connector = 'postgres-cdc',
    hostname = 'db.example.com',
    port = 5433,
    username = 'myuser',
    password = 'mypass',
    database.name = 'mydb',
    schema.name = 'public',
    slot.name = 'rw_replication_slot',
    publication.name = 'rw_publication',
    publication.create.enable = 'true'
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
    warehouse.path = 's3://my-bucket/',
    catalog.type = 'rest',
    catalog.uri = 'thrift://metastore:9083'
);

CREATE SINK products_sink
FROM products
WITH (
    connector = 'iceberg',
    database.name = 'analytics',
    table.name = 'products',
    primary_key = 'id',
    description = 'sync products table to analytics schema',
    create_table_if_not_exists = 'true',
    connection_name = 'iceberg_connection'
);

CREATE SINK customers_sink
FROM customers
WITH (
    connector = 'iceberg',
    database.name = 'analytics',
    table.name = 'customers',
    primary_key = 'id',
    description = 'sync customers table to analytics schema',
    create_table_if_not_exists = 'false',
    connection_name = 'iceberg_connection'
);