
CREATE SOURCE postgres_postgres_source
WITH (
    connector = 'postgres-cdc',
    hostname = 'postgres-source',
    port = 5432,
    username = 'postgres',
    password = 123456,
    database.name = 'postgres',
    schema.name = 'public',
    publication.name = 'rw_publication',
    publication.create.enable = 'true'
)

CREATE TABLE orders (*)
FROM postgres_postgres_source
TABLE 'public.orders';

CREATE TABLE customers (*)
FROM postgres_postgres_source
TABLE 'public.customers';

CREATE CONNECTION iceberg_connection
WITH (
    type = 'iceberg',
    warehouse.path = 's3a://hummock001/iceberg-data',
    s3.endpoint = 'minio-0:9301',
    s3.access_key = 'hummockadmin',
    s3.secret_key = 'hummockadmin',
    s3.region = 'us-east-1',
    catalog.name = 'demo',
    catalog.type = 'storage'
);

CREATE SINK orders_sink
FROM orders
WITH (
    connector = 'iceberg',
    database.name = 'iceberg_db',
    table.name = 'orders',
    type = 'append-only',
    primary_key = 'id',
    description = 'sync orders table to orders in iceberg',
    create_table_if_not_exists = 'true',
    connection_name = 'iceberg_connection'
);

CREATE SINK customers_sink
FROM customers
WITH (
    connector = 'iceberg',
    database.name = 'iceberg_db',
    table.name = 'customers',
    type = 'upsert',
    primary_key = 'id',
    description = 'sync customers table to customers in iceberg',
    create_table_if_not_exists = 'true',
    connection_name = 'iceberg_connection'
);