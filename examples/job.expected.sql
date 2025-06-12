
CREATE SOURCE postgres_postgres_source
WITH (
    connector = 'postgres-cdc',
    hostname = 'postgres-source',
    port = '5432',
    username = 'postgres',
    password = '123456',
    database.name = 'postgres',
    publication.name = 'rw_publication',
    publication.create.enable = 'True',
    schema.name = 'public'
)
FORMAT PLAIN ENCODE JSON;

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
    connection = iceberg_connection,
    type = 'upsert',
    database.name = 'iceberg_db',
    table.name = 'orders',
    primary_key = 'id',
    create_table_if_not_exists = 'true'
);

CREATE SINK customers_sink
FROM customers
WITH (
    connector = 'iceberg',
    connection = iceberg_connection,
    type = 'upsert',
    database.name = 'iceberg_db',
    table.name = 'customers',
    primary_key = 'id',
    create_table_if_not_exists = 'true'
);