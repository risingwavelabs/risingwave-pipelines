
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
);

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
    s3.endpoint = 'http://minio-0:9301',
    s3.access.key = 'hummockadmin',
    s3.secret.key = 'hummockadmin',
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
    connection = iceberg_connection,
    create_table_if_not_exists = 'true',
    type = 'append-only',
    primary_key = 'id',
    force_append_only = 'true',
    description = 'sync orders table to orders in iceberg'
);

CREATE SINK customers_sink
FROM customers
WITH (
    connector = 'iceberg',
    database.name = 'iceberg_db',
    table.name = 'customers',
    connection = iceberg_connection,
    create_table_if_not_exists = 'true',
    type = 'upsert',
    primary_key = 'id',
    description = 'sync customers table to customers in iceberg'
);