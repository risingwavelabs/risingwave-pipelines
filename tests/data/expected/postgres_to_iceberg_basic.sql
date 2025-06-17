
CREATE SOURCE postgres_ecommerce_source
WITH (
    connector = 'postgres-cdc',
    hostname = 'localhost',
    port = 5432,
    username = 'postgres',
    password = 'password',
    database.name = 'ecommerce',
    schema.name = 'public',
    publication.name = 'rw_publication',
    publication.create.enable = 'true'
);

CREATE TABLE orders (*)
FROM postgres_ecommerce_source
TABLE 'public.orders';

CREATE CONNECTION iceberg_connection
WITH (
    type = 'iceberg',
    warehouse.path = 's3://data-lake/',
    catalog.type = 'hive',
    catalog.uri = 'thrift://hive-metastore:9083'
);

CREATE SINK orders_sink
FROM orders
WITH (
    connector = 'iceberg',
    database.name = 'iceberg_db',
    table.name = 'orders',
    connection = iceberg_connection,
    primary_key = 'id',
    description = 'sync orders table to orders in iceberg'
);