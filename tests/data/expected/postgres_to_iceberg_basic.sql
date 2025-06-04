CREATE SOURCE postgres_ecommerce_source
WITH (
    connector = 'postgres-cdc',
    hostname = 'localhost',
    port = '5432',
    username = 'postgres',
    password = 'password',
    database.name = 'ecommerce',
    publication.name = 'rw_publication',
    publication.create.enable = 'True',
    schema.name = 'public'
)
FORMAT PLAIN ENCODE JSON;

CREATE TABLE orders (*) 
FROM postgres_ecommerce_source
TABLE 'public.orders';

CREATE CONNECTION iceberg_connection
WITH (
    type = 'iceberg',
    catalog.type = 'hive',
    warehouse.path = 's3://data-lake/',
    catalog.uri = 'thrift://hive-metastore:9083'
);

CREATE SINK orders_sink
FROM orders
WITH (
    connector = 'iceberg',
    connection = iceberg_connection,
    type = 'append-only',
    database.name = 'iceberg_db',
    table.name = 'orders',
    primary_key = 'id'
);