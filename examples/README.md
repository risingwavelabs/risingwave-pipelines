# RisingWave Pipelines Example

Example setup for PostgreSQL to Apache Iceberg CDC pipeline using RisingWave.

## Files
- `docker-compose.yml` - Docker Compose file to run the entire pipeline.
- `job.yaml` - CDC job configuration (PostgreSQL → Iceberg)
- `job.expected.sql` - Setup verification SQL
- `risingwave.toml` - RisingWave config

## Quick Start

1. Start services:
```bash
docker-compose up -d
```

2. Run the example:
```bash
# connect to Postgres source database
psql -h localhost -p 8433 -U postgres -d postgres
```

```sql
-- Create test data in PostgreSQL
CREATE TABLE orders (id INT PRIMARY KEY, item TEXT, quantity INT);
CREATE TABLE customers (id INT PRIMARY KEY, name TEXT, email TEXT);
INSERT INTO orders VALUES (1, 'Book', 2), (2, 'Pen', 5);
INSERT INTO customers VALUES (1, 'John', 'john@example.com');
```

```bash
# Submit CDC job
python ../main.py run -f job.yaml --submit \
    --host localhost \
    --port 4566 \
    --database dev \
    --user root \
    --password root
```

```bash
# connect to RisingWave
psql -h localhost -p 4566 -d dev -U root
```

```sql
-- Verify data in RisingWave tables
CREATE SOURCE iceberg_orders_source(*) WITH (
    connector = 'iceberg',
    connection = iceberg_connection,
    database.name = 'iceberg_db',
    table.name = 'orders'
);

CREATE SOURCE iceberg_customers_source(*) WITH (
    connector = 'iceberg',
    connection = iceberg_connection,
    database.name = 'iceberg_db',
    table.name = 'customers'
);

SELECT * FROM iceberg_orders_source;
SELECT * FROM iceberg_customers_source;
```

