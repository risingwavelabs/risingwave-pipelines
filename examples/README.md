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
# Create test data in PostgreSQL
psql -h localhost -p 8433 -U postgres -d postgres -c "
CREATE TABLE orders (id INT PRIMARY KEY, item TEXT, quantity INT);
CREATE TABLE customers (id INT PRIMARY KEY, name TEXT, email TEXT);
INSERT INTO orders VALUES (1, 'Book', 2), (2, 'Pen', 5);
INSERT INTO customers VALUES (1, 'John', 'john@example.com');
"

# Submit CDC job
python main.py run -f job.yaml --submit \
    --host localhost \
    --port 4566 \
    --database dev \
    --user root \
    --password root

# Verify data in Iceberg tables
```

