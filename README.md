# RisingWave Pipelines

A tool for managing RisingWave Pipelines (Change Data Capture) jobs. This tool helps you easily set up and manage data synchronization between PostgreSQL and Iceberg tables using RisingWave.

## Features

- YAML-based job configuration with validation
- PostgreSQL to Iceberg data synchronization
- Automatic SQL generation for RisingWave

## Installation

1. Clone the repository:
```bash
git clone https://github.com/risingwavelabs/risingwave-pipelines.git
cd risingwave-pipelines
```

2. Create and activate a virtual environment (recommended):
```bash
python -m venv venv
source venv/bin/activate  # On Windows use: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

## Usage

The tool supports two modes of operation:

1. Generate SQL without submitting:
```bash
python main.py run -f examples/job.yaml
```

2. Generate and submit SQL to RisingWave:
```bash
python main.py run -f examples/job.yaml --submit \
  --host localhost \
  --port 4566 \
  --database dev \
  --user root \
  --password password123  # Use a secure password in production
```

## Job Configuration

The job configuration is specified in YAML format. Here's a complete example:

```yaml
source:
  connector: postgres
  hostname: postgres-source    # PostgreSQL host
  port: 5432                  # Default PostgreSQL port
  username: postgres          # PostgreSQL username
  password: 123456           # Use secure password in production
  database:
    name: postgres           # Source database name
  schema:
    name: public            # Source schema name
  publication:
    name: rw_publication    # Publication name for CDC
    create:
      enable: true         # Auto-create publication if not exists

sink:
  connector: iceberg
  type: upsert             # Sink operation type
  warehouse:
    path: 's3a://hummock001/iceberg-data'  # S3 path for Iceberg tables
  s3:
    endpoint: http://minio-0:9301
    access:
      key: hummockadmin
    secret:
      key: hummockadmin
    region: us-east-1
  catalog:
    name: demo
    type: storage
  

route:
  - source_table: public.orders
    sink_table: iceberg_db.orders
    primary_key: id
    create_table_if_not_exists: 'true'
    description: sync orders table to orders in iceberg
  - source_table: public.customers
    sink_table: iceberg_db.customers
    primary_key: id
    create_table_if_not_exists: 'false'
    description: sync customers table to customers in iceberg
```

### Configuration Fields

The sections below describe the configuration options. This tool requires the following fields to be present in the YAML file:
- `source`
  - `connector`
- `sink`
  - `connector`
- `route`
  - `source_table`
  - `sink_table`

For connector-specific parameters, including which ones are required by RisingWave, please refer to the official RisingWave documentation linked in each section.

#### Source Configuration
- `connector`: Source connector type (currently supports: `postgres`)
- `hostname`: PostgreSQL hostname
- `port`: PostgreSQL port (default: 5432)
- `username`: PostgreSQL username
- `password`: PostgreSQL password (use environment variables in production)
- `database.name`: PostgreSQL database name
- `schema.name`: PostgreSQL schema name
- `publication`: PostgreSQL publication configuration
  - `name`: Publication name
  - `create.enable`: Whether to auto-create publication

For a comprehensive list of all supported parameters for a PostgreSQL source, please refer to the [official RisingWave documentation](https://docs.risingwave.com/ingestion/sources/postgresql/pg-cdc#connector-parameters).

#### Sink Configuration
- `connector`: Sink connector type (currently supports: `iceberg`)
- `type`: Sink operation type (e.g., 'upsert')
- `warehouse`: Target warehouse configuration
  - `path`: S3 path for Iceberg tables
- `s3`: S3 configuration
  - `endpoint`: S3 endpoint
  - `access`:
    - `key`: S3 access key
  - `secret`:
    - `key`: S3 secret key
  - `region`: S3 region
- `catalog`: Iceberg catalog configuration
  - `name`: Catalog name
  - `type`: Catalog type

For a comprehensive list of all supported parameters for an Iceberg sink, please refer to the [official RisingWave documentation](https://docs.risingwave.com/sql/commands/sql-create-connection#click-to-see-all-supported-properties-for-iceberg-connection).

#### Route Configuration
List of source to sink table mappings.
- `source_table`: Source table name (format: schema.table)
- `sink_table`: Target table name in the sink system
- `primary_key`: Primary key column name in Iceberg table
- `description`: Optional description of the sync

In addition to the fields above, you can specify other sink-related parameters for each route. These parameters will apply to a specific table sink. For the available parameters for an Iceberg sink, please refer to the [Basic Parameters section of the RisingWave documentation](https://docs.risingwave.com/integrations/destinations/apache-iceberg#basic-parameters). You can use any of those parameters here, except for those already defined in the main `sink` configuration section.

## Examples

Check out the [examples](examples) directory for a complete PostgreSQL to Iceberg CDC pipeline example.

## Development

### Running Tests

```bash
# Run all tests
python -m unittest discover tests/

# Run data-driven tests
python -m unittest tests/test_data_driven.py
```

### Adding New Connector Types

To add support for a new connector type:

1. Create a new connector class in the `connectors` directory:
```python
from connectors.common import BaseConnector

class NewConnector(BaseConnector):
    def get_connector_type(self) -> str:
        return 'new-connector-type'
    
    def create_source(self, config):
        # Implementation
        pass
```

2. Register it in `generator.py`:
```python
CONNECTOR_REGISTRY = {
    'postgres': PostgreSQLConnector,
    'iceberg': IcebergConnector,
    'new-connector': NewConnector,  # Add new connector
}
```

3. Add appropriate templates and validation logic.

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

Please ensure:
- All tests pass
- New features include tests
- Documentation is updated
- Code follows the project's style guide

## License

This project is licensed under the Apache License 2.0 - see the LICENSE file for details.
