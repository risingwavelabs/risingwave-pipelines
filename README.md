# RisingWave CDC

A CLI tool for managing RisingWave CDC (Change Data Capture) jobs. This tool helps you easily set up and manage data synchronization between PostgreSQL and Iceberg tables using RisingWave.

## Features

- YAML-based job configuration
- Support for PostgreSQL to Iceberg data synchronization
- Automatic SQL generation for RisingWave
- Optional direct SQL submission to RisingWave
- Configuration validation
- Modular and extensible design

## Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/risingwave-cdc.git
cd risingwave-cdc
```

2. Install dependencies:
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
  --password root
```

### Command Line Arguments

- `-f, --file`: Path to the job YAML file (required)
- `--submit`: Submit SQL to RisingWave (optional)
- `--host`: RisingWave host (required if --submit is used)
- `--port`: RisingWave port (required if --submit is used)
- `--database`: RisingWave database name (required if --submit is used)
- `--user`: RisingWave username (required if --submit is used)
- `--password`: RisingWave password (required if --submit is used)

## Job Configuration

The job configuration is specified in YAML format. Here's an example:

```yaml
job:
  source:
    type: postgres
    hostname: pg.local
    port: 5432
    username: user
    password: pass
    database: mydb
    table: public.orders
  sink:
    type: iceberg
    catalog:
      uri: thrift://hive-metastore:9083
      warehouse: s3://my-lakehouse/
    table: analytics.orders
```

### Configuration Fields

#### Source Configuration
- `type`: Source type (currently only 'postgres' is supported)
- `hostname`: PostgreSQL hostname
- `port`: PostgreSQL port
- `username`: PostgreSQL username
- `password`: PostgreSQL password
- `database`: PostgreSQL database name
- `table`: PostgreSQL table name (format: schema.table)

#### Sink Configuration
- `type`: Sink type (currently only 'iceberg' is supported)
- `catalog`: Iceberg catalog configuration
  - `uri`: Hive metastore URI
  - `warehouse`: S3 warehouse path
- `table`: Target Iceberg table name (format: database.table)

## Development

### Project Structure

```
risingwave-cdc/
├── main.py                 # CLI entry point
├── sql_templates.py        # Jinja2 templates for SQL generation
├── parser.py              # YAML parsing and validation
├── generator.py           # SQL generation logic
├── examples/
│   └── job.yaml          # Example job configuration
└── README.md
```

### Adding New Features

1. **New Source Types**:
   - Add new connection template in `sql_templates.py`
   - Update validation in `parser.py`
   - Add source-specific logic in `generator.py`

2. **New Sink Types**:
   - Add new sink template in `sql_templates.py`
   - Update validation in `parser.py`
   - Add sink-specific logic in `generator.py`

### Running Tests

```bash
# TODO: Add test instructions when tests are implemented
```

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- [RisingWave](https://www.risingwave.dev/) for the streaming database
- [PyYAML](https://pyyaml.org/) for YAML parsing
- [Jinja2](https://jinja.palletsprojects.com/) for template rendering
- [psycopg2](https://www.psycopg.org/) for PostgreSQL connectivity 