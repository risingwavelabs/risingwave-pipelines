# Testing Guide for RisingWave Pipelines

This document provides comprehensive information about testing the RisingWave Pipelines project.

## 🧪 Test Suite Overview

The project includes a comprehensive test suite that covers:

- **Core Generator Functionality**: Tests for SQL generation, connector registry, and configuration validation
- **Data-Driven Tests**: Tests using YAML configurations and expected outputs
- **Error Handling**: Tests for various error conditions and edge cases
- **SQL Structure**: Tests for proper SQL statement ordering and content

## 📁 Test Files

```
tests/
├── test_data_driven.py     # Data-driven tests using YAML configs
├── data/                   # Test data directory
│   ├── configs/            # YAML test configurations
│   └── expected/          # Expected SQL/error outputs
├── run_tests.py           # Python test runner
└── TEST_README.md         # This file
```

## 🚀 Quick Start

### Option 1: Using the Python Test Runner (Recommended)

```bash
# The Python runner works on all platforms
python tests/run_tests.py
```

### Option 2: Manual Testing

```bash
# Activate virtual environment first
source venv/bin/activate  # or venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Run all tests
python -m unittest discover tests/

# Run data-driven tests
python -m unittest tests/test_data_driven.py
```

## 🔧 Virtual Environment Setup

The tests require a virtual environment. If you don't have one set up:

```bash
# Create virtual environment
python -m venv venv

# Activate it
source venv/bin/activate  # Unix/macOS
# or
venv\Scripts\activate     # Windows

# Install dependencies
pip install -r requirements.txt
```

## 📊 Test Coverage

### TestDataDriven
- ✅ Tests various configuration scenarios using YAML files
- ✅ Validates error cases with expected error messages
- ✅ Tests different configuration variations
- ✅ Ensures configuration validation
- ✅ Tests route configuration validation
- ✅ Tests SQL generation with different connectors

## 🎯 Running Specific Tests

### Run all tests:
```bash
python -m unittest discover tests/
```

### Run data-driven tests:
```bash
python -m unittest tests/test_data_driven.py
```

### Run with verbose output:
```bash
python -m unittest tests/test_data_driven.py -v
```

### Run specific test method:
```bash
python -m unittest tests.test_data_driven.TestDataDriven.test_basic_config
```

## 🐛 Debugging Failed Tests

### Common Issues and Solutions

1. **Import Errors**
   ```
   ModuleNotFoundError: No module named 'generator'
   ```
   - **Solution**: Ensure you're running from the project root directory
   - **Solution**: Check that virtual environment is activated

2. **Missing Dependencies**
   ```
   ModuleNotFoundError: No module named 'yaml'
   ```
   - **Solution**: Install requirements: `pip install -r requirements.txt`

3. **Virtual Environment Not Found**
   ```
   ❌ No virtual environment found!
   ```
   - **Solution**: Create venv: `python -m venv venv`

4. **Test Data Not Found**
   ```
   FileNotFoundError: [Errno 2] No such file or directory: 'tests/data/configs'
   ```
   - **Solution**: Check that test data directories exist:
     ```bash
     mkdir -p tests/data/{configs,expected}
     ```

## ⚙️ Test Runner (run_tests.py)

The `run_tests.py` script is the recommended way to run tests. It automatically handles:

- Virtual environment activation
- Dependency installation
- Running all test suites
- Graceful error handling

## ➕ Adding New Tests

### Adding Data-Driven Tests

1. Add configuration file in `tests/data/configs/`:
```yaml
name: "New Test Case"
description: "Test new functionality"

source:
  connector: postgres
  hostname: postgres-source
  port: 5432
  username: postgres
  password: 123456
  database:
    name: postgres
  schema:
    name: public
  publication:
    name: rw_publication
    create:
      enable: true

sink:
  connector: iceberg
  type: upsert
  warehouse:
    path: 's3a://hummock001/iceberg-data'
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
  create_table_if_not_exists: 'true'

route:
  - source_table: public.test
    sink_table: iceberg_db.test
    primary_key: id
    description: test sync configuration
```

2. Add expected output in `tests/data/expected/`:
```sql
-- Expected SQL output for successful case
CREATE SOURCE test_source
FROM POSTGRES CONNECTOR
OPTIONS (
  hostname = 'postgres-source',
  port = '5432',
  username = 'postgres',
  password = '123456',
  database = 'postgres',
  schema = 'public',
  publication = 'rw_publication'
);

CREATE SINK iceberg_db_test
FROM test_source
INTO ICEBERG
OPTIONS (
  type = 'upsert',
  warehouse = 's3a://hummock001/iceberg-data',
  s3.endpoint = 'http://minio-0:9301',
  s3.access.key = 'hummockadmin',
  s3.secret.key = 'hummockadmin',
  s3.region = 'us-east-1',
  catalog.name = 'demo',
  catalog.type = 'storage',
  create_table_if_not_exists = 'true'
);
```

### Test Naming Conventions

- Test files: `test_*.py`
- Test classes: `Test*` (e.g., `TestDataDriven`)
- Test methods: `test_*` (e.g., `test_basic_config`)
- Data files: `{test_case_name}.{yaml|sql|error}`

## 📈 Continuous Integration

The test suite is designed to be CI-friendly:

```yaml
# Example GitHub Actions workflow
name: Tests
on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      
      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.9'

      - name: Create virtual environment
        run: python -m venv venv

      - name: Install dependencies
        run: |
          source venv/bin/activate
          pip install -r requirements.txt

      - name: Run tests
        run: |
          source venv/bin/activate
          python tests/run_tests.py

      - name: Upload test results
        if: always()
        uses: actions/upload-artifact@v4
        with:
          name: test-results
          path: test-results/
```

This ensures that all tests are automatically run on every push and pull request, maintaining code quality and stability.

## 🔍 Test Output Examples

### Successful Test Run
```
🚀 RisingWave Pipelines Test Runner (Python)
=====================================
📦 Found virtual environment: venv
✅ Using Python: venv/bin/python
📋 Installing/updating dependencies...
🧪 Running tests...
=================
📝 Running data-driven tests...
test_basic_config ... ok
test_missing_source_type ... ok
test_invalid_connector ... ok
...
✅ All tests completed successfully!
```

### Failed Test Example
```
❌ Failed test: test_basic_config
AssertionError: Expected SQL not found in generated output
Expected: 'CREATE SOURCE test_source'
Generated SQL:
CREATE SOURCE test_source
FROM POSTGRES CONNECTOR
OPTIONS (
  hostname = 'postgres-source',
  ...
);
``` 