# CLI Reference

This document provides comprehensive reference for all command-line tools and scripts in the CDC demo project.

## Table of Contents

- [Makefile Commands](#makefile-commands)
- [CDC CLI Tool](#cdc-cli-tool)
- [Utility Scripts](#utility-scripts)
- [Docker Commands](#docker-commands)
- [Testing Commands](#testing-commands)

---

## Makefile Commands

The project uses a Makefile for common development tasks.

### Setup and Installation

```bash
# Quick start - Start all services and run demo
make quickstart
```
- Validates Docker is running
- Starts all services via docker-compose
- Generates sample data
- Starts CDC pipelines
- Runs validation checks

```bash
# Install Python dependencies
make install
```
- Installs dependencies via Poetry
- Sets up virtual environment
- Installs pre-commit hooks

```bash
# Setup pre-commit hooks
make setup-hooks
```
- Installs Git pre-commit hooks for code quality

### Service Management

```bash
# Start all services
make up
```
- Starts all Docker containers
- Equivalent to: `docker-compose up -d`

```bash
# Stop all services
make down
```
- Stops and removes all containers
- Preserves volumes

```bash
# Restart all services
make restart
```
- Stops and starts all services
- Useful for applying configuration changes

```bash
# Clean restart (removes volumes)
make clean
```
- Stops containers
- Removes volumes (WARNING: deletes all data)
- Useful for fresh start

```bash
# View logs
make logs
```
- Tails logs from all services
- Press Ctrl+C to exit

```bash
# View logs for specific service
make logs SERVICE=postgres
make logs SERVICE=kafka
```

### Testing

```bash
# Run all tests
make test
```
- Runs unit, integration, data quality, and e2e tests
- Equivalent to: `pytest tests/`

```bash
# Run unit tests only
make test-unit
```
- Fast tests with no external dependencies
- Equivalent to: `pytest tests/unit/`

```bash
# Run integration tests
make test-integration
```
- Tests requiring Docker services
- Equivalent to: `pytest tests/integration/ -m integration`

```bash
# Run data quality tests
make test-data-quality
```
- Schema evolution and validation tests
- Equivalent to: `pytest tests/data_quality/`

```bash
# Run end-to-end tests
make test-e2e
```
- Full workflow tests (slow)
- Equivalent to: `pytest tests/e2e/ -m e2e`

```bash
# Generate coverage report
make test-coverage
```
- Runs all tests with coverage
- Generates HTML report in htmlcov/
- Opens report in browser

### Code Quality

```bash
# Run linting
make lint
```
- Runs ruff code linter
- Checks for code quality issues
- Equivalent to: `ruff check src tests`

```bash
# Format code
make format
```
- Formats code with black and isort
- Equivalent to: `black src tests && isort src tests`

```bash
# Run type checking
make typecheck
```
- Runs mypy static type checker
- Equivalent to: `mypy src`

```bash
# Run all quality checks
make quality
```
- Runs lint + format + typecheck
- Use before committing code

### Data Generation

```bash
# Generate sample data
make generate-data
```
- Generates 10,000 customer records
- Inserts into PostgreSQL and MySQL

```bash
# Generate large dataset
make generate-data-large
```
- Generates 100,000 records across all tables

### Monitoring

```bash
# Open Grafana dashboards
make grafana
```
- Opens http://localhost:3000 in browser
- Login: admin/admin

```bash
# Open Prometheus
make prometheus
```
- Opens http://localhost:9090 in browser

```bash
# Check CDC status
make status
```
- Shows status of all CDC connectors
- Displays CDC lag metrics

### Cleanup

```bash
# Clean Python cache files
make clean-python
```
- Removes `__pycache__`, `.pyc`, `.pyo` files
- Removes `.pytest_cache`

```bash
# Clean Docker resources
make clean-docker
```
- Stops containers
- Removes volumes
- Removes images (optional)

```bash
# Full cleanup
make clean-all
```
- Runs clean-python + clean-docker
- Removes virtual environment

---

## CDC CLI Tool

The `cdc-cli` tool provides commands for managing CDC pipelines.

### Installation

```bash
# Install via Poetry
poetry install

# Activate environment
poetry shell

# Verify installation
cdc-cli --version
```

### Global Options

```
Options:
  --config FILE        Path to configuration file [default: config.yaml]
  --verbose, -v        Enable verbose output
  --quiet, -q          Suppress non-error output
  --help, -h           Show help message
  --version            Show version
```

### Commands

#### `cdc-cli status`

Show status of all CDC pipelines.

```bash
# Show status of all pipelines
cdc-cli status

# Show status with lag metrics
cdc-cli status --show-lag

# Show status for specific pipeline
cdc-cli status --pipeline postgres_customers_cdc

# Output in JSON format
cdc-cli status --format json
```

**Options:**
- `--show-lag`: Include CDC lag metrics
- `--pipeline NAME`: Filter by pipeline name
- `--format {table|json|yaml}`: Output format (default: table)

**Example Output:**
```
Pipeline                      Status    Lag (s)    Events/sec    Errors
---------------------------  --------  ---------  ------------  --------
postgres_customers_cdc       RUNNING   2.1        150           0
mysql_orders_cdc             RUNNING   1.8        200           0
deltalake_inventory_cdc      RUNNING   4.5        80            0
iceberg_analytics_cdc        RUNNING   8.2        50            0
```

#### `cdc-cli monitor`

Monitor CDC pipelines in real-time.

```bash
# Monitor all pipelines
cdc-cli monitor

# Monitor specific pipeline
cdc-cli monitor --pipeline postgres_customers_cdc

# Set refresh interval
cdc-cli monitor --interval 5

# Monitor with alerts
cdc-cli monitor --alert-on-lag 10
```

**Options:**
- `--pipeline NAME`: Pipeline to monitor
- `--interval SECONDS`: Refresh interval (default: 2)
- `--alert-on-lag SECONDS`: Alert if lag exceeds threshold
- `--duration SECONDS`: Monitor duration (default: infinite)

**Example Output:**
```
[2024-01-15 10:30:45] Monitoring: postgres_customers_cdc
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Lag:           2.3s  ████████████████████░░ (OK)
Throughput:    145 events/sec
Errors:        0
Last Event:    2024-01-15 10:30:43

Press Ctrl+C to stop monitoring
```

#### `cdc-cli validate`

Run data quality validation checks.

```bash
# Validate all pipelines
cdc-cli validate

# Validate specific pipeline
cdc-cli validate --pipeline postgres_customers_cdc

# Run specific validation type
cdc-cli validate --type row_count
cdc-cli validate --type checksum
cdc-cli validate --type schema

# Set tolerance for row count validation
cdc-cli validate --type row_count --tolerance 0.01  # 1% tolerance
```

**Options:**
- `--pipeline NAME`: Pipeline to validate
- `--type {row_count|checksum|schema|all}`: Validation type (default: all)
- `--tolerance FLOAT`: Tolerance for row count mismatch (default: 0.0)
- `--fix`: Attempt to fix issues (where possible)

**Example Output:**
```
Validation Results for: postgres_customers_cdc
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

✅ Row Count Validation
   Source:      10,000 rows
   Destination: 10,000 rows
   Match:       100%

✅ Checksum Validation
   Source:      0x1a2b3c4d
   Destination: 0x1a2b3c4d
   Match:       ✓

✅ Schema Validation
   Columns:     12/12 match
   Types:       All compatible

Overall: PASSED (3/3 checks)
```

#### `cdc-cli start`

Start a CDC pipeline.

```bash
# Start specific pipeline
cdc-cli start --pipeline postgres_customers_cdc

# Start with specific configuration
cdc-cli start --pipeline postgres_customers_cdc --config custom.yaml

# Start in debug mode
cdc-cli start --pipeline postgres_customers_cdc --debug
```

**Options:**
- `--pipeline NAME`: Pipeline to start (required)
- `--config FILE`: Configuration file
- `--debug`: Enable debug logging

#### `cdc-cli stop`

Stop a CDC pipeline.

```bash
# Stop specific pipeline
cdc-cli stop --pipeline postgres_customers_cdc

# Force stop (kill immediately)
cdc-cli stop --pipeline postgres_customers_cdc --force

# Stop all pipelines
cdc-cli stop --all
```

**Options:**
- `--pipeline NAME`: Pipeline to stop
- `--all`: Stop all pipelines
- `--force`: Force stop without graceful shutdown

#### `cdc-cli restart`

Restart a CDC pipeline.

```bash
# Restart specific pipeline
cdc-cli restart --pipeline postgres_customers_cdc

# Restart with new configuration
cdc-cli restart --pipeline postgres_customers_cdc --reload-config
```

**Options:**
- `--pipeline NAME`: Pipeline to restart (required)
- `--reload-config`: Reload configuration from file

#### `cdc-cli logs`

View logs for CDC pipelines.

```bash
# View logs for specific pipeline
cdc-cli logs --pipeline postgres_customers_cdc

# Tail logs in real-time
cdc-cli logs --pipeline postgres_customers_cdc --follow

# Show last N lines
cdc-cli logs --pipeline postgres_customers_cdc --tail 100

# Filter logs by level
cdc-cli logs --pipeline postgres_customers_cdc --level ERROR
```

**Options:**
- `--pipeline NAME`: Pipeline to view logs for (required)
- `--follow, -f`: Follow logs in real-time
- `--tail N`: Show last N lines
- `--level {DEBUG|INFO|WARN|ERROR}`: Filter by log level
- `--since TIMESTAMP`: Show logs since timestamp

#### `cdc-cli config`

Manage pipeline configurations.

```bash
# Show current configuration
cdc-cli config show --pipeline postgres_customers_cdc

# Validate configuration
cdc-cli config validate --file config.yaml

# Export configuration
cdc-cli config export --pipeline postgres_customers_cdc --output config.yaml

# Update configuration
cdc-cli config update --pipeline postgres_customers_cdc --set "batch_size=1000"
```

**Options:**
- `show`: Display current configuration
- `validate`: Validate configuration file
- `export`: Export configuration to file
- `update`: Update configuration setting

---

## Utility Scripts

### `scripts/generate-data.sh`

Generate mock data for testing CDC pipelines.

```bash
# Generate 10K customer records
./scripts/generate-data.sh --table customers --count 10000

# Generate all tables
./scripts/generate-data.sh --dataset large

# Generate with specific seed (reproducible)
./scripts/generate-data.sh --table customers --count 1000 --seed 42

# Generate and insert into specific database
./scripts/generate-data.sh --table orders --count 5000 --database mysql
```

**Options:**
- `--table TABLE`: Table to generate data for
- `--count N`: Number of records to generate
- `--dataset {small|medium|large}`: Predefined dataset size
- `--seed N`: Random seed for reproducibility
- `--database {postgres|mysql}`: Target database
- `--output FILE`: Save to file instead of inserting

**Datasets:**
- `small`: 1,000 records per table
- `medium`: 10,000 records per table
- `large`: 100,000 records per table

### `scripts/monitor-schema-evolution.sh`

Monitor schema changes in real-time.

```bash
# Monitor all tables
./scripts/monitor-schema-evolution.sh

# Monitor specific table
./scripts/monitor-schema-evolution.sh --table customers

# Set polling interval and duration
./scripts/monitor-schema-evolution.sh --interval 5 --duration 60

# Save output to file
./scripts/monitor-schema-evolution.sh --output schema-changes.log

# Verbose output
./scripts/monitor-schema-evolution.sh --verbose
```

**Options:**
- `--table TABLE`: Monitor specific table (default: all)
- `--database DB`: Target database (default: demo_db)
- `--interval SECONDS`: Polling interval (default: 5)
- `--duration SECONDS`: Total duration (default: 60)
- `--output FILE`: Save log to file
- `--verbose`: Enable verbose output

**Example Output:**
```
[INFO] 2024-01-15 10:30:00 - Starting schema evolution monitoring
[INFO] 2024-01-15 10:30:00 - Monitoring table: customers
[WARN] 2024-01-15 10:30:15 - Schema change detected for table: customers
[SUCCESS] 2024-01-15 10:30:15 - Table 'customers': Column(s) ADDED - loyalty_points
[INFO] 2024-01-15 10:30:15 - Table 'customers': Schema Registry version = 3
```

### `scripts/check-kafka-lag.sh`

Check Kafka consumer lag.

```bash
# Check lag for all consumer groups
./scripts/check-kafka-lag.sh

# Check specific consumer group
./scripts/check-kafka-lag.sh --group spark-streaming-consumer

# Alert if lag exceeds threshold
./scripts/check-kafka-lag.sh --alert-threshold 1000

# Output in JSON
./scripts/check-kafka-lag.sh --format json
```

**Options:**
- `--group GROUP`: Consumer group name
- `--alert-threshold N`: Alert if lag > N messages
- `--format {table|json}`: Output format

### `scripts/setup-connectors.sh`

Setup Debezium connectors.

```bash
# Setup all connectors
./scripts/setup-connectors.sh

# Setup specific connector
./scripts/setup-connectors.sh --connector postgres-customers

# Dry run (show configuration without creating)
./scripts/setup-connectors.sh --dry-run

# Force recreate (delete existing)
./scripts/setup-connectors.sh --force
```

**Options:**
- `--connector NAME`: Setup specific connector
- `--dry-run`: Show configuration without creating
- `--force`: Delete and recreate if exists
- `--config FILE`: Use custom connector configuration

### `scripts/validate-data-integrity.py`

Run comprehensive data validation.

```bash
# Validate all pipelines
python scripts/validate-data-integrity.py

# Validate specific pipeline
python scripts/validate-data-integrity.py --pipeline postgres_customers_cdc

# Run specific checks
python scripts/validate-data-integrity.py --checks row_count,checksum

# Generate report
python scripts/validate-data-integrity.py --report validation-report.html
```

**Options:**
- `--pipeline NAME`: Pipeline to validate
- `--checks CHECKS`: Comma-separated list of checks
- `--tolerance FLOAT`: Row count tolerance (default: 0.0)
- `--report FILE`: Generate HTML report

---

## Docker Commands

### Service Management

```bash
# Start all services
docker-compose up -d

# Start specific service
docker-compose up -d postgres

# Stop all services
docker-compose down

# Restart service
docker-compose restart postgres

# View logs
docker-compose logs -f postgres

# Execute command in container
docker exec -it postgres psql -U postgres -d demo_db
```

### Common Service Commands

**PostgreSQL:**
```bash
# Connect to database
docker exec -it postgres psql -U postgres -d demo_db

# Run SQL file
docker exec -i postgres psql -U postgres -d demo_db < schema.sql

# Backup database
docker exec postgres pg_dump -U postgres demo_db > backup.sql

# Restore database
docker exec -i postgres psql -U postgres -d demo_db < backup.sql
```

**MySQL:**
```bash
# Connect to database
docker exec -it mysql mysql -uroot -pmysql demo_db

# Run SQL file
docker exec -i mysql mysql -uroot -pmysql demo_db < schema.sql

# Backup database
docker exec mysql mysqldump -uroot -pmysql demo_db > backup.sql

# Restore database
docker exec -i mysql mysql -uroot -pmysql demo_db < backup.sql
```

**Kafka:**
```bash
# List topics
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list

# Describe topic
docker exec kafka kafka-topics --bootstrap-server localhost:9092 \
  --describe --topic debezium.public.customers

# Consume messages
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic debezium.public.customers \
  --from-beginning

# Check consumer groups
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --list
```

**Debezium:**
```bash
# List connectors
curl http://localhost:8083/connectors

# Get connector status
curl http://localhost:8083/connectors/postgres-customers-connector/status

# Delete connector
curl -X DELETE http://localhost:8083/connectors/postgres-customers-connector

# Restart connector
curl -X POST http://localhost:8083/connectors/postgres-customers-connector/restart
```

---

## Testing Commands

### pytest Options

```bash
# Run with verbose output
pytest -v

# Run with extra verbose output
pytest -vv

# Run specific test
pytest tests/unit/test_iceberg_manager.py::test_create_table

# Run tests matching pattern
pytest -k "postgres"

# Run tests with markers
pytest -m integration
pytest -m "not e2e"

# Run failed tests only
pytest --lf

# Run tests in parallel
pytest -n auto

# Stop on first failure
pytest -x

# Enter debugger on failure
pytest --pdb

# Generate coverage
pytest --cov=src --cov-report=html
```

### Test Markers

Available test markers:

```python
@pytest.mark.unit          # Unit tests (fast, no dependencies)
@pytest.mark.integration   # Integration tests (requires services)
@pytest.mark.e2e           # End-to-end tests (slow)
@pytest.mark.slow          # Slow tests (> 5 seconds)
@pytest.mark.skipif        # Conditional skip
```

---

## Environment Variables

Key environment variables for configuration:

```bash
# Database connections
export POSTGRES_HOST=localhost
export POSTGRES_PORT=5432
export POSTGRES_USER=postgres
export POSTGRES_PASSWORD=postgres
export POSTGRES_DB=demo_db

export MYSQL_HOST=localhost
export MYSQL_PORT=3306
export MYSQL_USER=root
export MYSQL_PASSWORD=mysql
export MYSQL_DB=demo_db

# Kafka
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export SCHEMA_REGISTRY_URL=http://localhost:8081

# Monitoring
export PROMETHEUS_URL=http://localhost:9090
export GRAFANA_URL=http://localhost:3000
export GRAFANA_USER=admin
export GRAFANA_PASSWORD=admin

# CDC Configuration
export CDC_LAG_THRESHOLD=10
export CDC_BATCH_SIZE=1000

# Logging
export LOG_LEVEL=INFO
export LOG_FORMAT=json
```

---

## Quick Reference

### Most Common Commands

```bash
# Start everything
make quickstart

# View status
cdc-cli status --show-lag

# Monitor in real-time
cdc-cli monitor

# Run tests
make test

# Check data quality
cdc-cli validate

# View logs
cdc-cli logs --pipeline postgres_customers_cdc --follow

# Clean restart
make clean && make quickstart
```

### Troubleshooting Commands

```bash
# Check service health
docker-compose ps

# View all logs
docker-compose logs --tail=100

# Check connector status
curl http://localhost:8083/connectors/postgres-customers-connector/status

# Check Kafka lag
./scripts/check-kafka-lag.sh

# Validate data integrity
cdc-cli validate --type all
```

---

For more detailed information, see:
- [Architecture Documentation](architecture.md)
- [Troubleshooting Guide](troubleshooting.md)
- [Contributing Guidelines](../CONTRIBUTING.md)
