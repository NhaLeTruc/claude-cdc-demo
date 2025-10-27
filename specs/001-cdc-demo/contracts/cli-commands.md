# CLI Commands Contract

**Version**: 1.0.0
**Date**: 2025-10-27
**Purpose**: Define command-line interface for CDC demo operations

## Overview

The CDC demo project provides a CLI tool built with Python Click for managing CDC pipelines, generating mock data, and running validations. All commands follow the pattern: `cdc-demo <command> [options]`

## Base Command

```bash
cdc-demo --help
```

**Output**:
```
Usage: cdc-demo [OPTIONS] COMMAND [ARGS]...

  CDC Demo - Change Data Capture demonstrations for open-source data storage

Options:
  --version        Show version and exit
  --verbose        Enable verbose logging
  --config PATH    Path to config file (default: config/default.yaml)
  --help           Show this message and exit

Commands:
  setup       Setup and initialize the CDC demo environment
  start       Start CDC pipelines
  stop        Stop CDC pipelines
  status      Show status of all pipelines
  generate    Generate mock data
  validate    Run data quality validations
  monitor     Monitor CDC lag and metrics
  test        Run test suite
  cleanup     Cleanup demo environment
```

## 1. Setup Commands

### 1.1 `cdc-demo setup`

Initialize the demo environment (Docker Compose services, databases, Kafka topics).

**Synopsis**:
```bash
cdc-demo setup [OPTIONS]
```

**Options**:
- `--skip-docker` - Skip Docker Compose setup (assumes services already running)
- `--quick` - Quick setup (minimal services for Postgres CDC only)
- `--full` - Full setup (all services: Postgres, MySQL, Kafka, DeltaLake, Iceberg, Observability)
- `--force` - Force recreation of existing services
- `--timeout SECONDS` - Timeout for service health checks (default: 300)

**Examples**:
```bash
# Full setup (default)
cdc-demo setup --full

# Quick setup for Postgres CDC only
cdc-demo setup --quick

# Force recreate all services
cdc-demo setup --full --force
```

**Exit Codes**:
- 0: Success
- 1: Docker not found or not running
- 2: Service health check timeout
- 3: Configuration error

**Output**:
```
✓ Checking Docker installation...
✓ Starting Docker Compose services...
  - PostgreSQL: Starting... ✓ (5s)
  - MySQL: Starting... ✓ (6s)
  - Kafka: Starting... ✓ (15s)
  - Zookeeper: Starting... ✓ (10s)
  - Debezium Connect: Starting... ✓ (20s)
  - MinIO: Starting... ✓ (3s)
  - Prometheus: Starting... ✓ (2s)
  - Grafana: Starting... ✓ (3s)
✓ Creating Kafka topics...
✓ Initializing databases...
✓ Registering Debezium connectors...
✓ Setup complete! (Total time: 64s)

Next steps:
  1. Generate mock data: cdc-demo generate --dataset small
  2. Start CDC pipelines: cdc-demo start --all
  3. Validate pipelines: cdc-demo validate --all
```

## 2. Pipeline Management Commands

### 2.1 `cdc-demo start`

Start CDC pipelines.

**Synopsis**:
```bash
cdc-demo start [OPTIONS] [PIPELINE_ID]
```

**Options**:
- `--all` - Start all configured pipelines
- `--postgres` - Start Postgres CDC pipelines
- `--mysql` - Start MySQL CDC pipelines
- `--deltalake` - Start DeltaLake CDC pipelines
- `--iceberg` - Start Iceberg CDC pipelines
- `--cross-storage` - Start cross-storage pipelines
- `--dry-run` - Show what would be started without starting

**Examples**:
```bash
# Start all pipelines
cdc-demo start --all

# Start only Postgres CDC
cdc-demo start --postgres

# Start specific pipeline
cdc-demo start postgres_customers_cdc

# Dry run
cdc-demo start --all --dry-run
```

**Exit Codes**:
- 0: Success (all requested pipelines started)
- 1: Partial failure (some pipelines failed to start)
- 2: Complete failure (no pipelines started)

**Output**:
```
Starting CDC pipelines...
✓ postgres_customers_cdc: Started (connector: debezium)
✓ postgres_orders_cdc: Started (connector: debezium)
✓ mysql_products_cdc: Started (connector: debezium)
✓ postgres_to_iceberg_pipeline: Started

4/4 pipelines started successfully
```

### 2.2 `cdc-demo stop`

Stop CDC pipelines.

**Synopsis**:
```bash
cdc-demo stop [OPTIONS] [PIPELINE_ID]
```

**Options**:
- `--all` - Stop all running pipelines
- `--postgres` - Stop Postgres CDC pipelines
- `--mysql` - Stop MySQL CDC pipelines
- `--graceful` - Graceful shutdown (wait for inflight events, default: true)
- `--force` - Force immediate shutdown
- `--timeout SECONDS` - Graceful shutdown timeout (default: 30)

**Examples**:
```bash
# Stop all pipelines gracefully
cdc-demo stop --all

# Force stop specific pipeline
cdc-demo stop postgres_customers_cdc --force

# Stop with custom timeout
cdc-demo stop --all --timeout 60
```

**Exit Codes**:
- 0: Success
- 1: Partial failure (some pipelines failed to stop)
- 2: Timeout (pipelines did not stop within timeout)

### 2.3 `cdc-demo status`

Show status of CDC pipelines.

**Synopsis**:
```bash
cdc-demo status [OPTIONS]
```

**Options**:
- `--json` - Output in JSON format
- `--verbose` - Show detailed status including metrics
- `--watch` - Continuously update status (refresh every 5s)

**Examples**:
```bash
# Show status of all pipelines
cdc-demo status

# JSON output for programmatic access
cdc-demo status --json

# Watch mode
cdc-demo status --watch
```

**Output** (table format):
```
Pipeline ID                    Status    Lag (s)  Events/s  Last Event
------------------------------|---------|---------|---------|-------------------------
postgres_customers_cdc        RUNNING   2.1      45.3      2025-10-27 10:30:45
postgres_orders_cdc           RUNNING   1.8      23.1      2025-10-27 10:30:47
mysql_products_cdc            RUNNING   3.2      67.8      2025-10-27 10:30:44
deltalake_cdf_demo            STOPPED   -        -         -
iceberg_snapshot_demo         RUNNING   4.5      12.5      2025-10-27 10:30:40
postgres_to_iceberg_pipeline  RUNNING   6.2      18.9      2025-10-27 10:30:42
```

**Output** (JSON format):
```json
{
  "timestamp": "2025-10-27T10:30:50Z",
  "pipelines": [
    {
      "pipeline_id": "postgres_customers_cdc",
      "status": "RUNNING",
      "lag_seconds": 2.1,
      "events_per_second": 45.3,
      "last_event_timestamp": "2025-10-27T10:30:45Z",
      "total_events_processed": 12450,
      "errors": 0
    },
    // ... more pipelines
  ]
}
```

## 3. Data Generation Commands

### 3.1 `cdc-demo generate`

Generate mock data for CDC demonstrations.

**Synopsis**:
```bash
cdc-demo generate [OPTIONS]
```

**Options**:
- `--dataset SIZE` - Dataset size: small (1K), medium (10K), large (100K)
- `--table TABLE_NAME` - Generate for specific table
- `--all` - Generate for all tables (default)
- `--seed INTEGER` - Random seed for reproducibility
- `--batch-size INTEGER` - Batch size for inserts (default: 1000)

**Examples**:
```bash
# Generate medium dataset for all tables
cdc-demo generate --dataset medium

# Generate small dataset for customers only
cdc-demo generate --dataset small --table customers

# Reproducible generation with seed
cdc-demo generate --dataset small --seed 42
```

**Exit Codes**:
- 0: Success
- 1: Database connection error
- 2: Generation error

**Output**:
```
Generating mock data (dataset: medium, seed: random)...

PostgreSQL:
  ✓ customers: Generated 10,000 records (12.3s)
  ✓ orders: Generated 50,000 records (48.7s)
  ✓ cdc_schema_evolution_test: Generated 1,000 records (1.2s)

MySQL:
  ✓ products: Generated 5,000 records (6.1s)
  ✓ inventory_transactions: Generated 25,000 records (28.4s)

Total: 91,000 records generated in 96.7s
```

## 4. Validation Commands

### 4.1 `cdc-demo validate`

Run data quality validations on CDC pipelines.

**Synopsis**:
```bash
cdc-demo validate [OPTIONS] [PIPELINE_ID]
```

**Options**:
- `--all` - Validate all pipelines
- `--type TYPE` - Validation type: row_count, checksum, lag, schema, integrity
- `--fail-fast` - Stop on first validation failure
- `--report PATH` - Save validation report to file
- `--format FORMAT` - Report format: text, json, html (default: text)

**Examples**:
```bash
# Validate all pipelines with all checks
cdc-demo validate --all

# Validate specific pipeline with row count check only
cdc-demo validate postgres_customers_cdc --type row_count

# Generate JSON report
cdc-demo validate --all --report validation_report.json --format json
```

**Exit Codes**:
- 0: All validations passed
- 1: Some validations failed
- 2: Validation error (could not run validation)

**Output**:
```
Running validations...

Pipeline: postgres_customers_cdc
  ✓ Row Count: PASS (source: 10,000 | destination: 10,000)
  ✓ Checksum: PASS (hash match on primary key columns)
  ✓ Lag: PASS (current lag: 2.1s, threshold: 5s)
  ✓ Schema: PASS (schemas compatible)
  ✓ Integrity: PASS (no null violations, PK unique)

Pipeline: postgres_orders_cdc
  ✓ Row Count: PASS (source: 50,000 | destination: 50,000)
  ✗ Lag: FAIL (current lag: 7.3s, threshold: 5s)
  ✓ Schema: PASS
  ✓ Integrity: PASS

Pipeline: mysql_products_cdc
  ✓ Row Count: PASS (source: 5,000 | destination: 5,000)
  ✓ Checksum: PASS
  ✓ Lag: PASS (current lag: 3.2s)
  ✓ Schema: PASS
  ✓ Integrity: PASS

Summary: 14/15 validations passed (93%)
Failed: postgres_orders_cdc (lag check)
```

## 5. Monitoring Commands

### 5.1 `cdc-demo monitor`

Monitor CDC pipelines in real-time.

**Synopsis**:
```bash
cdc-demo monitor [OPTIONS]
```

**Options**:
- `--refresh SECONDS` - Refresh interval (default: 5)
- `--pipeline PIPELINE_ID` - Monitor specific pipeline
- `--metric METRIC_NAME` - Show specific metric: lag, throughput, errors, health
- `--dashboard` - Open Grafana dashboard in browser

**Examples**:
```bash
# Monitor all pipelines (default)
cdc-demo monitor

# Monitor with fast refresh
cdc-demo monitor --refresh 1

# Monitor specific metric
cdc-demo monitor --metric lag

# Open Grafana dashboard
cdc-demo monitor --dashboard
```

**Output** (interactive TUI):
```
CDC Pipeline Monitor (refresh: 5s)                    Press 'q' to quit

Aggregate Metrics:
  Total Events Processed: 1,245,678
  Average Lag: 2.8s
  Total Errors: 3
  Active Pipelines: 5/6

┌─ Pipeline: postgres_customers_cdc ──────────────────────────────────┐
│ Status: RUNNING | Lag: 2.1s | Throughput: 45.3 ev/s                │
│ Events Processed: 12,450 | Errors: 0                                │
│ Health: ████████████████████████████████████████ 100%               │
└─────────────────────────────────────────────────────────────────────┘

┌─ Pipeline: postgres_orders_cdc ─────────────────────────────────────┐
│ Status: RUNNING | Lag: 7.3s ⚠ | Throughput: 23.1 ev/s              │
│ Events Processed: 50,123 | Errors: 0                                │
│ Health: ████████████████████████████████████████ 100%               │
│ WARNING: Lag exceeds threshold (5s)                                 │
└─────────────────────────────────────────────────────────────────────┘

[... more pipelines ...]

Last Updated: 2025-10-27 10:31:00
```

## 6. Testing Commands

### 6.1 `cdc-demo test`

Run the test suite (TDD-compliant).

**Synopsis**:
```bash
cdc-demo test [OPTIONS] [TEST_PATH]
```

**Options**:
- `--unit` - Run unit tests only
- `--integration` - Run integration tests only
- `--data-quality` - Run data quality tests only
- `--e2e` - Run end-to-end tests only
- `--coverage` - Generate coverage report
- `--verbose` - Verbose pytest output
- `--markers MARKER` - Run tests with specific pytest marker

**Examples**:
```bash
# Run all tests
cdc-demo test

# Run unit tests with coverage
cdc-demo test --unit --coverage

# Run specific test file
cdc-demo test tests/integration/test_postgres_cdc.py

# Run tests matching marker
cdc-demo test --markers slow
```

**Exit Codes**:
- 0: All tests passed
- 1: Some tests failed
- 2: Test execution error

**Output**:
```
Running tests...

========================= test session starts ==========================
platform linux -- Python 3.11.5, pytest-7.4.2
collected 147 items

tests/unit/test_data_generators.py .................... [ 13%]
tests/unit/test_validation.py ..................       [ 27%]
tests/integration/test_postgres_cdc.py .........       [ 33%]
tests/integration/test_mysql_cdc.py ........            [ 39%]
tests/data_quality/test_integrity.py ...........        [ 46%]
tests/data_quality/test_schema_evolution.py ....       [ 49%]
tests/e2e/test_postgres_to_iceberg.py .....             [ 52%]

========================== 147 passed in 245.32s =======================

Coverage Report:
  Name                          Stmts   Miss  Cover
  -------------------------------------------------
  src/cdc_pipelines/postgres.py    123     5    96%
  src/data_generators/gen.py        87     2    98%
  src/validation/integrity.py       65     0   100%
  -------------------------------------------------
  TOTAL                            1245    23    98%
```

## 7. Cleanup Commands

### 7.1 `cdc-demo cleanup`

Cleanup demo environment (stop services, remove data, reset state).

**Synopsis**:
```bash
cdc-demo cleanup [OPTIONS]
```

**Options**:
- `--all` - Remove everything (services, volumes, data)
- `--data` - Remove data only (keep services running)
- `--services` - Stop and remove services only
- `--keep-volumes` - Don't remove Docker volumes (preserve data)
- `--force` - Skip confirmation prompts

**Examples**:
```bash
# Full cleanup (with confirmation)
cdc-demo cleanup --all

# Remove data only
cdc-demo cleanup --data

# Force cleanup without prompts
cdc-demo cleanup --all --force
```

**Exit Codes**:
- 0: Success
- 1: Cleanup error

**Output**:
```
WARNING: This will remove all CDC demo data and services.
Continue? [y/N]: y

Stopping CDC pipelines...
  ✓ Stopped all pipelines

Stopping Docker Compose services...
  ✓ Stopped PostgreSQL
  ✓ Stopped MySQL
  ✓ Stopped Kafka
  ✓ Stopped Debezium Connect
  ✓ Stopped MinIO
  ✓ Stopped Prometheus
  ✓ Stopped Grafana

Removing Docker volumes...
  ✓ Removed postgres-data
  ✓ Removed mysql-data
  ✓ Removed kafka-data
  ✓ Removed minio-data

Cleanup complete!
```

## 8. Error Handling

All commands follow consistent error handling:

### 8.1 Common Error Messages

**Error: Docker not running**
```
ERROR: Docker is not running or not installed.
Please start Docker and try again.

Installation guide: https://docs.docker.com/get-docker/
```

**Error: Configuration not found**
```
ERROR: Configuration file not found: config/default.yaml

Run 'cdc-demo setup' to initialize the demo environment.
```

**Error: Service health check failed**
```
ERROR: Service 'postgres' failed health check (timeout: 60s)

Troubleshooting:
  1. Check Docker logs: docker-compose logs postgres
  2. Verify port 5432 is not in use: netstat -an | grep 5432
  3. Check resource limits: docker stats

For more help: https://docs.example.com/troubleshooting
```

### 8.2 Exit Code Convention

| Exit Code | Meaning |
|-----------|---------|
| 0 | Success |
| 1 | General error (operation failed) |
| 2 | Configuration error |
| 3 | Service unavailable |
| 4 | Validation failure |
| 5 | Timeout |

## 9. Environment Variables

The CLI respects these environment variables:

| Variable | Description | Default |
|----------|-------------|---------|
| `CDC_DEMO_CONFIG` | Path to config file | `config/default.yaml` |
| `CDC_DEMO_LOG_LEVEL` | Logging level | `INFO` |
| `CDC_DEMO_HOME` | Installation directory | `.` (current dir) |
| `DOCKER_HOST` | Docker daemon host | `unix:///var/run/docker.sock` |

**Example**:
```bash
export CDC_DEMO_LOG_LEVEL=DEBUG
cdc-demo start --all
```

## 10. Configuration File Format

Default configuration: `config/default.yaml`

```yaml
demo:
  name: "CDC Demo for Open-Source Storage"
  version: "1.0.0"

docker:
  compose_file: "docker-compose.yml"
  timeout: 300

pipelines:
  postgres_customers_cdc:
    enabled: true
    config_file: "config/pipelines/postgres_customers.yaml"
  postgres_orders_cdc:
    enabled: true
    config_file: "config/pipelines/postgres_orders.yaml"
  mysql_products_cdc:
    enabled: true
    config_file: "config/pipelines/mysql_products.yaml"
  # ... more pipelines

validation:
  default_checks: ["row_count", "checksum", "lag", "schema"]
  lag_threshold_seconds: 5
  checksum_sample_size: 1000

observability:
  prometheus_port: 9090
  grafana_port: 3000
  health_check_interval: 5

logging:
  level: "INFO"
  format: "json"
  output: "stdout"
```

## 11. API Versioning

CLI commands are versioned following semantic versioning:
- MAJOR: Breaking changes to command syntax or behavior
- MINOR: New commands or non-breaking feature additions
- PATCH: Bug fixes, documentation improvements

Current version: **1.0.0**

**Compatibility Promise**:
- Commands in 1.x.x will remain backward compatible
- Deprecated commands will show warnings for 2 minor versions before removal
- Configuration file format changes will be migrated automatically

## 12. Examples & Tutorials

### Quick Start Example

```bash
# 1. Setup environment
cdc-demo setup --full

# 2. Generate sample data
cdc-demo generate --dataset medium

# 3. Start Postgres CDC
cdc-demo start --postgres

# 4. Monitor in real-time
cdc-demo monitor

# 5. Validate data quality
cdc-demo validate --all

# 6. Run tests
cdc-demo test --integration

# 7. Cleanup
cdc-demo cleanup --all
```

### Schema Evolution Demo Example

```bash
# 1. Start with Postgres CDC
cdc-demo setup --quick
cdc-demo generate --dataset small
cdc-demo start postgres_schema_evolution_cdc

# 2. Observe initial state
cdc-demo monitor --pipeline postgres_schema_evolution_cdc

# 3. Trigger schema evolution (via SQL)
# (ALTER TABLE commands executed manually)

# 4. Observe CDC handling schema change
cdc-demo validate postgres_schema_evolution_cdc --type schema

# 5. Check data integrity preserved
cdc-demo validate postgres_schema_evolution_cdc --type integrity
```

## 13. Integration with External Tools

### CI/CD Integration

```bash
#!/bin/bash
# Example CI/CD script

set -e  # Exit on error

# Run in CI environment
cdc-demo setup --full --timeout 600
cdc-demo generate --dataset small --seed 42
cdc-demo start --all
sleep 30  # Wait for CDC to process events
cdc-demo validate --all --report validation_report.json --format json
cdc-demo test --coverage

# Check validation report
if [ $? -ne 0 ]; then
  echo "Validation failed"
  cdc-demo cleanup --all --force
  exit 1
fi

echo "All checks passed"
cdc-demo cleanup --all --force
```

### Programmatic Access (Python)

```python
import subprocess
import json

# Run command and capture JSON output
result = subprocess.run(
    ["cdc-demo", "status", "--json"],
    capture_output=True,
    text=True
)

status = json.loads(result.stdout)

for pipeline in status["pipelines"]:
    if pipeline["lag_seconds"] > 5:
        print(f"WARNING: {pipeline['pipeline_id']} lag: {pipeline['lag_seconds']}s")
```

## 14. Future CLI Enhancements

Planned for future versions:
- `cdc-demo replay` - Replay CDC events from specific timestamp
- `cdc-demo diff` - Compare source and destination data
- `cdc-demo export` - Export CDC events to file
- `cdc-demo import` - Import CDC events from file
- `cdc-demo benchmark` - Run performance benchmarks
- `cdc-demo upgrade` - Upgrade demo environment to latest version
