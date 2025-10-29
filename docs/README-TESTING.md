# Testing Guide: CDC Demo Project

This guide covers the test infrastructure setup and usage for the CDC Demo project.

## Quick Start (60 seconds)

```bash
# 1. Start test infrastructure services
docker compose up -d spark alertmanager

# 2. Verify services are healthy
curl -f http://localhost:8080  # Spark UI
curl -f http://localhost:9093/-/healthy  # AlertManager

# 3. Run tests
make test
```

## Test Infrastructure Overview

The project includes comprehensive test infrastructure for:
- **Apache Spark 3.5.0** with Delta Lake support
- **Prometheus AlertManager v0.26.0** for alert testing
- **MySQL Connector Python 8.4.0** for MySQL CDC tests

### Architecture

```
┌─────────────┐    ┌──────────────┐    ┌─────────────┐
│   Spark     │    │ AlertManager │    │  Prometheus │
│   Master    │◄───┤   v0.26.0    │◄───┤   (existing)│
│   3.5.0     │    │              │    │             │
└─────────────┘    └──────────────┘    └─────────────┘
      │                    │
      │                    │
      ▼                    ▼
┌─────────────────────────────────────┐
│          Test Fixtures               │
│  - spark_session                     │
│  - delta_table_path                  │
│  - alertmanager_client               │
│  - mysql_credentials (existing)      │
└─────────────────────────────────────┘
```

## Services

### 1. Apache Spark (Delta Lake)

**Purpose**: Provides Spark cluster for Delta Lake CDC operations testing

**Endpoints**:
- Master: `spark://localhost:7077`
- Web UI: http://localhost:8080
- Job UI: http://localhost:4040

**Configuration**: [configs/spark/spark-defaults.conf](configs/spark/spark-defaults.conf)

**Resources**:
- Memory: 2GB (1GB driver + 1GB executor)
- CPUs: 1.0

**Test Fixtures**:
```python
def test_delta_operations(spark_session, delta_table_path):
    # spark_session: PySpark session connected to cluster
    # delta_table_path: Unique temp path with auto-cleanup
    df = spark_session.createDataFrame([(1, "test")], ["id", "name"])
    df.write.format("delta").save(delta_table_path)
```

### 2. AlertManager

**Purpose**: Alert management for testing alerting and notification flows

**Endpoints**:
- Web UI: http://localhost:9093
- API: http://localhost:9093/api/v1/
- Health: http://localhost:9093/-/healthy

**Configuration**: [configs/alertmanager/alertmanager.yml](configs/alertmanager/alertmanager.yml)

**Resources**:
- Memory: 256MB
- CPUs: 0.5

**Test Fixtures**:
```python
def test_alert_delivery(alertmanager_client):
    # Post alert
    alert = {
        "labels": {"alertname": "TestAlert", "severity": "warning"},
        "annotations": {"description": "Test alert"}
    }
    alertmanager_client.post_alert(alert)

    # Verify
    alerts = alertmanager_client.get_alerts()
    assert len(alerts["data"]) > 0
```

### 3. MySQL Connector

**Purpose**: MySQL database connectivity for CDC testing

**Version**: mysql-connector-python 8.4.0

**Verification**:
```bash
python -c "import mysql.connector; print(mysql.connector.__version__)"
# Output: 8.4.0
```

## Running Tests

### Full Test Suite

```bash
# Run all tests
make test

# Run with coverage
make test-coverage

# Run specific test categories
pytest tests/unit -v                    # Unit tests only
pytest tests/integration -v             # Integration tests only
pytest tests/data_quality -v            # Data quality tests only
```

### Test Categories

#### Unit Tests
Fast tests with mocks, no external dependencies:
```bash
pytest tests/unit/test_cdc_pipelines/ -v
```

#### Integration Tests
Tests requiring Docker services:
```bash
# Start services first
docker compose up -d

# Run integration tests
pytest tests/integration/ -v
```

#### Data Quality Tests
Schema evolution and validation tests:
```bash
pytest tests/data_quality/ -v
```

### Test Markers

```bash
# Run only tests marked as 'integration'
pytest -m integration

# Run only tests marked as 'unit'
pytest -m unit

# Skip slow tests
pytest -m "not slow"
```

## Makefile Targets

### Infrastructure Management

```bash
# Start test infrastructure
make start-test-infra

# Stop test infrastructure
make stop-test-infra

# Verify infrastructure health
make verify-test-infra

# Clean Delta Lake test artifacts
make clean-delta-tables
```

### Testing

```bash
# Run full test suite
make test

# Run with coverage report
make test-coverage

# Run specific test file
make test FILE=tests/unit/test_cdc_pipelines/test_delta_writer.py
```

## Test Fixtures

### Database Credentials

Fixtures are defined in [tests/conftest.py](tests/conftest.py):

```python
@pytest.fixture(scope="session")
def postgres_credentials():
    """PostgreSQL connection credentials from environment."""
    return {
        "host": os.getenv("POSTGRES_HOST", "localhost"),
        "port": int(os.getenv("POSTGRES_PORT", "5432")),
        "database": os.getenv("POSTGRES_DB", "cdcdb"),
        "user": os.getenv("POSTGRES_USER", "cdcuser"),
        "password": os.getenv("POSTGRES_PASSWORD", "cdcpass"),
    }

@pytest.fixture(scope="session")
def mysql_credentials():
    """MySQL connection credentials from environment."""
    # Similar structure...
```

### Spark Session

```python
@pytest.fixture(scope="session")
def spark_session():
    """
    Create a Spark session for testing Delta Lake operations.
    Session-scoped to reuse across tests for performance.
    """
    spark = (
        SparkSession.builder
        .appName("CDC-Demo-Tests")
        .master("spark://localhost:7077")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    yield spark
    spark.stop()
```

### Delta Table Path

```python
@pytest.fixture
def delta_table_path(tmp_path):
    """
    Provide a unique Delta table path for each test.
    Cleans up after test completes.
    """
    table_name = f"test_{uuid.uuid4().hex[:8]}"
    table_path = tmp_path / "delta-tables" / table_name
    table_path.mkdir(parents=True, exist_ok=True)

    yield str(table_path)

    # Cleanup
    if table_path.exists():
        shutil.rmtree(table_path, ignore_errors=True)
```

### AlertManager Client

```python
@pytest.fixture
def alertmanager_client():
    """
    HTTP client for AlertManager API with helper methods.
    Automatically cleans up test artifacts.
    """
    # Returns client with methods:
    # - post_alert(alert)
    # - get_alerts()
    # - post_silence(silence)
    # - delete_silence(silence_id)
    # - get_status()
```

## Troubleshooting

### 1. Spark Master Not Starting

**Symptom**: `curl http://localhost:8080` fails

**Solutions**:
```bash
# Check logs
docker compose logs spark --tail 50

# Verify container running
docker compose ps spark

# Restart service
docker compose restart spark

# Check memory limits
docker stats cdc-spark
```

### 2. AlertManager Not Reachable

**Symptom**: `curl http://localhost:9093/-/healthy` fails

**Solutions**:
```bash
# Check logs
docker compose logs alertmanager --tail 50

# Verify configuration
cat configs/alertmanager/alertmanager.yml

# Restart service
docker compose restart alertmanager
```

### 3. Port Already in Use

**Symptom**: Service fails to start with "port already allocated"

**Solutions**:
```bash
# Find process using port (example: 8080)
lsof -i :8080

# Kill process or change port in docker-compose.yml
```

### 4. Out of Memory

**Symptom**: Spark jobs fail with OOM errors

**Solutions**:
```bash
# Reduce Spark memory in configs/spark/spark-defaults.conf
spark.driver.memory=512m
spark.executor.memory=512m

# Restart Spark
docker compose restart spark
```

### 5. Delta Lake JAR Not Downloaded

**Symptom**: Tests fail with "Delta Lake package not found"

**Solutions**:
```bash
# Download JAR manually (first run takes longer)
docker compose exec spark bash
# Wait for automatic download on first query

# Check Spark logs for download progress
docker compose logs spark | grep delta
```

### 6. Tests Failing with Connection Refused

**Symptom**: Tests fail connecting to services

**Solutions**:
```bash
# Verify all services are up
docker compose ps

# Check service health
curl http://localhost:8080    # Spark
curl http://localhost:9093/-/healthy  # AlertManager

# Restart services
docker compose restart spark alertmanager

# Check network connectivity
docker compose exec spark ping alertmanager
```

### 7. MySQL Connector Import Error

**Symptom**: `ModuleNotFoundError: No module named 'mysql'`

**Solutions**:
```bash
# Install dependency
poetry install

# Verify installation
python -c "import mysql.connector; print(mysql.connector.__version__)"

# Reinstall if needed
poetry add mysql-connector-python
```

## Environment Variables

Test configuration can be customized via environment variables:

```bash
# Database connections
export POSTGRES_HOST=localhost
export POSTGRES_PORT=5432
export POSTGRES_USER=cdcuser
export POSTGRES_PASSWORD=cdcpass
export POSTGRES_DB=cdcdb

export MYSQL_HOST=localhost
export MYSQL_PORT=3306
export MYSQL_USER=cdcuser
export MYSQL_PASSWORD=cdcpass
export MYSQL_DB=cdcdb

# Service URLs (optional)
export SPARK_MASTER=spark://localhost:7077
export ALERTMANAGER_URL=http://localhost:9093
```

## Performance Optimization

### Spark Session Reuse

The `spark_session` fixture uses session scope to avoid repeated initialization:
```python
@pytest.fixture(scope="session")  # Reused across all tests
def spark_session():
    # ...
```

### Parallel Test Execution

```bash
# Run tests in parallel (requires pytest-xdist)
pytest -n auto

# Run with specific worker count
pytest -n 4
```

### Skip Slow Tests During Development

```python
# Mark slow tests
@pytest.mark.slow
def test_large_dataset_processing():
    # ...

# Run without slow tests
pytest -m "not slow"
```

## CI/CD Integration

### GitHub Actions Example

```yaml
name: Tests

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3

      - name: Start test infrastructure
        run: docker compose up -d spark alertmanager

      - name: Install dependencies
        run: poetry install

      - name: Run tests
        run: make test

      - name: Upload coverage
        uses: codecov/codecov-action@v3
```

## Resource Requirements

### Minimum System Requirements

- **RAM**: 8GB minimum (4GB for services + 4GB for tests)
- **Disk**: 2GB free space for Docker images
- **CPU**: 2 cores recommended

### Docker Resource Allocation

```yaml
# Spark
resources:
  limits:
    memory: 2g
    cpus: '1.0'

# AlertManager
resources:
  limits:
    memory: 256m
    cpus: '0.5'
```

## Additional Resources

- **Specification**: [specs/001-test-infrastructure/spec.md](specs/001-test-infrastructure/spec.md)
- **Implementation Summary**: [specs/001-test-infrastructure/IMPLEMENTATION_SUMMARY.md](specs/001-test-infrastructure/IMPLEMENTATION_SUMMARY.md)
- **Task List**: [specs/001-test-infrastructure/tasks.md](specs/001-test-infrastructure/tasks.md)
- **Quickstart Guide**: [specs/001-test-infrastructure/quickstart.md](specs/001-test-infrastructure/quickstart.md)

## Support

For issues or questions:
1. Check troubleshooting section above
2. Review service logs: `docker compose logs <service>`
3. Open an issue in the project repository

---

**Last Updated**: 2025-10-29
**Maintained By**: CDC Demo Team
