# Apache Iceberg Testing Infrastructure Setup

This document describes the Apache Iceberg testing infrastructure added to enable CDC testing with Iceberg tables.

## Overview

The Iceberg infrastructure provides:
- **Apache Iceberg REST Catalog** for table metadata management
- **MinIO S3-compatible storage** for Iceberg table data and metadata
- **PyIceberg integration** for Python-based table operations
- **Pytest fixtures** for easy test setup and teardown

## Architecture

```
┌─────────────────┐         ┌──────────────────────┐
│   Test Code     │────────▶│  PyIceberg Library   │
│   (pytest)      │         │  (Python Client)     │
└─────────────────┘         └──────────────────────┘
                                      │
                                      ▼
                           ┌────────────────────────┐
                           │ Iceberg REST Catalog   │
                           │ (port 8181)            │
                           └────────────────────────┘
                                      │
                                      ▼
                           ┌────────────────────────┐
                           │       MinIO            │
                           │  (S3-compatible)       │
                           │  (port 9000)           │
                           └────────────────────────┘
                                      │
                                      ▼
                           ┌────────────────────────┐
                           │   Buckets:             │
                           │   - warehouse/         │
                           │   - iceberg/           │
                           │   - delta/             │
                           └────────────────────────┘
```

## Services

### Iceberg REST Catalog

**Image**: `tabulario/iceberg-rest:latest`
**Port**: `8181`
**Health Check**: `http://localhost:8181/v1/config`

The REST Catalog provides:
- Namespace and table management
- Metadata storage and versioning
- Schema evolution support
- Transaction coordination

**Configuration**: `docker/iceberg-rest/catalog-config.yaml`

### MinIO Object Storage

**Image**: `minio/minio:latest`
**Ports**: `9000` (API), `9001` (Console)
**Credentials**: `minioadmin` / `minioadmin`

Buckets automatically created:
- `warehouse` - Primary warehouse location for all tables
- `iceberg` - Iceberg-specific data
- `delta` - Delta Lake data (for cross-storage testing)

## Starting the Infrastructure

### Start All Services

```bash
docker compose up -d
```

### Start Only Iceberg Services

```bash
docker compose up -d minio minio-init iceberg-rest
```

### Verify Services are Healthy

```bash
# Check Iceberg REST Catalog
curl http://localhost:8181/v1/config

# Check MinIO
curl http://localhost:9000/minio/health/live

# Run infrastructure health tests
poetry run pytest tests/integration/test_infrastructure.py -v
```

## Using Iceberg in Tests

### Pytest Fixtures

Three fixtures are available in `tests/conftest.py`:

#### 1. `iceberg_catalog` (session-scoped)

Provides a PyIceberg catalog instance for the entire test session.

```python
def test_with_catalog(iceberg_catalog):
    namespaces = list(iceberg_catalog.list_namespaces())
    assert isinstance(namespaces, list)
```

#### 2. `iceberg_namespace` (function-scoped)

Creates a unique namespace for each test and cleans up automatically.

```python
def test_with_namespace(iceberg_catalog, iceberg_namespace):
    # namespace is automatically created and will be cleaned up
    tables = list(iceberg_catalog.list_tables(iceberg_namespace))
    assert len(tables) == 0
```

#### 3. `iceberg_test_namespace` (session-scoped)

Shared namespace `cdc_test` for tests that can share data.

```python
def test_with_shared_namespace(iceberg_catalog, iceberg_test_namespace):
    # Uses shared "cdc_test" namespace
    assert iceberg_test_namespace == "cdc_test"
```

### Direct Catalog Access

For tests that need more control:

```python
from tests.fixtures.iceberg_catalog import get_iceberg_catalog, setup_test_namespace

def test_custom_setup():
    catalog = get_iceberg_catalog()

    # Create a namespace
    catalog, namespace = setup_test_namespace("my_namespace")

    # Use the catalog...

    # Cleanup when done
    cleanup_test_namespace("my_namespace")
```

### Configuration

Default configuration is in `tests/fixtures/config.py`:

```python
from tests.fixtures.config import IcebergCatalogConfig

config = IcebergCatalogConfig(
    catalog_uri="http://localhost:8181",
    warehouse_path="s3://warehouse/",
    s3_endpoint="http://minio:9000",
    aws_access_key_id="minioadmin",
    aws_secret_access_key="minioadmin",
    aws_region="us-east-1"
)
```

## Running Iceberg Tests

### All Iceberg Tests

```bash
poetry run pytest tests/unit/test_cdc_pipelines/test_iceberg_*.py \
                 tests/integration/test_iceberg_cdc.py \
                 tests/e2e/test_iceberg_workflow.py \
                 tests/integration/test_cross_storage.py -v
```

### Unit Tests Only

```bash
poetry run pytest tests/unit/test_cdc_pipelines/test_iceberg_*.py -v
```

### Integration Tests Only

```bash
poetry run pytest tests/integration/test_iceberg_cdc.py -v
```

### E2E Tests Only

```bash
poetry run pytest tests/e2e/test_iceberg_workflow.py -v
```

## Troubleshooting

### Catalog Connection Failed

**Symptom**: `ConnectionError` when accessing catalog

**Solutions**:
1. Verify Iceberg REST service is running: `docker compose ps iceberg-rest`
2. Check logs: `docker logs cdc-iceberg-rest`
3. Wait for health check: The service may take 10-15 seconds to start

### MinIO Access Denied

**Symptom**: S3 access errors or permission denied

**Solutions**:
1. Verify MinIO is running: `docker compose ps minio`
2. Check buckets exist: `docker logs cdc-minio-init`
3. Verify credentials in test configuration match `minioadmin/minioadmin`

### PyIceberg Import Error

**Symptom**: `ModuleNotFoundError: No module named 'pyiceberg'`

**Solutions**:
1. Install dependencies: `poetry install`
2. Verify installation: `poetry run python -c "import pyiceberg; print(pyiceberg.__version__)"`
3. Expected version: `0.10.0` or higher

### Tests Skipped

**Symptom**: Tests show as `SKIPPED` instead of running

**Solutions**:
1. Verify PyIceberg is installed (see above)
2. Check test file has `PYICEBERG_AVAILABLE = True`
3. Ensure `@pytest.mark.skipif` uses `not PYICEBERG_AVAILABLE` (not `condition=True`)

## Configuration Files

- **Docker Compose**: `docker-compose.yml` (lines 238-273)
- **Iceberg Catalog Config**: `docker/iceberg-rest/catalog-config.yaml`
- **MinIO Init Script**: `docker/minio/init-buckets.sh`
- **Test Fixtures**: `tests/fixtures/iceberg_catalog.py`
- **Test Config**: `tests/fixtures/config.py`

## Test Utilities

### Health Checks

```python
from tests.fixtures.health_checks import HealthChecker

checker = HealthChecker()

# Check Iceberg catalog
if checker.check_iceberg_catalog():
    print("Iceberg catalog is healthy")

# Check MinIO
if checker.check_minio():
    print("MinIO is healthy")
```

### MinIO Management

```python
from tests.fixtures.minio_setup import MinIOSetup

setup = MinIOSetup()

# Ensure bucket exists
setup.ensure_bucket("my-test-bucket")

# Cleanup bucket
setup.cleanup_bucket("my-test-bucket")

# List all buckets
buckets = setup.list_buckets()
```

## Performance Considerations

- **Session-scoped fixtures** reuse catalog connections across tests for better performance
- **Function-scoped namespaces** provide isolation but have setup/teardown overhead
- **MinIO initialization** is done once on startup via the `minio-init` service
- **Health checks** use exponential backoff to avoid hammering services during startup

## Next Steps

- See `quickstart.md` for end-to-end validation scenarios
- See `research.md` for technical decisions and alternatives considered
- See test files for usage examples
