# Test Infrastructure Expansion - Quickstart Guide

**Feature:** Test Infrastructure Expansion
**Date:** 2025-10-29
**Version:** 1.0

---

## Overview

This quickstart guide provides step-by-step instructions for setting up, validating, and testing the expanded CDC infrastructure including:

- Apache Iceberg REST Catalog with MinIO
- Confluent Schema Registry
- Delta Lake with Change Data Feed
- Debezium PostgreSQL connector management

**Estimated Setup Time:** 10-15 minutes (first time), 2-3 minutes (subsequent runs)

---

## Prerequisites

### Required Software

Ensure the following are installed and available in your PATH:

| Software | Minimum Version | Check Command | Installation |
|----------|----------------|---------------|--------------|
| Docker | 20.10+ | `docker --version` | https://docs.docker.com/get-docker/ |
| Docker Compose | 2.0+ | `docker compose version` | Included with Docker Desktop |
| Python | 3.11+ | `python --version` | https://www.python.org/downloads/ |
| Poetry | 1.5+ | `poetry --version` | `pip install poetry` |
| curl | Any | `curl --version` | Usually pre-installed |

### System Requirements

- **RAM:** Minimum 8GB, Recommended 16GB
- **Disk:** 10GB free space
- **CPU:** 4+ cores recommended

### Verify Prerequisites

Run this command to check all prerequisites:

```bash
# Check Docker
docker --version && docker compose version

# Check Python and Poetry
python --version && poetry --version

# Check Docker daemon is running
docker ps

# Check available resources
docker system info | grep -E "CPUs|Total Memory"
```

**Expected Output:**
```
Docker version 24.0.5, build ced0996
Docker Compose version v2.20.2

Python 3.11.6
Poetry (version 1.5.1)

Containers: 0
 Running: 0
 Paused: 0
 Stopped: 0

CPUs: 8
Total Memory: 15.54GiB
```

---

## Setup Steps

### 1. Clone Repository and Navigate to Project

```bash
cd /home/bob/WORK/claude-cdc-demo
```

### 2. Install Python Dependencies

```bash
# Install all dependencies including new packages (pyiceberg, delta-spark)
poetry install

# Activate virtual environment
poetry shell
```

**Expected Output:**
```
Installing dependencies from lock file

Package operations: 15 installs, 0 updates, 0 removals

  • Installing pyiceberg (0.10.0)
  • Installing delta-spark (3.3.2)
  • Installing pyspark (3.5.0)
  • ...

Creating virtualenv claude-cdc-demo in ...
```

### 3. Start Infrastructure Services

```bash
# Start all services in background
docker compose up -d

# Follow logs (optional, Ctrl+C to stop following)
docker compose logs -f
```

**Expected Output:**
```
[+] Running 10/10
 ✔ Network cdc-network              Created
 ✔ Container cdc-postgres           Started
 ✔ Container cdc-minio              Started
 ✔ Container cdc-zookeeper          Started
 ✔ Container cdc-kafka              Started
 ✔ Container cdc-iceberg-rest       Started
 ✔ Container cdc-schema-registry    Started
 ✔ Container cdc-spark-master       Started
 ✔ Container cdc-spark-worker       Started
 ✔ Container cdc-debezium           Started
```

**Startup Time:** 30-60 seconds for all services to become healthy

### 4. Verify Services Are Running

```bash
# Check all containers are up and healthy
docker compose ps
```

**Expected Output:**
```
NAME                    IMAGE                                      STATUS
cdc-postgres            postgres:15                                Up 1 minute (healthy)
cdc-minio               minio/minio:latest                         Up 1 minute (healthy)
cdc-zookeeper           confluentinc/cp-zookeeper:7.5.0           Up 1 minute (healthy)
cdc-kafka               confluentinc/cp-kafka:7.5.0               Up 1 minute (healthy)
cdc-iceberg-rest        tabulario/iceberg-rest:latest             Up 1 minute (healthy)
cdc-schema-registry     confluentinc/cp-schema-registry:7.5.0     Up 1 minute (healthy)
cdc-spark-master        apache/spark:3.5.0                         Up 1 minute
cdc-spark-worker        apache/spark:3.5.0                         Up 1 minute
cdc-debezium            custom-debezium-connect                    Up 1 minute (healthy)
```

All containers should show `(healthy)` or `Up` status.

---

## Service Validation

Validate each service is operational by running health checks and simple operations.

### PostgreSQL Database

**Health Check:**
```bash
docker exec cdc-postgres pg_isready -U cdcuser -d cdcdb
```

**Expected Output:**
```
/var/run/postgresql:5432 - accepting connections
```

**Connect to Database:**
```bash
docker exec -it cdc-postgres psql -U cdcuser -d cdcdb
```

**Test Query (inside psql):**
```sql
SELECT version();
\q
```

### MinIO Object Storage

**Health Check:**
```bash
curl -f http://localhost:9000/minio/health/live
```

**Expected Output:** Empty response with HTTP 200 status

**Access MinIO Console:**
- URL: http://localhost:9001
- Username: `minioadmin`
- Password: `minioadmin`

**Verify Warehouse Bucket Exists:**
```bash
docker exec cdc-minio mc ls local/
```

**Expected Output:**
```
[2025-10-29 10:30:00 UTC]     0B warehouse/
```

### Kafka

**Health Check:**
```bash
docker exec cdc-kafka kafka-broker-api-versions --bootstrap-server localhost:9092 | head -n 1
```

**Expected Output:**
```
localhost:9092 (id: 1 rack: null) -> (
```

**List Topics:**
```bash
docker exec cdc-kafka kafka-topics --bootstrap-server localhost:9092 --list
```

**Expected Output (minimal topics):**
```
__consumer_offsets
_schemas
```

### Apache Iceberg REST Catalog

**Health Check:**
```bash
curl -f http://localhost:8181/v1/config
```

**Expected Output:**
```json
{
  "defaults": {
    "warehouse": "s3://warehouse/"
  },
  "overrides": {}
}
```

**Test Catalog Operations (Python):**
```bash
python3 << 'EOF'
from pyiceberg.catalog import load_catalog

# Load catalog
catalog = load_catalog(
    "rest",
    **{
        "uri": "http://localhost:8181",
        "warehouse": "s3://warehouse/",
        "s3.endpoint": "http://localhost:9000",
        "s3.access-key-id": "minioadmin",
        "s3.secret-access-key": "minioadmin",
        "s3.region": "us-east-1",
        "s3.path-style-access": "true",
    }
)

# List namespaces
print("Iceberg catalog connected successfully")
print(f"Namespaces: {catalog.list_namespaces()}")
EOF
```

**Expected Output:**
```
Iceberg catalog connected successfully
Namespaces: []
```

### Confluent Schema Registry

**Health Check:**
```bash
curl -f http://localhost:8081/subjects
```

**Expected Output:**
```json
[]
```

**Register Test Schema:**
```bash
curl -X POST http://localhost:8081/subjects/test-value/versions \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{
    "schema": "{\"type\":\"record\",\"name\":\"Test\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"}]}"
  }'
```

**Expected Output:**
```json
{"id":1}
```

**Verify Schema Registered:**
```bash
curl http://localhost:8081/subjects
```

**Expected Output:**
```json
["test-value"]
```

**Cleanup Test Schema:**
```bash
curl -X DELETE http://localhost:8081/subjects/test-value
curl -X DELETE "http://localhost:8081/subjects/test-value?permanent=true"
```

### Spark with Delta Lake

**Health Check (Spark Master UI):**
```bash
curl -f http://localhost:8080 | grep -o "Spark Master at"
```

**Expected Output:**
```
Spark Master at
```

**Access Spark Master UI:**
- URL: http://localhost:8080
- Should show 1 worker registered

**Test Spark Session with Delta (Python):**
```bash
python3 << 'EOF'
from pyspark.sql import SparkSession

# Create Spark session with Delta
spark = (
    SparkSession.builder
    .appName("QuickstartTest")
    .master("spark://localhost:7077")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.3.2")
    .config("spark.driver.memory", "1g")
    .getOrCreate()
)

# Verify Delta is loaded
print("Spark session created successfully")
print(f"Spark version: {spark.version}")
print(f"Delta catalog: {spark.conf.get('spark.sql.catalog.spark_catalog')}")

# Test simple query
df = spark.sql("SELECT 1 as test")
df.show()

spark.stop()
print("Spark test completed successfully")
EOF
```

**Expected Output:**
```
Spark session created successfully
Spark version: 3.5.0
Delta catalog: org.apache.spark.sql.delta.catalog.DeltaCatalog
+----+
|test|
+----+
|   1|
+----+

Spark test completed successfully
```

### Debezium Connector

**Health Check:**
```bash
curl -f http://localhost:8083/
```

**Expected Output:**
```json
{"version":"7.5.0","commit":"...","kafka_cluster_id":"..."}
```

**List Available Connector Plugins:**
```bash
curl http://localhost:8083/connector-plugins | python3 -m json.tool
```

**Expected Output (should include PostgreSQL connector):**
```json
[
  {
    "class": "io.debezium.connector.postgresql.PostgresConnector",
    "type": "source",
    "version": "2.x.x"
  },
  ...
]
```

**List Active Connectors:**
```bash
curl http://localhost:8083/connectors
```

**Expected Output:**
```json
[]
```

---

## Run Test Suites

Now that all services are validated, run the test suites for each infrastructure component.

### Test Suite Overview

| Test Category | Test Count | Duration | Command |
|--------------|-----------|----------|---------|
| Iceberg Integration | 48 tests | ~2 min | `pytest tests/integration/test_iceberg.py -v` |
| Delta Lake Integration | 25 tests | ~3 min | `pytest tests/integration/test_delta_lake.py -v` |
| Schema Registry | 6 tests | ~10 sec | `pytest tests/integration/test_schema_registry.py -v` |
| Debezium Connectors | 2 tests | ~30 sec | `pytest tests/integration/test_debezium_connector.py -v` |
| **All Tests** | **81 tests** | **~6 min** | `pytest tests/integration/ -v` |

### Run All Integration Tests

```bash
# Run all integration tests with verbose output
pytest tests/integration/ -v

# Run with coverage report
pytest tests/integration/ --cov=src --cov-report=term-missing

# Run specific test file
pytest tests/integration/test_iceberg.py -v

# Run tests matching pattern
pytest tests/integration/ -v -k "iceberg"
```

**Expected Output (All Tests):**
```
========================================== test session starts ===========================================
platform linux -- Python 3.11.6, pytest-7.4.3, pluggy-1.3.0
rootdir: /home/bob/WORK/claude-cdc-demo
plugins: cov-4.1.0, mock-3.12.0
collected 81 items

tests/integration/test_iceberg.py::test_iceberg_catalog_connection PASSED                         [  1%]
tests/integration/test_iceberg.py::test_create_namespace PASSED                                   [  2%]
tests/integration/test_iceberg.py::test_create_table PASSED                                       [  3%]
...
tests/integration/test_delta_lake.py::test_delta_table_creation PASSED                            [ 62%]
tests/integration/test_delta_lake.py::test_change_data_feed_enabled PASSED                        [ 63%]
...
tests/integration/test_schema_registry.py::test_register_schema PASSED                            [ 88%]
tests/integration/test_schema_registry.py::test_check_compatibility PASSED                        [ 89%]
...
tests/integration/test_debezium_connector.py::test_create_connector PASSED                        [ 99%]
tests/integration/test_debezium_connector.py::test_connector_status PASSED                        [100%]

========================================== 81 passed in 360.42s ===========================================
```

### Run Individual Test Suites

**Iceberg Tests (48 tests):**
```bash
pytest tests/integration/test_iceberg.py -v --tb=short
```

**Delta Lake Tests (25 tests):**
```bash
pytest tests/integration/test_delta_lake.py -v --tb=short
```

**Schema Registry Tests (6 tests):**
```bash
pytest tests/integration/test_schema_registry.py -v --tb=short
```

**Debezium Tests (2 tests):**
```bash
pytest tests/integration/test_debezium_connector.py -v --tb=short
```

### Test Markers

Tests are organized with pytest markers for selective execution:

```bash
# Run only Iceberg tests
pytest -v -m iceberg

# Run only Delta Lake tests
pytest -v -m delta

# Run only Schema Registry tests
pytest -v -m schema_registry

# Run only Debezium tests
pytest -v -m debezium

# Run quick smoke tests only
pytest -v -m smoke

# Skip slow tests
pytest -v -m "not slow"
```

---

## Troubleshooting

### Common Issues and Solutions

#### 1. Service Won't Start

**Symptom:**
```
cdc-iceberg-rest    exited (1)
```

**Diagnosis:**
```bash
# Check service logs
docker compose logs iceberg-rest

# Check dependencies
docker compose ps minio
```

**Solutions:**
- Ensure MinIO is healthy before Iceberg starts
- Check environment variables in `docker-compose.yml`
- Restart service: `docker compose restart iceberg-rest`

#### 2. Tests Failing to Connect

**Symptom:**
```
ConnectionRefusedError: [Errno 111] Connection refused
```

**Diagnosis:**
```bash
# Check service is listening on expected port
docker compose ps

# Test port connectivity
curl -f http://localhost:8181/v1/config
```

**Solutions:**
- Wait for services to become fully healthy (30-60 seconds)
- Check health status: `docker compose ps`
- Verify port mappings: `docker compose port iceberg-rest 8181`
- Check firewall rules blocking localhost ports

#### 3. Memory Issues

**Symptom:**
```
docker: Error response from daemon: failed to create shim task: OCI runtime create failed
```

**Diagnosis:**
```bash
# Check Docker memory allocation
docker system info | grep Memory

# Check current memory usage
docker stats --no-stream
```

**Solutions:**
- Increase Docker Desktop memory allocation (Settings > Resources > Memory)
- Stop unused containers: `docker container prune`
- Reduce Spark memory settings in `docker-compose.yml`:
  ```yaml
  SPARK_DRIVER_MEMORY: 512m
  SPARK_EXECUTOR_MEMORY: 512m
  ```

#### 4. Port Already in Use

**Symptom:**
```
Error starting userland proxy: listen tcp4 0.0.0.0:8181: bind: address already in use
```

**Diagnosis:**
```bash
# Find process using port
lsof -i :8181
# or
netstat -tulpn | grep 8181
```

**Solutions:**
- Stop conflicting process
- Change port mapping in `docker-compose.yml`
- Use different port: `8182:8181`

#### 5. Spark Connection Timeout

**Symptom:**
```
Py4JJavaError: An error occurred while calling None.org.apache.spark.api.java.JavaSparkContext
```

**Diagnosis:**
```bash
# Check Spark master is running
curl http://localhost:8080

# Check worker is registered
docker compose logs spark-worker | grep "Successfully registered"
```

**Solutions:**
- Wait for Spark services to fully start (30-60 seconds)
- Restart Spark services:
  ```bash
  docker compose restart spark-master spark-worker
  ```
- Check Spark logs: `docker compose logs spark-master spark-worker`

#### 6. Schema Registry Connection Error

**Symptom:**
```
requests.exceptions.ConnectionError: HTTPConnectionPool(host='localhost', port=8081)
```

**Diagnosis:**
```bash
# Check Schema Registry health
curl http://localhost:8081/subjects

# Check Kafka dependency
docker compose ps kafka
```

**Solutions:**
- Ensure Kafka is healthy before Schema Registry
- Wait for Schema Registry to start (10-20 seconds)
- Check logs: `docker compose logs schema-registry`

#### 7. Debezium Connector Creation Fails

**Symptom:**
```
{"error_code":400,"message":"Connector configuration is invalid..."}
```

**Diagnosis:**
```bash
# Check Debezium logs
docker compose logs debezium

# Test PostgreSQL connectivity from Debezium container
docker exec cdc-debezium curl -f postgres:5432
```

**Solutions:**
- Verify PostgreSQL credentials in connector config
- Check table permissions for CDC user
- Ensure logical replication is enabled:
  ```bash
  docker exec cdc-postgres psql -U cdcuser -d cdcdb -c "SHOW wal_level;"
  ```
  (Should output: `logical`)

#### 8. Pytest Fixture Scope Errors

**Symptom:**
```
ScopeMismatch: You tried to access the function scoped fixture spark_session with a session scoped request
```

**Diagnosis:**
- Check fixture scopes in `conftest.py`
- Verify fixture dependencies

**Solutions:**
- Ensure session-scoped fixtures don't depend on function-scoped fixtures
- Review fixture scope documentation in test files
- Common fix: Make `spark_session` session-scoped in `conftest.py`

#### 9. Delta Lake Package Not Found

**Symptom:**
```
java.lang.NoClassDefFoundError: io/delta/sql/DeltaSparkSessionExtension
```

**Diagnosis:**
```bash
# Check Delta package is specified in Spark config
grep -r "delta-spark" tests/
```

**Solutions:**
- Verify `spark.jars.packages` includes `io.delta:delta-spark_2.12:3.3.2`
- Check internet connectivity for Maven package download
- Use `--packages` flag: `spark-submit --packages io.delta:delta-spark_2.12:3.3.2`

#### 10. Iceberg S3 Access Denied

**Symptom:**
```
org.apache.iceberg.exceptions.ForbiddenException: Access Denied
```

**Diagnosis:**
```bash
# Check MinIO credentials
docker exec cdc-iceberg-rest env | grep AWS

# Test MinIO connectivity
curl -u minioadmin:minioadmin http://localhost:9000
```

**Solutions:**
- Verify AWS credentials match MinIO credentials
- Check bucket exists: `docker exec cdc-minio mc ls local/warehouse/`
- Ensure path-style access is enabled in Iceberg config

---

## Cleanup

### Stop Services (Preserve Data)

```bash
# Stop all services but keep volumes
docker compose stop
```

### Stop and Remove Containers

```bash
# Stop and remove containers (keeps volumes)
docker compose down
```

### Complete Cleanup (Remove All Data)

```bash
# Stop, remove containers, volumes, and networks
docker compose down -v

# Remove orphaned volumes
docker volume prune -f

# Remove unused networks
docker network prune -f
```

### Cleanup Test Data Only

```bash
# Remove test databases/tables via PostgreSQL
docker exec -it cdc-postgres psql -U cdcuser -d cdcdb -c "
  DROP SCHEMA IF EXISTS test_schema CASCADE;
"

# Clear Kafka topics
docker exec cdc-kafka kafka-topics --bootstrap-server localhost:9092 --delete --topic 'test.*'

# Clear Schema Registry subjects
curl -X DELETE http://localhost:8081/subjects/test-value
curl -X DELETE "http://localhost:8081/subjects/test-value?permanent=true"

# Remove MinIO test buckets
docker exec cdc-minio mc rb --force local/test-bucket
```

### Restart from Scratch

```bash
# Complete teardown
docker compose down -v

# Rebuild images (if Dockerfile changed)
docker compose build --no-cache

# Start fresh
docker compose up -d

# Wait for services to be healthy
sleep 60

# Run validation again
curl -f http://localhost:8181/v1/config
```

---

## Next Steps

After completing the quickstart:

1. **Review Architecture:** Read `/home/bob/WORK/claude-cdc-demo/specs/002-infrastructure-expansion/data-model.md` for data models and state transitions

2. **Explore Contracts:** Check `/home/bob/WORK/claude-cdc-demo/specs/002-infrastructure-expansion/contracts/` for API specifications

3. **Write Tests:** Create new integration tests using the established patterns in `tests/integration/`

4. **Configure CI/CD:** Set up GitHub Actions or Jenkins to run tests automatically

5. **Performance Tuning:** Adjust memory allocations and parallelism for production workloads

6. **Monitoring:** Set up Prometheus/Grafana for infrastructure monitoring

---

## Quick Reference

### Service URLs

| Service | URL | Description |
|---------|-----|-------------|
| Iceberg REST | http://localhost:8181 | Catalog API |
| Schema Registry | http://localhost:8081 | Schema management |
| Debezium Connect | http://localhost:8083 | Connector management |
| MinIO Console | http://localhost:9001 | Object storage UI |
| Spark Master UI | http://localhost:8080 | Spark cluster UI |
| PostgreSQL | localhost:5432 | Database |
| Kafka Bootstrap | localhost:29092 | Kafka client connection |

### Common Commands

```bash
# Start infrastructure
docker compose up -d

# Check service health
docker compose ps

# View logs
docker compose logs -f [service-name]

# Run tests
pytest tests/integration/ -v

# Stop services
docker compose down

# Complete cleanup
docker compose down -v
```

### Test Execution

```bash
# All tests
pytest tests/integration/ -v

# Specific component
pytest tests/integration/test_iceberg.py -v
pytest tests/integration/test_delta_lake.py -v
pytest tests/integration/test_schema_registry.py -v
pytest tests/integration/test_debezium_connector.py -v

# With markers
pytest -v -m iceberg
pytest -v -m delta
pytest -v -m schema_registry
pytest -v -m debezium
```

---

## Support

For issues or questions:

1. Check logs: `docker compose logs [service-name]`
2. Review troubleshooting section above
3. Check service health: `docker compose ps`
4. Verify connectivity: `curl -f http://localhost:[port]`
5. Consult official documentation linked in each service section

---

**Last Updated:** 2025-10-29
**Maintainer:** CDC Demo Team
**Version:** 1.0
