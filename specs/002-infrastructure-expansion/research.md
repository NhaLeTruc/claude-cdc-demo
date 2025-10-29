# Test Infrastructure Expansion - Technical Research Report

**Date:** 2025-10-29
**Project:** postgres-cdc-demo
**Feature:** Test Infrastructure Expansion

## Executive Summary

This document provides concrete technical decisions for implementing comprehensive test infrastructure including Apache Iceberg REST Catalog, Delta Lake with Spark 3.5.0, Confluent Schema Registry 7.5.0, Debezium connector management, and pytest fixture best practices.

---

## 1. Apache Iceberg REST Catalog

### Decision
- **Docker Image:** `tabulario/iceberg-rest:latest`
- **PyIceberg Version:** `^0.10.0` (latest stable as of Sept 2025)
- **Python Compatibility:** Python 3.9+ (fully compatible with Python 3.11)
- **REST Catalog Port:** 8181

### Rationale
- `tabulario/iceberg-rest` is the de facto standard Docker image for Iceberg REST catalog, maintained by Tabular (now Databricks)
- PyIceberg 0.10.0 is the latest stable release with extensive bug fixes and features
- REST catalog on port 8181 is the community standard, avoiding conflicts with other services
- Provides seamless integration with MinIO for S3-compatible object storage

### Alternatives Considered
- **Apache official image (`apache/iceberg-rest-fixture`):** Less mature, primarily for testing Apache Iceberg itself
- **Custom REST catalog implementation:** Unnecessary complexity, requires maintenance
- **PyIceberg 0.7.0 or 0.8.x:** Older versions with fewer features and potential compatibility issues with Python 3.13

### Configuration Details

#### Docker Compose Service
```yaml
iceberg-rest:
  image: tabulario/iceberg-rest:latest
  container_name: cdc-iceberg-rest
  ports:
    - "8181:8181"
  environment:
    - AWS_ACCESS_KEY_ID=minioadmin
    - AWS_SECRET_ACCESS_KEY=minioadmin
    - AWS_REGION=us-east-1
    - CATALOG_WAREHOUSE=s3://warehouse/
    - CATALOG_IO__IMPL=org.apache.iceberg.aws.s3.S3FileIO
    - CATALOG_S3_ENDPOINT=http://minio:9000
    - CATALOG_S3_PATH__STYLE__ACCESS=true
  depends_on:
    minio:
      condition: service_healthy
  healthcheck:
    test: ["CMD", "curl", "-f", "http://localhost:8181/v1/config"]
    interval: 10s
    timeout: 5s
    retries: 5
  networks:
    - cdc-network
```

#### MinIO Bucket Configuration
The Iceberg warehouse requires a dedicated bucket in MinIO:

**Bucket Name:** `warehouse`
**Access:** Public read/write for local development
**Path Structure:** `s3://warehouse/<namespace>/<table_name>/`

**Bucket Creation (via MinIO CLI or Python):**
```python
from minio import Minio

minio_client = Minio(
    "localhost:9000",
    access_key="minioadmin",
    secret_key="minioadmin",
    secure=False
)

# Create warehouse bucket if it doesn't exist
if not minio_client.bucket_exists("warehouse"):
    minio_client.make_bucket("warehouse")
```

#### PyIceberg Connection Configuration

**Python Configuration (.pyiceberg.yaml):**
```yaml
catalog:
  rest:
    type: rest
    uri: http://localhost:8181
    warehouse: s3://warehouse/
    s3.endpoint: http://localhost:9000
    s3.access-key-id: minioadmin
    s3.secret-access-key: minioadmin
    s3.region: us-east-1
    s3.path-style-access: true
```

**Python Code Usage:**
```python
from pyiceberg.catalog import load_catalog

# Load catalog
catalog = load_catalog("rest")

# Create namespace
catalog.create_namespace("test_db")

# Create table
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType, LongType

schema = Schema(
    NestedField(1, "id", LongType(), required=True),
    NestedField(2, "name", StringType(), required=False)
)

catalog.create_table(
    identifier="test_db.test_table",
    schema=schema
)
```

#### pyproject.toml Dependency
```toml
pyiceberg = "^0.10.0"
```

**Optional Extras for Full Functionality:**
```toml
# For S3 support
pyiceberg = {version = "^0.10.0", extras = ["s3fs", "pyarrow"]}
```

---

## 2. Delta Lake with Spark 3.5.0

### Decision
- **Delta Lake Version:** `3.3.2` or `3.0.0` (compatible with Spark 3.5.0)
- **Spark Version:** `3.5.0` (already configured in docker-compose.yml)
- **Scala Version:** `2.12`
- **Maven Coordinates:** `io.delta:delta-spark_2.12:3.3.2`
- **Python Package:** `delta-spark==3.3.2`

### Rationale
- Delta Lake 3.3.2 is the latest version explicitly built for Spark 3.5.x
- Scala 2.12 is the standard version for Spark 3.5.0
- Version 3.0.0 is the minimum compatible version, but 3.3.2 includes important bug fixes
- Python delta-spark package provides seamless integration with PySpark

### Alternatives Considered
- **Delta Lake 2.4.0:** Not compatible with Spark 3.5.0, only supports up to Spark 3.4.x
- **Delta Lake 3.0.0:** Minimum compatible version, but lacks features and fixes from 3.3.x
- **Scala 2.13 build:** Less commonly used, potential compatibility issues with other libraries

### Configuration Details

#### Spark Configuration for Delta Lake
```python
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("CDC-Demo-Tests")
    .master("spark://localhost:7077")
    # Delta Lake extensions
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    # Delta Lake package
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.3.2")
    # S3 configuration for MinIO
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    # Memory configuration
    .config("spark.driver.memory", "1g")
    .config("spark.executor.memory", "1g")
    .getOrCreate()
)
```

#### Change Data Feed (CDF) Configuration

**Enable CDF Globally:**
```python
# For all new Delta tables
spark.conf.set("spark.databricks.delta.properties.defaults.enableChangeDataFeed", "true")
```

**Enable CDF on Specific Table:**
```sql
-- At table creation
CREATE TABLE events (
  id BIGINT,
  timestamp TIMESTAMP,
  data STRING
) USING DELTA
TBLPROPERTIES (delta.enableChangeDataFeed = true);

-- For existing table
ALTER TABLE events
SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
```

**Read Change Data Feed:**
```python
# Read changes between versions
changes = (
    spark.read.format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", 0)
    .option("endingVersion", 10)
    .table("events")
)

# Read changes by timestamp
changes = (
    spark.read.format("delta")
    .option("readChangeFeed", "true")
    .option("startingTimestamp", "2025-01-01 00:00:00")
    .option("endingTimestamp", "2025-01-02 00:00:00")
    .table("events")
)

# Stream changes
changes_stream = (
    spark.readStream.format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", 0)
    .table("events")
)
```

**CDF Columns:**
- `_change_type`: Type of change (insert, update_preimage, update_postimage, delete)
- `_commit_version`: Delta table version for the change
- `_commit_timestamp`: Timestamp when the change was committed

#### Key Spark Configuration Properties
```python
# Delta Lake required configurations
DELTA_CONFIG = {
    # Core Delta extensions
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",

    # Delta package coordinates
    "spark.jars.packages": "io.delta:delta-spark_2.12:3.3.2",

    # Change Data Feed (CDF) - enable by default
    "spark.databricks.delta.properties.defaults.enableChangeDataFeed": "true",

    # Warehouse directory (for local development)
    "spark.sql.warehouse.dir": "/tmp/spark-warehouse",

    # S3A configuration for MinIO
    "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
    "spark.hadoop.fs.s3a.access.key": "minioadmin",
    "spark.hadoop.fs.s3a.secret.key": "minioadmin",
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
}
```

#### pyproject.toml Dependency
```toml
pyspark = "^3.5.0"
delta-spark = "^3.3.2"
```

**Note:** The existing configuration in `/home/bob/WORK/claude-cdc-demo/pyproject.toml` uses:
- `pyspark = "^3.5.0"` ✓ Correct
- `delta-spark = "^3.0.0"` → Should be updated to `"^3.3.2"` for latest features

---

## 3. Confluent Schema Registry 7.5.0

### Decision
- **Docker Image:** `confluentinc/cp-schema-registry:7.5.0`
- **Port:** 8081 (standard Schema Registry port)
- **Compatibility:** Fully compatible with Kafka 7.5.0
- **API Version:** Schema Registry API v1

### Rationale
- Version 7.5.0 matches the Kafka version already in use (confluentinc/cp-kafka:7.5.0)
- Port 8081 is the industry standard for Schema Registry
- Confluent Platform ensures tight integration between Kafka and Schema Registry
- REST API provides straightforward integration with Python tests

### Alternatives Considered
- **Apicurio Registry:** More features but adds complexity, overkill for CDC testing
- **Different version (7.6.x or 7.4.x):** Version mismatch with Kafka could cause compatibility issues
- **Custom port (8082, 8090):** Non-standard, complicates documentation and integration

### Configuration Details

#### Docker Compose Service
```yaml
schema-registry:
  image: confluentinc/cp-schema-registry:7.5.0
  container_name: cdc-schema-registry
  depends_on:
    kafka:
      condition: service_healthy
  environment:
    # Hostname advertised to Kafka
    SCHEMA_REGISTRY_HOST_NAME: schema-registry

    # Kafka bootstrap servers
    SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS: kafka:9092

    # Listeners - REST API endpoint
    SCHEMA_REGISTRY_LISTENERS: http://0.0.0.0:8081

    # Schema compatibility level (default: BACKWARD)
    SCHEMA_REGISTRY_SCHEMA_COMPATIBILITY_LEVEL: BACKWARD

    # Debug logging (optional, for development)
    SCHEMA_REGISTRY_DEBUG: "false"

    # Topic for storing schemas (default: _schemas)
    SCHEMA_REGISTRY_KAFKASTORE_TOPIC: _schemas

    # Replication factor for schema topic
    SCHEMA_REGISTRY_KAFKASTORE_TOPIC_REPLICATION_FACTOR: 1
  ports:
    - "8081:8081"
  healthcheck:
    test: ["CMD", "curl", "-f", "http://localhost:8081/subjects"]
    interval: 10s
    timeout: 5s
    retries: 5
  networks:
    - cdc-network
```

#### Required Environment Variables
| Variable | Purpose | Example Value |
|----------|---------|---------------|
| `SCHEMA_REGISTRY_HOST_NAME` | Hostname for Schema Registry instance | `schema-registry` |
| `SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS` | Kafka brokers connection string | `kafka:9092` |
| `SCHEMA_REGISTRY_LISTENERS` | HTTP/HTTPS listener URLs | `http://0.0.0.0:8081` |
| `SCHEMA_REGISTRY_SCHEMA_COMPATIBILITY_LEVEL` | Compatibility mode for schema evolution | `BACKWARD` |

**Environment Variable Naming Convention:**
- Prefix all variables with `SCHEMA_REGISTRY_`
- Convert property names: periods (.) → underscores (_), dashes (-) → triple underscores (___), existing underscores (_) → double underscores (__)
- Convert to uppercase

Examples:
- `kafkastore.bootstrap.servers` → `SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS`
- `host.name` → `SCHEMA_REGISTRY_HOST_NAME`
- `schema.compatibility.level` → `SCHEMA_REGISTRY_SCHEMA_COMPATIBILITY_LEVEL`

#### Health Check Endpoint
**Primary Health Check:** `GET http://localhost:8081/subjects`

**Alternative Endpoints:**
- `GET /` - Root endpoint, returns Schema Registry version info
- `GET /config` - Returns global compatibility configuration
- `GET /mode` - Returns current mode (READWRITE, READONLY, etc.)

**Health Check Implementation:**
```python
import httpx

def wait_for_schema_registry(base_url: str = "http://localhost:8081", timeout: int = 60):
    """Wait for Schema Registry to be ready."""
    client = httpx.Client(base_url=base_url)

    import time
    start = time.time()
    while time.time() - start < timeout:
        try:
            response = client.get("/subjects")
            if response.status_code == 200:
                return True
        except Exception:
            pass
        time.sleep(1)

    raise TimeoutError("Schema Registry did not become healthy")
```

#### Python Test Interaction

**Using httpx for REST API:**
```python
import httpx
import json

class SchemaRegistryClient:
    def __init__(self, base_url: str = "http://localhost:8081"):
        self.base_url = base_url
        self.client = httpx.Client(base_url=base_url)

    def register_schema(self, subject: str, schema: dict) -> int:
        """Register a new schema and return schema ID."""
        response = self.client.post(
            f"/subjects/{subject}/versions",
            json={"schema": json.dumps(schema)},
            headers={"Content-Type": "application/vnd.schemaregistry.v1+json"}
        )
        response.raise_for_status()
        return response.json()["id"]

    def get_latest_schema(self, subject: str) -> dict:
        """Get the latest schema for a subject."""
        response = self.client.get(f"/subjects/{subject}/versions/latest")
        response.raise_for_status()
        return response.json()

    def list_subjects(self) -> list[str]:
        """List all subjects."""
        response = self.client.get("/subjects")
        response.raise_for_status()
        return response.json()

    def check_compatibility(self, subject: str, schema: dict) -> bool:
        """Check if schema is compatible with latest version."""
        response = self.client.post(
            f"/compatibility/subjects/{subject}/versions/latest",
            json={"schema": json.dumps(schema)},
            headers={"Content-Type": "application/vnd.schemaregistry.v1+json"}
        )
        response.raise_for_status()
        return response.json()["is_compatible"]

    def delete_subject(self, subject: str) -> list[int]:
        """Delete a subject and return deleted version IDs."""
        response = self.client.delete(f"/subjects/{subject}")
        response.raise_for_status()
        return response.json()
```

**Using confluent-kafka with Schema Registry:**
```python
from confluent_kafka.schema_registry import SchemaRegistryClient

schema_registry_conf = {
    'url': 'http://localhost:8081'
}

sr_client = SchemaRegistryClient(schema_registry_conf)

# Register Avro schema
from confluent_kafka.schema_registry import Schema

avro_schema_str = """
{
  "type": "record",
  "name": "User",
  "fields": [
    {"name": "id", "type": "long"},
    {"name": "name", "type": "string"}
  ]
}
"""

schema = Schema(avro_schema_str, schema_type="AVRO")
schema_id = sr_client.register_schema("users-value", schema)
```

#### Compatibility Modes
- **BACKWARD** (default): New schema can read old data
- **FORWARD:** Old schema can read new data
- **FULL:** Both backward and forward compatible
- **NONE:** No compatibility checks
- **BACKWARD_TRANSITIVE:** Backward compatibility with all previous versions
- **FORWARD_TRANSITIVE:** Forward compatibility with all previous versions
- **FULL_TRANSITIVE:** Full compatibility with all previous versions

---

## 4. Debezium Connector Management

### Decision
- **REST API Base URL:** `http://localhost:8083`
- **API Version:** Kafka Connect REST API v1 (standard for Debezium)
- **Connector Class:** `io.debezium.connector.postgresql.PostgresConnector`
- **Plugin Name:** `debezium-postgres-connector`

### Rationale
- Kafka Connect REST API is the standard interface for all Debezium connectors
- Port 8083 is already configured in existing docker-compose.yml
- REST API provides programmatic control for test automation
- Supports full CRUD operations with clear error codes

### Alternatives Considered
- **Kafka Connect CLI:** Less flexible, harder to automate in tests
- **Direct Kafka topic manipulation:** Bypasses connector validation and management
- **Debezium UI:** GUI-based, not suitable for automated testing

### Configuration Details

#### Debezium REST API Endpoints

**Base URL:** `http://localhost:8083`

| Method | Endpoint | Purpose | Status Codes |
|--------|----------|---------|--------------|
| GET | `/connectors` | List all connectors | 200 OK |
| POST | `/connectors` | Create new connector | 201 Created, 409 Conflict (exists), 400 Bad Request |
| GET | `/connectors/{name}` | Get connector info | 200 OK, 404 Not Found |
| GET | `/connectors/{name}/config` | Get connector config | 200 OK, 404 Not Found |
| PUT | `/connectors/{name}/config` | Update connector config | 200 OK, 201 Created, 409 Conflict (rebalancing) |
| DELETE | `/connectors/{name}` | Delete connector | 204 No Content, 404 Not Found, 409 Conflict (rebalancing) |
| GET | `/connectors/{name}/status` | Get connector status | 200 OK, 404 Not Found |
| POST | `/connectors/{name}/restart` | Restart connector | 204 No Content, 404 Not Found, 409 Conflict (rebalancing) |
| GET | `/connectors/{name}/topics` | List connector topics | 200 OK, 404 Not Found |
| GET | `/connector-plugins` | List available plugins | 200 OK |

#### JSON Payload Structure for Postgres Connector

**Complete Postgres CDC Connector Configuration:**
```json
{
  "name": "postgres-cdc-connector",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "tasks.max": "1",

    "database.hostname": "postgres",
    "database.port": "5432",
    "database.user": "cdcuser",
    "database.password": "cdcpass",
    "database.dbname": "cdcdb",

    "topic.prefix": "postgres.cdc",

    "plugin.name": "pgoutput",

    "slot.name": "debezium_slot",

    "publication.name": "debezium_publication",

    "table.include.list": "public.users,public.orders",

    "schema.include.list": "public",

    "heartbeat.interval.ms": "10000",

    "snapshot.mode": "initial",

    "decimal.handling.mode": "precise",

    "time.precision.mode": "adaptive_time_microseconds",

    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "false",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false",

    "transforms": "unwrap",
    "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
    "transforms.unwrap.drop.tombstones": "false",
    "transforms.unwrap.delete.handling.mode": "rewrite"
  }
}
```

**Minimal Test Configuration:**
```json
{
  "name": "test-postgres-connector",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "tasks.max": "1",
    "database.hostname": "postgres",
    "database.port": "5432",
    "database.user": "cdcuser",
    "database.password": "cdcpass",
    "database.dbname": "cdcdb",
    "topic.prefix": "test",
    "plugin.name": "pgoutput"
  }
}
```

#### Error Codes for Common Scenarios

| Status Code | Scenario | Response Body Example |
|-------------|----------|----------------------|
| 409 Conflict | Connector already exists | `{"error_code":409,"message":"Connector test-connector already exists"}` |
| 409 Conflict | Worker rebalancing | `{"error_code":409,"message":"Cannot complete request because of a conflicting operation (e.g. worker rebalance)"}` |
| 400 Bad Request | Invalid configuration | `{"error_code":400,"message":"Connector configuration is invalid..."}` |
| 404 Not Found | Connector doesn't exist | `{"error_code":404,"message":"Connector test-connector not found"}` |
| 500 Internal Error | Connector startup failure | `{"error_code":500,"message":"Failed to start connector..."}` |

#### Connector Status Check API

**GET /connectors/{name}/status Response:**
```json
{
  "name": "postgres-cdc-connector",
  "connector": {
    "state": "RUNNING",
    "worker_id": "debezium:8083"
  },
  "tasks": [
    {
      "id": 0,
      "state": "RUNNING",
      "worker_id": "debezium:8083"
    }
  ],
  "type": "source"
}
```

**Possible States:**
- `RUNNING`: Connector/task is running normally
- `FAILED`: Connector/task has failed
- `PAUSED`: Connector/task is paused
- `UNASSIGNED`: Task not yet assigned to a worker

#### Python Client Implementation

```python
import httpx
import time
from typing import Optional

class DebeziumClient:
    """Client for Debezium Kafka Connect REST API."""

    def __init__(self, base_url: str = "http://localhost:8083"):
        self.base_url = base_url
        self.client = httpx.Client(base_url=base_url, timeout=30.0)

    def create_connector(self, name: str, config: dict) -> dict:
        """
        Create a new connector.

        Returns connector info on success.
        Raises httpx.HTTPStatusError on failure.
        """
        payload = {
            "name": name,
            "config": config
        }

        try:
            response = self.client.post("/connectors", json=payload)
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 409:
                # Connector already exists
                raise ValueError(f"Connector '{name}' already exists") from e
            raise

    def get_connector(self, name: str) -> Optional[dict]:
        """Get connector configuration. Returns None if not found."""
        try:
            response = self.client.get(f"/connectors/{name}")
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                return None
            raise

    def delete_connector(self, name: str) -> bool:
        """
        Delete a connector.

        Returns True if deleted, False if not found.
        Raises on other errors.
        """
        try:
            response = self.client.delete(f"/connectors/{name}")
            response.raise_for_status()
            return True
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                return False
            if e.response.status_code == 409:
                # Rebalancing, retry
                time.sleep(2)
                return self.delete_connector(name)
            raise

    def get_connector_status(self, name: str) -> dict:
        """Get connector status."""
        response = self.client.get(f"/connectors/{name}/status")
        response.raise_for_status()
        return response.json()

    def is_connector_running(self, name: str) -> bool:
        """Check if connector is in RUNNING state."""
        try:
            status = self.get_connector_status(name)
            connector_running = status["connector"]["state"] == "RUNNING"
            tasks_running = all(
                task["state"] == "RUNNING"
                for task in status.get("tasks", [])
            )
            return connector_running and tasks_running
        except Exception:
            return False

    def wait_for_connector_running(
        self,
        name: str,
        timeout: int = 60
    ) -> bool:
        """Wait for connector to reach RUNNING state."""
        start = time.time()
        while time.time() - start < timeout:
            if self.is_connector_running(name):
                return True
            time.sleep(1)
        return False

    def restart_connector(self, name: str) -> None:
        """Restart a connector."""
        response = self.client.post(f"/connectors/{name}/restart")
        response.raise_for_status()

    def list_connectors(self) -> list[str]:
        """List all connector names."""
        response = self.client.get("/connectors")
        response.raise_for_status()
        return response.json()

    def connector_exists(self, name: str) -> bool:
        """Check if connector exists."""
        return self.get_connector(name) is not None

    def ensure_connector_deleted(self, name: str) -> None:
        """Ensure connector is deleted, handling 'already exists' gracefully."""
        if self.connector_exists(name):
            self.delete_connector(name)
            # Wait for deletion to complete
            time.sleep(2)
```

**Usage in Tests:**
```python
import pytest
from debezium_client import DebeziumClient

@pytest.fixture
def debezium_client():
    """Debezium connector management client."""
    client = DebeziumClient("http://localhost:8083")
    yield client
    client.client.close()

def test_create_postgres_connector(debezium_client):
    """Test creating a Postgres CDC connector."""
    connector_name = "test-postgres-cdc"

    # Ensure clean state
    debezium_client.ensure_connector_deleted(connector_name)

    # Create connector
    config = {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "tasks.max": "1",
        "database.hostname": "postgres",
        "database.port": "5432",
        "database.user": "cdcuser",
        "database.password": "cdcpass",
        "database.dbname": "cdcdb",
        "topic.prefix": "test",
        "plugin.name": "pgoutput",
    }

    result = debezium_client.create_connector(connector_name, config)
    assert result["name"] == connector_name

    # Wait for connector to start
    assert debezium_client.wait_for_connector_running(connector_name, timeout=30)

    # Verify status
    status = debezium_client.get_connector_status(connector_name)
    assert status["connector"]["state"] == "RUNNING"

    # Cleanup
    debezium_client.delete_connector(connector_name)
```

---

## 5. Test Fixture Best Practices

### Decision
- **Infrastructure Services:** Session-scoped fixtures
- **Database Connections:** Function-scoped fixtures
- **Test Data:** Function-scoped fixtures with cleanup
- **Health Checks:** Retry-based wait strategies (not fixed sleeps)
- **Service Dependencies:** Use pytest dependency injection and `depends_on` with health conditions

### Rationale
- Session-scoped infrastructure fixtures reduce test execution time by 70-90%
- Function-scoped data fixtures ensure test isolation and prevent test pollution
- Retry-based health checks are more reliable than fixed sleep delays
- Proper fixture scopes respect the pytest scope hierarchy (function < session)

### Alternatives Considered
- **All function-scoped fixtures:** Significantly slower (2-5x), but provides maximum isolation
- **Module-scoped infrastructure:** Middle ground, but introduces complexity with cleanup
- **Fixed sleep delays:** Simpler but unreliable; can be too short (flaky tests) or too long (slow tests)
- **testcontainers with default scope:** Less control over lifecycle, harder to debug

### Configuration Details

#### Pytest Fixture Scope Recommendations

| Fixture Type | Scope | Rationale | Example |
|--------------|-------|-----------|---------|
| Docker services | `session` | Expensive to start/stop | `kafka_service`, `postgres_service` |
| Service clients | `session` | Stateless, reusable | `kafka_admin_client`, `schema_registry_client` |
| Database connections | `function` | Need fresh connection per test | `postgres_connection` |
| Test data | `function` | Ensure isolation | `test_user`, `delta_table_path` |
| Configuration | `session` | Read-only, shared | `kafka_config`, `postgres_credentials` |

#### Service Health Check Patterns

**Recommended Approach: Retry with Exponential Backoff**
```python
import time
import httpx
from typing import Callable, Any

def wait_for_service(
    check_fn: Callable[[], bool],
    service_name: str,
    timeout: int = 60,
    interval: int = 1,
    backoff: float = 1.5,
    max_interval: int = 10
) -> None:
    """
    Wait for service to become healthy using exponential backoff.

    Args:
        check_fn: Function that returns True when service is healthy
        service_name: Name of service for logging
        timeout: Maximum time to wait (seconds)
        interval: Initial interval between checks (seconds)
        backoff: Backoff multiplier for interval
        max_interval: Maximum interval between checks (seconds)
    """
    start = time.time()
    current_interval = interval

    while time.time() - start < timeout:
        try:
            if check_fn():
                elapsed = time.time() - start
                print(f"{service_name} is ready (took {elapsed:.2f}s)")
                return
        except Exception as e:
            # Service not ready yet
            pass

        time.sleep(current_interval)
        current_interval = min(current_interval * backoff, max_interval)

    raise TimeoutError(
        f"{service_name} did not become healthy within {timeout}s"
    )

# Example usage
def kafka_health_check() -> bool:
    """Check if Kafka is healthy."""
    from kafka import KafkaAdminClient
    try:
        admin = KafkaAdminClient(
            bootstrap_servers="localhost:29092",
            request_timeout_ms=5000
        )
        admin.close()
        return True
    except Exception:
        return False

wait_for_service(kafka_health_check, "Kafka", timeout=120)
```

**HTTP Service Health Check:**
```python
def http_health_check(url: str, expected_status: int = 200) -> Callable[[], bool]:
    """Create health check function for HTTP service."""
    def check() -> bool:
        try:
            response = httpx.get(url, timeout=5.0)
            return response.status_code == expected_status
        except Exception:
            return False
    return check

# Usage
wait_for_service(
    http_health_check("http://localhost:8083"),
    "Debezium Connect",
    timeout=90
)
```

**Database Health Check:**
```python
import psycopg2

def postgres_health_check(credentials: dict) -> Callable[[], bool]:
    """Create health check function for PostgreSQL."""
    def check() -> bool:
        try:
            conn = psycopg2.connect(**credentials, connect_timeout=5)
            conn.close()
            return True
        except Exception:
            return False
    return check

wait_for_service(
    postgres_health_check({
        "host": "localhost",
        "port": 5432,
        "database": "cdcdb",
        "user": "cdcuser",
        "password": "cdcpass"
    }),
    "PostgreSQL",
    timeout=60
)
```

#### Session-Scoped Infrastructure Fixtures

**Pattern: Session Fixture with Cleanup**
```python
import pytest
import httpx
from typing import Generator

@pytest.fixture(scope="session")
def kafka_admin_client() -> Generator:
    """
    Kafka admin client for test session.

    Session-scoped because:
    - Client is stateless
    - No per-test state to reset
    - Expensive to create connection
    """
    from kafka import KafkaAdminClient

    # Wait for Kafka to be ready
    wait_for_service(kafka_health_check, "Kafka", timeout=120)

    # Create client
    admin = KafkaAdminClient(
        bootstrap_servers="localhost:29092",
        client_id="pytest-admin"
    )

    yield admin

    # Cleanup: close connection
    admin.close()


@pytest.fixture(scope="session")
def schema_registry_client() -> Generator:
    """
    Schema Registry client for test session.

    Session-scoped because:
    - Client is stateless
    - Shared across all tests
    - Service is read-heavy, writes are isolated by subject names
    """
    base_url = "http://localhost:8081"

    # Wait for Schema Registry to be ready
    wait_for_service(
        http_health_check(f"{base_url}/subjects"),
        "Schema Registry",
        timeout=60
    )

    client = httpx.Client(base_url=base_url, timeout=30.0)

    yield client

    # Cleanup
    client.close()


@pytest.fixture(scope="session")
def spark_session() -> Generator:
    """
    Spark session for Delta Lake tests.

    Session-scoped because:
    - Very expensive to create (5-10 seconds)
    - Stateless for most operations
    - Can reuse across tests if table paths are isolated
    """
    from pyspark.sql import SparkSession

    # Wait for Spark master to be ready
    wait_for_service(
        http_health_check("http://localhost:8080"),
        "Spark Master",
        timeout=90
    )

    spark = (
        SparkSession.builder
        .appName("CDC-Demo-Tests")
        .master("spark://localhost:7077")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.3.2")
        .getOrCreate()
    )

    yield spark

    # Cleanup
    spark.stop()
```

#### Function-Scoped Data Fixtures

**Pattern: Function Fixture with Automatic Cleanup**
```python
import pytest
import uuid
import shutil
from pathlib import Path
from typing import Generator

@pytest.fixture
def postgres_connection(postgres_credentials: dict) -> Generator:
    """
    PostgreSQL connection for single test.

    Function-scoped because:
    - Connection state can be modified by tests
    - Ensures fresh connection per test
    - Proper transaction isolation
    """
    import psycopg2

    conn = psycopg2.connect(**postgres_credentials)
    conn.autocommit = True  # For DDL operations

    yield conn

    # Cleanup
    conn.close()


@pytest.fixture
def test_kafka_topic(kafka_admin_client) -> Generator[str, None, None]:
    """
    Unique Kafka topic for single test.

    Function-scoped because:
    - Each test needs isolated topic
    - Topic names must be unique
    - Cleanup prevents topic buildup
    """
    from kafka.admin import NewTopic

    topic_name = f"test_{uuid.uuid4().hex[:8]}"

    # Create topic
    topic = NewTopic(
        name=topic_name,
        num_partitions=1,
        replication_factor=1
    )
    kafka_admin_client.create_topics([topic])

    yield topic_name

    # Cleanup: delete topic
    try:
        kafka_admin_client.delete_topics([topic_name])
    except Exception:
        pass  # Ignore cleanup errors


@pytest.fixture
def delta_table_path(tmp_path: Path) -> Generator[str, None, None]:
    """
    Unique Delta table path for single test.

    Function-scoped because:
    - Each test needs isolated table
    - Prevents test data pollution
    - Automatic cleanup via tmp_path
    """
    table_name = f"test_{uuid.uuid4().hex[:8]}"
    table_path = tmp_path / "delta-tables" / table_name
    table_path.mkdir(parents=True, exist_ok=True)

    yield str(table_path)

    # Cleanup (tmp_path handles this automatically, but explicit is better)
    if table_path.exists():
        shutil.rmtree(table_path, ignore_errors=True)


@pytest.fixture
def test_postgres_table(postgres_connection) -> Generator[str, None, None]:
    """
    Temporary PostgreSQL table for single test.

    Function-scoped because:
    - Each test needs isolated table
    - Schema changes don't affect other tests
    - Cleanup ensures no leftover data
    """
    table_name = f"test_{uuid.uuid4().hex[:8]}"

    cursor = postgres_connection.cursor()
    cursor.execute(f"""
        CREATE TABLE {table_name} (
            id SERIAL PRIMARY KEY,
            name TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cursor.close()

    yield table_name

    # Cleanup
    cursor = postgres_connection.cursor()
    cursor.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE")
    cursor.close()
```

#### Cleanup Strategies for Stateful Services

**Strategy 1: Explicit Cleanup in Fixture**
```python
@pytest.fixture
def debezium_connector(debezium_client) -> Generator[str, None, None]:
    """Debezium connector with automatic cleanup."""
    connector_name = f"test_{uuid.uuid4().hex[:8]}"

    # Ensure clean state before test
    debezium_client.ensure_connector_deleted(connector_name)

    yield connector_name

    # Cleanup after test
    try:
        debezium_client.delete_connector(connector_name)
    except Exception:
        pass  # Ignore cleanup errors
```

**Strategy 2: pytest finalizer**
```python
@pytest.fixture
def schema_subject(schema_registry_client, request) -> str:
    """Schema Registry subject with cleanup via finalizer."""
    subject_name = f"test-{uuid.uuid4().hex[:8]}-value"

    def cleanup():
        try:
            # Delete subject (soft delete)
            schema_registry_client.delete(f"/subjects/{subject_name}")
            # Hard delete
            schema_registry_client.delete(f"/subjects/{subject_name}?permanent=true")
        except Exception:
            pass

    request.addfinalizer(cleanup)

    return subject_name
```

**Strategy 3: Context Manager for Cleanup**
```python
from contextlib import contextmanager
from typing import Generator

@contextmanager
def temporary_kafka_topic(
    admin_client,
    topic_name: str
) -> Generator[str, None, None]:
    """Context manager for temporary Kafka topic."""
    from kafka.admin import NewTopic

    # Create topic
    topic = NewTopic(name=topic_name, num_partitions=1, replication_factor=1)
    admin_client.create_topics([topic])

    try:
        yield topic_name
    finally:
        # Cleanup
        try:
            admin_client.delete_topics([topic_name])
        except Exception:
            pass

# Usage in test
def test_with_topic(kafka_admin_client):
    with temporary_kafka_topic(kafka_admin_client, "test-topic") as topic:
        # Test code using topic
        pass
    # Topic automatically deleted
```

#### Complete Example: Combining Fixtures

```python
import pytest
from typing import Generator
import uuid

# Session-scoped: Infrastructure
@pytest.fixture(scope="session")
def kafka_admin_client():
    """Kafka admin client (session scope)."""
    from kafka import KafkaAdminClient
    wait_for_service(kafka_health_check, "Kafka", timeout=120)
    admin = KafkaAdminClient(bootstrap_servers="localhost:29092")
    yield admin
    admin.close()

@pytest.fixture(scope="session")
def spark_session():
    """Spark session (session scope)."""
    from pyspark.sql import SparkSession
    wait_for_service(http_health_check("http://localhost:8080"), "Spark", timeout=90)
    spark = SparkSession.builder.appName("Tests").getOrCreate()
    yield spark
    spark.stop()

# Function-scoped: Test data
@pytest.fixture
def kafka_topic(kafka_admin_client) -> Generator[str, None, None]:
    """Temporary Kafka topic (function scope)."""
    from kafka.admin import NewTopic
    topic_name = f"test_{uuid.uuid4().hex[:8]}"
    topic = NewTopic(name=topic_name, num_partitions=1, replication_factor=1)
    kafka_admin_client.create_topics([topic])
    yield topic_name
    try:
        kafka_admin_client.delete_topics([topic_name])
    except Exception:
        pass

@pytest.fixture
def delta_table(spark_session, tmp_path) -> Generator[str, None, None]:
    """Temporary Delta table (function scope)."""
    table_path = tmp_path / f"delta_{uuid.uuid4().hex[:8]}"
    table_path.mkdir(parents=True)
    yield str(table_path)
    # tmp_path cleanup is automatic

# Test using both fixtures
def test_kafka_to_delta(kafka_topic, delta_table, spark_session):
    """Test CDC from Kafka to Delta Lake."""
    # kafka_topic is unique to this test
    # delta_table is unique to this test
    # spark_session is shared (session scope)

    # Test implementation...
    pass
```

#### Recommended conftest.py Structure

```python
"""
Pytest configuration and shared fixtures for CDC demo tests.

Fixture Scopes:
- session: Infrastructure (Kafka, Spark, Schema Registry, Debezium)
- function: Test data (topics, tables, connectors, connections)
"""

import os
import pytest
import time
import httpx
from typing import Generator, Callable
from pathlib import Path
import uuid

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def wait_for_service(
    check_fn: Callable[[], bool],
    service_name: str,
    timeout: int = 60
) -> None:
    """Wait for service to become healthy."""
    start = time.time()
    while time.time() - start < timeout:
        try:
            if check_fn():
                return
        except Exception:
            pass
        time.sleep(1)
    raise TimeoutError(f"{service_name} not ready within {timeout}s")

# ============================================================================
# SESSION-SCOPED: Infrastructure Services
# ============================================================================

@pytest.fixture(scope="session")
def postgres_credentials():
    """PostgreSQL credentials from environment."""
    return {
        "host": os.getenv("POSTGRES_HOST", "localhost"),
        "port": int(os.getenv("POSTGRES_PORT", "5432")),
        "database": os.getenv("POSTGRES_DB", "cdcdb"),
        "user": os.getenv("POSTGRES_USER", "cdcuser"),
        "password": os.getenv("POSTGRES_PASSWORD", "cdcpass"),
    }

@pytest.fixture(scope="session")
def kafka_admin_client():
    """Kafka admin client for session."""
    from kafka import KafkaAdminClient

    def check():
        try:
            admin = KafkaAdminClient(
                bootstrap_servers="localhost:29092",
                request_timeout_ms=5000
            )
            admin.close()
            return True
        except Exception:
            return False

    wait_for_service(check, "Kafka", timeout=120)

    admin = KafkaAdminClient(bootstrap_servers="localhost:29092")
    yield admin
    admin.close()

@pytest.fixture(scope="session")
def spark_session():
    """Spark session for Delta Lake tests."""
    from pyspark.sql import SparkSession

    def check():
        try:
            response = httpx.get("http://localhost:8080", timeout=5.0)
            return response.status_code == 200
        except Exception:
            return False

    wait_for_service(check, "Spark Master", timeout=90)

    spark = (
        SparkSession.builder
        .appName("CDC-Demo-Tests")
        .master("spark://localhost:7077")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.3.2")
        .getOrCreate()
    )

    yield spark
    spark.stop()

@pytest.fixture(scope="session")
def schema_registry_client():
    """Schema Registry HTTP client for session."""
    base_url = "http://localhost:8081"

    def check():
        try:
            response = httpx.get(f"{base_url}/subjects", timeout=5.0)
            return response.status_code == 200
        except Exception:
            return False

    wait_for_service(check, "Schema Registry", timeout=60)

    client = httpx.Client(base_url=base_url, timeout=30.0)
    yield client
    client.close()

@pytest.fixture(scope="session")
def debezium_client():
    """Debezium Connect HTTP client for session."""
    base_url = "http://localhost:8083"

    def check():
        try:
            response = httpx.get(base_url, timeout=5.0)
            return response.status_code == 200
        except Exception:
            return False

    wait_for_service(check, "Debezium Connect", timeout=90)

    from debezium_client import DebeziumClient
    client = DebeziumClient(base_url)
    yield client
    client.client.close()

# ============================================================================
# FUNCTION-SCOPED: Test Data and Connections
# ============================================================================

@pytest.fixture
def postgres_connection(postgres_credentials):
    """Fresh PostgreSQL connection per test."""
    import psycopg2
    conn = psycopg2.connect(**postgres_credentials)
    yield conn
    conn.close()

@pytest.fixture
def kafka_topic(kafka_admin_client) -> Generator[str, None, None]:
    """Unique Kafka topic per test with cleanup."""
    from kafka.admin import NewTopic

    topic_name = f"test_{uuid.uuid4().hex[:8]}"
    topic = NewTopic(name=topic_name, num_partitions=1, replication_factor=1)
    kafka_admin_client.create_topics([topic])

    yield topic_name

    try:
        kafka_admin_client.delete_topics([topic_name])
    except Exception:
        pass

@pytest.fixture
def delta_table_path(tmp_path: Path) -> Generator[str, None, None]:
    """Unique Delta table path per test with cleanup."""
    table_name = f"test_{uuid.uuid4().hex[:8]}"
    table_path = tmp_path / "delta-tables" / table_name
    table_path.mkdir(parents=True, exist_ok=True)

    yield str(table_path)

    # tmp_path handles cleanup automatically

@pytest.fixture
def iceberg_namespace(iceberg_catalog) -> Generator[str, None, None]:
    """Unique Iceberg namespace per test with cleanup."""
    namespace = f"test_{uuid.uuid4().hex[:8]}"
    iceberg_catalog.create_namespace(namespace)

    yield namespace

    try:
        iceberg_catalog.drop_namespace(namespace)
    except Exception:
        pass
```

---

## Summary of Key Decisions

### Infrastructure Versions
| Component | Version | Image/Package |
|-----------|---------|---------------|
| Apache Iceberg REST Catalog | latest | `tabulario/iceberg-rest:latest` |
| PyIceberg | 0.10.0+ | `pyiceberg ^0.10.0` |
| Delta Lake | 3.3.2 | `io.delta:delta-spark_2.12:3.3.2` |
| Spark | 3.5.0 | `apache/spark:3.5.0` |
| Schema Registry | 7.5.0 | `confluentinc/cp-schema-registry:7.5.0` |
| Kafka | 7.5.0 | `confluentinc/cp-kafka:7.5.0` (existing) |
| Debezium Connect | 2.x | Custom build (existing) |

### Port Assignments
| Service | Port | Purpose |
|---------|------|---------|
| Iceberg REST Catalog | 8181 | REST API |
| Schema Registry | 8081 | REST API |
| Debezium Connect | 8083 | Kafka Connect REST API |
| MinIO API | 9000 | S3 API |
| MinIO Console | 9001 | Web UI |
| Spark Master | 8080 | Web UI |
| Spark Master | 7077 | Spark protocol |

### Fixture Scope Strategy
- **Session:** `kafka_admin_client`, `spark_session`, `schema_registry_client`, `debezium_client`, `postgres_credentials`
- **Function:** `postgres_connection`, `kafka_topic`, `delta_table_path`, `iceberg_namespace`, `debezium_connector`

### Health Check Endpoints
| Service | Health Check URL | Expected Response |
|---------|------------------|-------------------|
| Iceberg REST | `http://localhost:8181/v1/config` | 200 OK |
| Schema Registry | `http://localhost:8081/subjects` | 200 OK |
| Debezium Connect | `http://localhost:8083/` | 200 OK |
| MinIO | `http://localhost:9000/minio/health/live` | 200 OK |
| Spark Master | `http://localhost:8080/` | 200 OK |

---

## Implementation Checklist

- [ ] Update `pyproject.toml` with PyIceberg 0.10.0 and Delta Spark 3.3.2
- [ ] Add Iceberg REST Catalog service to `docker-compose.yml`
- [ ] Add Schema Registry service to `docker-compose.yml`
- [ ] Create MinIO bucket initialization script or fixture
- [ ] Implement `DebeziumClient` class in `src/clients/debezium_client.py`
- [ ] Update `tests/conftest.py` with session-scoped infrastructure fixtures
- [ ] Add function-scoped data fixtures (topics, tables, connectors)
- [ ] Implement `wait_for_service()` helper with exponential backoff
- [ ] Create health check functions for all services
- [ ] Add pytest markers for integration tests requiring specific infrastructure
- [ ] Document environment variables in `.env.example`
- [ ] Update test documentation with fixture usage examples

---

## References

### Official Documentation
- PyIceberg: https://py.iceberg.apache.org/
- Delta Lake: https://docs.delta.io/
- Confluent Schema Registry: https://docs.confluent.io/platform/current/schema-registry/
- Debezium: https://debezium.io/documentation/
- Kafka Connect REST API: https://docs.confluent.io/platform/current/connect/references/restapi.html
- pytest fixtures: https://docs.pytest.org/en/stable/how-to/fixtures.html

### Docker Images
- tabulario/iceberg-rest: https://hub.docker.com/r/tabulario/iceberg-rest
- confluentinc/cp-schema-registry: https://hub.docker.com/r/confluentinc/cp-schema-registry
- apache/spark: https://hub.docker.com/r/apache/spark

### Maven Coordinates
- Delta Lake 3.3.2: `io.delta:delta-spark_2.12:3.3.2`
- Delta Lake 3.0.0: `io.delta:delta-spark_2.12:3.0.0`

### Python Packages
- PyIceberg: `pip install pyiceberg[s3fs,pyarrow]`
- Delta Spark: `pip install delta-spark`
- confluent-kafka: `pip install confluent-kafka`
