# Test Infrastructure Expansion - Data Models

**Feature:** Test Infrastructure Expansion
**Date:** 2025-10-29
**Status:** Phase 1 Design

---

## Overview

This document defines the data models, configuration structures, and state transitions for the expanded test infrastructure including Apache Iceberg REST Catalog, Schema Registry, enhanced Delta Lake integration, and Debezium connector management.

---

## 1. Service Configuration Models

### 1.1 Iceberg REST Catalog Service

**Docker Compose Service Definition:**

```yaml
iceberg-rest:
  image: tabulario/iceberg-rest:latest
  container_name: cdc-iceberg-rest
  ports:
    - "8181:8181"
  environment:
    AWS_ACCESS_KEY_ID: string         # MinIO access key
    AWS_SECRET_ACCESS_KEY: string     # MinIO secret key
    AWS_REGION: string                # S3 region (us-east-1)
    CATALOG_WAREHOUSE: string         # S3 warehouse path (s3://warehouse/)
    CATALOG_IO__IMPL: string          # S3FileIO implementation
    CATALOG_S3_ENDPOINT: string       # MinIO endpoint URL
    CATALOG_S3_PATH__STYLE__ACCESS: boolean  # Path-style access (true)
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

**Configuration Properties:**

| Property | Type | Required | Default | Description |
|----------|------|----------|---------|-------------|
| `AWS_ACCESS_KEY_ID` | string | Yes | - | MinIO access credentials |
| `AWS_SECRET_ACCESS_KEY` | string | Yes | - | MinIO secret credentials |
| `AWS_REGION` | string | No | us-east-1 | S3 region identifier |
| `CATALOG_WAREHOUSE` | string | Yes | - | S3 warehouse path |
| `CATALOG_IO__IMPL` | string | Yes | - | S3FileIO implementation class |
| `CATALOG_S3_ENDPOINT` | string | Yes | - | MinIO endpoint for S3 operations |
| `CATALOG_S3_PATH__STYLE__ACCESS` | boolean | Yes | - | Enable path-style S3 access |

### 1.2 Schema Registry Service

**Docker Compose Service Definition:**

```yaml
schema-registry:
  image: confluentinc/cp-schema-registry:7.5.0
  container_name: cdc-schema-registry
  depends_on:
    kafka:
      condition: service_healthy
  environment:
    SCHEMA_REGISTRY_HOST_NAME: string              # Hostname (schema-registry)
    SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS: string  # Kafka brokers
    SCHEMA_REGISTRY_LISTENERS: string              # HTTP listeners
    SCHEMA_REGISTRY_SCHEMA_COMPATIBILITY_LEVEL: string   # Compatibility mode
    SCHEMA_REGISTRY_DEBUG: boolean                 # Debug logging
    SCHEMA_REGISTRY_KAFKASTORE_TOPIC: string       # Schema storage topic
    SCHEMA_REGISTRY_KAFKASTORE_TOPIC_REPLICATION_FACTOR: integer  # Replication
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

**Configuration Properties:**

| Property | Type | Required | Default | Description |
|----------|------|----------|---------|-------------|
| `SCHEMA_REGISTRY_HOST_NAME` | string | Yes | - | Hostname for registry instance |
| `SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS` | string | Yes | - | Kafka bootstrap servers |
| `SCHEMA_REGISTRY_LISTENERS` | string | Yes | - | HTTP listener URLs |
| `SCHEMA_REGISTRY_SCHEMA_COMPATIBILITY_LEVEL` | string | No | BACKWARD | Schema compatibility mode |
| `SCHEMA_REGISTRY_DEBUG` | boolean | No | false | Enable debug logging |
| `SCHEMA_REGISTRY_KAFKASTORE_TOPIC` | string | No | _schemas | Schema storage topic name |
| `SCHEMA_REGISTRY_KAFKASTORE_TOPIC_REPLICATION_FACTOR` | integer | No | 1 | Topic replication factor |

**Compatibility Modes:**
- `BACKWARD`: New schema can read old data
- `FORWARD`: Old schema can read new data
- `FULL`: Both backward and forward compatible
- `NONE`: No compatibility checks
- `BACKWARD_TRANSITIVE`: Backward with all previous versions
- `FORWARD_TRANSITIVE`: Forward with all previous versions
- `FULL_TRANSITIVE`: Full with all previous versions

### 1.3 Enhanced Spark Service (Delta Lake)

**Spark Session Configuration Model:**

```python
DELTA_SPARK_CONFIG = {
    # Core Delta Lake extensions
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",

    # Delta Lake packages
    "spark.jars.packages": "io.delta:delta-spark_2.12:3.3.2",

    # Change Data Feed (CDF) default
    "spark.databricks.delta.properties.defaults.enableChangeDataFeed": "true",

    # Warehouse configuration
    "spark.sql.warehouse.dir": "/tmp/spark-warehouse",

    # S3A configuration for MinIO
    "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
    "spark.hadoop.fs.s3a.access.key": "minioadmin",
    "spark.hadoop.fs.s3a.secret.key": "minioadmin",
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",

    # Memory settings
    "spark.driver.memory": "1g",
    "spark.executor.memory": "1g",
}
```

**Configuration Properties:**

| Property | Type | Required | Default | Description |
|----------|------|----------|---------|-------------|
| `spark.sql.extensions` | string | Yes | - | Delta Spark session extension |
| `spark.sql.catalog.spark_catalog` | string | Yes | - | Delta catalog implementation |
| `spark.jars.packages` | string | Yes | - | Delta Lake Maven coordinates |
| `spark.databricks.delta.properties.defaults.enableChangeDataFeed` | boolean | No | false | Enable CDF by default |
| `spark.hadoop.fs.s3a.endpoint` | string | Yes | - | MinIO S3 endpoint |
| `spark.hadoop.fs.s3a.access.key` | string | Yes | - | MinIO access key |
| `spark.hadoop.fs.s3a.secret.key` | string | Yes | - | MinIO secret key |
| `spark.hadoop.fs.s3a.path.style.access` | boolean | Yes | - | Path-style S3 access |

---

## 2. Test Fixture Data Models

### 2.1 IcebergCatalogConfig

**Python Data Model:**

```python
from dataclasses import dataclass
from typing import Optional

@dataclass
class IcebergCatalogConfig:
    """Configuration for Iceberg REST catalog connection."""

    # REST catalog endpoint
    uri: str = "http://localhost:8181"

    # Warehouse location
    warehouse: str = "s3://warehouse/"

    # S3 configuration
    s3_endpoint: str = "http://localhost:9000"
    s3_access_key_id: str = "minioadmin"
    s3_secret_access_key: str = "minioadmin"
    s3_region: str = "us-east-1"
    s3_path_style_access: bool = True

    # Connection settings
    timeout_ms: int = 30000
    retry_attempts: int = 3

    def to_catalog_properties(self) -> dict[str, str]:
        """Convert to PyIceberg catalog properties."""
        return {
            "type": "rest",
            "uri": self.uri,
            "warehouse": self.warehouse,
            "s3.endpoint": self.s3_endpoint,
            "s3.access-key-id": self.s3_access_key_id,
            "s3.secret-access-key": self.s3_secret_access_key,
            "s3.region": self.s3_region,
            "s3.path-style-access": str(self.s3_path_style_access).lower(),
        }
```

**Properties:**

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `uri` | string | Yes | http://localhost:8181 | REST catalog endpoint |
| `warehouse` | string | Yes | s3://warehouse/ | Warehouse S3 path |
| `s3_endpoint` | string | Yes | http://localhost:9000 | MinIO endpoint |
| `s3_access_key_id` | string | Yes | minioadmin | MinIO access key |
| `s3_secret_access_key` | string | Yes | minioadmin | MinIO secret key |
| `s3_region` | string | No | us-east-1 | S3 region |
| `s3_path_style_access` | boolean | Yes | true | Path-style access |
| `timeout_ms` | integer | No | 30000 | Connection timeout |
| `retry_attempts` | integer | No | 3 | Retry attempts |

### 2.2 DeltaSparkConfig

**Python Data Model:**

```python
from dataclasses import dataclass, field
from typing import Dict

@dataclass
class DeltaSparkConfig:
    """Configuration for Delta Lake with Spark."""

    # Application settings
    app_name: str = "CDC-Demo-Tests"
    master: str = "spark://localhost:7077"

    # Delta Lake version
    delta_version: str = "3.3.2"
    scala_version: str = "2.12"

    # Change Data Feed settings
    enable_cdf: bool = True

    # S3/MinIO settings
    s3_endpoint: str = "http://localhost:9000"
    s3_access_key: str = "minioadmin"
    s3_secret_key: str = "minioadmin"
    s3_path_style_access: bool = True

    # Memory settings
    driver_memory: str = "1g"
    executor_memory: str = "1g"

    # Additional configuration
    extra_config: Dict[str, str] = field(default_factory=dict)

    def to_spark_config(self) -> Dict[str, str]:
        """Convert to Spark configuration dictionary."""
        config = {
            # Delta Lake core
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            "spark.jars.packages": f"io.delta:delta-spark_{self.scala_version}:{self.delta_version}",

            # Change Data Feed
            "spark.databricks.delta.properties.defaults.enableChangeDataFeed": str(self.enable_cdf).lower(),

            # S3A configuration
            "spark.hadoop.fs.s3a.endpoint": self.s3_endpoint,
            "spark.hadoop.fs.s3a.access.key": self.s3_access_key,
            "spark.hadoop.fs.s3a.secret.key": self.s3_secret_key,
            "spark.hadoop.fs.s3a.path.style.access": str(self.s3_path_style_access).lower(),
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",

            # Memory
            "spark.driver.memory": self.driver_memory,
            "spark.executor.memory": self.executor_memory,
        }

        # Merge extra configuration
        config.update(self.extra_config)

        return config
```

**Properties:**

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `app_name` | string | No | CDC-Demo-Tests | Spark application name |
| `master` | string | Yes | spark://localhost:7077 | Spark master URL |
| `delta_version` | string | No | 3.3.2 | Delta Lake version |
| `scala_version` | string | No | 2.12 | Scala version |
| `enable_cdf` | boolean | No | true | Enable Change Data Feed |
| `s3_endpoint` | string | Yes | http://localhost:9000 | MinIO endpoint |
| `s3_access_key` | string | Yes | minioadmin | S3 access key |
| `s3_secret_key` | string | Yes | minioadmin | S3 secret key |
| `s3_path_style_access` | boolean | Yes | true | Path-style access |
| `driver_memory` | string | No | 1g | Driver memory allocation |
| `executor_memory` | string | No | 1g | Executor memory allocation |
| `extra_config` | dict | No | {} | Additional Spark config |

### 2.3 SchemaRegistryConfig

**Python Data Model:**

```python
from dataclasses import dataclass
from enum import Enum

class CompatibilityMode(Enum):
    """Schema Registry compatibility modes."""
    BACKWARD = "BACKWARD"
    FORWARD = "FORWARD"
    FULL = "FULL"
    NONE = "NONE"
    BACKWARD_TRANSITIVE = "BACKWARD_TRANSITIVE"
    FORWARD_TRANSITIVE = "FORWARD_TRANSITIVE"
    FULL_TRANSITIVE = "FULL_TRANSITIVE"

@dataclass
class SchemaRegistryConfig:
    """Configuration for Schema Registry connection."""

    # Connection settings
    url: str = "http://localhost:8081"

    # Compatibility settings
    compatibility_mode: CompatibilityMode = CompatibilityMode.BACKWARD

    # Request settings
    timeout_seconds: int = 30
    max_retries: int = 3

    # Auth settings (optional)
    username: str | None = None
    password: str | None = None

    def to_client_config(self) -> dict[str, str]:
        """Convert to Schema Registry client configuration."""
        config = {
            "url": self.url,
        }

        if self.username and self.password:
            config["basic.auth.credentials.source"] = "USER_INFO"
            config["basic.auth.user.info"] = f"{self.username}:{self.password}"

        return config
```

**Properties:**

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `url` | string | Yes | http://localhost:8081 | Schema Registry URL |
| `compatibility_mode` | enum | No | BACKWARD | Default compatibility mode |
| `timeout_seconds` | integer | No | 30 | HTTP request timeout |
| `max_retries` | integer | No | 3 | Max retry attempts |
| `username` | string | No | None | Basic auth username |
| `password` | string | No | None | Basic auth password |

### 2.4 DebeziumConnectorConfig

**Python Data Model:**

```python
from dataclasses import dataclass, field
from typing import Dict, List
from enum import Enum

class SnapshotMode(Enum):
    """Debezium snapshot modes."""
    INITIAL = "initial"
    ALWAYS = "always"
    NEVER = "never"
    WHEN_NEEDED = "when_needed"
    INITIAL_ONLY = "initial_only"

class PluginName(Enum):
    """PostgreSQL logical decoding plugin."""
    PGOUTPUT = "pgoutput"
    WAL2JSON = "wal2json"
    DECODERBUFS = "decoderbufs"

@dataclass
class DebeziumConnectorConfig:
    """Configuration for Debezium PostgreSQL connector."""

    # Connector identity
    name: str

    # Database connection
    database_hostname: str = "postgres"
    database_port: int = 5432
    database_user: str = "cdcuser"
    database_password: str = "cdcpass"
    database_dbname: str = "cdcdb"

    # Topic configuration
    topic_prefix: str = "postgres.cdc"

    # PostgreSQL plugin
    plugin_name: PluginName = PluginName.PGOUTPUT

    # Replication configuration
    slot_name: str = "debezium_slot"
    publication_name: str = "debezium_publication"

    # Table filtering
    table_include_list: List[str] = field(default_factory=list)
    schema_include_list: List[str] = field(default_factory=lambda: ["public"])

    # Snapshot settings
    snapshot_mode: SnapshotMode = SnapshotMode.INITIAL

    # Heartbeat
    heartbeat_interval_ms: int = 10000

    # Data type handling
    decimal_handling_mode: str = "precise"
    time_precision_mode: str = "adaptive_time_microseconds"

    # Converter configuration
    key_converter: str = "org.apache.kafka.connect.json.JsonConverter"
    value_converter: str = "org.apache.kafka.connect.json.JsonConverter"
    key_converter_schemas_enable: bool = False
    value_converter_schemas_enable: bool = False

    # Tasks
    tasks_max: int = 1

    # Additional config
    extra_config: Dict[str, str] = field(default_factory=dict)

    def to_connector_payload(self) -> dict:
        """Convert to Kafka Connect REST API payload."""
        config = {
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
            "tasks.max": str(self.tasks_max),

            # Database
            "database.hostname": self.database_hostname,
            "database.port": str(self.database_port),
            "database.user": self.database_user,
            "database.password": self.database_password,
            "database.dbname": self.database_dbname,

            # Topics
            "topic.prefix": self.topic_prefix,

            # Plugin
            "plugin.name": self.plugin_name.value,

            # Replication
            "slot.name": self.slot_name,
            "publication.name": self.publication_name,

            # Snapshot
            "snapshot.mode": self.snapshot_mode.value,

            # Heartbeat
            "heartbeat.interval.ms": str(self.heartbeat_interval_ms),

            # Data types
            "decimal.handling.mode": self.decimal_handling_mode,
            "time.precision.mode": self.time_precision_mode,

            # Converters
            "key.converter": self.key_converter,
            "key.converter.schemas.enable": str(self.key_converter_schemas_enable).lower(),
            "value.converter": self.value_converter,
            "value.converter.schemas.enable": str(self.value_converter_schemas_enable).lower(),
        }

        # Table filters
        if self.table_include_list:
            config["table.include.list"] = ",".join(self.table_include_list)

        if self.schema_include_list:
            config["schema.include.list"] = ",".join(self.schema_include_list)

        # Merge extra config
        config.update(self.extra_config)

        return {
            "name": self.name,
            "config": config
        }
```

**Properties:**

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `name` | string | Yes | - | Connector name |
| `database_hostname` | string | Yes | postgres | PostgreSQL hostname |
| `database_port` | integer | Yes | 5432 | PostgreSQL port |
| `database_user` | string | Yes | cdcuser | Database user |
| `database_password` | string | Yes | cdcpass | Database password |
| `database_dbname` | string | Yes | cdcdb | Database name |
| `topic_prefix` | string | Yes | postgres.cdc | Kafka topic prefix |
| `plugin_name` | enum | No | pgoutput | Logical decoding plugin |
| `slot_name` | string | No | debezium_slot | Replication slot name |
| `publication_name` | string | No | debezium_publication | Publication name |
| `table_include_list` | list[string] | No | [] | Tables to capture |
| `schema_include_list` | list[string] | No | [public] | Schemas to capture |
| `snapshot_mode` | enum | No | initial | Snapshot mode |
| `heartbeat_interval_ms` | integer | No | 10000 | Heartbeat interval |
| `tasks_max` | integer | No | 1 | Max connector tasks |

---

## 3. State Transitions

### 3.1 Service Lifecycle

All infrastructure services follow this lifecycle:

```
┌─────────┐
│ STOPPED │
└────┬────┘
     │ docker compose up
     ▼
┌──────────┐
│ STARTING │ ◄──────────┐
└────┬─────┘            │
     │ healthcheck      │ retry (max 5)
     │ succeeds         │
     ▼                  │
┌─────────┐    fail     │
│ HEALTHY ├─────────────┘
└────┬────┘
     │ tests run
     ▼
┌─────────┐
│ RUNNING │
└────┬────┘
     │ docker compose down
     ▼
┌──────────┐
│ STOPPING │
└────┬─────┘
     │
     ▼
┌─────────┐
│ STOPPED │
└─────────┘
```

**States:**

| State | Description | Health Check | Actions |
|-------|-------------|--------------|---------|
| `STOPPED` | Service not running | N/A | Can start service |
| `STARTING` | Container starting, initializing | Pending | Retry health check |
| `HEALTHY` | Health check passed | Pass | Ready for connections |
| `RUNNING` | Actively serving requests | Pass | Normal operation |
| `STOPPING` | Graceful shutdown in progress | Fail | Wait for termination |

**Transitions:**

- `STOPPED → STARTING`: `docker compose up` command
- `STARTING → HEALTHY`: Health check returns 200 OK
- `STARTING → STARTING`: Health check fails, retry (max 5 times)
- `STARTING → STOPPED`: Health check fails 5 times (service_start failure)
- `HEALTHY → RUNNING`: First successful client connection
- `RUNNING → STOPPING`: `docker compose down` or `docker compose stop`
- `STOPPING → STOPPED`: Process terminated

### 3.2 Connector Lifecycle (Debezium)

Kafka Connect connectors follow this lifecycle:

```
┌──────┐
│ NONE │ (connector doesn't exist)
└──┬───┘
   │ POST /connectors
   ▼
┌──────────┐
│ CREATING │
└────┬─────┘
     │ validation succeeds
     │ worker assigns task
     ▼
┌─────────┐
│ RUNNING │◄────────────┐
└────┬────┘             │
     │                  │ POST /restart
     ├─────────────────►│
     │ error occurs     │
     ▼                  │
┌────────┐              │
│ FAILED ├──────────────┘
└────┬───┘
     │ POST /pause
     ▼
┌────────┐
│ PAUSED │
└────┬───┘
     │ POST /resume
     ├───────────────┐
     │               ▼
     │          ┌─────────┐
     │          │ RUNNING │
     │          └─────────┘
     │ DELETE /connectors/{name}
     ▼
┌─────────┐
│ DELETED │
└─────────┘
```

**States:**

| State | Description | Can Accept Tasks | Actions Available |
|-------|-------------|------------------|-------------------|
| `NONE` | Doesn't exist | No | Create connector |
| `CREATING` | Validation and assignment | No | Wait |
| `RUNNING` | Active CDC capture | Yes | Pause, Delete, Restart |
| `FAILED` | Error occurred | No | Restart, Delete |
| `PAUSED` | Manually paused | No | Resume, Delete |
| `UNASSIGNED` | Task not assigned to worker | No | Wait for rebalance |
| `DELETED` | Removed from cluster | No | Recreate |

**Transitions:**

- `NONE → CREATING`: POST `/connectors` with valid config
- `CREATING → RUNNING`: Validation passed, task assigned
- `CREATING → FAILED`: Validation failed or startup error
- `RUNNING → FAILED`: Runtime error (connection lost, etc.)
- `RUNNING → PAUSED`: POST `/connectors/{name}/pause`
- `FAILED → RUNNING`: POST `/connectors/{name}/restart`
- `PAUSED → RUNNING`: POST `/connectors/{name}/resume`
- `RUNNING → DELETED`: DELETE `/connectors/{name}`
- `FAILED → DELETED`: DELETE `/connectors/{name}`
- `PAUSED → DELETED`: DELETE `/connectors/{name}`

### 3.3 Test Fixture Lifecycle

Test fixtures follow pytest's lifecycle:

```
┌──────────────┐
│ TEST SESSION │
│    START     │
└──────┬───────┘
       │ session-scoped fixtures created
       ▼
┌────────────────┐
│ INFRASTRUCTURE │
│   READY        │ (Kafka, Spark, Schema Registry, etc.)
└──────┬─────────┘
       │ test module loaded
       ▼
┌──────────────┐
│ TEST MODULE  │
│   START      │
└──────┬───────┘
       │ test function starts
       ▼
┌──────────────┐
│ SETUP PHASE  │ (function-scoped fixtures)
└──────┬───────┘
       │ fixtures ready
       ▼
┌──────────────┐
│ TEST RUNNING │
└──────┬───────┘
       │ test completes (pass/fail)
       ▼
┌──────────────┐
│ TEARDOWN     │ (cleanup function-scoped)
│   PHASE      │
└──────┬───────┘
       │ next test or end of module
       ├────────────────┐
       │ more tests     │ last test
       ▼                ▼
  (loop back)    ┌──────────────┐
                 │ TEST MODULE  │
                 │    END       │
                 └──────┬───────┘
                        │ all modules complete
                        ▼
                 ┌──────────────┐
                 │ SESSION      │
                 │  TEARDOWN    │ (cleanup session-scoped)
                 └──────┬───────┘
                        │
                        ▼
                 ┌──────────────┐
                 │ TEST SESSION │
                 │    END       │
                 └──────────────┘
```

**Phases:**

| Phase | Scope | Description | Cleanup Trigger |
|-------|-------|-------------|----------------|
| Session Start | session | Create infrastructure clients | End of test session |
| Setup | function | Create test-specific resources | After test completes |
| Test Running | function | Execute test code | Test completion |
| Teardown | function | Delete topics, tables, connectors | Automatic |
| Session Teardown | session | Close clients, stop connections | Automatic |

**Fixture Scopes:**

- **Session-scoped**: `kafka_admin_client`, `spark_session`, `schema_registry_client`, `debezium_client`, `iceberg_catalog`
  - Created once per test session
  - Cleaned up after all tests complete
  - Shared across all tests

- **Function-scoped**: `postgres_connection`, `kafka_topic`, `delta_table_path`, `iceberg_namespace`, `debezium_connector`
  - Created for each test function
  - Cleaned up immediately after test
  - Isolated per test

**Cleanup Order:**

1. Function-scoped fixtures (LIFO - last created, first cleaned)
2. Module-scoped fixtures (if any)
3. Session-scoped fixtures (after all tests complete)

---

## 4. REST API Response Models

### 4.1 Debezium Connector Status Response

```json
{
  "name": "string",
  "connector": {
    "state": "RUNNING" | "FAILED" | "PAUSED" | "UNASSIGNED",
    "worker_id": "string"
  },
  "tasks": [
    {
      "id": 0,
      "state": "RUNNING" | "FAILED" | "PAUSED" | "UNASSIGNED",
      "worker_id": "string",
      "trace": "string (if failed)"
    }
  ],
  "type": "source" | "sink"
}
```

### 4.2 Schema Registry Subject Response

```json
{
  "subject": "string",
  "version": 1,
  "id": 123,
  "schema": "string (JSON-encoded schema)"
}
```

### 4.3 Iceberg REST Config Response

```json
{
  "defaults": {
    "warehouse": "s3://warehouse/"
  },
  "overrides": {}
}
```

---

## 5. Change Data Feed (CDF) Schema

Delta Lake Change Data Feed adds metadata columns to track changes:

```python
CDF_SCHEMA = {
    # Original table columns
    "id": "BIGINT",
    "name": "STRING",
    "created_at": "TIMESTAMP",

    # CDF metadata columns
    "_change_type": "STRING",          # insert, update_preimage, update_postimage, delete
    "_commit_version": "BIGINT",       # Delta table version
    "_commit_timestamp": "TIMESTAMP",  # Commit timestamp
}
```

**Change Types:**

| Change Type | Description | Row Content |
|-------------|-------------|-------------|
| `insert` | New row inserted | New row values |
| `update_preimage` | Row before update | Old row values |
| `update_postimage` | Row after update | New row values |
| `delete` | Row deleted | Deleted row values |

---

## Summary

This data model document defines:

1. **Service Configuration Models** for Docker Compose services (Iceberg, Schema Registry, Spark)
2. **Test Fixture Data Models** for Python test configuration (catalog, Spark, registry, connectors)
3. **State Transitions** for services, connectors, and test fixtures
4. **REST API Response Models** for status checking
5. **Change Data Feed Schema** for Delta Lake CDC operations

These models provide the foundation for implementing the test infrastructure expansion with proper type safety, configuration management, and lifecycle handling.
