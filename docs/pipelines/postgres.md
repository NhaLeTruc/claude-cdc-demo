# Postgres CDC Pipeline

## Overview

The Postgres CDC pipeline captures change data from PostgreSQL using Debezium and logical replication, processing INSERT, UPDATE, and DELETE operations in real-time and replicating them to DeltaLake with Change Data Feed (CDF) enabled.

## Architecture

```
PostgreSQL (wal_level=logical)
    ↓
Logical Replication Slot
    ↓
Debezium Connector
    ↓
Apache Kafka
    ↓
CDC Event Parser
    ↓
DeltaLake Writer (with CDF)
    ↓
DeltaLake Table
```

## Components

### 1. PostgresConnectionManager
**File**: [src/cdc_pipelines/postgres/connection.py](../../src/cdc_pipelines/postgres/connection.py)

Manages PostgreSQL database connections with automatic retry logic and connection pooling.

**Features**:
- Automatic connection retry with exponential backoff
- Context manager support for resource cleanup
- Schema introspection (get table schema, row counts)
- CDC enablement checking (wal_level validation)

**Example Usage**:
```python
from src.cdc_pipelines.postgres.connection import PostgresConnectionManager

config = {
    "host": "localhost",
    "port": 5432,
    "user": "cdcuser",
    "password": "cdcpass",
    "database": "cdcdb",
}

with PostgresConnectionManager(**config) as manager:
    schema = manager.get_table_schema("customers")
    count = manager.get_table_count("customers")
```

### 2. DebeziumConfigBuilder
**File**: [src/cdc_pipelines/postgres/debezium_config.py](../../src/cdc_pipelines/postgres/debezium_config.py)

Builds Debezium connector configuration for Postgres CDC.

**Features**:
- Automatic configuration generation
- Table whitelist support
- Custom replication slot and publication names
- Unwrap transformation support
- Configuration validation

**Example Usage**:
```python
from src.cdc_pipelines.postgres.debezium_config import DebeziumConfigBuilder

builder = DebeziumConfigBuilder(
    connector_name="postgres-connector",
    database_hostname="localhost",
    database_port=5432,
    database_user="cdcuser",
    database_password="cdcpass",
    database_name="cdcdb",
    table_include_list=["public.customers", "public.orders"],
)

config = builder.build()
json_config = builder.to_json()
```

### 3. CDCEventParser
**File**: [src/cdc_pipelines/postgres/event_parser.py](../../src/cdc_pipelines/postgres/event_parser.py)

Parses Debezium CDC events into a standardized format.

**Features**:
- Converts Debezium format to standard CDC format
- Extracts operation type (INSERT/UPDATE/DELETE)
- Preserves metadata (LSN, transaction ID, timestamps)
- Batch processing support
- Primary key extraction

**Event Format**:
```json
{
  "operation": "INSERT",
  "table": "customers",
  "data": {
    "customer_id": 1,
    "email": "john@example.com"
  },
  "timestamp": "2024-01-01T00:00:00",
  "metadata": {
    "database": "cdcdb",
    "lsn": 123456789,
    "txId": 999
  },
  "primary_key": 1
}
```

### 4. DeltaLakeWriter
**File**: [src/cdc_pipelines/postgres/delta_writer.py](../../src/cdc_pipelines/postgres/delta_writer.py)

Writes CDC events to DeltaLake tables with Change Data Feed support.

**Features**:
- Change Data Feed (CDF) enabled by default
- Support for all CDC operations (INSERT/UPDATE/DELETE)
- MERGE operations for updates (upsert)
- Partitioning support
- Table versioning

**Example Usage**:
```python
from src.cdc_pipelines.postgres.delta_writer import DeltaLakeWriter

writer = DeltaLakeWriter(
    table_path="s3://bucket/delta/customers",
    table_name="customers",
    enable_cdf=True,
    primary_key="customer_id",
)

writer.write(cdc_event)
writer.write_batch(cdc_events)
```

### 5. PostgresCDCPipeline
**File**: [src/cdc_pipelines/postgres/pipeline.py](../../src/cdc_pipelines/postgres/pipeline.py)

Orchestrates the complete Postgres CDC pipeline.

**Features**:
- Kafka consumer integration
- Batch processing for efficiency
- Automatic offset management
- Error handling and retry logic
- Status reporting

**Example Usage**:
```python
from src.cdc_pipelines.postgres.pipeline import PostgresCDCPipeline

pipeline = PostgresCDCPipeline(
    pipeline_name="postgres-cdc",
    kafka_topic="cdc.postgres.public.customers",
    delta_table_path="./delta-lake/customers",
    primary_key_field="customer_id",
)

pipeline.start(batch_size=100)
```

## Setup

### 1. Configure PostgreSQL for CDC

Enable logical replication in `postgresql.conf`:
```ini
wal_level = logical
max_replication_slots = 5
max_wal_senders = 5
```

Restart PostgreSQL after configuration changes.

### 2. Register Debezium Connector

Use the provided script:
```bash
./scripts/register-postgres-connector.sh
```

Or register manually:
```bash
curl -X POST http://localhost:8083/connectors/ \
  -H "Content-Type: application/json" \
  -d @connector-config.json
```

### 3. Start the Pipeline

```bash
# Using CLI
cdc-demo pipeline start postgres-cdc

# Or programmatically
python -c "from src.cdc_pipelines.postgres.pipeline import PostgresCDCPipeline; \
           pipeline = PostgresCDCPipeline(); \
           pipeline.start()"
```

## Monitoring

### Metrics

The pipeline exports the following Prometheus metrics:

- `postgres_cdc_events_processed_total{table, operation}` - Total events processed
- `postgres_cdc_lag_seconds{pipeline, table}` - Current CDC lag
- `postgres_cdc_batch_size` - Event batch sizes
- `cdc_errors_total{pipeline, error_type}` - Processing errors

### Health Checks

Health check endpoints:
- `GET /health` - Overall health status
- `GET /health/ready` - Readiness probe
- `GET /health/live` - Liveness probe

### Dashboards

View real-time metrics in Grafana:
- **Postgres CDC Dashboard**: http://localhost:3000/d/postgres-cdc
- **CDC Overview**: http://localhost:3000/d/cdc-overview

## Data Quality

The pipeline includes comprehensive data quality validation:

### Row Count Validation
Ensures the same number of records in source and destination.

### Checksum Validation
Validates data integrity using MD5 checksums.

### CDC Lag Monitoring
Monitors replication lag and alerts if it exceeds thresholds.

### Schema Validation
Validates schema compatibility between Postgres and DeltaLake.

## Testing

Run the test suite:
```bash
# All tests
make test

# Unit tests only
pytest tests/unit/test_cdc_pipelines/test_postgres_*.py -v

# Integration tests
pytest tests/integration/test_postgres_cdc.py -v

# Data quality tests
pytest tests/data_quality/ -v
```

## Troubleshooting

### Pipeline Not Consuming Events

1. Check Debezium connector status:
```bash
curl http://localhost:8083/connectors/postgres-connector/status
```

2. Verify Kafka topics exist:
```bash
docker exec cdc-kafka kafka-topics --list --bootstrap-server localhost:9092
```

3. Check replication slot:
```sql
SELECT * FROM pg_replication_slots;
```

### High CDC Lag

1. Increase batch size in pipeline configuration
2. Scale Kafka partitions for parallelism
3. Check DeltaLake write performance
4. Review Postgres workload

### Missing Events

1. Verify table is in Debezium whitelist
2. Check Postgres grants for CDC user
3. Review Debezium connector logs
4. Validate replication slot is active

## Performance Tuning

### Batch Size
Adjust batch size based on workload:
- **Low volume**: 10-50 events
- **Medium volume**: 100-500 events
- **High volume**: 1000+ events

### Kafka Configuration
```python
# Consumer settings for high throughput
consumer_config = {
    "fetch_min_bytes": 1024 * 1024,  # 1MB
    "fetch_max_wait_ms": 500,
    "max_partition_fetch_bytes": 10 * 1024 * 1024,  # 10MB
}
```

### DeltaLake Optimization
- Enable partitioning on high-cardinality columns
- Use Z-ordering for query optimization
- Configure appropriate file sizes

## References

- [Debezium Postgres Connector](https://debezium.io/documentation/reference/connectors/postgresql.html)
- [PostgreSQL Logical Replication](https://www.postgresql.org/docs/current/logical-replication.html)
- [DeltaLake Change Data Feed](https://docs.delta.io/latest/delta-change-data-feed.html)
