# CDC Streaming Pipelines Documentation

## Overview

This document describes the Spark Structured Streaming pipelines for Change Data Capture (CDC) that have been implemented to enable end-to-end testing.

## Architecture

```
PostgreSQL → Debezium → Kafka → Spark Streaming → {Delta Lake, Iceberg}
```

### Components

1. **Source**: PostgreSQL database with CDC enabled (pgoutput plugin)
2. **CDC Capture**: Debezium PostgreSQL connector
3. **Message Broker**: Apache Kafka
4. **Stream Processing**: Spark Structured Streaming
5. **Destinations**:
   - Delta Lake with Change Data Feed (CDF) enabled
   - Apache Iceberg

## Implemented Scripts

### 1. Kafka → Delta Lake Streaming (`scripts/kafka_to_delta_streaming.py`)

Spark Structured Streaming job that:
- Consumes Debezium CDC events from Kafka topic `postgres.public.customers`
- Parses Debezium JSON format (before/after states, operation type, metadata)
- Applies transformations:
  - Concatenates `first_name` + `last_name` → `full_name`
  - Derives `location` from `city`, `state`, `country`
  - Maps Debezium operation codes (c/u/d/r) to readable names
  - Adds pipeline metadata (ingestion timestamp, source system, pipeline ID)
- Writes to Delta Lake table with Change Data Feed enabled
- Uses checkpointing for exactly-once semantics

**Configuration**:
```bash
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC=postgres.public.customers
DELTA_TABLE_PATH=/tmp/delta-cdc/customers
CHECKPOINT_LOCATION=/tmp/spark-checkpoints/kafka-to-delta
```

**Usage**:
```bash
poetry run python scripts/kafka_to_delta_streaming.py
```

### 2. Kafka → Iceberg Streaming (`scripts/kafka_to_iceberg_streaming.py`)

Similar to Delta Lake streaming but writes to Apache Iceberg:
- Same Debezium parsing and transformations
- Uses Iceberg Spark connector
- Writes to Hadoop-based Iceberg catalog
- Supports Iceberg format v2
- Enables fanout writes for performance

**Configuration**:
```bash
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC=postgres.public.customers
ICEBERG_CATALOG=iceberg
ICEBERG_WAREHOUSE=/tmp/iceberg-warehouse
ICEBERG_NAMESPACE=analytics
ICEBERG_TABLE=customers_analytics
CHECKPOINT_LOCATION=/tmp/spark-checkpoints/kafka-to-iceberg
```

**Usage**:
```bash
poetry run python scripts/kafka_to_iceberg_streaming.py
```

### 3. Orchestration Script (`scripts/orchestrate_streaming_pipelines.sh`)

Bash script to manage both streaming pipelines:

**Commands**:
```bash
# Start all pipelines
./scripts/orchestrate_streaming_pipelines.sh start

# Start specific pipeline
./scripts/orchestrate_streaming_pipelines.sh start delta
./scripts/orchestrate_streaming_pipelines.sh start iceberg

# Stop pipelines
./scripts/orchestrate_streaming_pipelines.sh stop all

# Check status
./scripts/orchestrate_streaming_pipelines.sh status

# View logs
./scripts/orchestrate_streaming_pipelines.sh logs delta
./scripts/orchestrate_streaming_pipelines.sh logs iceberg

# Restart
./scripts/orchestrate_streaming_pipelines.sh restart all
```

**Features**:
- Checks prerequisites (Kafka, Debezium connector)
- Manages PID files for process tracking
- Background execution with log files
- Graceful shutdown with SIGTERM
- Automatic Debezium connector setup if needed

## Data Flow

### 1. Source Data (PostgreSQL)

```sql
CREATE TABLE customers (
    customer_id SERIAL PRIMARY KEY,
    email VARCHAR(255),
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    phone VARCHAR(20),
    address_line1 VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(50),
    postal_code VARCHAR(20),
    country VARCHAR(50),
    registration_date TIMESTAMP,
    last_updated TIMESTAMP,
    is_active BOOLEAN,
    customer_tier VARCHAR(20),
    lifetime_value NUMERIC(10,2)
);
```

### 2. Debezium CDC Event Format

```json
{
  "before": null,
  "after": {
    "customer_id": 1,
    "email": "john@example.com",
    "first_name": "John",
    "last_name": "Doe",
    ...
  },
  "source": {
    "version": "2.4.0.Final",
    "connector": "postgresql",
    "name": "postgres",
    "ts_ms": 1699000000000,
    "db": "cdcdb",
    "schema": "public",
    "table": "customers",
    "lsn": 123456
  },
  "op": "c",  // c=create, u=update, d=delete, r=read (snapshot)
  "ts_ms": 1699000000000
}
```

### 3. Transformed Data (Delta Lake / Iceberg)

```
customer_id          BIGINT
email                STRING
full_name            STRING       -- Derived: first_name + last_name
location             STRING       -- Derived: city, state, country
customer_tier        STRING
lifetime_value       DOUBLE
registration_date    TIMESTAMP
is_active            BOOLEAN
total_orders         INTEGER      -- Placeholder for future join enrichment
_ingestion_timestamp TIMESTAMP    -- Pipeline metadata
_source_system       STRING       -- 'postgres_cdc'
_cdc_operation       STRING       -- Mapped operation: INSERT/UPDATE/DELETE
_pipeline_id         STRING       -- 'postgres_to_delta' or 'postgres_to_iceberg'
_processed_timestamp TIMESTAMP
_event_timestamp_ms  BIGINT       -- Original Debezium timestamp
_source_lsn          BIGINT       -- PostgreSQL Log Sequence Number
_kafka_timestamp     TIMESTAMP    -- Kafka ingestion time
```

## Delta Lake Change Data Feed

Delta Lake tables are created with Change Data Feed enabled:

```python
.option("delta.enableChangeDataFeed", "true")
```

This enables querying changes with CDF API:

```python
from delta.tables import DeltaTable

# Read changes between versions
changes = (
    spark.read
    .format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", 0)
    .option("endingVersion", 10)
    .load(delta_table_path)
)

# Changes include metadata columns:
# - _change_type: insert, update_preimage, update_postimage, delete
# - _commit_version: Delta Lake version
# - _commit_timestamp: Commit timestamp
```

## Testing Integration

### Prerequisites

1. **Docker Services Running**:
   ```bash
   docker-compose up -d
   ```

2. **Debezium Connector Registered**:
   ```bash
   poetry run python scripts/setup_debezium_connector.py
   ```

3. **Kafka Topics Created**:
   - `postgres.public.customers`
   - `postgres.public.orders`
   - `postgres.public.products`
   - `postgres.public.inventory`

### Running End-to-End Tests

1. **Start Streaming Pipelines**:
   ```bash
   ./scripts/orchestrate_streaming_pipelines.sh start
   ```

2. **Insert Test Data**:
   ```bash
   poetry run python scripts/generate_sample_data.py
   ```

3. **Run Tests**:
   ```bash
   make test
   ```

4. **Check Pipeline Status**:
   ```bash
   ./scripts/orchestrate_streaming_pipelines.sh status
   ```

5. **View Logs**:
   ```bash
   ./scripts/orchestrate_streaming_pipelines.sh logs delta
   ```

### Enabled Test Categories

With streaming pipelines running, the following tests are enabled:

- **Delta Lake CDF Tests** (9 tests): `tests/integration/test_deltalake_cdc.py`
  - Version-to-version changes
  - Time range queries
  - Operation type detection (INSERT/UPDATE/DELETE)
  - Partitioned tables
  - Incremental processing
  - Schema evolution
  - Large dataset handling

- **Cross-Storage Pipeline Tests**: End-to-end validation of transformations

## Monitoring

### Pipeline Health Checks

The orchestration script provides built-in health checks:

1. **Kafka Availability**: Checks connection to `localhost:9092`
2. **Debezium Connector Status**: Verifies connector is RUNNING
3. **Process Status**: PID-based tracking for streaming jobs

### Logs

Log files are created at:
- Delta Lake pipeline: `/tmp/kafka-to-delta.log`
- Iceberg pipeline: `/tmp/kafka-to-iceberg.log`

### Kafka Monitoring

Check topic contents:
```bash
kafka-console-consumer --bootstrap-server localhost:9092 \
    --topic postgres.public.customers \
    --from-beginning
```

Check Debezium connector status:
```bash
curl http://localhost:8083/connectors/postgres-cdc-connector/status
```

## Troubleshooting

### Pipeline Not Starting

1. **Check Kafka**: `nc -z localhost 9092`
2. **Check Debezium**: `curl http://localhost:8083/connectors`
3. **Check PostgreSQL**: `psql -h localhost -U cdcuser -d cdcdb -c "SELECT 1"`

### No Data Flowing

1. **Verify Debezium Connector**: Should be in RUNNING state
2. **Check Kafka Topics**: Topics should exist and have data
3. **Check PostgreSQL Replication Slot**:
   ```sql
   SELECT * FROM pg_replication_slots WHERE slot_name = 'postgres_debezium_slot';
   ```

### Checkpoint Recovery Issues

Delete checkpoint directory to restart from beginning:
```bash
rm -rf /tmp/spark-checkpoints/kafka-to-delta
```

## Performance Considerations

### Throughput

- **Trigger Interval**: 10 seconds (configurable)
- **Max Offsets Per Trigger**: 1000 messages
- **Spark Parallelism**: `local[*]` (all available cores)

### Tuning

For higher throughput, adjust in the Python scripts:

```python
.option("maxOffsetsPerTrigger", 10000)  # Increase batch size
.trigger(processingTime="5 seconds")    # Decrease trigger interval
```

For production:
```python
.config("spark.sql.shuffle.partitions", "200")
.config("spark.default.parallelism", "200")
.master("spark://master:7077")  # Use cluster mode
```

## Future Enhancements

1. **Schema Registry Integration**: Add Confluent Schema Registry for schema evolution
2. **Stateful Processing**: Add session windows and aggregations
3. **Enrichment Joins**: Join with other streams for `total_orders` calculation
4. **Metrics**: Add Prometheus metrics for pipeline monitoring
5. **Dead Letter Queue**: Handle poison messages
6. **Backpressure**: Implement adaptive rate limiting
7. **Multi-Table Support**: Extend to all CDC tables (orders, products, inventory)

## References

- [Debezium PostgreSQL Connector](https://debezium.io/documentation/reference/stable/connectors/postgresql.html)
- [Spark Structured Streaming](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [Delta Lake Change Data Feed](https://docs.delta.io/latest/delta-change-data-feed.html)
- [Apache Iceberg](https://iceberg.apache.org/)
