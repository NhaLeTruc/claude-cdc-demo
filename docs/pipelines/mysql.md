# MySQL CDC Pipeline Documentation

## Overview

The MySQL CDC (Change Data Capture) pipeline captures real-time changes from MySQL databases using Debezium's MySQL connector with binlog replication. It streams INSERT, UPDATE, and DELETE operations to Apache Kafka and replicates them to DeltaLake for analytics.

## Architecture

```
┌─────────────┐
│   MySQL     │
│  (binlog)   │
└──────┬──────┘
       │ Binary Log Events
       ▼
┌─────────────┐
│  Debezium   │
│  Connector  │
└──────┬──────┘
       │ CDC Events (JSON)
       ▼
┌─────────────┐
│   Kafka     │
│   Topic     │
└──────┬──────┘
       │ Consumer Poll
       ▼
┌──────────────────┐
│ MySQL CDC        │
│ Pipeline         │
│ ┌──────────────┐ │
│ │Event Parser  │ │
│ └──────┬───────┘ │
│        ▼         │
│ ┌──────────────┐ │
│ │Delta Writer  │ │
│ └──────────────┘ │
└────────┬─────────┘
         │
         ▼
   ┌──────────────┐
   │  DeltaLake   │
   │  (CDF enabled)│
   └──────────────┘
```

## Components

### 1. MySQLConnectionManager

Manages MySQL database connections with retry logic and health monitoring.

**Features:**
- Automatic connection retry with exponential backoff
- Connection pooling and reuse
- Schema introspection
- Binlog position tracking
- Context manager support

**Example Usage:**

```python
from src.cdc_pipelines.mysql.connection import MySQLConnectionManager

# Create connection manager
manager = MySQLConnectionManager(
    host="mysql.example.com",
    port=3306,
    user="cdcuser",
    password="cdcpass",
    database="production",
    max_retries=3,
    retry_delay=2.0,
)

# Check if binlog is enabled
if manager.check_binlog_enabled():
    print("Binary logging is enabled")

    # Get current binlog position
    position = manager.get_binlog_position()
    print(f"Binlog file: {position['file']}, Position: {position['position']}")

# Query table schema
schema = manager.get_table_schema("products")
for column, data_type in schema.items():
    print(f"{column}: {data_type}")

# Use as context manager
with MySQLConnectionManager(**config) as conn:
    result = conn.execute_query("SELECT COUNT(*) FROM products")
    print(f"Product count: {result[0]['count']}")
```

### 2. DebeziumMySQLConfigBuilder

Builds Debezium connector configuration for MySQL CDC with proper binlog settings.

**Configuration Options:**
- `connector_name`: Unique connector identifier
- `database_hostname`, `database_port`: MySQL server location
- `database_user`, `database_password`: Credentials
- `database_include_list`: List of databases to monitor
- `table_include_list`: Specific tables to capture (format: `db.table`)
- `server_id`: MySQL replication server ID (must be unique)
- `snapshot_mode`: `initial`, `when_needed`, `never`, or `schema_only`
- `enable_unwrap_transform`: Extract new record state (recommended)

**Example Usage:**

```python
from src.cdc_pipelines.mysql.debezium_config import DebeziumMySQLConfigBuilder

builder = DebeziumMySQLConfigBuilder(
    connector_name="mysql-products-connector",
    database_hostname="mysql.example.com",
    database_port=3306,
    database_user="debezium",
    database_password="dbz_password",
    database_include_list=["ecommerce"],
    table_include_list=["ecommerce.products", "ecommerce.inventory_transactions"],
    topic_prefix="cdc.mysql.ecommerce",
    server_id=1001,
    snapshot_mode="initial",
    enable_unwrap_transform=True,
)

# Validate configuration
if builder.validate():
    config = builder.build()

    # Register with Debezium Connect
    import requests
    response = requests.post(
        "http://debezium:8083/connectors",
        json=config,
        headers={"Content-Type": "application/json"},
    )
    print(f"Connector registered: {response.status_code}")
else:
    print("Invalid configuration")
```

### 3. BinlogEventParser

Parses Debezium MySQL binlog events into a standardized CDC format.

**Supported Operations:**
- `INSERT` (op: `c` - create)
- `UPDATE` (op: `u` - update)
- `DELETE` (op: `d` - delete)
- `READ` (op: `r` - snapshot read)

**Data Type Handling:**
- VARCHAR, TEXT: Preserved as strings
- INT, BIGINT: Preserved as integers
- DECIMAL, FLOAT: Preserved with precision
- DATETIME, TIMESTAMP: Converted to ISO format
- JSON: Preserved as JSON string or dict
- BOOLEAN: Preserved as boolean or tinyint(1)
- BLOB: Base64 encoded
- ENUM, SET: Preserved as strings

**Example Usage:**

```python
from src.cdc_pipelines.mysql.event_parser import BinlogEventParser

parser = BinlogEventParser()

# Parse single event
debezium_event = {
    "op": "c",
    "after": {
        "product_id": 101,
        "name": "Widget Pro",
        "price": 99.99,
        "stock": 50,
    },
    "source": {
        "db": "ecommerce",
        "table": "products",
        "ts_ms": 1704067200000,
        "file": "mysql-bin.000042",
        "pos": 12345,
    },
}

event = parser.parse(debezium_event)
print(f"Operation: {event['operation']}")
print(f"Table: {event['table']}")
print(f"Data: {event['data']}")
print(f"Binlog position: {event['metadata']['binlog_file']}:{event['metadata']['binlog_position']}")

# Parse batch
events = parser.parse_batch([debezium_event])

# Get changed fields (for UPDATE)
if event['operation'] == 'UPDATE':
    changed = parser.get_changed_fields(event)
    print(f"Changed fields: {changed}")
```

### 4. MySQLDeltaLakeWriter

Writes CDC events to DeltaLake with Change Data Feed (CDF) support.

**Features:**
- Batch write optimization
- Automatic table initialization
- MERGE operations for updates
- CDF metadata tracking (`_cdc_operation`, `_cdc_timestamp`)
- Table compaction and vacuum
- Partition support

**Example Usage:**

```python
from src.cdc_pipelines.mysql.delta_writer import MySQLDeltaLakeWriter

writer = MySQLDeltaLakeWriter(
    table_path="/data/delta/products",
    primary_key="product_id",
    enable_cdf=True,
    partition_by=["category"],
)

# Write batch of events
events = [
    {
        "operation": "INSERT",
        "table": "products",
        "data": {"product_id": 101, "name": "Widget", "price": 99.99},
        "timestamp": "2024-01-01T12:00:00",
    },
    {
        "operation": "UPDATE",
        "table": "products",
        "data": {"product_id": 101, "name": "Widget Pro", "price": 129.99},
        "timestamp": "2024-01-01T13:00:00",
    },
]

writer.write_batch(events)

# Maintenance operations
writer.compact()  # Compact small files
writer.vacuum(retention_hours=168)  # Clean up old files (7 days)

# Get table version
version = writer.get_table_version()
print(f"Current Delta table version: {version}")
```

### 5. MySQLCDCPipeline

Orchestrates the complete CDC pipeline from MySQL to DeltaLake.

**Example Usage:**

```python
from src.cdc_pipelines.mysql.pipeline import MySQLCDCPipeline

pipeline = MySQLCDCPipeline(
    pipeline_name="products_cdc",
    kafka_bootstrap_servers="kafka:9092",
    kafka_topic="cdc.mysql.ecommerce.products",
    delta_table_path="/data/delta/products",
    primary_key="product_id",
)

# Start pipeline (blocking)
pipeline.start(batch_size=100, poll_timeout_ms=1000)

# In another thread/process, you can:
# pipeline.stop()  # Graceful shutdown

# Get statistics
stats = pipeline.get_stats()
print(f"Events processed: {stats['events_processed']}")

# Health check
is_healthy = pipeline.health_check()
```

## Setup Instructions

### 1. MySQL Configuration

Enable binary logging in MySQL configuration (`my.cnf`):

```ini
[mysqld]
# Binary logging
server-id = 1
log_bin = mysql-bin
binlog_format = ROW
binlog_row_image = FULL
expire_logs_days = 7

# For Debezium
gtid_mode = ON
enforce_gtid_consistency = ON
```

Restart MySQL after configuration changes.

### 2. Create CDC User

```sql
-- Create Debezium user with necessary privileges
CREATE USER 'debezium'@'%' IDENTIFIED BY 'dbz_password';

GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT
ON *.* TO 'debezium'@'%';

-- Grant access to specific database
GRANT ALL PRIVILEGES ON ecommerce.* TO 'debezium'@'%';

FLUSH PRIVILEGES;
```

### 3. Configure Debezium Connector

Use the `DebeziumMySQLConfigBuilder` to create and register the connector (see component examples above).

### 4. Start CDC Pipeline

```bash
# Using Python
python -m src.cdc_pipelines.mysql.pipeline \
    --pipeline-name products_cdc \
    --kafka-servers kafka:9092 \
    --kafka-topic cdc.mysql.ecommerce.products \
    --delta-path /data/delta/products \
    --primary-key product_id
```

Or use the CLI tool:

```bash
cdc-demo start mysql --table products
```

## Monitoring

### Prometheus Metrics

```python
# Events processed by table and operation
mysql_cdc_events_processed_total{table="products", operation="INSERT"}
mysql_cdc_events_processed_total{table="products", operation="UPDATE"}
mysql_cdc_events_processed_total{table="products", operation="DELETE"}

# Batch processing metrics
mysql_cdc_batch_size_bucket{le="100"}
mysql_cdc_batch_size_sum
mysql_cdc_batch_size_count

# Lag metrics
mysql_cdc_lag_seconds{pipeline="products_cdc", table="products"}

# Binlog position
mysql_binlog_position{server="mysql-1", binlog_file="mysql-bin.000042"}
```

### Grafana Dashboards

Import the MySQL CDC dashboard:

```bash
# Located at: docker/observability/grafana/dashboards/mysql_cdc.json
```

Panels include:
- Total events processed
- Current lag by table
- Event throughput (events/sec)
- Operations breakdown (INSERT/UPDATE/DELETE)
- Binlog position tracking
- Error rates
- Integrated Loki logs

### Health Checks

```bash
# Liveness check
curl http://localhost:8080/health/live

# Readiness check
curl http://localhost:8080/health/ready

# Full health report
curl http://localhost:8080/health
```

## Troubleshooting

### High CDC Lag

**Symptoms:** `mysql_cdc_lag_seconds` metric increasing

**Possible Causes:**
1. Slow DeltaLake writes
2. Insufficient batch size
3. Network latency
4. Resource constraints

**Solutions:**
```python
# Increase batch size
pipeline.start(batch_size=500, poll_timeout_ms=2000)

# Partition DeltaLake table
writer = MySQLDeltaLakeWriter(
    table_path="/data/delta/products",
    primary_key="product_id",
    partition_by=["category", "created_date"],  # Improves write performance
)

# Scale horizontally (multiple consumers in same group)
```

### Missing Events

**Possible Causes:**
1. Binlog expired before capture
2. Table not in include list
3. Deserialization errors

**Diagnostic Steps:**
```bash
# Check binlog retention
mysql> SHOW VARIABLES LIKE 'expire_logs_days';

# Check Debezium connector status
curl http://debezium:8083/connectors/mysql-products-connector/status

# Review logs
docker logs mysql-cdc-pipeline | grep ERROR
```

### Data Type Mismatches

**Issue:** Decimal precision lost or wrong data types

**Solution:**
```python
# Configure decimal handling in Debezium
builder = DebeziumMySQLConfigBuilder(
    # ... other config ...
    decimal_handling_mode="precise",  # Use precise decimal handling
    time_precision_mode="adaptive_time_microseconds",
)
```

### Connection Errors

**Issue:** `ERROR 1045 (28000): Access denied`

**Solution:**
```sql
-- Verify user privileges
SHOW GRANTS FOR 'debezium'@'%';

-- Ensure REPLICATION privileges
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'debezium'@'%';
FLUSH PRIVILEGES;
```

## Performance Tuning

### Batch Size Optimization

```python
# Monitor batch metrics
# Aim for 100-500 events per batch for optimal throughput

# Too small: High overhead
pipeline.start(batch_size=10)  # ❌ Not recommended

# Optimal for most workloads
pipeline.start(batch_size=100)  # ✅ Good starting point

# High-volume scenarios
pipeline.start(batch_size=500)  # ✅ For heavy workloads
```

### Delta Table Maintenance

```python
# Schedule regular compaction
import schedule

def compact_table():
    writer.compact()
    writer.vacuum(retention_hours=168)

schedule.every().day.at("02:00").do(compact_table)
```

### Consumer Tuning

```python
# For high-throughput scenarios, tune Kafka consumer settings
consumer_config = {
    "fetch.min.bytes": 1048576,  # 1MB
    "fetch.max.wait.ms": 500,
    "max.partition.fetch.bytes": 10485760,  # 10MB
}
```

## Testing

### Unit Tests

```bash
pytest tests/unit/test_cdc_pipelines/test_mysql_connection.py
pytest tests/unit/test_cdc_pipelines/test_binlog_parser.py
pytest tests/unit/test_cdc_pipelines/test_data_types.py
```

### Integration Tests

```bash
# Requires running MySQL + Debezium
pytest tests/integration/test_mysql_cdc.py -v
```

### End-to-End Tests

```bash
# Requires full infrastructure
pytest tests/e2e/test_mysql_to_delta.py -v --run-e2e
```

## References

- [Debezium MySQL Connector Documentation](https://debezium.io/documentation/reference/stable/connectors/mysql.html)
- [MySQL Binary Log Documentation](https://dev.mysql.com/doc/refman/8.0/en/binary-log.html)
- [Delta Lake Change Data Feed](https://docs.delta.io/latest/delta-change-data-feed.html)
- [Architecture Diagrams](../architecture.md)
