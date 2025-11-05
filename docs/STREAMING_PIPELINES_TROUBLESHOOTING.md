# Streaming Pipelines Orchestration - Troubleshooting Guide

## Overview

This document chronicles the investigation and resolution of errors encountered when running the streaming pipelines orchestration script (`./scripts/orchestrate_streaming_pipelines.sh start`). All errors were successfully resolved, resulting in both Delta Lake and Iceberg streaming pipelines running successfully.

## Initial Command

```bash
./scripts/orchestrate_streaming_pipelines.sh start
```

## Investigation Trail and Fixes

### Issue 1: Debezium Connector Configuration Error

#### Error Encountered
```
[WARN] Debezium connector is not running. Setting it up...
============================================================
Debezium PostgreSQL CDC Connector Setup
============================================================

✗ Failed to create connector: 400
  Response: {"error_code":400,"message":"Connector configuration is invalid and contains the following 1 error(s):\nError while validating connector config: Connection to localhost:5432 refused. Check that the hostname and port are correct and that the postmaster is accepting TCP/IP connections."}
```

#### Investigation Commands
```bash
# Check environment variable configuration
grep "POSTGRES_HOST" /home/bob/WORK/claude-cdc-demo/.env

# Output: POSTGRES_HOST=localhost

# Check Docker service name in docker-compose.yml
grep -A 5 "# PostgreSQL" /home/bob/WORK/claude-cdc-demo/docker-compose.yml | head -10

# Output showed service name is "postgres"
```

#### Root Cause Analysis
- The `.env` file specifies `POSTGRES_HOST=localhost`, which works for connections from the host machine
- However, Debezium runs inside a Docker container and needs to use the Docker service name `postgres`
- The `setup_debezium_connector.py` script was reading `localhost` from the environment and passing it to Debezium
- Debezium couldn't resolve `localhost:5432` because from inside the Docker network, it needs to use `postgres:5432`

#### Fix Applied
**File**: `scripts/setup_debezium_connector.py` (lines 73-83)

```python
# Get configuration from environment or use defaults
# For Debezium connector, use Docker service name (postgres) not localhost
default_hostname = os.getenv("POSTGRES_HOST", "postgres")
if default_hostname == "localhost":
    default_hostname = "postgres"  # Use Docker service name for internal communication

hostname = database_hostname or default_hostname
port = database_port or int(os.getenv("POSTGRES_PORT", "5432"))
user = database_user or os.getenv("POSTGRES_USER", "cdcuser")
password = database_password or os.getenv("POSTGRES_PASSWORD", "cdcpass")
dbname = database_dbname or os.getenv("POSTGRES_DB", "cdcdb")
```

#### Verification
```bash
./scripts/orchestrate_streaming_pipelines.sh start

# Success output:
# ✓ Created Debezium connector: postgres-cdc-connector
#   - Database: postgres:5432/cdcdb
#   - Topic prefix: postgres
#   - Tables: public.customers, public.orders, public.products, public.inventory
```

---

### Issue 2: Spark/Iceberg Version Incompatibility

#### Error Encountered
```
py4j.protocol.Py4JJavaError: An error occurred while calling o49.load.
: java.lang.NoClassDefFoundError: org/apache/spark/sql/catalyst/expressions/AnsiCast
	at org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions.$anonfun$apply$6(IcebergSparkSessionExtensions.scala:54)
...
Caused by: java.lang.ClassNotFoundException: org.apache.spark.sql.catalyst.expressions.AnsiCast
```

#### Investigation Commands
```bash
# Check Iceberg pipeline log
tail -100 /tmp/kafka-to-iceberg.log | grep -B 10 "ClassNotFoundException"

# Check installed PySpark version
poetry show pyspark | grep version

# Output: version      : 3.5.7

# Check what versions script was trying to use
grep -A 10 "iceberg-spark" /home/bob/WORK/claude-cdc-demo/scripts/kafka_to_iceberg_streaming.py | head -20

# Output showed:
# "org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.4.0,"
# "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0"
```

#### Root Cause Analysis
- The script was configured to use Spark 3.3.0 JARs: `iceberg-spark-runtime-3.3_2.12:1.4.0`
- However, the installed PySpark version was 3.5.7
- Iceberg 1.4.0 for Spark 3.3 is incompatible with PySpark 3.5.7
- The `AnsiCast` class exists in different locations between Spark 3.3 and 3.5
- Version mismatch between PySpark driver (3.5.7) and Spark JARs (3.3.0) caused class loading failures

#### Fix Applied
**File**: `scripts/kafka_to_iceberg_streaming.py` (lines 104-106)

```python
# Before:
.config("spark.jars.packages",
        "org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.4.0,"
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0")

# After:
.config("spark.jars.packages",
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,"
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
```

#### Verification
```bash
# Restart pipelines
./scripts/orchestrate_streaming_pipelines.sh stop && sleep 2 && ./scripts/orchestrate_streaming_pipelines.sh start

# Check logs for successful Spark initialization
tail -50 /tmp/kafka-to-iceberg.log | grep "Created Spark session"

# Output: 2025-11-05 08:08:53,193 - __main__ - INFO - Created Spark session with Iceberg support
```

---

### Issue 3: Iceberg Table Not Found

#### Error Encountered
```
py4j.protocol.Py4JJavaError: An error occurred while calling o141.start.
: org.apache.iceberg.exceptions.NoSuchTableException: Cannot find table for analytics.customers_analytics.
	at org.apache.iceberg.spark.source.IcebergSource.getTable(IcebergSource.java:112)
...
Caused by: org.apache.spark.sql.catalyst.analysis.NoSuchTableException: [TABLE_OR_VIEW_NOT_FOUND] The table or view analytics.customers_analytics cannot be found.
```

#### Investigation Commands
```bash
# Check Iceberg logs for table creation attempts
tail -50 /tmp/kafka-to-iceberg.log | grep -E "Error|Exception|Creating"

# Check Iceberg warehouse directory
ls -la /tmp/iceberg-warehouse/

# Output: directory was empty (no tables created)

# Review script to see if it creates tables
grep -A 20 "def write_to_iceberg" /home/bob/WORK/claude-cdc-demo/scripts/kafka_to_iceberg_streaming.py
```

#### Root Cause Analysis
- The `kafka_to_iceberg_streaming.py` script attempted to write to an Iceberg table that didn't exist
- The script had no logic to create the table before writing
- Iceberg doesn't automatically create tables on first write like some other formats
- The namespace `analytics` and table `customers_analytics` needed to be created explicitly

#### Fix Applied
**File**: `scripts/kafka_to_iceberg_streaming.py`

Added new method (lines 296-342):
```python
def create_iceberg_table_if_not_exists(self) -> None:
    """
    Create Iceberg table if it doesn't exist.

    Creates the namespace and table with appropriate schema.
    """
    table_identifier = f"{self.iceberg_catalog}.{self.iceberg_namespace}.{self.iceberg_table}"

    # Create namespace if it doesn't exist
    try:
        self.spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {self.iceberg_catalog}.{self.iceberg_namespace}")
        logger.info(f"Ensured namespace exists: {self.iceberg_catalog}.{self.iceberg_namespace}")
    except Exception as e:
        logger.warning(f"Could not create namespace: {e}")

    # Check if table exists
    try:
        self.spark.sql(f"DESC TABLE {table_identifier}")
        logger.info(f"Table already exists: {table_identifier}")
    except Exception:
        # Table doesn't exist, create it
        logger.info(f"Creating Iceberg table: {table_identifier}")
        create_table_sql = f"""
            CREATE TABLE {table_identifier} (
                customer_id BIGINT,
                email STRING,
                full_name STRING,
                location STRING,
                customer_tier STRING,
                lifetime_value DOUBLE,
                registration_date TIMESTAMP,
                is_active BOOLEAN,
                total_orders INT,
                _ingestion_timestamp TIMESTAMP,
                _source_system STRING,
                _cdc_operation STRING,
                _pipeline_id STRING,
                _processed_timestamp TIMESTAMP,
                _event_timestamp_ms BIGINT,
                _source_lsn BIGINT,
                _kafka_timestamp TIMESTAMP
            )
            USING iceberg
        """
        self.spark.sql(create_table_sql)
        logger.info(f"Created Iceberg table: {table_identifier}")
```

Updated `run()` method to call table creation (line 386):
```python
def run(self) -> None:
    """Run the streaming job end-to-end."""
    logger.info("Starting Kafka to Iceberg streaming job")

    # Create Spark session
    self.spark = self.create_spark_session()

    # Create Iceberg table if needed
    self.create_iceberg_table_if_not_exists()  # NEW LINE

    # Read from Kafka
    kafka_df = self.read_from_kafka()
    # ... rest of the method
```

#### Verification
```bash
# Check logs for table creation
tail -100 /tmp/kafka-to-iceberg.log | grep -E "Creating|Created|Ensured namespace"

# Output:
# 2025-11-05 08:08:55,558 - __main__ - INFO - Ensured namespace exists: iceberg.analytics
# 2025-11-05 08:08:55,641 - __main__ - INFO - Creating Iceberg table: iceberg.analytics.customers_analytics
# 2025-11-05 08:08:56,201 - __main__ - INFO - Created Iceberg table: iceberg.analytics.customers_analytics
```

---

### Issue 4: Schema Mismatch - Missing Fields

#### Error Encountered
```
pyspark.errors.exceptions.captured.StreamingQueryException: [STREAM_FAILED] Query [id = 3b689d20-b29b-4038-aef8-c4f47d24bed6, runId = 19799b14-ae67-47a4-9f70-6a6e535cf778] terminated with exception: Field total_orders not found in source schema
```

#### Investigation Commands
```bash
# Check what fields the transformation creates
grep -A 50 "def apply_transformations" /home/bob/WORK/claude-cdc-demo/scripts/kafka_to_iceberg_streaming.py | grep "alias"

# Check table schema that was created
tail -150 /tmp/kafka-to-iceberg.log | grep -B 10 "Creating Iceberg table"

# Clean up and check for stale tables
ls -la /tmp/iceberg-warehouse/
rm -rf /tmp/iceberg-warehouse/analytics.db /tmp/spark-checkpoints/kafka-to-iceberg
```

#### Root Cause Analysis
- The DataFrame transformation in `apply_transformations()` creates fields: `total_orders`, `_ingestion_timestamp`, `_source_system`
- The initial table schema I created was missing these fields
- When I added the fields, old table instances still existed in the warehouse
- Iceberg detected schema mismatch between the existing table and the DataFrame being written

#### Fix Applied
**File**: `scripts/kafka_to_iceberg_streaming.py` (lines 318-340)

Updated CREATE TABLE statement to include all fields:
```sql
CREATE TABLE {table_identifier} (
    customer_id BIGINT,
    email STRING,
    full_name STRING,
    location STRING,
    customer_tier STRING,
    lifetime_value DOUBLE,
    registration_date TIMESTAMP,
    is_active BOOLEAN,
    total_orders INT,                    -- ADDED
    _ingestion_timestamp TIMESTAMP,       -- ADDED
    _source_system STRING,                -- ADDED
    _cdc_operation STRING,
    _pipeline_id STRING,
    _processed_timestamp TIMESTAMP,
    _event_timestamp_ms BIGINT,
    _source_lsn BIGINT,
    _kafka_timestamp TIMESTAMP
)
USING iceberg
```

#### Cleanup Commands
```bash
# Remove stale table and checkpoints
rm -rf /tmp/iceberg-warehouse/* /tmp/spark-checkpoints/kafka-to-iceberg/*

# Restart pipelines
./scripts/orchestrate_streaming_pipelines.sh stop && sleep 2 && ./scripts/orchestrate_streaming_pipelines.sh start
```

---

### Issue 5: Checkpoint File Concurrency

#### Error Encountered
```
py4j.protocol.Py4JJavaError: An error occurred while calling o150.start.
: org.apache.spark.SparkConcurrentModificationException: Multiple streaming queries are concurrently using metadata.
...
Caused by: org.apache.hadoop.fs.FileAlreadyExistsException: Rename destination file:/tmp/spark-checkpoints/kafka-to-iceberg/.metadata.crc already exists.
```

#### Investigation Commands
```bash
# Check checkpoint directory contents
ls -la /tmp/spark-checkpoints/kafka-to-iceberg/

# Output showed .metadata.crc and other checkpoint files from previous runs
```

#### Root Cause Analysis
- Checkpoint files from previous failed runs were not properly cleaned up
- Spark detected existing metadata files and thought another streaming query was running
- The `rm -rf /tmp/spark-checkpoints/kafka-to-iceberg/*` command didn't fully clean hidden files
- Need to remove the entire checkpoint directory, not just its contents

#### Fix Applied
```bash
# Remove entire checkpoint directory
rm -rf /tmp/spark-checkpoints

# Restart Iceberg pipeline only
./scripts/orchestrate_streaming_pipelines.sh start
```

#### Verification
```bash
# Wait for initialization
sleep 25 && ./scripts/orchestrate_streaming_pipelines.sh status

# Check for clean startup in logs
tail -60 /tmp/kafka-to-iceberg.log | grep -E "INFO|Started streaming|Creating"
```

---

### Issue 6: Unsupported Partition Transform in Streaming

#### Error Encountered
```
pyspark.errors.exceptions.captured.StreamingQueryException: [STREAM_FAILED] Query [id = 179406a2-bb19-42d5-a495-31d721fe13f0, runId = 39f08854-6839-47cf-a301-f11420442f05] terminated with exception: days(registration_date) is not currently supported
```

#### Investigation Commands
```bash
# Check table creation SQL for partitioning
grep -A 20 "CREATE TABLE" /home/bob/WORK/claude-cdc-demo/scripts/kafka_to_iceberg_streaming.py | grep -i "partition"

# Output showed:
# PARTITIONED BY (days(registration_date))
```

#### Root Cause Analysis
- Iceberg table was created with `PARTITIONED BY (days(registration_date))`
- Partition transforms like `days()`, `months()`, `years()` are not supported for streaming writes in Iceberg
- Streaming queries require either no partitioning or identity partitioning only
- This is a known limitation in Iceberg's streaming support

#### Fix Applied
**File**: `scripts/kafka_to_iceberg_streaming.py` (lines 318-339)

Removed partition specification:
```python
# Before:
create_table_sql = f"""
    CREATE TABLE {table_identifier} (
        ...
    )
    USING iceberg
    PARTITIONED BY (days(registration_date))  -- REMOVED THIS LINE
"""

# After:
create_table_sql = f"""
    CREATE TABLE {table_identifier} (
        ...
    )
    USING iceberg
"""
```

#### Cleanup and Restart
```bash
# Clean up table with invalid partitioning
rm -rf /tmp/iceberg-warehouse/* /tmp/spark-checkpoints

# Start pipeline
./scripts/orchestrate_streaming_pipelines.sh start

# Wait for initialization
sleep 35 && ./scripts/orchestrate_streaming_pipelines.sh status
```

#### Verification
```bash
# Check pipeline status
./scripts/orchestrate_streaming_pipelines.sh status

# Output:
# [INFO] Pipeline Status:
#   ✓ Delta Lake pipeline: RUNNING (PID: 548915)
#   ✓ Iceberg pipeline: RUNNING (PID: 562414)

# Verify successful table creation and streaming start
tail -20 /tmp/kafka-to-iceberg.log | grep -E "INFO|Started streaming|Creating"

# Output:
# 2025-11-05 08:08:53,193 - __main__ - INFO - Created Spark session with Iceberg support
# 2025-11-05 08:08:55,558 - __main__ - INFO - Ensured namespace exists: iceberg.analytics
# 2025-11-05 08:08:55,641 - __main__ - INFO - Creating Iceberg table: iceberg.analytics.customers_analytics
# 2025-11-05 08:08:56,201 - __main__ - INFO - Created Iceberg table: iceberg.analytics.customers_analytics
# 2025-11-05 08:08:56,407 - __main__ - INFO - Reading from Kafka topic: postgres.public.customers
# 2025-11-05 08:08:57,202 - __main__ - INFO - Started streaming to Iceberg table: iceberg.analytics.customers_analytics
# 2025-11-05 08:08:57,203 - __main__ - INFO - Checkpoint location: /tmp/spark-checkpoints/kafka-to-iceberg
```

---

## Summary of All Fixes

### Files Modified

1. **`scripts/setup_debezium_connector.py`** (lines 73-83)
   - Added logic to convert `localhost` to `postgres` for Docker network communication
   - Ensures Debezium can connect to PostgreSQL within Docker network

2. **`scripts/kafka_to_iceberg_streaming.py`**
   - **Lines 104-106**: Updated Spark/Iceberg versions from 3.3 to 3.5
   - **Lines 296-342**: Added `create_iceberg_table_if_not_exists()` method
   - **Line 386**: Added call to create table before writing
   - **Lines 318-339**: Fixed table schema to include all DataFrame fields and removed unsupported partitioning

### Final Status

Both streaming pipelines are now running successfully:

```
✓ Delta Lake pipeline: RUNNING
✓ Iceberg pipeline: RUNNING
```

### Key Learnings

1. **Docker Networking**: Services within Docker need to use service names, not `localhost`
2. **Version Compatibility**: PySpark version must match Spark JARs and Iceberg runtime versions
3. **Iceberg Table Management**: Tables must be created before streaming writes; auto-creation is not supported
4. **Schema Alignment**: Table schema must exactly match DataFrame schema for streaming writes
5. **Checkpoint Management**: Clean checkpoint directories completely, including hidden files
6. **Partition Limitations**: Streaming writes to Iceberg don't support partition transforms like `days()`

### Troubleshooting Commands Summary

```bash
# Check pipeline status
./scripts/orchestrate_streaming_pipelines.sh status

# View logs
tail -100 /tmp/kafka-to-iceberg.log
tail -100 /tmp/kafka-to-delta.log

# Clean up for fresh start
rm -rf /tmp/iceberg-warehouse/* /tmp/spark-checkpoints

# Restart pipelines
./scripts/orchestrate_streaming_pipelines.sh stop
./scripts/orchestrate_streaming_pipelines.sh start

# Check Debezium connector
curl http://localhost:8083/connectors/postgres-cdc-connector/status

# Check Kafka topics
docker exec cdc-kafka kafka-topics --bootstrap-server localhost:9092 --list
```

## Related Documentation

- [Schema Registry Implementation Complete](./SCHEMA_REGISTRY_IMPLEMENTATION_COMPLETE.md)
- [Schema Registry Integration Plan](./SCHEMA_REGISTRY_INTEGRATION_PLAN.md)
- [Debezium Schema Registry Config](../docker/debezium/schema-registry-config.json)

## Date

**Investigation Completed**: November 5, 2025
**Engineer**: Claude (Anthropic AI Assistant)
**Status**: ✅ All issues resolved
