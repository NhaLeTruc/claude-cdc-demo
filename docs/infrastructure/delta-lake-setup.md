# Delta Lake Testing Infrastructure Setup

This document describes the Delta Lake testing infrastructure with Apache Spark and Change Data Feed (CDF) support.

## Overview

The Delta Lake infrastructure provides:
- **Apache Spark** for distributed data processing
- **Delta Lake 3.3.2** with Change Data Feed (CDF) support
- **MinIO S3-compatible storage** for Delta table data
- **Pytest fixtures** for easy test setup and teardown

## Architecture

```
┌─────────────────┐         ┌──────────────────────┐
│   Test Code     │────────▶│  PySpark + Delta     │
│   (pytest)      │         │  (Spark Session)     │
└─────────────────┘         └──────────────────────┘
                                      │
                                      ▼
                           ┌────────────────────────┐
                           │   Apache Spark Master  │
                           │   (port 7077, 8080)    │
                           └────────────────────────┘
                                      │
                                      ▼
                           ┌────────────────────────┐
                           │       MinIO            │
                           │  (S3-compatible)       │
                           │  s3a://warehouse/      │
                           └────────────────────────┘
                                      │
                                      ▼
                           ┌────────────────────────┐
                           │   Delta Tables with:   │
                           │   - Transaction log    │
                           │   - Parquet data files │
                           │   - CDF metadata       │
                           └────────────────────────┘
```

## Services

### Apache Spark

**Image**: `apache/spark:3.5.0`
**Ports**: `7077` (Master), `8080` (Web UI), `4040` (Application UI)
**Health Check**: `http://localhost:8080`

The Spark service provides:
- Spark Master for job coordination
- Distributed data processing engine
- Delta Lake integration via spark-defaults.conf

**Configuration**: `docker/spark/spark-defaults.conf`

### Delta Lake Configuration

**Version**: Delta Lake 3.3.2 (for Spark 3.5)
**Maven Coordinates**: `io.delta:delta-spark_2.12:3.3.2`

**Key Features Enabled**:
- ✅ Change Data Feed (CDF) - Tracks all changes to Delta tables
- ✅ ACID transactions - Ensures data consistency
- ✅ Time travel - Query historical versions of data
- ✅ Schema evolution - Automatic schema changes
- ✅ Optimize write - Better performance for writes
- ✅ Auto compaction - Automatic file optimization

### MinIO Object Storage

**Buckets for Delta Lake**:
- `warehouse` - Primary warehouse location (s3a://warehouse/)
- `delta` - Delta Lake-specific storage

**S3A Configuration**:
- Endpoint: `http://minio:9000`
- Path style access enabled
- SSL disabled (for local testing)

## Starting the Infrastructure

### Start All Services

```bash
docker compose up -d
```

### Start Only Delta Lake Services

```bash
docker compose up -d minio spark
```

### Verify Services are Healthy

```bash
# Check Spark Master
curl http://localhost:8080

# Check MinIO
curl http://localhost:9000/minio/health/live

# Run infrastructure health tests
poetry run pytest tests/integration/test_infrastructure.py::TestSparkDeltaInfrastructure -v
```

## Using Delta Lake in Tests

### Pytest Fixtures

Three fixtures are available in `tests/conftest.py`:

#### 1. `spark_session` (session-scoped)

Provides a Spark session with Delta Lake configured for the entire test session.

```python
def test_with_spark(spark_session):
    # Create a DataFrame
    data = [(1, "Alice"), (2, "Bob")]
    df = spark_session.createDataFrame(data, ["id", "name"])

    # Write as Delta table
    df.write.format("delta").mode("overwrite").save("s3a://warehouse/test_table")
```

#### 2. `delta_spark` (session-scoped)

Provides a `DeltaSparkManager` instance for advanced Delta operations.

```python
def test_with_delta_manager(delta_spark):
    # Create DataFrame
    df = delta_spark.get_spark_session().createDataFrame(
        [(1, "test")], ["id", "value"]
    )

    # Write Delta table
    delta_spark.create_delta_table(df, "s3a://warehouse/my_table")

    # Read it back
    result_df = delta_spark.read_delta_table("s3a://warehouse/my_table")
```

#### 3. `delta_table_path` (function-scoped)

Creates a unique temporary path for each test and cleans up automatically.

```python
def test_isolated_table(spark_session, delta_table_path):
    # delta_table_path is automatically created and cleaned up
    df = spark_session.createDataFrame([(1, "test")], ["id", "value"])
    df.write.format("delta").save(delta_table_path)
```

### Using Change Data Feed (CDF)

CDF is enabled by default for all Delta tables. To read changes:

```python
def test_cdf(delta_spark):
    table_path = "s3a://warehouse/cdf_test"

    # Create initial data
    df1 = delta_spark.get_spark_session().createDataFrame(
        [(1, "Alice")], ["id", "name"]
    )
    delta_spark.create_delta_table(df1, table_path)

    # Update data (creates version 1)
    df2 = delta_spark.get_spark_session().createDataFrame(
        [(1, "Alice Updated")], ["id", "name"]
    )
    df2.write.format("delta").mode("overwrite").save(table_path)

    # Read CDF between versions
    changes_df = delta_spark.read_delta_cdf(
        table_path,
        starting_version=0,
        ending_version=1
    )

    # CDF DataFrame includes:
    # - All original columns
    # - _change_type: 'insert', 'update_preimage', 'update_postimage', 'delete'
    # - _commit_version: Version number
    # - _commit_timestamp: Commit timestamp
    changes_df.show()
```

### Direct Delta Lake Access

For tests that need more control:

```python
from tests.fixtures.delta_spark import DeltaSparkManager

def test_custom_delta():
    manager = DeltaSparkManager()
    spark = manager.get_spark_session()

    # Create Delta table with partitioning
    df = spark.createDataFrame(
        [(1, "2025-01", "data1"), (2, "2025-02", "data2")],
        ["id", "month", "value"]
    )

    manager.create_delta_table(
        df,
        "s3a://warehouse/partitioned_table",
        partition_by=["month"]
    )

    # Cleanup
    manager.stop_spark_session()
```

## Running Delta Lake Tests

### All Delta Lake Tests

```bash
poetry run pytest tests/integration/test_deltalake_cdc.py \
                 tests/e2e/test_delta_cdf_workflow.py \
                 tests/e2e/test_postgres_to_delta.py \
                 tests/e2e/test_mysql_to_delta.py -v
```

### Integration Tests Only

```bash
poetry run pytest tests/integration/test_deltalake_cdc.py -v
```

### E2E Workflow Tests

```bash
poetry run pytest tests/e2e/test_delta_cdf_workflow.py -v
```

### CDC Pipeline Tests

```bash
# Postgres to Delta
poetry run pytest tests/e2e/test_postgres_to_delta.py -v

# MySQL to Delta
poetry run pytest tests/e2e/test_mysql_to_delta.py -v
```

## Configuration

Default configuration is in `tests/fixtures/config.py`:

```python
from tests.fixtures.config import SparkConfig, DeltaLakeConfig

spark_config = SparkConfig(
    master_url="spark://localhost:7077",
    app_name="cdc-test",
    driver_memory="1g",
    executor_memory="1g",
    warehouse_dir="s3a://warehouse/",
    s3_endpoint="http://localhost:9000",
    aws_access_key_id="minioadmin",
    aws_secret_access_key="minioadmin"
)

delta_config = DeltaLakeConfig(
    table_path="s3a://delta/",
    warehouse_path="s3a://warehouse/",
    enable_cdf=True,  # CDF enabled by default
    s3_endpoint="http://localhost:9000"
)
```

## Troubleshooting

### Spark Connection Failed

**Symptom**: `Cannot connect to Spark Master` or connection timeout

**Solutions**:
1. Verify Spark is running: `docker compose ps spark`
2. Check Spark logs: `docker logs cdc-spark`
3. Verify port 7077 is accessible: `nc -zv localhost 7077`
4. Wait for Spark to start (can take 30-60 seconds on first run)

### Delta Lake Packages Not Found

**Symptom**: `java.lang.ClassNotFoundException: io.delta.sql.DeltaSparkSessionExtension`

**Solutions**:
1. Verify spark-defaults.conf is mounted: `docker exec cdc-spark ls /opt/spark/conf/`
2. Check package configuration: `grep delta /home/bob/WORK/claude-cdc-demo/docker/spark/spark-defaults.conf`
3. Restart Spark service: `docker compose restart spark`

### S3A Access Errors

**Symptom**: `No FileSystem for scheme: s3a` or S3 access denied

**Solutions**:
1. Verify MinIO is running: `docker compose ps minio`
2. Check S3A configuration in spark-defaults.conf
3. Verify Hadoop AWS packages are loaded: `spark.jars.packages` includes `hadoop-aws:3.3.4`
4. Check MinIO credentials match `minioadmin/minioadmin`

### PySpark Import Error

**Symptom**: `ModuleNotFoundError: No module named 'pyspark'`

**Solutions**:
1. Install dependencies: `poetry install`
2. Verify installation: `poetry run python -c "import pyspark; print(pyspark.__version__)"`
3. Expected version: `3.5.0` or compatible

### Tests Skipped

**Symptom**: Tests show as `SKIPPED` instead of running

**Solutions**:
1. Verify PySpark is installed (see above)
2. Ensure Spark Master is running and healthy
3. Check test skip conditions use proper availability checks (not `condition=True`)

## Performance Optimization

### Spark Configuration Tuning

For better test performance, adjust in `spark-defaults.conf`:

```properties
# Increase memory for larger datasets
spark.driver.memory=2g
spark.executor.memory=2g

# Enable optimizations
spark.databricks.delta.optimizeWrite.enabled=true
spark.databricks.delta.autoCompact.enabled=true
```

### S3A Performance Tuning

Already configured in spark-defaults.conf:

```properties
spark.hadoop.fs.s3a.fast.upload=true
spark.hadoop.fs.s3a.block.size=128M
spark.hadoop.fs.s3a.multipart.size=100M
```

## Advanced Features

### Time Travel Queries

```python
# Read table at specific version
df = spark.read.format("delta").option("versionAsOf", 0).load(table_path)

# Read table at specific timestamp
df = spark.read.format("delta").option("timestampAsOf", "2025-10-30").load(table_path)
```

### Table History

```python
from delta.tables import DeltaTable

delta_table = DeltaTable.forPath(spark, table_path)
history = delta_table.history()
history.show()
```

### Optimize and Vacuum

```python
from delta.tables import DeltaTable

delta_table = DeltaTable.forPath(spark, table_path)

# Compact small files
delta_table.optimize().executeCompaction()

# Remove old files (older than 7 days)
delta_table.vacuum(168)  # hours
```

## Configuration Files

- **Docker Compose**: `docker-compose.yml` (lines 208-236)
- **Spark Defaults**: `docker/spark/spark-defaults.conf`
- **Test Fixtures**: `tests/fixtures/delta_spark.py`
- **Test Config**: `tests/fixtures/config.py`

## Next Steps

- See `quickstart.md` for end-to-end validation scenarios
- See `research.md` for technical decisions and alternatives considered
- See test files for usage examples
- Check [Delta Lake documentation](https://docs.delta.io/) for more features
