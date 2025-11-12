# Integration Tests

## Overview
Integration tests verify the CDC pipeline components work together correctly.

## Test Categories

### 1. Infrastructure Tests
- Basic connectivity tests for databases, Kafka, Debezium
- No special requirements

### 2. CDC Tests (Postgres, MySQL, DeltaLake)
- Test individual CDC pipelines
- Require running services: Database + Debezium + Kafka

### 3. Cross-Storage Tests (Postgres → Kafka → Iceberg)
- Test end-to-end flow across storage systems
- **Additional Requirement**: Spark streaming job

## Known Limitations

### Cross-Storage Tests Require Spark Streaming Job

The following tests in `test_cross_storage.py` require a **Spark streaming application** to be running:

- `test_postgres_kafka_iceberg_flow`
- `test_schema_drift_handling`
- `test_data_transformation_accuracy`
- `test_high_throughput_scenario`
- `test_idempotent_writes_to_iceberg`

**Why**: These tests verify the complete pipeline: Postgres → Debezium → Kafka → **Spark → Iceberg**

Without the Spark streaming job:
- ✅ Data flows from Postgres to Kafka (verified by tests)
- ❌ No data appears in Iceberg tables
- Tests fail with: `AssertionError: assert 0 > 0` (expecting Iceberg data)

**To run these tests successfully**:

1. Start the Spark streaming job that processes Kafka messages into Iceberg:
   ```bash
   # Example - adjust based on your setup
   ./scripts/pipelines/submit-spark-job.sh postgres_to_iceberg
   ```

2. Verify the job is running:
   ```bash
   # Check for Spark application
   docker ps | grep spark

   # Check Iceberg tables have data
   # (tables should be populated from Kafka topics)
   ```

3. Run the tests:
   ```bash
   make test-integration
   ```

## Fixing Test Failures

### Transaction Errors
**Error**: `psycopg2.errors.InFailedSqlTransaction: current transaction is aborted`

**Fix**: Applied - test fixtures now properly rollback failed transactions

### Schema Mismatch Errors
**Error**: `KeyError: 'id'` or `ValueError: Could not find field with name id`

**Fix**: Applied - tests now use `customer_id` to match Iceberg schema

### Duplicate Key Errors
**Error**: `psycopg2.errors.UniqueViolation: duplicate key value violates unique constraint`

**Fix**: Applied - tests now use unique emails with UUID suffixes

### Table Name Errors
**Error**: `psycopg2.errors.UndefinedTable: relation "schema_evo_test" does not exist`

**Fix**: Applied - corrected table name from `schema_evo_test` to `schema_evolution_test`

## Running Specific Test Groups

```bash
# Run only infrastructure tests (fast, no CDC required)
pytest tests/integration/test_infrastructure.py -v

# Run CDC tests (requires Debezium + Kafka)
pytest tests/integration/test_postgres_cdc.py -v
pytest tests/integration/test_mysql_cdc.py -v

# Run cross-storage tests (requires Spark streaming)
pytest tests/integration/test_cross_storage.py -v

# Skip tests that require external services
pytest tests/integration -m "not slow" -v
```
