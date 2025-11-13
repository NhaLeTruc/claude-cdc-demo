# End-to-End Testing Guide

This guide explains how to run end-to-end (E2E) tests for the CDC pipelines, specifically the Postgres → Kafka → Delta Lake pipeline tests.

## Overview

The E2E tests in `tests/e2e/test_postgres_to_delta.py` validate the complete CDC pipeline:

1. **PostgreSQL** - Source database with CDC enabled
2. **Debezium** - Captures database changes
3. **Kafka** - Message broker for CDC events
4. **Spark Streaming** - Processes and transforms CDC events
5. **Delta Lake** - Target data lake with Change Data Feed

## Prerequisites

- Docker and Docker Compose installed
- Python 3.11+ with Poetry
- At least 4GB RAM available for Docker
- Ports available: 5432 (Postgres), 9092 (Kafka), 8083 (Debezium), 9000 (MinIO)

## Quick Start

### Automated Setup (Recommended)

Use the helper script to start all required infrastructure:

```bash
# Start all infrastructure and streaming pipeline
./scripts/e2e/start_test_infrastructure.sh

# Run E2E tests
poetry run pytest tests/e2e/test_postgres_to_delta.py -v

# Stop infrastructure when done
./scripts/e2e/stop_test_infrastructure.sh
```

### Manual Setup

If you prefer manual control:

#### 1. Start Docker Services

```bash
# From project root
docker compose -f compose/docker-compose.yml up -d postgres kafka debezium minio
```

#### 2. Wait for Services (30-60 seconds)

Check service status:
```bash
# PostgreSQL
docker exec cdc-postgres pg_isready -U cdcuser

# Kafka
docker exec cdc-kafka kafka-topics --bootstrap-server localhost:9092 --list

# Debezium
curl http://localhost:8083/connectors
```

#### 3. Register Debezium Connector

```bash
# If using CLI tool
poetry run python -m src.cli.main setup debezium

# Or if using standalone script
poetry run python scripts/setup_debezium_connector.py
```

Verify connector is running:
```bash
curl http://localhost:8083/connectors/postgres-cdc-connector/status
# Should show "state": "RUNNING"
```

#### 4. Generate Test Data (if needed)

```bash
# Check if data exists
docker exec cdc-postgres psql -U cdcuser -d cdcdb -c "SELECT COUNT(*) FROM customers"

# Generate if needed
poetry run python scripts/generate_sample_data.py
```

#### 5. Start Delta Lake Streaming Pipeline

```bash
# Start Delta Lake pipeline only
./scripts/pipelines/orchestrate_streaming_pipelines.sh start delta

# Check status
./scripts/pipelines/orchestrate_streaming_pipelines.sh status

# View logs
./scripts/pipelines/orchestrate_streaming_pipelines.sh logs delta
```

#### 6. Run Tests

```bash
# Run all E2E tests
poetry run pytest tests/e2e/test_postgres_to_delta.py -v

# Run specific test
poetry run pytest tests/e2e/test_postgres_to_delta.py::TestPostgresToDeltaLakePipeline::test_full_pipeline_insert_flow -v

# Run with markers
poetry run pytest -m e2e -v
```

## Understanding the Tests

### Test Structure

The E2E tests validate the following scenarios:

1. **INSERT Flow** - New records appear in Delta Lake
2. **UPDATE Flow** - Updates are captured and versioned
3. **DELETE Flow** - Deletes are handled appropriately
4. **Throughput** - Pipeline can handle batch inserts (50 records)
5. **Lag** - CDC lag is under 30 seconds
6. **Data Quality** - Transformations are applied correctly
7. **Recovery** - Pipeline recovers from interruptions
8. **Schema Evolution** - Schema changes are handled

### Test Timing

Each test includes sleep intervals to allow CDC propagation:
- Initial insert: 20 seconds wait
- After update: 20 seconds wait
- Throughput test: 30 seconds wait

### Why Tests Skip

Tests will skip with message "Delta table not found - streaming job not running" if:
- Delta Lake streaming pipeline is not running
- Delta table path doesn't exist at `/tmp/delta/customers`
- No CDC events have been processed yet (cold start)

## Troubleshooting

### Tests Are Skipping

**Problem**: All tests skip with "Delta table not found"

**Solutions**:
1. Check if streaming pipeline is running:
   ```bash
   ./scripts/pipelines/orchestrate_streaming_pipelines.sh status
   ```

2. Check if Delta table exists:
   ```bash
   ls -la /tmp/delta/customers/
   ```

3. Check streaming pipeline logs:
   ```bash
   tail -f /tmp/kafka-to-delta.log
   ```

4. Verify Debezium connector is capturing changes:
   ```bash
   curl http://localhost:8083/connectors/postgres-cdc-connector/status
   ```

### Pipeline Not Starting

**Problem**: Streaming pipeline fails to start

**Solutions**:
1. Check Kafka is running:
   ```bash
   nc -z localhost 29092
   ```

2. Check PySpark is installed:
   ```bash
   poetry run python -c "from pyspark.sql import SparkSession; print('OK')"
   ```

3. Check environment variables:
   ```bash
   source .env
   echo $DELTA_TABLE_PATH
   echo $KAFKA_BOOTSTRAP_SERVERS
   ```

### No Data Flowing

**Problem**: Pipeline runs but no data appears in Delta Lake

**Solutions**:
1. Check Kafka topic has messages:
   ```bash
   docker exec cdc-kafka kafka-console-consumer \
     --bootstrap-server localhost:9092 \
     --topic debezium.public.customers \
     --from-beginning \
     --max-messages 5
   ```

2. Check PostgreSQL replication slot:
   ```bash
   docker exec cdc-postgres psql -U cdcuser -d cdcdb -c \
     "SELECT * FROM pg_replication_slots WHERE slot_name = 'postgres_debezium_slot'"
   ```

3. Restart streaming pipeline:
   ```bash
   ./scripts/pipelines/orchestrate_streaming_pipelines.sh restart delta
   ```

### Checkpoint Recovery Issues

**Problem**: Pipeline stuck or not processing new events

**Solution**: Clear checkpoint and restart:
```bash
# Stop pipeline
./scripts/pipelines/orchestrate_streaming_pipelines.sh stop delta

# Clear checkpoint
rm -rf /tmp/spark-checkpoints/kafka-to-delta

# Restart
./scripts/pipelines/orchestrate_streaming_pipelines.sh start delta
```

## Test Data Cleanup

E2E tests create and clean up their own test data. Each test:
1. Inserts records with unique timestamps in email
2. Performs test validations
3. Deletes test records in cleanup (see `customer_id` cleanup at end of each test)

## Performance Expectations

### Normal Operation
- **CDC Lag**: < 30 seconds (typically 10-20s)
- **Throughput**: 50+ records/batch
- **Recovery Time**: < 60 seconds after restart

### Cold Start
- **First Event**: May take 30-60 seconds
- **Debezium Snapshot**: Can take several minutes for large databases
- **Spark Initialization**: 20-30 seconds

## Environment Variables

Key environment variables for E2E tests (in `.env`):

```bash
# Kafka
KAFKA_BOOTSTRAP_SERVERS=localhost:29092
KAFKA_TOPIC=debezium.public.customers

# Delta Lake
DELTA_TABLE_PATH=/tmp/delta/customers
CHECKPOINT_LOCATION=/tmp/spark-checkpoints/kafka-to-delta

# PostgreSQL
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_USER=cdcuser
POSTGRES_PASSWORD=cdcpass
POSTGRES_DB=cdcdb

# Debezium
DEBEZIUM_URL=http://localhost:8083
```

## CI/CD Integration

For automated testing in CI/CD:

```bash
# GitHub Actions / GitLab CI example
steps:
  - name: Start Infrastructure
    run: ./scripts/e2e/start_test_infrastructure.sh

  - name: Run E2E Tests
    run: poetry run pytest tests/e2e/test_postgres_to_delta.py -v --junit-xml=test-results.xml

  - name: Stop Infrastructure
    if: always()
    run: ./scripts/e2e/stop_test_infrastructure.sh
```

## Docker-Based Streaming (Phase 2)

Coming soon: Docker container for Delta Lake streaming pipeline that auto-starts with `docker compose up`.

## Automatic Test Fixtures (Phase 3)

Coming soon: Pytest fixtures that automatically start/stop streaming pipeline for each test session.

## Related Documentation

- [Streaming Pipelines](STREAMING_PIPELINES.md) - Detailed pipeline architecture
- [Infrastructure Setup](../compose/README.md) - Docker Compose services
- [CDC Configuration](CDC_SETUP.md) - Debezium and CDC setup

## Support

If you encounter issues:
1. Check logs in `/tmp/kafka-to-delta.log`
2. Review Docker logs: `docker compose -f compose/docker-compose.yml logs -f`
3. Verify all services are healthy: `docker compose -f compose/docker-compose.yml ps`