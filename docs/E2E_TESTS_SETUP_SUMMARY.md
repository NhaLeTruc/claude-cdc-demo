# E2E Tests Setup Summary

This document summarizes the implementation of the comprehensive E2E testing infrastructure for the Postgres → Kafka → Delta Lake CDC pipeline.

## Problem Statement

The tests in [tests/e2e/test_postgres_to_delta.py](../tests/e2e/test_postgres_to_delta.py) were skipping with the message:
```
Delta table not found at /tmp/delta-cdc/customers - streaming job not running
```

**Root Cause**: Tests require a running Kafka → Delta Lake streaming pipeline, but:
1. Missing environment variable configuration (`.env` incomplete)
2. No streaming job was running
3. Delta table directory didn't exist
4. No automated way to start required infrastructure

## Solution: 3-Phase Implementation Plan

### Phase 1: Quick Win (Manual Testing) ✅

**Objective**: Enable developers to manually run E2E tests

**Files Created/Modified**:
1. **[.env](./.env)** - Added missing streaming pipeline configuration:
   ```bash
   DELTA_TABLE_PATH=/tmp/delta/customers
   ICEBERG_WAREHOUSE=s3://warehouse/iceberg
   ICEBERG_CATALOG=demo_catalog
   ICEBERG_NAMESPACE=cdc
   ICEBERG_TABLE=customers
   ```

2. **[scripts/e2e/start_test_infrastructure.sh](../scripts/e2e/start_test_infrastructure.sh)** - Automated startup script:
   - Starts Docker services (Postgres, Kafka, Debezium, MinIO)
   - Waits for services to be ready
   - Registers Debezium connector
   - Generates test data if needed
   - Starts Delta Lake streaming pipeline
   - Shows status summary

3. **[scripts/e2e/stop_test_infrastructure.sh](../scripts/e2e/stop_test_infrastructure.sh)** - Cleanup script

4. **[docs/E2E_TESTING.md](./E2E_TESTING.md)** - Complete testing guide with:
   - Quick start instructions
   - Manual setup steps
   - Troubleshooting guide
   - Performance expectations

**Usage**:
```bash
# One command to start everything
./scripts/e2e/start_test_infrastructure.sh

# Run tests
poetry run pytest tests/e2e/test_postgres_to_delta.py -v

# Stop when done
./scripts/e2e/stop_test_infrastructure.sh
```

---

### Phase 2: Docker Integration (CI/CD Ready) ✅

**Objective**: Containerize the streaming pipeline for consistent environments

**Files Created/Modified**:
1. **[compose/streaming/spark-delta-streaming.yml](../compose/streaming/spark-delta-streaming.yml)** - Docker service definition:
   - Runs Kafka → Delta Lake streaming job in container
   - Uses shared Docker volumes for Delta data
   - Auto-starts with `docker compose up`
   - Health checks for monitoring

2. **[compose/base/volumes.yml](../compose/base/volumes.yml)** - Added volumes:
   ```yaml
   delta-data:
     name: cdc-delta-data
   delta-checkpoints:
     name: cdc-delta-checkpoints
   ```

3. **[compose/docker-compose.yml](../compose/docker-compose.yml)** - Included new service:
   ```yaml
   - compose/streaming/spark-delta-streaming.yml
   ```

4. **[tests/e2e/test_postgres_to_delta.py](../tests/e2e/test_postgres_to_delta.py)** - Enhanced `delta_table_path` fixture:
   - Auto-detects local vs Docker paths
   - Supports both `/tmp/delta/customers` (local) and `/opt/delta-lake/customers` (Docker)

**Usage**:
```bash
# Start all services including streaming pipeline
docker compose -f compose/docker-compose.yml up -d

# Run tests (pipeline auto-running in container)
poetry run pytest tests/e2e/test_postgres_to_delta.py -v
```

---

### Phase 3: Full Automation (Zero Manual Setup) ✅

**Objective**: Tests manage their own infrastructure automatically

**Files Created**:
1. **[tests/e2e/conftest.py](../tests/e2e/conftest.py)** - Pytest fixtures with auto-management:
   - `docker_services`: Starts Docker Compose services automatically
   - `debezium_connector`: Registers CDC connector
   - `delta_streaming_pipeline`: Starts/stops streaming job
   - `e2e_test_infrastructure`: Auto-use fixture for all tests

2. **[scripts/e2e/health_check.sh](../scripts/e2e/health_check.sh)** - Infrastructure health validation:
   - Checks all required services
   - Validates Debezium connector status
   - Confirms streaming pipeline is running
   - Verifies Delta table exists

**Usage**:
```bash
# Just run tests - fixtures handle everything
poetry run pytest tests/e2e/test_postgres_to_delta.py -v

# Or check health before running
./scripts/e2e/health_check.sh
poetry run pytest tests/e2e/test_postgres_to_delta.py -v

# Skip automatic pipeline management (if already running)
SKIP_PIPELINE_MANAGEMENT=1 poetry run pytest tests/e2e/test_postgres_to_delta.py -v
```

---

## File Structure Summary

```
claude-cdc-demo/
├── .env                                          [MODIFIED] Added streaming config
├── compose/
│   ├── docker-compose.yml                        [MODIFIED] Added Delta streaming service
│   ├── base/volumes.yml                          [MODIFIED] Added Delta volumes
│   └── streaming/
│       └── spark-delta-streaming.yml             [NEW] Docker service for Delta pipeline
├── docs/
│   ├── E2E_TESTING.md                            [NEW] Complete testing guide
│   └── E2E_TESTS_SETUP_SUMMARY.md                [NEW] This file
├── scripts/
│   └── e2e/
│       ├── start_test_infrastructure.sh          [NEW] Automated startup
│       ├── stop_test_infrastructure.sh           [NEW] Automated cleanup
│       └── health_check.sh                       [NEW] Infrastructure validation
└── tests/
    └── e2e/
        ├── conftest.py                           [NEW] Automatic fixtures
        └── test_postgres_to_delta.py             [MODIFIED] Enhanced path detection
```

## Testing Approaches

### Approach 1: Manual Control (Developer Workflow)
```bash
./scripts/e2e/start_test_infrastructure.sh       # Start once
poetry run pytest tests/e2e/... -v               # Run many times
./scripts/e2e/stop_test_infrastructure.sh        # Stop when done
```

### Approach 2: Docker-Based (CI/CD Pipeline)
```bash
docker compose -f compose/docker-compose.yml up -d
poetry run pytest tests/e2e/... -v
docker compose -f compose/docker-compose.yml down
```

### Approach 3: Fully Automated (Zero Setup)
```bash
pytest tests/e2e/... -v   # Fixtures handle everything
```

## Key Features

### ✅ Smart Path Detection
Tests automatically detect whether streaming pipeline is running locally or in Docker and use the appropriate Delta Lake path.

### ✅ Graceful Degradation
Tests skip gracefully with helpful messages if infrastructure isn't available, rather than failing with cryptic errors.

### ✅ Multiple Execution Modes
- Manual control for development
- Docker containers for CI/CD
- Automatic fixtures for convenience

### ✅ Health Checks
Comprehensive validation of all required services before running tests.

### ✅ Documentation
Complete guides for troubleshooting, performance expectations, and usage patterns.

## Environment Variables

| Variable | Default | Purpose |
|----------|---------|---------|
| `DELTA_TABLE_PATH` | `/tmp/delta/customers` | Where Delta Lake data is stored |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:29092` | Kafka connection |
| `KAFKA_TOPIC` | `debezium.public.customers` | CDC topic to consume |
| `DEBEZIUM_URL` | `http://localhost:8083` | Debezium Connect API |
| `CHECKPOINT_LOCATION` | `/tmp/spark-checkpoints/kafka-to-delta` | Spark streaming checkpoints |
| `SKIP_PIPELINE_MANAGEMENT` | (unset) | Set to `1` to disable auto-start in fixtures |

## CI/CD Integration Example

```yaml
# GitHub Actions / GitLab CI
test-e2e:
  runs-on: ubuntu-latest
  steps:
    - uses: actions/checkout@v3

    - name: Start Infrastructure
      run: |
        docker compose -f compose/docker-compose.yml up -d
        ./scripts/e2e/health_check.sh

    - name: Run E2E Tests
      run: |
        poetry install
        poetry run pytest tests/e2e/test_postgres_to_delta.py -v \
          --junit-xml=test-results.xml

    - name: Cleanup
      if: always()
      run: docker compose -f compose/docker-compose.yml down -v
```

## Performance Metrics

### Expected Performance
- **CDC Lag**: < 30 seconds (typically 10-20s)
- **Throughput**: 50+ records/batch
- **Cold Start**: 30-60 seconds for first event
- **Recovery**: < 60 seconds after restart

### Test Timing
- Each test includes 20-30s wait for CDC propagation
- Total test suite: ~5-10 minutes (8 tests)
- Parallelization not recommended (shared database state)

## Troubleshooting Quick Reference

| Issue | Solution |
|-------|----------|
| Tests skip | Run `./scripts/e2e/health_check.sh` to diagnose |
| No data in Delta | Check Kafka topic has messages |
| Connector not running | Re-register with setup script |
| Pipeline not starting | Check PySpark installation |
| Port conflicts | Adjust ports in `.env` |

## Next Steps / Future Enhancements

1. **Multi-table support**: Extend to orders, products, inventory tables
2. **Schema evolution tests**: Test DDL changes during streaming
3. **Performance benchmarks**: Automated throughput/latency measurement
4. **Chaos testing**: Test pipeline resilience to failures
5. **Data quality validation**: Automated comparison of source vs sink
6. **Metrics collection**: Export Prometheus metrics from tests

## Related Documentation

- [STREAMING_PIPELINES.md](STREAMING_PIPELINES.md) - Pipeline architecture details
- [E2E_TESTING.md](E2E_TESTING.md) - Comprehensive testing guide
- [compose/README.md](../compose/README.md) - Docker infrastructure details

## Support

For issues or questions:
1. Check [E2E_TESTING.md](E2E_TESTING.md) troubleshooting section
2. Run `./scripts/e2e/health_check.sh` for diagnostics
3. Check logs:
   - Streaming: `tail -f /tmp/kafka-to-delta.log`
   - Docker: `docker compose -f compose/docker-compose.yml logs -f`
   - Debezium: `curl http://localhost:8083/connectors/postgres-cdc-connector/status`