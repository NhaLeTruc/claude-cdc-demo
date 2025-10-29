# Implementation Plan: Test Infrastructure Expansion

**Branch**: `002-infrastructure-expansion` | **Date**: 2025-10-29 | **Spec**: [spec.md](./spec.md)
**Input**: Feature specification from `/specs/002-infrastructure-expansion/spec.md`

## Summary

This feature expands the Docker Compose testing environment to enable 111 currently skipped tests by adding:
1. **Apache Iceberg** infrastructure (REST Catalog + PyIceberg) for 48 tests
2. **Delta Lake** packages for Spark for 25 tests
3. **Confluent Schema Registry** service for 6 tests
4. **Debezium connector** registration automation for 2 tests
5. **Alertmanager** receiver configurations for 4 tests

Technical approach: Modify existing `docker-compose.yml` to add new services, update Spark configuration for Delta Lake, install PyIceberg in Python environment, and create test fixtures for connector registration.

## Technical Context

**Language/Version**: Python 3.11+ (existing test environment)
**Primary Dependencies**:
- Docker Compose v2+
- PyIceberg 0.6.0+
- Delta Lake 2.4.0 (Spark packages)
- Apache Iceberg REST Catalog (tabulario/iceberg-rest:latest)
- Confluent Schema Registry 7.5.0
- Existing: Kafka, Spark 3.5.0, MinIO, Debezium, Postgres, MySQL

**Storage**: MinIO (existing, configured as S3-compatible warehouse for Iceberg/Delta)
**Testing**: pytest (existing framework), test fixtures for infrastructure setup/teardown
**Target Platform**: Linux Docker containers (local development environment)
**Project Type**: Infrastructure/testing expansion (modifies docker-compose.yml, not application code)
**Performance Goals**:
- All tests complete in < 15 minutes
- Iceberg test suite (48 tests) < 10 minutes
- Delta Lake tests (25 tests) < 8 minutes
- Docker environment startup < 3 minutes with new services

**Constraints**:
- Total memory footprint < 8GB for all containers
- No custom Docker image builds (use published images + runtime configuration)
- No persistent storage required (ephemeral test data only)
- All services must support graceful shutdown/restart

**Scale/Scope**:
- 4 new Docker services to add
- 2 configuration file updates (docker-compose.yml, Spark config)
- 1 Python dependency addition (PyIceberg)
- 111 tests to enable across 5 priority levels

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

Verify compliance with [Postgres CDC Demo Constitution](../../.specify/memory/constitution.md):

- [x] **Test-First Development**: Tests already exist (currently skipped); implementation enables them (infrastructure-as-code approach)
- [x] **Data Quality Assurance**: Existing tests validate data integrity; infrastructure expansion allows these validations to run
- [x] **Local-First Demonstration**: All additions are Docker Compose services; maintains one-command setup (`docker-compose up`)
- [x] **Multi-Storage CDC Coverage**: Adds Iceberg and Delta Lake support, completing coverage of major table formats
- [x] **Clean Code**: Infrastructure changes (YAML config, no application code); follows Docker Compose best practices
- [x] **Observability**: New services will inherit existing observability stack (Prometheus, Grafana, Loki)
- [x] **Documentation**: Will update README, create quickstart for Iceberg/Delta/Schema Registry validation

**Complexity Justification Required For**:
- ❌ External dependencies: None (all containerized)
- ❌ Setup time > 10 minutes: Target is < 3 minutes with new services
- ⚠️ **Resource requirements**: 8GB may be tight with 4 new services (Iceberg Catalog, Schema Registry, and Spark memory for Delta)
  - **Justification**: Schema Registry is lightweight (<512MB), Iceberg Catalog is stateless (<256MB), Spark already allocated, total expected ~7.5GB
- ❌ CDC lag > 5 seconds: N/A (infrastructure enables tests; tests verify lag)

**Post-Phase 1 Re-check**: ✅ Constitution compliance maintained

## Project Structure

### Documentation (this feature)

```text
specs/002-infrastructure-expansion/
├── plan.md              # This file
├── research.md          # Technology decisions for Iceberg/Delta/Schema Registry
├── data-model.md        # Service configuration schemas
├── quickstart.md        # How to validate new infrastructure
├── contracts/           # Docker Compose service contracts
│   ├── iceberg-catalog.yml
│   ├── schema-registry.yml
│   └── spark-delta-config.yml
└── tasks.md             # Implementation tasks (created by /speckit.tasks)
```

### Source Code (repository root)

```text
# Infrastructure files
docker-compose.yml              # Updated with new services
docker/                        # Service-specific configs
├── iceberg-rest/
│   └── catalog-config.yaml    # Iceberg REST Catalog configuration
├── spark/
│   └── spark-defaults.conf    # Delta Lake package config
└── schema-registry/
    └── schema-registry.properties

# Python dependencies
pyproject.toml                 # Add PyIceberg
poetry.lock                    # Updated dependencies

# Test fixtures
tests/
├── conftest.py               # Updated with infrastructure fixtures
├── fixtures/
│   ├── iceberg_catalog.py   # Iceberg catalog setup/teardown
│   ├── delta_spark.py       # Delta-enabled Spark session
│   ├── schema_registry.py   # Schema Registry client
│   └── debezium_connector.py # Connector registration helpers
└── integration/
    └── test_infrastructure.py # Validate new services are healthy

# Documentation
README.md                      # Updated with new infrastructure
docs/
└── infrastructure/
    ├── iceberg-setup.md
    ├── delta-lake-setup.md
    └── schema-registry-setup.md
```

## Phase 0: Research & Decisions

### Research Questions

1. **Iceberg REST Catalog Configuration**
   - Which Docker image provides Apache Iceberg REST Catalog?
   - What MinIO configuration is needed for Iceberg warehouse?
   - How should PyIceberg connect to catalog (URL, auth)?

2. **Delta Lake Spark Integration**
   - Which Delta Lake version is compatible with Spark 3.5.0?
   - How to configure `spark.jars.packages` without rebuilding Docker image?
   - What Spark session configuration is required for CDF?

3. **Schema Registry Setup**
   - Which Confluent Schema Registry version matches Kafka 7.5.0?
   - What environment variables link Schema Registry to Kafka?
   - How do tests register/query schemas programmatically?

4. **Debezium Connector Registration**
   - What is the Debezium REST API endpoint for connector registration?
   - How to handle connector already exists errors in test fixtures?
   - What JSON payload structure is required for Postgres connectors?

5. **Test Fixture Design**
   - Should fixtures be session-scoped (once per test run) or function-scoped?
   - How to ensure infrastructure is healthy before tests run?
   - What cleanup strategy prevents test interference?

### Research Output

See [research.md](./research.md) for detailed findings and rationale.

## Phase 1: Design

### Data Model

See [data-model.md](./data-model.md) for service configuration schemas:
- Iceberg Catalog service definition
- Schema Registry service definition
- Delta Lake Spark configuration
- Test fixture data models

### API Contracts

See [contracts/](./contracts/) for:
- `iceberg-catalog.yml`: Iceberg REST Catalog API contract
- `schema-registry.yml`: Schema Registry REST API contract
- `spark-delta-config.yml`: Spark configuration schema

### Quickstart Guide

See [quickstart.md](./quickstart.md) for validation steps:
1. Start expanded environment: `docker-compose up`
2. Verify service health
3. Run Iceberg test suite
4. Run Delta Lake test suite
5. Run Schema Registry tests
6. Validate all 111 tests now enabled

## Phase 2: Task Breakdown

*Tasks generated by `/speckit.tasks` command - see [tasks.md](./tasks.md)*

Task categories (preview):
1. **Docker Compose Modifications** (P1)
   - Add Iceberg REST Catalog service
   - Add Schema Registry service
   - Configure service dependencies and health checks

2. **Configuration Updates** (P1-P2)
   - Create Iceberg catalog config
   - Update Spark with Delta Lake packages
   - Configure MinIO bucket for Iceberg warehouse

3. **Python Dependencies** (P1)
   - Add PyIceberg to pyproject.toml
   - Update poetry.lock
   - Verify import in test environment

4. **Test Fixtures** (P1-P4)
   - Create Iceberg catalog fixture
   - Create Delta Spark session fixture
   - Create Schema Registry client fixture
   - Create Debezium connector registration fixture

5. **Documentation** (P1-P5)
   - Update README with new services
   - Create infrastructure validation guide
   - Document service configurations

6. **Testing & Validation** (All priorities)
   - Run Iceberg test suite (48 tests)
   - Run Delta Lake test suite (25 tests)
   - Run Schema Registry tests (6 tests)
   - Run Debezium connector tests (2 tests)
   - Run Alertmanager tests (4 tests)
   - Verify all 186 + 111 = 297 tests pass

## Complexity Tracking

### Accidental Complexity (to minimize)
- Multiple configuration files for different services (unavoidable with Docker Compose)
- MinIO bucket creation for Iceberg (one-time setup, documented in quickstart)

### Essential Complexity (inherent to problem)
- Iceberg requires REST Catalog + S3 storage (architectural requirement)
- Delta Lake requires Spark package loading (JVM dependency management)
- Schema Registry requires Kafka connectivity (distributed systems coupling)
- Service orchestration order (health checks ensure correct startup sequence)

### Complexity Mitigations
- Use official Docker images (no custom builds)
- Centralize configuration in docker-compose.yml
- Document service dependencies in quickstart
- Create reusable test fixtures to hide complexity from individual tests
- Health checks ensure services ready before tests run

## Risks & Mitigations

| Risk | Impact | Probability | Mitigation |
|------|--------|-------------|------------|
| Memory exceeds 8GB with 4 new services | High (can't run on dev machines) | Medium | Allocate minimal memory to new services; Schema Registry <512MB, Iceberg Catalog <256MB |
| Iceberg/Delta version incompatibility | High (tests fail) | Low | Research phase validates versions; use known-compatible versions |
| Service startup order issues | Medium (intermittent failures) | Medium | Implement health checks; use `depends_on` with `condition: service_healthy` |
| MinIO bucket permission errors | Medium (Iceberg can't write) | Low | Pre-create bucket in MinIO init script with public access |
| Schema Registry not finding Kafka | High (service won't start) | Low | Use Docker network DNS; test connectivity in quickstart |
| PyIceberg import errors | High (Iceberg tests fail) | Low | Pin PyIceberg version; test import in fixture before running tests |
| Debezium connector conflicts | Low (2 tests affected) | Medium | Implement connector existence check; delete before create |
| Test flakiness from async operations | Medium (unreliable tests) | Medium | Add waits/retries in fixtures; verify service health before assertions |

## Dependencies

### Service Dependencies
- **Iceberg Catalog** depends on: MinIO (warehouse storage)
- **Schema Registry** depends on: Kafka, ZooKeeper
- **Delta Lake** depends on: Spark, MinIO (data storage)
- **All new services** depend on: Docker network, health check framework

### Package Dependencies
- **PyIceberg** requires: Python 3.8+, pyarrow, requests
- **Delta Lake** requires: Spark 3.x, Scala 2.12

### Test Dependencies
- **Iceberg tests** require: PyIceberg fixture, catalog health
- **Delta tests** require: Spark with Delta packages, MinIO accessible
- **Schema tests** require: Schema Registry fixture, Kafka topics
- **Debezium tests** require: Connector registration fixture, Debezium API

## Success Metrics

From spec.md Success Criteria:

1. **SC-001**: Test suite completion 186/297 → 297/297 (100%)
2. **SC-002**: Iceberg tests (48) complete in < 10 minutes
3. **SC-003**: Delta tests (25) pass with 0% flakiness over 10 runs
4. **SC-004**: Schema tests (6) complete in < 2 minutes
5. **SC-005**: Table operations: write < 5s/1000 rows, read < 2s
6. **SC-006**: Docker startup < 3 minutes (all services)
7. **SC-007**: Memory < 8GB total
8. **SC-008**: Data consistency validated (source count = destination count)
9. **SC-009**: Cross-storage flows validated (Postgres/MySQL → Iceberg/Delta)
10. **SC-010**: Schema Registry handles 100 registrations/minute

### Measurement Plan
- Run `make test` before/after to measure test count change
- Use `time docker-compose up` to measure startup time
- Use `docker stats` to measure memory footprint
- Run test suites 10 times to measure flakiness rate
- Profile table operations in integration tests to measure latency

## Agent Context Update

After Phase 1 completion, run:
```bash
.specify/scripts/bash/update-agent-context.sh claude
```

This will add to CLAUDE.md:
- Apache Iceberg (table format)
- Delta Lake (table format, CDF)
- PyIceberg (Python client)
- Confluent Schema Registry (schema management)
- Tabulario Iceberg REST Catalog (catalog service)

## Notes

- This is **infrastructure expansion only** - no application code changes
- All 111 tests already exist; this feature **enables** them to run
- Maintains constitutional principle: local-first, one-command setup
- Prioritization aligns with test coverage impact (Iceberg 43%, Delta 23%, etc.)
- Success measured by test enablement, not new functionality
