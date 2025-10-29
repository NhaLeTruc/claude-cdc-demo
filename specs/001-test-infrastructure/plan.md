# Implementation Plan: Test Infrastructure Enhancement

**Branch**: `001-test-infrastructure` | **Date**: 2025-10-29 | **Spec**: [spec.md](./spec.md)
**Input**: Feature specification from `/specs/001-test-infrastructure/spec.md`

## Summary

Add missing infrastructure components (Apache Spark with Delta Lake, Prometheus AlertManager, mysql-connector-python) to enable currently skipped/failed tests in the CDC demo project. This will reduce test failures from 12 to 0 and enable full local validation of CDC pipelines including Delta Lake operations, alert management, and MySQL change capture.

## Technical Context

**Language/Version**: Python 3.11+ (existing), Docker Compose v2+
**Primary Dependencies**:
- Apache Spark 3.5+ with Delta Lake 3.0+ (NEW)
- Prometheus AlertManager 0.26+ (NEW)
- mysql-connector-python 8.0+ (NEW)
- Existing: Prometheus, Grafana, MySQL, PostgreSQL, Debezium, Kafka

**Storage**:
- Delta Lake tables (local filesystem, docker volumes)
- Existing: PostgreSQL, MySQL, MinIO

**Testing**: pytest (existing), pytest-docker (for infrastructure health checks)
**Target Platform**: Docker Compose environment (Linux containers), developer laptops (8GB RAM minimum)
**Project Type**: Single project (CDC demo with multiple storage backends)
**Performance Goals**:
- Test suite completion: < 5 minutes
- Spark job initialization: < 10 seconds
- Delta Lake writes: < 5 seconds for < 1000 rows
- AlertManager API: < 500ms response time

**Constraints**:
- Infrastructure RAM usage: < 4GB total for new services
- Docker compose startup: < 2 minutes
- Must maintain existing test suite structure
- No cloud dependencies (local-first requirement)

**Scale/Scope**:
- 12 currently failing tests to fix
- ~120 currently skipped tests (reduce to <20)
- 7 Delta Lake writer tests
- 12 AlertManager tests
- 9 MySQL CDC tests

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

Verify compliance with [Postgres CDC Demo Constitution](../../.specify/memory/constitution.md):

- [x] **Test-First Development**: Existing tests already written, implementation enables them (infrastructure-as-code for test environment)
- [x] **Data Quality Assurance**: Delta Lake tests validate CDF captures, MySQL tests verify transaction ordering
- [x] **Local-First Demonstration**: All infrastructure via Docker Compose, no external dependencies, single command setup
- [x] **Multi-Storage CDC Coverage**: Enables Delta Lake CDC testing (CDF), MySQL CDC testing (binlog)
- [x] **Clean Code**: Docker compose configuration, no application code changes required
- [x] **Observability**: Spark UI, AlertManager UI, health checks for all new services
- [x] **Documentation**: Quickstart guide for new infrastructure, troubleshooting section

**Complexity Justification Required For**: None - all requirements met within constitutional limits

## Project Structure

### Documentation (this feature)

```text
specs/001-test-infrastructure/
├── plan.md              # This file
├── research.md          # Phase 0: Spark/AlertManager best practices
├── data-model.md        # Phase 1: N/A (infrastructure only)
├── quickstart.md        # Phase 1: Setup guide for new infrastructure
├── contracts/           # Phase 1: Docker service contracts (health checks, APIs)
│   ├── spark-service.yml
│   └── alertmanager-service.yml
└── tasks.md             # Phase 2: Implementation tasks (created by /speckit.tasks)
```

### Source Code (repository root)

```text
# Existing structure (no application code changes)
src/
├── cdc_pipelines/
│   ├── deltalake/          # Existing Delta Lake code (tests enabled)
│   ├── mysql/              # Existing MySQL code (tests enabled)
│   └── postgres/           # Existing
tests/
├── unit/
│   └── test_cdc_pipelines/
│       ├── test_delta_writer.py      # Currently failing (7 tests)
│       ├── test_delta_versions.py    # Currently failing (4 tests)
│       ├── test_delta_cdf_config.py  # Currently failing (1 test)
│       └── test_mysql_connection.py  # Currently skipped (9 tests)
└── integration/
    ├── test_alerting.py              # Partially enabled (12 AlertManager tests skipped)
    └── test_mysql_cdc.py             # Currently skipped (9 tests)

# New infrastructure configuration
docker-compose.yml                     # Add Spark, AlertManager services
configs/
├── spark/
│   ├── spark-defaults.conf           # Spark configuration
│   └── log4j.properties              # Logging configuration
└── alertmanager/
    └── alertmanager.yml               # Alert routing configuration

# New test fixtures
tests/conftest.py                      # Add spark_session, alertmanager fixtures

# Dependencies
pyproject.toml                         # Add mysql-connector-python
poetry.lock                            # Updated dependencies
```

**Structure Decision**: Infrastructure-only changes using existing project structure. No new source directories needed - only Docker Compose services, configuration files, and test fixtures.

## Complexity Tracking

No constitutional violations - all requirements met within specified limits.

---

## Phase 0: Research & Investigation

### Research Tasks

1. **Spark Docker Configuration**
   - Question: What's the minimal Spark configuration for Delta Lake testing?
   - Research: Bitnami Spark vs Apache Spark official images, memory settings, Delta Lake compatibility
   - Output: Recommended Spark image, memory limits, volume mounts

2. **AlertManager Configuration**
   - Question: How to configure AlertManager for test environment without production routes?
   - Research: AlertManager routing rules, receiver configuration, Prometheus integration
   - Output: Test-safe AlertManager config with mock receivers

3. **Test Isolation Strategy**
   - Question: How to prevent test data pollution in Delta Lake tables?
   - Research: Delta Lake cleanup strategies, table naming conventions, pytest fixtures for Spark sessions
   - Output: Isolation pattern (unique table names vs clean volumes)

4. **MySQL Connector Compatibility**
   - Question: Which mysql-connector-python version works with MySQL 8.x and existing dependencies?
   - Research: Version compatibility matrix, potential conflicts with psycopg2
   - Output: Specific version to pin in pyproject.toml

### Investigation Checklist

- [ ] Verify Spark 3.5 + Delta Lake 3.0 compatibility
- [ ] Test Spark container startup time and memory footprint
- [ ] Validate AlertManager API endpoints used by tests
- [ ] Confirm Prometheus can scrape AlertManager metrics
- [ ] Check mysql-connector-python does not conflict with other DB drivers
- [ ] Verify Delta Lake tables can be created in docker volumes
- [ ] Test cleanup strategies for Delta tables between test runs

---

## Phase 1: Design & Architecture

### Service Architecture

**New Services**:

```text
┌─────────────────────────────────────────────────────────────┐
│                     Docker Compose Stack                      │
├─────────────────────────────────────────────────────────────┤
│  Existing Services                                            │
│  - PostgreSQL (port 5432)                                    │
│  - MySQL (port 3306)                                         │
│  - Debezium (port 8083)                                      │
│  - Kafka + Zookeeper                                         │
│  - Prometheus (port 9090)                                    │
│  - Grafana (port 3000)                                       │
│  - MinIO, Loki                                               │
├─────────────────────────────────────────────────────────────┤
│  NEW Services                                                 │
│  ┌─────────────────────┐  ┌──────────────────────┐         │
│  │ Spark Master         │  │ AlertManager          │         │
│  │ - Port: 7077, 8080  │  │ - Port: 9093, 9094    │         │
│  │ - UI: 4040          │  │ - UI: 9093/alerts     │         │
│  │ - Delta Lake enabled│  │ - Test receivers only │         │
│  │ - Memory: 2GB       │  │ - Memory: 256MB       │         │
│  └─────────────────────┘  └──────────────────────┘         │
│           │                          │                        │
│           │ ┌────────────────────┐  │                        │
│           └─┤ Test Suite (pytest)│──┘                        │
│             │ - Delta Lake tests │                           │
│             │ - Alert tests      │                           │
│             │ - MySQL CDC tests  │                           │
│             └────────────────────┘                           │
└─────────────────────────────────────────────────────────────┘
```

### Docker Service Contracts

**Spark Service**:
```yaml
Health Check: GET http://localhost:8080/ (Spark Master UI)
Readiness: Port 7077 accepting connections
Dependencies: None
Volumes:
  - ./data/delta-tables:/opt/delta-tables
  - ./configs/spark:/opt/spark/conf
Environment:
  - SPARK_MODE=master
  - SPARK_MASTER_HOST=spark
  - SPARK_DRIVER_MEMORY=1g
  - SPARK_EXECUTOR_MEMORY=1g
```

**AlertManager Service**:
```yaml
Health Check: GET http://localhost:9093/-/healthy
Readiness: API accepting POST /api/v1/alerts
Dependencies: Prometheus (optional, for alert evaluation)
Volumes:
  - ./configs/alertmanager:/etc/alertmanager
Environment:
  - None (config file based)
```

### Test Fixture Design

**Spark Session Fixture** (tests/conftest.py):
```python
@pytest.fixture(scope="session")
def spark_session():
    """Provide Spark session with Delta Lake support"""
    # Connect to dockerized Spark master at spark://localhost:7077
    # Configure Delta Lake extensions
    # Set warehouse location to /opt/delta-tables
    yield session
    # Cleanup: optionally clear test tables
```

**AlertManager Fixture** (tests/conftest.py):
```python
@pytest.fixture
def alertmanager_client():
    """Provide AlertManager API client"""
    # Base URL: http://localhost:9093
    # Cleanup: delete test alerts/silences after test
    yield client
```

### Data Model

**N/A** - Infrastructure feature only. No application data models.

### API Contracts

**Spark Master API** (existing Spark REST API):
- GET `/` - Master UI
- GET `/json/` - Cluster metrics (JSON)
- POST `/v1/submissions/create` - Job submission (not used in tests)

**AlertManager API** (existing AlertManager API v2):
- POST `/api/v1/alerts` - Create alert
- GET `/api/v1/alerts` - List active alerts
- POST `/api/v1/silences` - Create silence
- DELETE `/api/v1/silence/{id}` - Delete silence
- GET `/api/v1/status` - Get config and status

---

## Phase 2: Implementation Breakdown

*Detailed task breakdown created by `/speckit.tasks` command - not included in plan*

### High-Level Task Groups

1. **Docker Compose Services** (Priority: P1)
   - Add Spark service definition
   - Add AlertManager service definition
   - Configure health checks and dependencies
   - Create docker volumes for Delta tables

2. **Service Configuration** (Priority: P1)
   - Create Spark configuration files (spark-defaults.conf)
   - Create AlertManager configuration (alertmanager.yml)
   - Configure AlertManager test receivers (mock webhook)
   - Update Prometheus to scrape AlertManager

3. **Python Dependencies** (Priority: P3)
   - Add mysql-connector-python to pyproject.toml
   - Run poetry lock to update dependencies
   - Verify no version conflicts

4. **Test Fixtures** (Priority: P1)
   - Create spark_session fixture in tests/conftest.py
   - Create alertmanager_client fixture in tests/conftest.py
   - Add cleanup logic for Delta tables
   - Add cleanup logic for test alerts

5. **Test Updates** (Priority: P1)
   - Remove skip markers from Delta Lake tests
   - Remove skip markers from AlertManager tests
   - Update MySQL tests to use mysql-connector-python
   - Verify all tests pass

6. **Documentation** (Priority: P2)
   - Create quickstart guide for new infrastructure
   - Document troubleshooting steps (memory issues, port conflicts)
   - Update README with new service ports
   - Add architecture diagram with new services

### Success Validation

**Pre-Merge Checklist**:
- [ ] `docker-compose up` starts all services in < 2 minutes
- [ ] Spark Master UI accessible at localhost:8080
- [ ] AlertManager UI accessible at localhost:9093
- [ ] `make test` shows 0 failed tests (down from 12)
- [ ] Delta Lake tests execute (7 tests passing)
- [ ] AlertManager tests execute (12 tests passing)
- [ ] MySQL CDC tests execute (9 tests passing)
- [ ] Infrastructure uses < 4GB RAM total
- [ ] Documentation updated (quickstart.md, README)
- [ ] Test cleanup verified (no data pollution between runs)

---

## Risks & Mitigation

| Risk | Impact | Mitigation |
|------|--------|------------|
| Spark memory exceeds laptop capacity | High | Conservative settings (1GB driver/executor), document 8GB minimum |
| Delta Lake table cleanup failures | Medium | Implement atomic cleanup in fixtures, add pre-test validation |
| AlertManager test alerts appear in Grafana | Low | Use separate test receivers, add "test" label filter |
| MySQL connector version conflict | Low | Pin specific version, test in CI before merge |
| Docker volume permissions issues | Medium | Use named volumes, document ownership settings |

---

## Dependencies & Prerequisites

**External Dependencies**:
- Docker Compose v2+ installed
- 8GB RAM available on developer machine
- Ports available: 7077, 8080, 4040 (Spark), 9093, 9094 (AlertManager)

**Internal Dependencies**:
- Existing Prometheus service (for AlertManager integration)
- Existing MySQL service (for MySQL CDC tests)
- Existing test structure (pytest, conftest.py)

**Blocking Issues**: None - all dependencies available

---

## Testing Strategy

**Test Levels**:

1. **Infrastructure Tests** (docker-compose)
   - Service health checks pass
   - Services start in correct order
   - Ports are accessible
   - Volumes mounted correctly

2. **Integration Tests** (pytest)
   - Spark session connects successfully
   - Delta Lake tables can be created/written
   - AlertManager API accepts test alerts
   - MySQL connector can query database

3. **End-to-End Tests** (existing test suite)
   - Delta Lake writer tests execute
   - Delta Lake CDF tests execute
   - Delta Lake version tests execute
   - AlertManager routing tests execute
   - MySQL CDC transaction tests execute

**Test Data**:
- Delta Lake: Small synthetic datasets (< 1000 rows)
- AlertManager: Mock alerts with "test" severity
- MySQL: Use existing products table with test data cleanup

---

## Rollout Plan

**Phase 1: Spark Infrastructure** (Day 1-2)
- Add Spark service to docker-compose
- Create Spark configuration files
- Add spark_session fixture
- Enable 7 Delta Lake writer tests
- Verify tests pass

**Phase 2: AlertManager Infrastructure** (Day 2-3)
- Add AlertManager service to docker-compose
- Create AlertManager configuration
- Add alertmanager_client fixture
- Enable 12 AlertManager tests
- Verify tests pass

**Phase 3: MySQL Connector** (Day 3)
- Add mysql-connector-python dependency
- Update MySQL test fixtures
- Enable 9 MySQL CDC tests
- Verify tests pass

**Phase 4: Documentation & Validation** (Day 4)
- Write quickstart.md
- Update README
- Run full test suite validation
- Document troubleshooting steps
- Verify constitution compliance

**Rollback Plan**: Git revert if tests fail; docker-compose services can be commented out without affecting existing functionality.

---

## Future Enhancements (Out of Scope)

- Multi-node Spark cluster (current: single-node)
- Production AlertManager configuration (current: test-only)
- Real notification integrations (email, Slack, PagerDuty)
- Spark performance tuning for larger datasets
- Alternative table formats (Iceberg) - Delta Lake only for now
- Other database connectors (PostgreSQL already working)
