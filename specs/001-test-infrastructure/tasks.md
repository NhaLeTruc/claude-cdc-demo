# Tasks: Test Infrastructure Enhancement

**Feature**: Test Infrastructure Enhancement
**Branch**: `001-test-infrastructure`
**Date**: 2025-10-29

## Overview

This task list implements the infrastructure needed to enable 28 currently skipped/failed tests:
- **US1 (P1)**: 7 Delta Lake writer tests → Requires Spark + Delta Lake
- **US2 (P2)**: 12 AlertManager tests → Requires AlertManager service
- **US3 (P3)**: 9 MySQL CDC tests → Requires mysql-connector-python

**Total Estimated Tasks**: 47
**Parallel Execution**: Tasks marked with [P] can run in parallel with other [P] tasks in the same phase

---

## Phase 1: Project Setup (US0 - Prerequisites)

- [x] T001 [P] Create directory structure for Docker configurations: `mkdir -p configs/spark configs/alertmanager data/delta-tables`
- [x] T002 [P] Create Docker volume definitions in docker-compose.yml for alertmanager-data
- [x] T003 Verify Docker Compose version >= 2.0: `docker-compose --version`
- [x] T004 Verify minimum 8GB RAM available for Docker Desktop
- [x] T005 [P] Verify ports 7077, 8080, 4040, 9093, 9094 are available: `lsof -i :7077`

---

## Phase 2: User Story 1 - Spark + Delta Lake Infrastructure (P1)

### Docker Service Configuration

- [x] T010 [US1] Add Spark service definition to docker-compose.yml (apache/spark:3.5.0)
- [x] T011 [US1] Configure Spark environment variables in docker-compose.yml (SPARK_MASTER_HOST=spark)
- [x] T012 [US1] Configure Spark memory settings in docker-compose.yml (SPARK_DRIVER_MEMORY=1g, SPARK_EXECUTOR_MEMORY=1g)
- [x] T013 [US1] Add Spark port mappings to docker-compose.yml (7077:7077, 8080:8080, 4040:4040)
- [x] T014 [US1] Add Spark volume mounts to docker-compose.yml (./data/delta-tables:/opt/delta-tables, ./configs/spark:/opt/spark/conf:ro)
- [x] T015 [US1] Add Spark health check to docker-compose.yml (curl -f http://localhost:8080)
- [x] T016 [US1] Add Spark resource limits to docker-compose.yml (mem_limit: 2g, cpus: 1.0)

### Spark Configuration Files

- [x] T020 [US1] Create configs/spark/spark-defaults.conf
- [x] T021 [US1] Add Delta Lake extension to spark-defaults.conf (spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension)
- [x] T022 [US1] Add Delta catalog to spark-defaults.conf (spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog)
- [x] T023 [US1] Add Delta Lake JAR package to spark-defaults.conf (spark.jars.packages=io.delta:delta-core_2.12:3.0.0)
- [x] T024 [US1] Add warehouse directory to spark-defaults.conf (spark.sql.warehouse.dir=/opt/delta-tables)
- [x] T025 [US1] Disable event logging in spark-defaults.conf (spark.eventLog.enabled=false)

### Python Dependencies

- [x] T030 [P] [US1] Add PySpark dependency to pyproject.toml (pyspark = "^3.5.0")
- [x] T031 [P] [US1] Add delta-spark dependency to pyproject.toml (delta-spark = "^3.0.0")
- [x] T032 [US1] Run poetry lock to update dependencies
- [x] T033 [US1] Run poetry install to install new packages

### Test Fixtures

- [x] T040 [US1] Create spark_session pytest fixture in tests/conftest.py (session scope)
- [x] T041 [US1] Configure spark_session fixture to connect to spark://localhost:7077
- [x] T042 [US1] Add Delta Lake configuration to spark_session fixture
- [x] T043 [US1] Create delta_table_path pytest fixture in tests/conftest.py (function scope)
- [x] T044 [US1] Implement unique table name generation in delta_table_path fixture (test_{uuid})
- [x] T045 [US1] Implement cleanup logic in delta_table_path fixture (shutil.rmtree)

### Testing & Validation

- [x] T050 [US1] Start Spark service: `docker-compose up -d spark`
- [x] T051 [US1] Verify Spark Master UI accessible at http://localhost:8080
- [ ] T052 [US1] Verify Spark session creation via Python REPL
- [ ] T053 [US1] Run Delta Lake writer tests: `pytest tests/unit/test_cdc_pipelines/test_delta_writer.py -v`
- [ ] T054 [US1] Run Delta CDF tests: `pytest tests/unit/test_cdc_pipelines/test_delta_cdf_config.py -v`
- [ ] T055 [US1] Verify all 7 Delta Lake tests pass (0 failed, 0 skipped)

---

## Phase 3: User Story 2 - AlertManager Infrastructure (P2)

### Docker Service Configuration

- [x] T100 [US2] Add AlertManager service definition to docker-compose.yml (prom/alertmanager:v0.26.0)
- [x] T101 [US2] Configure AlertManager command args in docker-compose.yml (--config.file, --storage.path)
- [x] T102 [US2] Add AlertManager port mappings to docker-compose.yml (9093:9093, 9094:9094)
- [x] T103 [US2] Add AlertManager volume mounts to docker-compose.yml (./configs/alertmanager:/etc/alertmanager:ro)
- [x] T104 [US2] Add AlertManager data volume mount to docker-compose.yml (alertmanager-data:/alertmanager)
- [x] T105 [US2] Add AlertManager health check to docker-compose.yml (wget --spider http://localhost:9093/-/healthy)
- [x] T106 [US2] Add AlertManager resource limits to docker-compose.yml (mem_limit: 256m, cpus: 0.5)

### AlertManager Configuration Files

- [x] T110 [US2] Create configs/alertmanager/alertmanager.yml
- [x] T111 [US2] Configure global settings in alertmanager.yml (resolve_timeout: 5m)
- [x] T112 [US2] Configure default route in alertmanager.yml (receiver: test-receiver, group_by: [alertname, severity])
- [x] T113 [US2] Configure timing settings in alertmanager.yml (group_wait: 10s, group_interval: 10s, repeat_interval: 1h)
- [x] T114 [US2] Add test-receiver webhook config in alertmanager.yml (url: http://localhost:5001/webhook)
- [x] T115 [US2] Add test environment route in alertmanager.yml (match environment: test)

### Prometheus Integration

- [x] T120 [P] [US2] Update configs/prometheus/prometheus.yml to add alertmanager targets
- [x] T121 [P] [US2] Configure Prometheus alerting section (alertmanagers: static_configs: alertmanager:9093)
- [x] T122 [US2] Restart Prometheus service: `docker-compose restart prometheus`
- [x] T123 [US2] Verify Prometheus can reach AlertManager: `curl http://localhost:9090/api/v1/alertmanagers`

### Test Fixtures

- [x] T130 [US2] Create alertmanager_client pytest fixture in tests/conftest.py (function scope)
- [x] T131 [US2] Implement post_alert method in alertmanager_client fixture
- [x] T132 [US2] Implement get_alerts method in alertmanager_client fixture
- [x] T133 [US2] Implement post_silence method in alertmanager_client fixture
- [x] T134 [US2] Implement delete_silence method in alertmanager_client fixture
- [x] T135 [US2] Add cleanup logic to alertmanager_client fixture (delete test alerts/silences)

### Testing & Validation

- [x] T140 [US2] Start AlertManager service: `docker-compose up -d alertmanager`
- [x] T141 [US2] Verify AlertManager UI accessible at http://localhost:9093
- [x] T142 [US2] Verify AlertManager API responds: `curl http://localhost:9093/api/v1/status`
- [ ] T143 [US2] Test manual alert creation via curl
- [x] T144 [US2] Update skip markers in tests/integration/test_alerting.py (remove skip from TestAlertDelivery)
- [ ] T145 [US2] Run AlertManager tests: `pytest tests/integration/test_alerting.py::TestAlertDelivery -v`
- [ ] T146 [US2] Verify all 12 AlertManager tests pass (0 failed, 0 skipped)

---

## Phase 4: User Story 3 - MySQL Connector (P3)

### Python Dependencies

- [x] T200 [US3] Add mysql-connector-python to pyproject.toml (mysql-connector-python = "^8.0.33")
- [x] T201 [US3] Run poetry lock to update dependencies
- [x] T202 [US3] Run poetry install to install mysql-connector-python
- [x] T203 [US3] Verify installation: `python -c "import mysql.connector; print(mysql.connector.__version__)"`

### Test Updates

- [x] T210 [P] [US3] Remove skip markers from tests/integration/test_mysql_cdc.py
- [ ] T211 [P] [US3] Remove skip markers from tests/unit/test_cdc_pipelines/test_mysql_connection.py
- [ ] T212 [US3] Verify MySQL service running: `docker-compose ps mysql`
- [ ] T213 [US3] Verify MySQL binlog enabled: Connect and check `SHOW VARIABLES LIKE 'log_bin'`

### Testing & Validation

- [ ] T220 [US3] Run MySQL connection tests: `pytest tests/unit/test_cdc_pipelines/test_mysql_connection.py -v`
- [ ] T221 [US3] Run MySQL CDC integration tests: `pytest tests/integration/test_mysql_cdc.py -v`
- [ ] T222 [US3] Verify all 9 MySQL tests pass (0 failed, 0 skipped)

---

## Phase 5: Documentation & Cleanup

### Documentation

- [x] T300 [P] Create README-TESTING.md in project root (link to quickstart guide)
- [ ] T301 [P] Update main README.md to reference new test infrastructure
- [x] T302 [P] Add troubleshooting section to README-TESTING.md (based on quickstart.md)
- [x] T303 [P] Document Makefile targets for test infrastructure (make test, make clean-delta-tables)

### Makefile Targets

- [x] T310 Create Makefile target: `make start-test-infra` (docker-compose up -d spark alertmanager)
- [x] T311 Create Makefile target: `make stop-test-infra` (docker-compose stop spark alertmanager)
- [x] T312 Create Makefile target: `make clean-delta-tables` (rm -rf data/delta-tables/test_*)
- [x] T313 Create Makefile target: `make verify-test-infra` (health checks for all services)

### Final Validation

- [ ] T320 Run full test suite: `make test`
- [ ] T321 Verify test results: 148 passed, 0 failed, <20 skipped
- [ ] T322 Verify specific improvements: Delta Lake tests 7/7 passing
- [ ] T323 Verify specific improvements: AlertManager tests 12/12 passing
- [ ] T324 Verify specific improvements: MySQL CDC tests 9/9 passing
- [ ] T325 Document actual test results in specs/001-test-infrastructure/RESULTS.md
- [ ] T326 Run test coverage: `make test-coverage`
- [ ] T327 Verify no regression in existing passing tests

### Cleanup & Optimization

- [ ] T330 [P] Add .gitignore entries for data/delta-tables/test_*
- [ ] T331 [P] Add .dockerignore for configs/spark and configs/alertmanager
- [ ] T332 Optimize Docker resource limits based on actual usage
- [ ] T333 Document minimum system requirements in README-TESTING.md

---

## Dependency Graph

```
Phase 1 (T001-T005) → All other phases
├─ Phase 2 (T010-T055) → Can proceed independently
├─ Phase 3 (T100-T146) → Can proceed independently
└─ Phase 4 (T200-T222) → Can proceed independently

Phase 5 (T300-T333) → Depends on Phases 2, 3, 4 completion
```

**Parallel Execution Opportunities**:
- Phase 1: T001, T002, T005 can run in parallel
- Phase 2: T030, T031 can run in parallel with Docker setup
- Phase 3: T120, T121 can run in parallel with AlertManager setup
- Phase 4: T210, T211 can run in parallel
- Phase 5: T300-T303 can run in parallel

---

## Success Criteria (from spec.md)

### Quantitative Metrics
- [ ] SC1: Test suite shows 0 failed tests (down from 12)
- [ ] SC2: Test suite shows <20 skipped tests (down from 137)
- [ ] SC3: Delta Lake tests: 7/7 passing (currently 0/7)
- [ ] SC4: AlertManager tests: 12/12 passing (currently 0/12)
- [ ] SC5: MySQL CDC tests: 9/9 passing (currently 0/9)
- [ ] SC6: Test suite completes in <5 minutes locally
- [ ] SC7: Docker services start in <60 seconds
- [ ] SC8: All services pass health checks within 30 seconds

---

## Risk Mitigation Checklist

- [ ] Verify 8GB RAM minimum before starting Spark
- [ ] Check port conflicts before docker-compose up (7077, 8080, 4040, 9093, 9094)
- [ ] Test cleanup fixtures work correctly (delta_table_path cleanup)
- [ ] Verify Delta Lake JAR downloads correctly on first run
- [ ] Document fallback if Spark memory pressure occurs (reduce to 512m)
- [ ] Test AlertManager alert delivery with polling + timeout (avoid flakiness)
- [ ] Verify mysql-connector-python has no conflicts with psycopg2-binary

---

## Notes

**Task Naming Convention**:
- T0xx: Setup/Prerequisites
- T0xx-T0xx: User Story 1 (Spark + Delta Lake)
- T1xx: User Story 2 (AlertManager)
- T2xx: User Story 3 (MySQL Connector)
- T3xx: Documentation & Cleanup

**Parallel Tags**:
- [P] = Can run in parallel with other [P] tasks in same phase
- [US1], [US2], [US3] = User story association

**Constitution Compliance**:
- All tests already written (TDD requirement satisfied)
- No new application features (infrastructure only)
- Backward compatible (existing tests unaffected)
- Documented via quickstart.md and contracts/

**Estimated Time**:
- Phase 1: 15 minutes
- Phase 2 (US1): 45 minutes
- Phase 3 (US2): 45 minutes
- Phase 4 (US3): 15 minutes
- Phase 5: 30 minutes
- **Total**: ~2.5 hours

---

**Last Updated**: 2025-10-29
**Status**: Ready for implementation
