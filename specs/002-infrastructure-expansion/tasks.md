# Tasks: Test Infrastructure Expansion

**Input**: Design documents from `/specs/002-infrastructure-expansion/`
**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/, quickstart.md

**Tests**: NOT REQUIRED - Tests already exist (currently skipped). This feature enables existing tests by adding infrastructure.

**Organization**: Tasks are grouped by user story (US1-US5) corresponding to priorities P1-P5 in spec.md. Each user story can be implemented and tested independently.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (e.g., US1, US2, US3)
- Include exact file paths in descriptions

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Project initialization and documentation structure

- [X] T001 Create infrastructure configuration directory at docker/iceberg-rest/
- [X] T002 Create infrastructure configuration directory at docker/spark/
- [X] T003 [P] Create infrastructure configuration directory at docker/schema-registry/
- [X] T004 [P] Create test fixtures directory at tests/fixtures/
- [X] T005 [P] Create infrastructure documentation directory at docs/infrastructure/
- [X] T006 Verify current docker-compose.yml structure and service definitions

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Core dependencies and shared configurations that MUST be complete before ANY user story

**⚠️ CRITICAL**: No user story work can begin until this phase is complete

- [X] T007 Update pyproject.toml to add PyIceberg dependency (^0.10.0)
- [X] T008 Update pyproject.toml to add confluent-kafka dependency for Schema Registry client
- [X] T009 Run `poetry lock` to update poetry.lock with new dependencies
- [X] T010 Run `poetry install` to install PyIceberg and verify import works
- [X] T011 [P] Create MinIO bucket initialization script at docker/minio/init-buckets.sh for Iceberg warehouse
- [X] T012 [P] Create shared test utilities for service health checks in tests/fixtures/health_checks.py
- [X] T013 Update docker-compose.yml to add MinIO bucket init service (depends_on: minio)

**Checkpoint**: Foundation ready - user story implementation can now begin in parallel

---

## Phase 3: User Story 1 - Enable Apache Iceberg Testing Infrastructure (Priority: P1) 🎯 MVP

**Goal**: Enable 48 Iceberg tests by adding Iceberg REST Catalog service and PyIceberg integration

**Independent Test**: Run `pytest tests/unit/test_cdc_pipelines/test_iceberg_*.py tests/integration/test_iceberg_cdc.py tests/e2e/test_iceberg_workflow.py` and verify all 48 tests pass

### Infrastructure for User Story 1

- [X] T014 [P] [US1] Create Iceberg REST Catalog configuration file at docker/iceberg-rest/catalog-config.yaml
- [X] T015 [P] [US1] Add iceberg-rest service to docker-compose.yml (image: tabulario/iceberg-rest:latest, port 8181)
- [X] T016 [US1] Configure iceberg-rest service environment variables in docker-compose.yml (AWS credentials, warehouse path, S3 endpoint)
- [X] T017 [US1] Add health check for iceberg-rest service in docker-compose.yml (curl http://localhost:8181/v1/config)
- [X] T018 [US1] Configure service dependencies: iceberg-rest depends_on minio (condition: service_healthy)

### Test Fixtures for User Story 1

- [X] T019 [P] [US1] Create IcebergCatalogConfig dataclass in tests/fixtures/iceberg_catalog.py
- [X] T020 [P] [US1] Implement iceberg_catalog pytest fixture (session-scoped) in tests/fixtures/iceberg_catalog.py
- [X] T021 [US1] Add health check wait logic for Iceberg catalog in iceberg_catalog fixture (retry with exponential backoff)
- [X] T022 [US1] Update tests/conftest.py to import and register iceberg_catalog fixture

### Validation for User Story 1

- [X] T023 [US1] Create infrastructure health test at tests/integration/test_infrastructure.py::test_iceberg_catalog_reachable
- [X] T024 [US1] Run Iceberg unit tests: `pytest tests/unit/test_cdc_pipelines/test_iceberg_*.py -v` (29 tests enabled)
- [X] T025 [US1] Run Iceberg integration tests: `pytest tests/integration/test_iceberg_cdc.py -v` (8 tests enabled)
- [X] T026 [US1] Run Iceberg E2E tests: `pytest tests/e2e/test_iceberg_workflow.py -v` (5 tests enabled)
- [X] T027 [US1] Validate cross-storage tests pass: `pytest tests/integration/test_cross_storage.py -v` (6 tests enabled)

### Documentation for User Story 1

- [X] T028 [P] [US1] Create Iceberg setup guide at docs/infrastructure/iceberg-setup.md
- [X] T029 [P] [US1] Update README.md to document Iceberg REST Catalog service and port 8181
- [X] T030 [US1] Add Iceberg validation commands to quickstart documentation

**Checkpoint**: ✅ Iceberg infrastructure COMPLETE - 48 tests enabled (48 out of 111 total skipped tests)

---

## Phase 4: User Story 2 - Enable Delta Lake Testing with Spark (Priority: P2)

**Goal**: Enable 25 Delta Lake tests by configuring Spark with Delta Lake packages and CDF support

**Independent Test**: Run `pytest tests/integration/test_deltalake_cdc.py tests/e2e/test_delta_cdf_workflow.py tests/e2e/test_postgres_to_delta.py tests/e2e/test_mysql_to_delta.py` and verify all 25 tests pass

### Infrastructure for User Story 2

- [X] T031 [P] [US2] Create Spark configuration file at docker/spark/spark-defaults.conf
- [X] T032 [P] [US2] Add Delta Lake package to spark-defaults.conf: spark.jars.packages=io.delta:delta-spark_2.12:3.3.2
- [X] T033 [US2] Add Delta extensions to spark-defaults.conf: spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension
- [X] T034 [US2] Add Delta catalog to spark-defaults.conf: spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog
- [X] T035 [US2] Configure S3A settings in spark-defaults.conf for MinIO access (endpoint, path style, credentials)
- [X] T036 [US2] Update docker-compose.yml spark service to mount docker/spark/spark-defaults.conf as volume (already configured)
- [X] T037 [US2] Add CDF configuration to spark-defaults.conf: spark.databricks.delta.properties.defaults.enableChangeDataFeed=true

### Test Fixtures for User Story 2

- [X] T038 [P] [US2] Create DeltaSparkManager class in tests/fixtures/delta_spark.py
- [X] T039 [P] [US2] Implement delta_spark pytest fixture (session-scoped) in tests/conftest.py
- [X] T040 [US2] Add Spark session initialization with Delta configuration in delta_spark fixture
- [X] T041 [US2] Add health check for Spark with Delta packages loaded via is_delta_available() method
- [X] T042 [US2] Update tests/conftest.py to import and register delta_spark fixture

### Validation for User Story 2

- [X] T043 [US2] Create Delta infrastructure health test at tests/integration/test_infrastructure.py::test_spark_delta_packages_loaded
- [X] T044 [US2] Validate Delta Lake integration tests: `pytest tests/integration/test_deltalake_cdc.py -v` (9 tests available)
- [X] T045 [US2] Validate Delta CDF workflow tests: `pytest tests/e2e/test_delta_cdf_workflow.py -v` (8 tests available)
- [X] T046 [US2] Validate Postgres to Delta tests: `pytest tests/e2e/test_postgres_to_delta.py -v` (8 tests available)
- [X] T047 [US2] Validate MySQL to Delta tests: `pytest tests/e2e/test_mysql_to_delta.py -v` (8 tests available)

### Documentation for User Story 2

- [X] T048 [P] [US2] Create Delta Lake setup guide at docs/infrastructure/delta-lake-setup.md
- [X] T049 [P] [US2] Update README.md to document Delta Lake configuration and CDF support
- [X] T050 [US2] Add Delta Lake validation commands to quickstart documentation

**Checkpoint**: ✅ Delta Lake infrastructure COMPLETE - 25 tests ready (73 out of 111 total skipped tests)

---

## Phase 5: User Story 3 - Enable Schema Evolution Testing with Schema Registry (Priority: P3)

**Goal**: Enable 6 schema evolution tests by adding Confluent Schema Registry service

**Independent Test**: Run `pytest tests/integration/test_postgres_schema_evolution.py` and verify all 6 tests pass

### Infrastructure for User Story 3

- [ ] T051 [P] [US3] Create Schema Registry configuration file at docker/schema-registry/schema-registry.properties
- [ ] T052 [P] [US3] Add schema-registry service to docker-compose.yml (image: confluentinc/cp-schema-registry:7.5.0, port 8081)
- [ ] T053 [US3] Configure schema-registry environment variables in docker-compose.yml (host name, Kafka bootstrap servers, listeners)
- [ ] T054 [US3] Add health check for schema-registry service in docker-compose.yml (curl http://localhost:8081/subjects)
- [ ] T055 [US3] Configure service dependencies: schema-registry depends_on kafka (condition: service_healthy)

### Test Fixtures for User Story 3

- [ ] T056 [P] [US3] Create SchemaRegistryConfig dataclass in tests/fixtures/schema_registry.py
- [ ] T057 [P] [US3] Implement schema_registry pytest fixture (session-scoped) in tests/fixtures/schema_registry.py
- [ ] T058 [US3] Add Schema Registry client initialization with URL configuration in schema_registry fixture
- [ ] T059 [US3] Add health check wait logic for Schema Registry in fixture (retry with exponential backoff)
- [ ] T060 [US3] Update tests/conftest.py to import and register schema_registry fixture

### Validation for User Story 3

- [ ] T061 [US3] Create Schema Registry health test at tests/integration/test_infrastructure.py::test_schema_registry_reachable
- [ ] T062 [US3] Run schema evolution tests: `pytest tests/integration/test_postgres_schema_evolution.py -v` (expect 6 tests pass)
- [ ] T063 [US3] Verify schema registration, compatibility checks, and version management work
- [ ] T064 [US3] Test schema evolution scenarios: add column, drop column, change type

### Documentation for User Story 3

- [ ] T065 [P] [US3] Create Schema Registry setup guide at docs/infrastructure/schema-registry-setup.md
- [ ] T066 [P] [US3] Update README.md to document Schema Registry service and port 8081
- [ ] T067 [US3] Add Schema Registry validation commands to quickstart documentation

**Checkpoint**: Schema Registry infrastructure complete - 6 tests enabled (79 out of 111 total)

---

## Phase 6: User Story 4 - Enable Debezium Connector Registration for Advanced CDC Testing (Priority: P4)

**Goal**: Enable 2 advanced CDC tests by automating Debezium connector registration in test fixtures

**Independent Test**: Run `pytest tests/integration/test_postgres_cdc.py::test_cdc_lag_within_threshold tests/integration/test_postgres_cdc.py::test_end_to_end_postgres_to_delta` and verify both tests pass

### Test Fixtures for User Story 4

- [ ] T068 [P] [US4] Create DebeziumConnectorConfig dataclass in tests/fixtures/debezium_connector.py
- [ ] T069 [P] [US4] Implement DebeziumClient class in tests/fixtures/debezium_connector.py with REST API methods
- [ ] T070 [US4] Add create_connector method to DebeziumClient (POST /connectors with Postgres config)
- [ ] T071 [US4] Add delete_connector method to DebeziumClient (DELETE /connectors/{name})
- [ ] T072 [US4] Add get_connector_status method to DebeziumClient (GET /connectors/{name}/status)
- [ ] T073 [US4] Implement connector_exists check logic in DebeziumClient (handle 409 errors)
- [ ] T074 [US4] Implement debezium_connector pytest fixture (function-scoped) in tests/fixtures/debezium_connector.py
- [ ] T075 [US4] Add connector registration logic in debezium_connector fixture setup
- [ ] T076 [US4] Add connector cleanup logic in debezium_connector fixture teardown (yield pattern)
- [ ] T077 [US4] Update tests/conftest.py to import and register debezium_connector fixture

### Validation for User Story 4

- [ ] T078 [US4] Create Debezium connector health test at tests/integration/test_infrastructure.py::test_debezium_connector_registration
- [ ] T079 [US4] Run CDC lag test: `pytest tests/integration/test_postgres_cdc.py::test_cdc_lag_within_threshold -v`
- [ ] T080 [US4] Run E2E CDC test: `pytest tests/integration/test_postgres_cdc.py::test_end_to_end_postgres_to_delta -v`
- [ ] T081 [US4] Verify connector registration, status checks, and cleanup work correctly

### Documentation for User Story 4

- [ ] T082 [P] [US4] Document Debezium connector registration process in docs/infrastructure/debezium-connectors.md
- [ ] T083 [P] [US4] Update README.md with Debezium connector management information
- [ ] T084 [US4] Add connector validation commands to quickstart documentation

**Checkpoint**: Debezium connector automation complete - 2 tests enabled (81 out of 111 total)

---

## Phase 7: User Story 5 - Configure Alertmanager Notification Receivers (Priority: P5)

**Goal**: Enable 4 alerting tests by configuring email, Slack, and webhook receivers in Alertmanager

**Independent Test**: Run `pytest tests/integration/test_alerting.py::TestAlertDelivery` and verify 4 additional tests pass (email, Slack, webhook, test receiver tests)

### Infrastructure for User Story 5

- [ ] T085 [P] [US5] Create Alertmanager configuration file at docker/alertmanager/alertmanager.yml
- [ ] T086 [P] [US5] Add email receiver configuration to alertmanager.yml (SMTP settings, to address, templates)
- [ ] T087 [P] [US5] Add Slack receiver configuration to alertmanager.yml (webhook URL, channel, templates)
- [ ] T088 [P] [US5] Add test webhook receiver configuration to alertmanager.yml (test endpoint URL)
- [ ] T089 [US5] Update docker-compose.yml alertmanager service to mount updated alertmanager.yml
- [ ] T090 [US5] Add mock SMTP server service to docker-compose.yml for email testing (mailhog or similar)
- [ ] T091 [US5] Add test webhook endpoint service to docker-compose.yml for webhook testing

### Validation for User Story 5

- [ ] T092 [US5] Run email delivery test: `pytest tests/integration/test_alerting.py::TestAlertDelivery::test_email_delivery_configuration -v`
- [ ] T093 [US5] Run Slack delivery test: `pytest tests/integration/test_alerting.py::TestAlertDelivery::test_slack_delivery_configuration -v`
- [ ] T094 [US5] Run webhook delivery test: `pytest tests/integration/test_alerting.py::TestAlertDelivery::test_webhook_delivery -v`
- [ ] T095 [US5] Verify alert routing, grouping, and repeat interval configurations work

### Documentation for User Story 5

- [ ] T096 [P] [US5] Document Alertmanager receiver configuration in docs/infrastructure/alertmanager-config.md
- [ ] T097 [P] [US5] Update README.md with Alertmanager notification receiver information
- [ ] T098 [US5] Add alerting validation commands to quickstart documentation

**Checkpoint**: Alertmanager receivers complete - 4 tests enabled (85 out of 111 total, accounting for some already passing)

---

## Phase 8: Polish & Cross-Cutting Concerns

**Purpose**: Final validation, documentation, and quality gates

### Final Infrastructure Validation

- [ ] T099 Verify all Docker services start successfully with `docker-compose up -d`
- [ ] T100 Measure Docker environment startup time (target: < 3 minutes)
- [ ] T101 Measure total memory footprint with `docker stats` (target: < 8GB)
- [ ] T102 Run complete test suite: `make test` and verify 297/297 tests pass (186 + 111)
- [ ] T103 Measure Iceberg test execution time (target: < 10 minutes for 48 tests)
- [ ] T104 Measure Delta Lake test execution time (target: < 8 minutes for 25 tests)
- [ ] T105 Measure Schema Registry test execution time (target: < 2 minutes for 6 tests)
- [ ] T106 Run test suite 10 times to measure flakiness rate (target: 0% for Delta tests)

### Documentation Completion

- [ ] T107 [P] Update main README.md with complete service list and port mappings
- [ ] T108 [P] Create architecture diagram showing all services and their relationships in docs/architecture/infrastructure.md
- [ ] T109 [P] Verify quickstart.md contains validation steps for all new services
- [ ] T110 [P] Add troubleshooting section to README.md for common infrastructure issues
- [ ] T111 Update CLAUDE.md with final technology list (Iceberg, Delta Lake, PyIceberg, Schema Registry)

### Quality Gates

- [ ] T112 Verify no secrets committed (check .env files, docker-compose.yml for hardcoded credentials)
- [ ] T113 Verify all services have health checks defined in docker-compose.yml
- [ ] T114 Verify all test fixtures properly clean up resources (connectors, tables, schemas)
- [ ] T115 Code review: Check docker-compose.yml follows best practices (depends_on, health checks, resource limits)
- [ ] T116 Validate constitution compliance: Local-first (✓), Test-enabled (✓), Observability (✓), Documentation (✓)

### Final Acceptance

- [ ] T117 Run full test suite on clean environment after `docker-compose down -v && docker-compose up`
- [ ] T118 Verify test completion metric: 186 → 297 tests (100% of runnable tests)
- [ ] T119 Verify data consistency in cross-storage tests (Postgres → Iceberg, MySQL → Delta)
- [ ] T120 Final checkpoint: All 5 user stories independently functional and tested

---

## Dependencies & Execution Order

### Phase Dependencies

```
Setup (Phase 1)
  ↓
Foundational (Phase 2) ← BLOCKS all user stories
  ↓
┌──────────┬──────────┬──────────┬──────────┬──────────┐
│   US1    │   US2    │   US3    │   US4    │   US5    │
│ (Iceberg)│ (Delta)  │ (Schema  │(Debezium)│ (Alert-  │
│  P1 🎯   │   P2     │ Registry)│   P4     │ manager) │
│          │          │   P3     │          │   P5     │
└──────────┴──────────┴──────────┴──────────┴──────────┘
  ↓          ↓          ↓          ↓          ↓
  └──────────┴──────────┴──────────┴──────────┘
                    ↓
           Polish (Phase 8)
```

### User Story Dependencies

**All user stories are INDEPENDENT** - can be implemented and tested in any order after Foundational phase:

- **US1 (Iceberg)**: No dependencies on other stories
- **US2 (Delta Lake)**: No dependencies on other stories
- **US3 (Schema Registry)**: No dependencies on other stories
- **US4 (Debezium Connectors)**: No dependencies on other stories
- **US5 (Alertmanager)**: No dependencies on other stories

**Recommended Implementation Order** (by priority):
1. US1 (P1) - Iceberg (48 tests, highest impact)
2. US2 (P2) - Delta Lake (25 tests, high impact)
3. US3 (P3) - Schema Registry (6 tests, medium impact)
4. US4 (P4) - Debezium Connectors (2 tests, quick win)
5. US5 (P5) - Alertmanager (4 tests, lowest priority)

### Parallel Execution Opportunities

**Within Setup Phase (T001-T006)**: All tasks can run in parallel

**Within Foundational Phase (T007-T013)**:
- Parallel: T007-T008 (pyproject.toml updates), T011-T012 (config files)
- Sequential: T009 → T010 (poetry lock → poetry install)
- Sequential: T011 → T013 (bucket script → docker-compose update)

**Across User Stories**: All 5 user stories can be implemented in parallel (different teams/developers)

**Within Each User Story**:
- Infrastructure tasks (marked [P]) can run in parallel
- Test fixture tasks (marked [P]) can run in parallel
- Documentation tasks (marked [P]) can run in parallel
- Validation tasks must run sequentially after implementation

**Example Parallel Execution for US1 (Iceberg)**:
```
Parallel:
- T014 (catalog config) || T015 (docker-compose) || T019 (fixture dataclass)

Sequential:
- T016-T018 (configure iceberg-rest in docker-compose)
- T020-T022 (implement fixture with health checks)
- T023-T027 (validation tests in sequence)

Parallel:
- T028 (Iceberg guide) || T029 (README update) || T030 (quickstart)
```

---

## Implementation Strategy

### MVP Scope (Minimum Viable Product)

**MVP = User Story 1 (Iceberg) ONLY**

Delivering just US1 enables:
- 48 out of 111 tests (43% of skipped tests)
- Complete Iceberg CDC pipeline validation
- Cross-storage replication testing
- Demonstrates feasibility of infrastructure expansion approach

**MVP Delivery**: Tasks T001-T030 (30 tasks, estimated 2-3 days)

### Incremental Delivery Plan

1. **Sprint 1 (MVP)**: US1 (Iceberg) - 48 tests enabled
2. **Sprint 2**: US2 (Delta Lake) - Additional 25 tests enabled (73 total)
3. **Sprint 3**: US3 (Schema Registry) + US4 (Debezium) - Additional 8 tests (81 total)
4. **Sprint 4**: US5 (Alertmanager) + Polish - Final 4-30 tests (85-111 total)

### Rollback Strategy

Each user story is independent and can be disabled by:
1. Commenting out the service in docker-compose.yml
2. Removing the test fixture from conftest.py
3. Tests will skip automatically if infrastructure unavailable

No rollback affects other user stories.

---

## Success Metrics

### Primary Metrics (from spec.md SC-001 to SC-010)

- [ ] **SC-001**: Test completion rate 186/297 → 297/297 (100%)
- [ ] **SC-002**: Iceberg test suite (48 tests) completes in < 10 minutes
- [ ] **SC-003**: Delta Lake tests (25 tests) have 0% flakiness over 10 runs
- [ ] **SC-004**: Schema evolution tests (6 tests) complete in < 2 minutes
- [ ] **SC-005**: Table operations within latency targets (write < 5s/1000 rows, read < 2s)
- [ ] **SC-006**: Docker environment startup < 3 minutes
- [ ] **SC-007**: Memory footprint < 8GB total
- [ ] **SC-008**: Data consistency validated (source count = destination count)
- [ ] **SC-009**: Cross-storage flows validated (Postgres/MySQL → Iceberg/Delta)
- [ ] **SC-010**: Schema Registry handles 100 registrations/minute

### Measurement Commands

```bash
# SC-001: Test completion
make test | grep "passed"

# SC-002: Iceberg test timing
time pytest tests/unit/test_cdc_pipelines/test_iceberg_*.py tests/integration/test_iceberg_cdc.py tests/e2e/test_iceberg_workflow.py

# SC-003: Delta test flakiness
for i in {1..10}; do pytest tests/integration/test_deltalake_cdc.py || echo "FAIL"; done

# SC-006: Startup time
time docker-compose up -d

# SC-007: Memory footprint
docker stats --no-stream --format "table {{.Name}}\t{{.MemUsage}}"
```

---

## Task Summary

**Total Tasks**: 120
**Setup**: 6 tasks (T001-T006)
**Foundational**: 7 tasks (T007-T013)
**User Story 1 (Iceberg, P1)**: 17 tasks (T014-T030)
**User Story 2 (Delta, P2)**: 20 tasks (T031-T050)
**User Story 3 (Schema Registry, P3)**: 17 tasks (T051-T067)
**User Story 4 (Debezium, P4)**: 17 tasks (T068-T084)
**User Story 5 (Alertmanager, P5)**: 14 tasks (T085-T098)
**Polish**: 22 tasks (T099-T120)

**Parallel Tasks Identified**: 45 tasks marked [P] can run in parallel
**Independent User Stories**: All 5 user stories can be implemented independently
**MVP Scope**: 30 tasks (T001-T030) delivers 48 tests enabled

**Format Validation**: ✅ All 120 tasks follow strict checklist format:
- Checkbox: `- [ ]`
- Task ID: T001-T120
- [P] marker: 45 tasks appropriately marked
- [Story] label: 85 tasks labeled US1-US5
- File paths: All implementation tasks include exact file paths
