# Feature Specification: Test Infrastructure Expansion

**Feature Branch**: `002-infrastructure-expansion`
**Created**: 2025-10-29
**Status**: Draft
**Input**: User description: "There are 111 skipped tests show in 'make test' command's result, which require additional infrastructures. Analyze these infrastructure needs and specify the requirements for expanding current docker compose environment to meet these needs."

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Enable Apache Iceberg Testing Infrastructure (Priority: P1)

Development teams need to test CDC pipelines that write to Apache Iceberg tables, including incremental reads, snapshot tracking, and cross-storage replication. Currently, 48 Iceberg-related tests are skipped due to missing infrastructure.

**Why this priority**: Iceberg tests represent 43% of all skipped tests (48 out of 111) and are foundational for cross-storage functionality. Without Iceberg support, teams cannot validate one of the three major table formats (Delta, Iceberg, Hudi) in their CDC pipeline.

**Independent Test**: Can be fully tested by running the Iceberg test suite (`pytest tests/unit/test_cdc_pipelines/test_iceberg_*.py tests/integration/test_iceberg_cdc.py tests/e2e/test_iceberg_workflow.py`) and verifying all 48 tests execute successfully.

**Acceptance Scenarios**:

1. **Given** the Docker environment is started, **When** a developer runs Iceberg unit tests, **Then** all 38 unit tests execute without infrastructure errors
2. **Given** Iceberg REST catalog is running, **When** integration tests create Iceberg tables, **Then** tables are accessible and queryable through the catalog
3. **Given** MinIO is configured as Iceberg warehouse, **When** tests write CDC data to Iceberg tables, **Then** data is persisted to S3-compatible storage and can be read back
4. **Given** PyIceberg is installed in the test environment, **When** tests use Iceberg Python APIs, **Then** operations complete without import or dependency errors
5. **Given** cross-storage tests run, **When** data flows from Postgres through Kafka to Iceberg, **Then** all 6 cross-storage integration tests pass

---

### User Story 2 - Enable Delta Lake Testing with Spark (Priority: P2)

Development teams need to test CDC pipelines that write to Delta Lake tables with Change Data Feed (CDF) enabled. Currently, 25 Delta Lake E2E tests are skipped because Spark lacks Delta Lake packages.

**Why this priority**: Delta Lake is the primary table format for the CDC demo (more existing integration than Iceberg), and these 25 tests validate critical E2E workflows. Delta requires only package configuration, not new services, making it medium complexity.

**Independent Test**: Can be fully tested by running Delta test suites (`pytest tests/integration/test_deltalake_cdc.py tests/e2e/test_delta_cdf_workflow.py tests/e2e/test_postgres_to_delta.py tests/e2e/test_mysql_to_delta.py`) and verifying all 25 tests execute successfully.

**Acceptance Scenarios**:

1. **Given** Spark is configured with Delta Lake jars, **When** tests create Delta tables, **Then** tables support Delta Lake features (ACID, time travel, CDF)
2. **Given** Delta CDF is enabled on tables, **When** tests query change data, **Then** historical changes are accessible via CDF queries
3. **Given** Postgres CDC writes to Delta Lake, **When** E2E tests run the full pipeline, **Then** data flows from Postgres through Debezium/Kafka to Delta tables
4. **Given** MySQL CDC writes to Delta Lake, **When** E2E tests run, **Then** data flows from MySQL to Delta with schema preservation
5. **Given** tests use Delta table versioning, **When** version tracking operations execute, **Then** all 9 DeltaLake CDC integration tests pass

---

### User Story 3 - Enable Schema Evolution Testing with Schema Registry (Priority: P3)

Development teams need to validate that schema changes (adding columns, changing types, dropping fields) propagate correctly through the CDC pipeline. Currently, 6 schema evolution tests are skipped due to missing Schema Registry.

**Why this priority**: Schema evolution is critical for production CDC systems, but represents only 5% of skipped tests (6 out of 111). This is lower priority than table format support but important for data quality assurance.

**Independent Test**: Can be fully tested by running schema evolution test suite (`pytest tests/integration/test_postgres_schema_evolution.py`) and verifying all 6 tests execute, demonstrating schema changes flow from source database through Kafka Schema Registry to destination tables.

**Acceptance Scenarios**:

1. **Given** Schema Registry is running and connected to Kafka, **When** source schema changes occur, **Then** Schema Registry stores and versions schema definitions
2. **Given** a column is added to source table, **When** CDC captures changes, **Then** downstream systems receive schema update notifications
3. **Given** a field type is changed, **When** evolution tests run, **Then** type changes are validated and backward compatibility is verified
4. **Given** a column is dropped, **When** tests simulate the change, **Then** downstream consumers handle the schema evolution gracefully
5. **Given** multiple concurrent schema changes occur, **When** tests execute, **Then** all 6 schema evolution scenarios pass validation
6. **Given** schema compatibility rules are defined, **When** incompatible changes are attempted, **Then** Schema Registry rejects the change and tests verify error handling

---

### User Story 4 - Enable Debezium Connector Registration for Advanced CDC Testing (Priority: P4)

Development teams need to test CDC lag monitoring and complete E2E pipeline workflows. Currently, 2 Postgres CDC tests are skipped because they require registered Debezium connectors.

**Why this priority**: This is a quick win (only 2 tests, low complexity) but lower business value than table format support. It validates CDC pipeline performance characteristics rather than core functionality.

**Independent Test**: Can be fully tested by running advanced CDC tests (`pytest tests/integration/test_postgres_cdc.py::test_cdc_lag_within_threshold tests/integration/test_postgres_cdc.py::test_end_to_end_postgres_to_delta`) after registering Debezium connectors.

**Acceptance Scenarios**:

1. **Given** Debezium REST API is accessible, **When** test setup registers Postgres connector, **Then** connector appears in Debezium's connector list
2. **Given** Postgres connector is registered, **When** CDC lag test runs, **Then** test can measure and validate lag stays within acceptable thresholds (< 5 seconds)
3. **Given** E2E test runs, **When** data flows from Postgres through Kafka to destination, **Then** complete pipeline is verified end-to-end
4. **Given** connector health checks are implemented, **When** tests monitor connector status, **Then** both advanced CDC tests pass

---

### User Story 5 - Configure Alertmanager Notification Receivers (Priority: P5)

Development teams need to test alert delivery mechanisms (email, Slack, webhooks). Currently, 4 alerting tests are skipped or failing because specific receiver types are not configured.

**Why this priority**: Lowest priority as only 4 tests (4% of skipped tests) and alerting infrastructure is already functional. This is configuration-only work with minimal business impact.

**Independent Test**: Can be fully tested by configuring email/Slack receivers in Alertmanager config and running `pytest tests/integration/test_alerting.py::TestAlertDelivery` to verify delivery tests pass.

**Acceptance Scenarios**:

1. **Given** Alertmanager has email receiver configured, **When** email delivery tests run, **Then** tests verify SMTP configuration and email template rendering
2. **Given** Slack webhook is configured, **When** Slack delivery tests run, **Then** tests verify webhook connectivity and message formatting
3. **Given** test webhook endpoint is available, **When** webhook delivery tests run, **Then** tests validate webhook payload structure
4. **Given** all receiver types are configured, **When** full alerting test suite runs, **Then** 4 additional delivery tests pass

---

### Edge Cases

- What happens when Iceberg REST catalog service fails during test execution? Tests should handle catalog unavailability gracefully with clear error messages.
- How does the system handle when Spark workers exhaust memory with Delta/Iceberg operations? Tests should have appropriate resource limits to prevent test hangs.
- What happens when Schema Registry versions exceed storage capacity? Tests should clean up old schema versions between test runs.
- How do tests handle race conditions when multiple concurrent schema changes occur? Tests should include proper synchronization and ordering validation.
- What happens when MinIO (S3) storage becomes full during Iceberg writes? Tests should fail fast with clear storage error messages.
- How does the system handle network partitions between Spark and Iceberg catalog? Tests should timeout appropriately and not hang indefinitely.
- What happens when Debezium connector registration fails during test setup? Tests should skip or fail early rather than proceed with incomplete setup.

## Requirements *(mandatory)*

### Functional Requirements

#### Iceberg Infrastructure (Priority: P1)

- **FR-001**: Docker Compose environment MUST include Apache Iceberg REST Catalog service
- **FR-002**: Test environment MUST have PyIceberg Python package installed and accessible
- **FR-003**: MinIO object storage MUST be configured as Iceberg table warehouse
- **FR-004**: Iceberg catalog MUST support creating, reading, and querying table metadata
- **FR-005**: Iceberg tables MUST support incremental reads and snapshot operations
- **FR-006**: Test environment MUST allow cross-storage data flows (Postgres → Kafka → Iceberg)

#### Delta Lake Configuration (Priority: P2)

- **FR-007**: Spark service MUST be configured with Delta Lake jars and dependencies
- **FR-008**: Spark MUST support creating Delta tables with Change Data Feed (CDF) enabled
- **FR-009**: Delta tables MUST support ACID transactions and time travel queries
- **FR-010**: Test environment MUST allow E2E workflows (Postgres/MySQL → Kafka → Delta Lake)
- **FR-011**: Delta table metadata MUST persist across Spark session restarts

#### Schema Registry (Priority: P3)

- **FR-012**: Docker Compose MUST include Confluent Schema Registry service
- **FR-013**: Schema Registry MUST connect to existing Kafka cluster
- **FR-014**: Schema Registry MUST support Avro schema registration and versioning
- **FR-015**: Test environment MUST validate schema evolution scenarios (add/drop/change columns)
- **FR-016**: Schema Registry MUST enforce compatibility rules (backward, forward, full)

#### Debezium Connector Management (Priority: P4)

- **FR-017**: Test setup MUST programmatically register Debezium connectors via REST API
- **FR-018**: Registered connectors MUST appear in Debezium connector health checks
- **FR-019**: Test teardown MUST clean up registered connectors
- **FR-020**: Connector registration MUST handle conflicts (connector already exists)

#### Alertmanager Notification Configuration (Priority: P5)

- **FR-021**: Alertmanager MUST support configurable email notification receivers
- **FR-022**: Alertmanager MUST support Slack webhook notification receivers
- **FR-023**: Alert delivery tests MUST verify notification formatting and delivery
- **FR-024**: Test environment MUST support mock/test notification endpoints

### Data Quality Requirements

- **DQ-001**: Iceberg table reads MUST verify data integrity against source Postgres/MySQL tables
- **DQ-002**: Delta CDF queries MUST accurately reflect historical data changes
- **DQ-003**: Schema evolution tests MUST validate data consistency before and after schema changes
- **DQ-004**: Cross-storage pipeline tests MUST verify row counts match at each stage
- **DQ-005**: CDC lag tests MUST measure and log time from source commit to destination write

### Key Entities

- **Iceberg Table**: Represents table format with snapshot isolation, time travel, and schema evolution capabilities; stores metadata in REST catalog and data files in MinIO
- **Delta Table**: Represents ACID-compliant table format with transaction log and CDF; stores data and logs in distributed file system (MinIO)
- **Schema Version**: Represents versioned schema definition in Schema Registry; tracks schema evolution history with compatibility rules
- **Debezium Connector**: Represents CDC connector configuration; captures database changes and publishes to Kafka topics
- **Iceberg Snapshot**: Represents point-in-time table state; enables time travel queries and incremental reads
- **Change Data Feed**: Represents Delta Lake feature tracking row-level changes; enables CDC consumers to query historical modifications
- **Schema Registry Subject**: Represents schema namespace for Kafka topic; enforces compatibility rules for schema evolution

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: Test suite completion rate increases from 63% (186/297) to 100% (297/297) with all infrastructure-dependent tests enabled
- **SC-002**: Developers can execute full Iceberg test suite (48 tests) in under 10 minutes on local Docker environment
- **SC-003**: Delta Lake E2E tests (25 tests) pass consistently with 0% flakiness over 10 consecutive runs
- **SC-004**: Schema evolution tests complete within 2 minutes and validate all 6 schema change scenarios
- **SC-005**: Iceberg and Delta table operations perform within acceptable latency (write: < 5s for 1000 rows, read: < 2s)
- **SC-006**: Docker environment startup time remains under 3 minutes with all new services added
- **SC-007**: Memory footprint of expanded Docker environment stays under 8GB total (current + additions)
- **SC-008**: All CDC pipeline tests achieve data consistency validation (source row count = destination row count)
- **SC-009**: Cross-storage tests demonstrate data flows from both Postgres and MySQL to Iceberg and Delta within same environment
- **SC-010**: Schema Registry handles 100 schema registration requests per minute without errors

## Assumptions

1. **Infrastructure Context**: Existing Docker Compose already includes Postgres, MySQL, Kafka, Debezium, Spark, MinIO, Prometheus, Alertmanager, Grafana, and Loki
2. **Resource Availability**: Development machines have at least 16GB RAM and 4 CPU cores available for Docker
3. **Network Configuration**: All services will communicate via Docker internal network (no external routing required)
4. **Storage**: MinIO already exists and has sufficient storage capacity to serve as warehouse for both Iceberg and Delta tables
5. **Spark Version**: Existing Spark 3.5.0 is compatible with Delta Lake and Iceberg connectors
6. **Schema Format**: Schema Registry will use Avro as primary schema format (standard for Debezium)
7. **PyIceberg Installation**: PyIceberg will be installed via pip in Python test environment (not in Spark containers)
8. **Catalog Backend**: Iceberg REST Catalog will use in-memory storage for test purposes (no persistent catalog needed)
9. **Compatibility**: Delta Lake version will be 2.4.0 (compatible with Spark 3.5.0 and Scala 2.12)
10. **Service Startup Order**: New services (Schema Registry, Iceberg Catalog) will wait for dependencies (Kafka, MinIO) to be healthy before starting
11. **Test Isolation**: Tests will clean up created tables, schemas, and connectors after execution to prevent interference
12. **Package Management**: Spark Delta packages will be added via `spark.jars.packages` configuration (no custom Docker image rebuild)

## Dependencies

- **Docker Compose v2+**: Required for service orchestration and health checks
- **Existing Kafka Cluster**: Schema Registry and Debezium depend on Kafka being operational
- **Existing MinIO**: Iceberg and Delta table storage requires S3-compatible object storage
- **Existing Spark**: Delta Lake features require Spark to be pre-configured and running
- **PyIceberg Package**: Python Iceberg API must be installed in test environment (via requirements.txt or Poetry)
- **Delta Core Package**: `io.delta:delta-core_2.12:2.4.0` must be available to Spark JVM
- **Iceberg REST Catalog Image**: Docker image for Apache Iceberg REST Catalog service (e.g., `tabulario/iceberg-rest:latest`)
- **Schema Registry Image**: Confluent Schema Registry Docker image (e.g., `confluentinc/cp-schema-registry:7.5.0`)

## Out of Scope

- Persistent storage for Iceberg catalog metadata (test environment uses ephemeral in-memory catalog)
- Production-grade Schema Registry clustering or HA configuration
- Custom Spark image building (Delta packages added via runtime configuration only)
- Hudi table format support (not required by any skipped tests)
- Multi-region or geo-distributed table storage
- Table format migration tooling (Delta ↔ Iceberg conversions)
- Production monitoring and alerting for new infrastructure components
- Performance tuning of Spark workers for large-scale data processing
- Security configurations (authentication, encryption, access control) for test services
- Automated schema compatibility testing beyond the 6 existing test scenarios
