<!--
Sync Impact Report:
- Version: 0.0.0 → 1.0.0 (Initial constitution ratification)
- New principles: All principles are new (first version)
- Added sections: Data Quality Assurance, Local Development Standards, Governance
- Removed sections: None
- Templates requiring updates:
  ✅ plan-template.md - Constitution Check section will reference these principles
  ✅ spec-template.md - Functional requirements align with data quality principles
  ✅ tasks-template.md - Test-first workflow enforced in task structure
- Follow-up TODOs: None
-->

# Postgres CDC Demo Constitution

## Core Principles

### I. Test-First Development (NON-NEGOTIABLE)

**Rules**:
- ALL code MUST be written using Test-Driven Development (TDD)
- Tests MUST be written BEFORE implementation
- Tests MUST fail initially, then pass after implementation (Red-Green-Refactor)
- No code merges permitted without corresponding tests
- Test coverage MUST include: unit tests, integration tests, and end-to-end CDC pipeline tests

**Rationale**: CDC systems handle critical data change events. Bugs can cause data loss, corruption, or inconsistencies across systems. TDD ensures reliability and prevents regressions in data capture and transformation logic.

### II. Data Quality Assurance

**Rules**:
- ALL CDC pipelines MUST validate data integrity at every stage: source capture → transformation → destination
- Schema evolution MUST be explicitly tested (add column, drop column, type changes)
- Data consistency checks MUST be automated: row counts, checksums, field-level validation
- CDC lag monitoring MUST be implemented for all pipelines
- Failed change events MUST be logged with full context for debugging and replay

**Rationale**: CDC systems are only valuable if they maintain data integrity. Silent data corruption or missed events can cascade into business-critical failures. Explicit validation catches issues before they propagate.

### III. Local-First Demonstration

**Rules**:
- ENTIRE stack MUST run locally using Docker Compose or equivalent
- NO external dependencies required (cloud services, remote databases, paid APIs)
- Setup time MUST be under 10 minutes on a standard developer machine
- All data sources (Postgres, MySQL, DeltaLake, MinIO, Iceberg) MUST be containerized
- Documentation MUST include one-command setup: `docker-compose up`
- Sample data MUST be seeded automatically for immediate demonstration

**Rationale**: Educational and demonstration value requires frictionless local execution. Complex setup blocks learning and evaluation. Self-contained environments ensure reproducibility across different machines and timeframes.

### IV. Multi-Storage CDC Coverage

**Rules**:
- Project MUST demonstrate CDC for: Postgres, MySQL, DeltaLake, MinIO (object storage events), Iceberg
- Each storage technology MUST showcase its all common CDC mechanisms:
  - **Postgres**: Logical replication (pg_logical, wal2json), Debezium, Trigger-based, Log-based, Timestamp-based
  - **MySQL**: Binlog replication or Debezium
  - **DeltaLake**: Change Data Feed (CDF)
  - **Iceberg**: Incremental read with snapshot metadata
- CDC approaches MUST be clearly documented with pros/cons for each technology
- Cross-storage CDC patterns MUST be demonstrated (e.g., Postgres → Iceberg via CDC)

**Rationale**: Different storage systems have fundamentally different CDC mechanisms. Real-world users need to understand trade-offs and implementation patterns across the ecosystem, not just one vendor's approach.

### V. Clean, Idiomatic Code

**Rules**:
- Code MUST follow language-specific best practices and style guides
- No "clever" code; prioritize readability over brevity
- Configuration MUST be externalized (environment variables, config files)
- Secrets MUST NEVER be committed (use `.env.example` templates)
- All scripts and services MUST have clear, single-responsibility functions
- Dead code and commented-out blocks MUST be removed before merge

**Rationale**: This is a demonstration and educational project. Code clarity directly impacts learning effectiveness. Future maintainers and learners should read the code as documentation.

### VI. Comprehensive Observability

**Rules**:
- ALL CDC pipelines MUST emit structured logs (JSON format)
- Metrics MUST be exposed: events processed, lag time, error rates, throughput
- Health checks MUST be implemented for all services
- Docker Compose MUST include observability stack: logs aggregation (e.g., Loki), metrics (e.g., Prometheus), dashboards (e.g., Grafana)
- Error scenarios MUST be observable: connection failures, schema mismatches, data validation failures

**Rationale**: CDC systems are asynchronous and distributed. Without observability, debugging is impossible. Learners need to see how professional CDC systems are monitored in production.

### VII. Documentation as Code

**Rules**:
- Architecture diagrams MUST be version-controlled (use Mermaid, PlantUML, or similar)
- Each CDC pipeline MUST have a README explaining: data flow, technologies used, how to validate it works
- Breaking changes to setup MUST update documentation in the same commit
- Quickstart guide MUST be tested on a clean machine before each release

**Rationale**: Demonstration projects live or die by documentation quality. Outdated docs destroy trust and waste user time. Treating docs as code ensures they evolve with the system.

## Data Quality Assurance Standards

### Schema Validation

- All CDC consumers MUST validate incoming schema matches expected schema
- Schema registry or explicit schema versioning MUST be used for all pipelines
- Backward and forward compatibility rules MUST be documented and tested

### Data Integrity Checks

- Primary key uniqueness MUST be validated in destination
- Referential integrity MUST be checked for related table CDC
- NULL constraints MUST be preserved across CDC pipelines
- Data type conversions MUST be explicitly handled and tested

### Consistency Guarantees

- Eventually consistent systems MUST document their convergence time
- Strong consistency claims MUST be backed by transactional guarantees
- Idempotency MUST be ensured for replay scenarios
- Error handling, Retry Strategy, Interruption handling MUST be developed and incorporated.

## Local Development Standards

### Environment Setup

- `docker-compose.yml` MUST be the primary setup mechanism
- All services MUST support graceful shutdown and restart
- Data persistence MUST be configurable (ephemeral for demos, persistent for development)
- Port conflicts MUST be avoided (use non-standard ports if needed, document them)

### Performance Standards

- CDC lag MUST be under 5 seconds for demo workloads
- System MUST run on 8GB RAM, 4 CPU cores
- Startup time (cold start) MUST be under 5 minutes
- Sample data generation MUST complete in under 2 minutes

### Developer Experience

- Error messages MUST be actionable (not just stack traces)
- Common failure modes MUST be documented (port conflicts, insufficient resources, etc.)
- Automated health checks MUST verify setup success
- Sample queries and validation scripts MUST be provided

## Governance

### Constitution Authority

This constitution is the supreme governing document for the Postgres CDC Demo project. All design decisions, code reviews, and architectural choices MUST align with these principles.

### Amendment Process

1. Proposed changes MUST be documented with rationale in a GitHub issue
2. Amendment MUST be approved by project maintainer(s)
3. Version number MUST be updated per semantic versioning:
   - **MAJOR**: Removal or fundamental change to existing principles
   - **MINOR**: Addition of new principles or significant expansion
   - **PATCH**: Clarifications, wording improvements, non-semantic changes
4. All dependent templates (plan, spec, tasks) MUST be updated for consistency

### Compliance Verification

- ALL pull requests MUST reference relevant constitutional principles
- Code reviews MUST verify adherence to test-first development
- CI/CD pipeline MUST enforce: test coverage minimums, linting, Docker build success
- Complexity that violates principles MUST be justified in `plan.md` Complexity Tracking section

### Principle Violations

Violations are only permitted when:
1. Documented with clear rationale in the implementation plan
2. Simpler constitutional approach is proven insufficient
3. Violation is temporary with a remediation plan

**Version**: 1.0.0 | **Ratified**: 2025-10-27 | **Last Amended**: 2025-10-27
