# postgres-cdc-demo Development Guidelines

Auto-generated from all feature plans. Last updated: 2025-10-27

## Active Technologies
- Python 3.11+ (existing), Docker Compose v2+ (001-test-infrastructure)
- Python 3.11+ (existing test environment) (002-infrastructure-expansion)
- MinIO (existing, configured as S3-compatible warehouse for Iceberg/Delta) (002-infrastructure-expansion)

- Python 3.11+ (for orchestration, data generation, validation scripts) (001-cdc-demo)

## Project Structure

```text
src/
tests/
```

## Commands

cd src [ONLY COMMANDS FOR ACTIVE TECHNOLOGIES][ONLY COMMANDS FOR ACTIVE TECHNOLOGIES] pytest [ONLY COMMANDS FOR ACTIVE TECHNOLOGIES][ONLY COMMANDS FOR ACTIVE TECHNOLOGIES] ruff check .

## Code Style

Python 3.11+ (for orchestration, data generation, validation scripts): Follow standard conventions

## Recent Changes
- 002-infrastructure-expansion: Added Python 3.11+ (existing test environment)
- 001-test-infrastructure: Added Python 3.11+ (existing), Docker Compose v2+

- 001-cdc-demo: Added Python 3.11+ (for orchestration, data generation, validation scripts)

<!-- MANUAL ADDITIONS START -->
<!-- MANUAL ADDITIONS END -->
