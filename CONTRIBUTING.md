# Contributing to CDC Demo Project

Thank you for your interest in contributing! This document provides guidelines and instructions for contributing to the project.

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [Getting Started](#getting-started)
- [Development Setup](#development-setup)
- [Development Workflow](#development-workflow)
- [Coding Standards](#coding-standards)
- [Testing Guidelines](#testing-guidelines)
- [Documentation](#documentation)
- [Submitting Changes](#submitting-changes)
- [Review Process](#review-process)

## Code of Conduct

We are committed to providing a welcoming and inclusive environment. All contributors are expected to:

- Be respectful and constructive in communications
- Accept constructive criticism gracefully
- Focus on what's best for the community
- Show empathy towards other community members

## Getting Started

### Prerequisites

Before you begin, ensure you have:

- **Docker Desktop 20.10+** with Docker Compose
- **Python 3.11+** installed
- **Poetry** for Python dependency management
- **Git** for version control
- **8GB RAM** and **4 CPU cores** available for local development

### Fork and Clone

1. Fork the repository on GitHub
2. Clone your fork locally:

```bash
git clone https://github.com/YOUR-USERNAME/claude-cdc-demo.git
cd claude-cdc-demo
```

3. Add the upstream repository:

```bash
git remote add upstream https://github.com/yourorg/claude-cdc-demo.git
```

## Development Setup

### 1. Install Dependencies

```bash
# Install Python dependencies
poetry install

# Activate virtual environment
poetry shell

# Install pre-commit hooks
pre-commit install
```

### 2. Start Development Environment

```bash
# Start all services
docker-compose up -d

# Verify services are running
docker-compose ps

# Check logs
docker-compose logs -f
```

### 3. Verify Setup

```bash
# Run tests
make test

# Run linting
make lint

# Run type checking
make typecheck
```

### Project Structure

```
claude-cdc-demo/
├── src/
│   ├── cdc_pipelines/          # CDC pipeline implementations
│   │   ├── postgres/           # PostgreSQL CDC
│   │   ├── mysql/              # MySQL CDC
│   │   ├── deltalake/          # DeltaLake CDC
│   │   ├── iceberg/            # Iceberg CDC
│   │   └── cross_storage/      # Cross-storage pipelines
│   ├── validation/             # Data quality validation
│   └── monitoring/             # Observability utilities
├── tests/
│   ├── unit/                   # Unit tests
│   ├── integration/            # Integration tests
│   ├── data_quality/           # Data quality tests
│   └── e2e/                    # End-to-end tests
├── scripts/                    # Utility scripts
├── docker/                     # Docker configurations
│   ├── postgres/
│   ├── mysql/
│   └── observability/
├── docs/                       # Documentation
└── specs/                      # Feature specifications
```

## Development Workflow

### 1. Create a Feature Branch

```bash
# Sync with upstream
git fetch upstream
git checkout main
git merge upstream/main

# Create feature branch
git checkout -b feature/your-feature-name
```

### Branch Naming Conventions

- `feature/description` - New features
- `fix/description` - Bug fixes
- `docs/description` - Documentation changes
- `refactor/description` - Code refactoring
- `test/description` - Test additions/improvements

### 2. Make Changes

Follow the coding standards and guidelines below while making changes.

### 3. Test Your Changes

```bash
# Run relevant tests
pytest tests/unit/test_your_changes.py -v

# Run all tests
make test

# Check coverage
make test-coverage
```

### 4. Commit Your Changes

We follow [Conventional Commits](https://www.conventionalcommits.org/) specification:

```bash
# Commit format
<type>(<scope>): <subject>

# Examples
git commit -m "feat(iceberg): add snapshot expiration policy"
git commit -m "fix(postgres): resolve replication slot leak"
git commit -m "docs(readme): update quickstart instructions"
git commit -m "test(deltalake): add CDF integration tests"
```

**Commit Types:**
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation only
- `style`: Code style changes (formatting, no logic change)
- `refactor`: Code refactoring
- `test`: Adding or updating tests
- `chore`: Maintenance tasks

### 5. Push to Your Fork

```bash
git push origin feature/your-feature-name
```

### 6. Open a Pull Request

1. Navigate to your fork on GitHub
2. Click "Pull Request"
3. Select your feature branch
4. Fill out the PR template (see below)
5. Submit the PR

## Coding Standards

### Python Style Guide

We follow [PEP 8](https://peps.python.org/pep-0008/) with some modifications:

- **Line Length**: 100 characters (not 79)
- **Quotes**: Double quotes for strings
- **Imports**: Sorted with `isort`
- **Formatting**: Automated with `black`
- **Linting**: `ruff` for code quality

### Code Quality Tools

```bash
# Format code
make format
# or
black src tests
isort src tests

# Lint code
make lint
# or
ruff check src tests

# Type checking
make typecheck
# or
mypy src
```

### Pre-commit Hooks

Pre-commit hooks automatically run on every commit:

```yaml
# .pre-commit-config.yaml
repos:
  - repo: https://github.com/psf/black
    hooks:
      - id: black
  - repo: https://github.com/pycqa/isort
    hooks:
      - id: isort
  - repo: https://github.com/charliermarsh/ruff-pre-commit
    hooks:
      - id: ruff
  - repo: https://github.com/pre-commit/mirrors-mypy
    hooks:
      - id: mypy
```

### Naming Conventions

**Python:**
- Classes: `PascalCase` (e.g., `IcebergTableManager`)
- Functions/Methods: `snake_case` (e.g., `get_current_snapshot`)
- Constants: `UPPER_SNAKE_CASE` (e.g., `DEFAULT_BATCH_SIZE`)
- Private members: `_leading_underscore` (e.g., `_internal_method`)

**Files:**
- Python modules: `snake_case.py` (e.g., `table_manager.py`)
- Test files: `test_*.py` (e.g., `test_iceberg_manager.py`)
- Scripts: `kebab-case.sh` (e.g., `generate-data.sh`)

### Docstrings

Use Google-style docstrings:

```python
def read_incremental(
    self,
    start_snapshot_id: int,
    end_snapshot_id: int,
    filter_condition: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Read incremental changes between two snapshots.

    Args:
        start_snapshot_id: Starting snapshot ID (exclusive)
        end_snapshot_id: Ending snapshot ID (inclusive)
        filter_condition: Optional filter expression (e.g., "region = 'US'")

    Returns:
        List of changed records as dictionaries

    Raises:
        ValueError: If snapshot IDs are invalid
        IcebergException: If read operation fails

    Example:
        >>> reader = IncrementalReader(table_manager)
        >>> changes = reader.read_incremental(100, 105, "status = 'active'")
        >>> len(changes)
        42
    """
```

## Testing Guidelines

### Test-Driven Development (TDD)

We follow TDD practices:

1. **Write tests first** - Define expected behavior before implementation
2. **Make tests pass** - Implement minimal code to pass tests
3. **Refactor** - Improve code while keeping tests green

### Test Structure

```python
import pytest

class TestIcebergTableManager:
    """Tests for Iceberg table management."""

    @pytest.fixture
    def table_manager(self):
        """Create table manager instance for testing."""
        config = IcebergTableConfig(...)
        return IcebergTableManager(config)

    def test_create_table_with_schema(self, table_manager):
        """Test creating a table with explicit schema."""
        # Arrange
        schema = Schema(...)

        # Act
        table = table_manager.create_table("test_table", schema)

        # Assert
        assert table is not None
        assert table.name == "test_table"
```

### Test Categories

**Unit Tests** (`tests/unit/`):
- Test individual functions/methods in isolation
- Use mocks for external dependencies
- Fast execution (< 1 second per test)
- No external services required

```python
@pytest.mark.unit
def test_parse_debezium_event():
    """Test parsing Debezium event payload."""
    event = {"payload": {"op": "c", "after": {"id": 1}}}
    result = parse_debezium_event(event)
    assert result["operation"] == "CREATE"
```

**Integration Tests** (`tests/integration/`):
- Test interaction between components
- Use real services (Docker containers)
- Moderate execution time (< 30 seconds per test)
- Mark with `@pytest.mark.integration`

```python
@pytest.mark.integration
@pytest.mark.skipif(reason="Requires Postgres + Kafka", condition=False)
def test_postgres_to_kafka_flow():
    """Test full flow from Postgres to Kafka."""
    # Insert into Postgres
    # Wait for CDC
    # Verify in Kafka
```

**Data Quality Tests** (`tests/data_quality/`):
- Validate data integrity
- Schema evolution testing
- Row count/checksum validation

**E2E Tests** (`tests/e2e/`):
- Test complete workflows
- Slow execution (minutes)
- Run in CI only
- Mark with `@pytest.mark.e2e`

### Test Coverage Requirements

- **Minimum coverage**: 98%
- **Target coverage**: 100% for critical paths

```bash
# Generate coverage report
pytest --cov=src --cov-report=html --cov-report=term

# View HTML report
open htmlcov/index.html
```

### Running Tests

```bash
# Run all tests
make test

# Run specific test category
make test-unit
make test-integration
make test-data-quality
make test-e2e

# Run specific test file
pytest tests/unit/test_iceberg_manager.py -v

# Run specific test
pytest tests/unit/test_iceberg_manager.py::TestIcebergTableManager::test_create_table -v

# Run with coverage
pytest --cov=src tests/

# Run failed tests only
pytest --lf
```

## Documentation

### Code Documentation

- **Docstrings**: All public functions, classes, and methods must have docstrings
- **Type hints**: Use type hints for all function signatures
- **Comments**: Explain "why", not "what" (code should be self-explanatory)

### User Documentation

When adding features, update relevant documentation:

- **README.md**: Update if adding major features
- **docs/**: Add detailed documentation
- **Quickstart Guide**: Update if changing setup process
- **API Documentation**: Document new APIs

### Documentation Style

- Use Markdown for all documentation
- Include code examples
- Add diagrams where helpful (Mermaid, ASCII art)
- Keep it concise and actionable

## Submitting Changes

### Pull Request Template

When opening a PR, use this template:

```markdown
## Description
Brief description of changes

## Type of Change
- [ ] Bug fix (non-breaking change which fixes an issue)
- [ ] New feature (non-breaking change which adds functionality)
- [ ] Breaking change (fix or feature that would cause existing functionality to not work as expected)
- [ ] Documentation update

## Related Issues
Fixes #123
Related to #456

## Changes Made
- Added XYZ feature
- Fixed ABC bug
- Updated documentation

## Testing
- [ ] All existing tests pass
- [ ] Added new tests for changes
- [ ] Coverage remains ≥ 98%
- [ ] Integration tests pass
- [ ] E2E tests pass (if applicable)

## Checklist
- [ ] Code follows style guidelines
- [ ] Self-review completed
- [ ] Comments added for complex logic
- [ ] Documentation updated
- [ ] No new warnings introduced
- [ ] Dependent changes merged

## Screenshots (if applicable)
Add screenshots for UI changes
```

### PR Best Practices

1. **Keep PRs small** - Easier to review, faster to merge
2. **One concern per PR** - Don't mix features and bug fixes
3. **Write descriptive titles** - "Add Iceberg snapshot expiration" not "Update code"
4. **Provide context** - Explain why the change is needed
5. **Link related issues** - Use "Fixes #123" syntax
6. **Respond to feedback** - Address review comments promptly

## Review Process

### What Reviewers Look For

1. **Correctness**: Does the code work as intended?
2. **Tests**: Are there adequate tests? Do they pass?
3. **Code Quality**: Is the code readable and maintainable?
4. **Performance**: Are there any performance concerns?
5. **Documentation**: Is the code well-documented?
6. **Breaking Changes**: Does this break existing functionality?

### Review Timeline

- **Initial review**: Within 2 business days
- **Follow-up reviews**: Within 1 business day
- **Merge**: After approval from 2 maintainers

### Addressing Review Feedback

```bash
# Make requested changes
git add .
git commit -m "fix: address review feedback"
git push origin feature/your-feature-name

# PR will automatically update
```

### After Your PR is Merged

```bash
# Sync your fork
git checkout main
git pull upstream main
git push origin main

# Delete feature branch (optional)
git branch -d feature/your-feature-name
git push origin --delete feature/your-feature-name
```

## Specific Contribution Areas

### Adding New CDC Pipelines

To add support for a new database/storage system:

1. Create directory: `src/cdc_pipelines/your_system/`
2. Implement core classes (follow existing patterns)
3. Add comprehensive tests (unit + integration + e2e)
4. Add documentation to `docs/pipelines/your_system.md`
5. Update main README.md feature matrix

### Improving Observability

To add new metrics/dashboards/alerts:

1. Add Prometheus metrics in relevant pipeline code
2. Create Grafana dashboard JSON in `docker/observability/grafana/dashboards/`
3. Add alert rules in `docker/observability/grafana/alerts/`
4. Document in `docs/monitoring.md`

### Enhancing Data Quality

To add new validation checks:

1. Implement in `src/validation/`
2. Add tests in `tests/data_quality/`
3. Integrate into validation framework
4. Document in `docs/api/validation.md`

## Getting Help

- **Questions**: Open a [Discussion](https://github.com/yourorg/claude-cdc-demo/discussions)
- **Bugs**: Open an [Issue](https://github.com/yourorg/claude-cdc-demo/issues)
- **Chat**: Join our [Discord/Slack] (if available)

## Recognition

Contributors will be:
- Listed in [Contributors](https://github.com/yourorg/claude-cdc-demo/graphs/contributors)
- Mentioned in release notes
- Thanked in project README

## License

By contributing, you agree that your contributions will be licensed under the MIT License.

---

Thank you for contributing to the CDC Demo Project! 🎉
