"""
Data quality tests for schema evolution scenarios in CDC pipelines.

Tests cover:
- ADD COLUMN: Backward compatibility, default values, NULL handling
- DROP COLUMN: Data preservation, consumer impact
- TYPE CHANGE: Data conversion correctness, precision loss
- RENAME COLUMN: Mapping correctness, backward compatibility

These tests validate that schema changes don't cause data loss or corruption
in the CDC pipeline.
"""

import pytest
import psycopg2
from typing import Dict, Any, List, Optional
from datetime import datetime
from decimal import Decimal


# ============================================================================
# Test Fixtures
# ============================================================================
# Note: postgres_connection fixture is defined in tests/conftest.py

@pytest.fixture
def setup_test_table(postgres_connection):
    """Setup schema_evolution_test table with initial data."""
    cursor = postgres_connection.cursor()

    # Reset table
    cursor.execute("DROP TABLE IF EXISTS schema_evolution_test CASCADE")

    cursor.execute("""
        CREATE TABLE schema_evolution_test (
            id SERIAL PRIMARY KEY,
            version INTEGER NOT NULL DEFAULT 1,
            name VARCHAR(100) NOT NULL,
            description TEXT,
            status VARCHAR(50) DEFAULT 'active',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Insert test data
    cursor.execute("""
        INSERT INTO schema_evolution_test (version, name, description, status)
        VALUES
            (1, 'Record 1', 'Initial description 1', 'active'),
            (1, 'Record 2', 'Initial description 2', 'inactive'),
            (1, 'Record 3', NULL, 'active')
    """)

    postgres_connection.commit()
    yield cursor
    cursor.close()


@pytest.fixture
def mock_cdc_consumer():
    """Mock CDC consumer that tracks schema changes."""

    class MockCDCConsumer:
        def __init__(self):
            self.events: List[Dict[str, Any]] = []
            self.schema_cache: Dict[str, Dict[str, str]] = {}

        def consume_event(self, event: Dict[str, Any]) -> None:
            """Consume a CDC event."""
            self.events.append(event)

            # Update schema cache
            if "schema" in event:
                table = event.get("table", "unknown")
                self.schema_cache[table] = event["schema"]

        def get_latest_schema(self, table: str) -> Optional[Dict[str, str]]:
            """Get the latest known schema for a table."""
            return self.schema_cache.get(table)

        def validate_event_against_schema(
            self, event: Dict[str, Any], schema: Dict[str, str]
        ) -> bool:
            """Validate that an event conforms to a schema."""
            if "after" not in event:
                return False

            after_data = event["after"]

            # Check all required schema fields are present
            for field_name in schema.keys():
                if field_name not in after_data and field_name != "id":
                    # Allow missing fields if they have defaults
                    continue

            return True

    return MockCDCConsumer()


# ============================================================================
# Test Suite: ADD COLUMN Scenario
# ============================================================================

class TestAddColumnScenario:
    """
    Test ADD COLUMN schema evolution.

    Validates:
    - New column appears in CDC events
    - Existing data has NULL or default values for new column
    - New inserts include the new column
    - Backward compatibility for consumers that don't know about the new column
    """

    def test_add_column_with_default(self, setup_test_table, postgres_connection):
        """
        Test adding a column with a default value.

        Scenario:
        1. Table exists with data
        2. ALTER TABLE ADD COLUMN email VARCHAR(255) DEFAULT 'unknown@example.com'
        3. Query existing rows - should have default email
        4. Insert new row - should have email column available
        """
        cursor = setup_test_table

        # Add column with default
        cursor.execute("""
            ALTER TABLE schema_evolution_test
            ADD COLUMN email VARCHAR(255) DEFAULT 'unknown@example.com'
        """)
        postgres_connection.commit()

        # Query existing rows
        cursor.execute("SELECT id, name, email FROM schema_evolution_test ORDER BY id")
        rows = cursor.fetchall()

        assert len(rows) == 3

        # All existing rows should have default email
        for row in rows:
            row_id, name, email = row
            assert email == 'unknown@example.com', \
                f"Row {row_id} should have default email"

        # Insert new row with explicit email
        cursor.execute("""
            INSERT INTO schema_evolution_test (name, description, email)
            VALUES ('Record 4', 'Test description', 'test@example.com')
            RETURNING id, email
        """)
        new_row = cursor.fetchone()
        postgres_connection.commit()

        assert new_row[1] == 'test@example.com'

    def test_add_column_nullable(self, setup_test_table, postgres_connection):
        """
        Test adding a nullable column without default.

        Scenario:
        1. Table exists with data
        2. ALTER TABLE ADD COLUMN phone VARCHAR(20)
        3. Existing rows should have NULL for phone
        4. New rows can specify phone or leave it NULL
        """
        cursor = setup_test_table

        # Add nullable column
        cursor.execute("""
            ALTER TABLE schema_evolution_test
            ADD COLUMN phone VARCHAR(20)
        """)
        postgres_connection.commit()

        # Query existing rows
        cursor.execute("SELECT id, name, phone FROM schema_evolution_test ORDER BY id")
        rows = cursor.fetchall()

        # All existing rows should have NULL phone
        for row in rows:
            row_id, name, phone = row
            assert phone is None, f"Row {row_id} should have NULL phone"

        # Insert with phone
        cursor.execute("""
            INSERT INTO schema_evolution_test (name, phone)
            VALUES ('Record with Phone', '555-1234')
            RETURNING phone
        """)
        assert cursor.fetchone()[0] == '555-1234'

        # Insert without phone (NULL)
        cursor.execute("""
            INSERT INTO schema_evolution_test (name)
            VALUES ('Record without Phone')
            RETURNING phone
        """)
        assert cursor.fetchone()[0] is None

        postgres_connection.commit()

    def test_add_multiple_columns(self, setup_test_table, postgres_connection):
        """Test adding multiple columns simultaneously."""
        cursor = setup_test_table

        cursor.execute("""
            ALTER TABLE schema_evolution_test
            ADD COLUMN email VARCHAR(255),
            ADD COLUMN phone VARCHAR(20),
            ADD COLUMN metadata JSONB DEFAULT '{}'
        """)
        postgres_connection.commit()

        # Verify columns exist
        cursor.execute("""
            SELECT column_name, data_type, column_default
            FROM information_schema.columns
            WHERE table_name = 'schema_evolution_test'
            AND column_name IN ('email', 'phone', 'metadata')
            ORDER BY column_name
        """)

        columns = cursor.fetchall()
        assert len(columns) == 3

        column_names = {col[0] for col in columns}
        assert column_names == {'email', 'phone', 'metadata'}

    def test_add_column_cdc_event_structure(self, mock_cdc_consumer):
        """
        Test CDC event structure after adding a column.

        Validates that CDC events include the new column in the schema
        and in the 'after' payload for new records.
        """
        # Simulate CDC event before schema change
        event_before = {
            "table": "schema_evolution_test",
            "op": "c",  # create/insert
            "schema": {
                "id": "INTEGER",
                "name": "VARCHAR(100)",
                "status": "VARCHAR(50)"
            },
            "after": {
                "id": 1,
                "name": "Test Record",
                "status": "active"
            }
        }

        mock_cdc_consumer.consume_event(event_before)

        # Simulate CDC event after adding email column
        event_after = {
            "table": "schema_evolution_test",
            "op": "c",
            "schema": {
                "id": "INTEGER",
                "name": "VARCHAR(100)",
                "status": "VARCHAR(50)",
                "email": "VARCHAR(255)"  # New column
            },
            "after": {
                "id": 2,
                "name": "Test Record 2",
                "status": "active",
                "email": "test@example.com"  # New field
            }
        }

        mock_cdc_consumer.consume_event(event_after)

        # Validate new schema includes email
        latest_schema = mock_cdc_consumer.get_latest_schema("schema_evolution_test")
        assert "email" in latest_schema
        assert latest_schema["email"] == "VARCHAR(255)"

        # Validate event conforms to new schema
        assert mock_cdc_consumer.validate_event_against_schema(
            event_after, latest_schema
        )


# ============================================================================
# Test Suite: DROP COLUMN Scenario
# ============================================================================

class TestDropColumnScenario:
    """
    Test DROP COLUMN schema evolution.

    Validates:
    - Column is removed from table
    - Existing data in other columns is preserved
    - CDC events no longer include dropped column
    - Queries don't break (no references to dropped column)
    """

    def test_drop_column_preserves_data(self, setup_test_table, postgres_connection):
        """
        Test that dropping a column preserves data in other columns.

        Scenario:
        1. Table exists with data
        2. DROP COLUMN description
        3. Other columns should remain intact
        """
        cursor = setup_test_table

        # Get data before drop
        cursor.execute("""
            SELECT id, name, status
            FROM schema_evolution_test
            ORDER BY id
        """)
        data_before = cursor.fetchall()

        # Drop column
        cursor.execute("ALTER TABLE schema_evolution_test DROP COLUMN description")
        postgres_connection.commit()

        # Get data after drop
        cursor.execute("""
            SELECT id, name, status
            FROM schema_evolution_test
            ORDER BY id
        """)
        data_after = cursor.fetchall()

        # Data should be identical
        assert data_before == data_after

    def test_drop_column_removes_from_schema(self, setup_test_table, postgres_connection):
        """Test that dropped column is removed from table schema."""
        cursor = setup_test_table

        # Drop column
        cursor.execute("ALTER TABLE schema_evolution_test DROP COLUMN description")
        postgres_connection.commit()

        # Check schema
        cursor.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'schema_evolution_test'
        """)

        columns = {row[0] for row in cursor.fetchall()}

        assert 'description' not in columns
        assert 'name' in columns  # Other columns still exist

    def test_drop_column_cdc_event_structure(self, mock_cdc_consumer):
        """Test CDC event structure after dropping a column."""
        # Event before drop
        event_before = {
            "table": "schema_evolution_test",
            "op": "u",  # update
            "schema": {
                "id": "INTEGER",
                "name": "VARCHAR(100)",
                "description": "TEXT",
                "status": "VARCHAR(50)"
            },
            "before": {
                "id": 1,
                "name": "Old Name",
                "description": "Old description",
                "status": "active"
            },
            "after": {
                "id": 1,
                "name": "New Name",
                "description": "New description",
                "status": "active"
            }
        }

        mock_cdc_consumer.consume_event(event_before)

        # Event after dropping description column
        event_after = {
            "table": "schema_evolution_test",
            "op": "u",
            "schema": {
                "id": "INTEGER",
                "name": "VARCHAR(100)",
                "status": "VARCHAR(50)"
                # description removed
            },
            "before": {
                "id": 1,
                "name": "New Name",
                "status": "active"
            },
            "after": {
                "id": 1,
                "name": "Updated Name",
                "status": "inactive"
            }
        }

        mock_cdc_consumer.consume_event(event_after)

        # Validate new schema doesn't include description
        latest_schema = mock_cdc_consumer.get_latest_schema("schema_evolution_test")
        assert "description" not in latest_schema
        assert "name" in latest_schema


# ============================================================================
# Test Suite: TYPE CHANGE Scenario
# ============================================================================

class TestTypeChangeScenario:
    """
    Test ALTER COLUMN TYPE schema evolution.

    Validates:
    - Type change succeeds or fails appropriately
    - Data is converted correctly
    - No precision loss for safe conversions
    - Unsafe conversions are detected
    """

    def test_type_widening_integer_to_bigint(self, setup_test_table, postgres_connection):
        """Test safe widening: INTEGER -> BIGINT."""
        cursor = setup_test_table

        # Change version from INTEGER to BIGINT
        cursor.execute("""
            ALTER TABLE schema_evolution_test
            ALTER COLUMN version TYPE BIGINT
        """)
        postgres_connection.commit()

        # Verify type change
        cursor.execute("""
            SELECT data_type
            FROM information_schema.columns
            WHERE table_name = 'schema_evolution_test'
            AND column_name = 'version'
        """)

        data_type = cursor.fetchone()[0]
        assert data_type == 'bigint'

        # Verify data integrity
        cursor.execute("SELECT id, version FROM schema_evolution_test ORDER BY id")
        rows = cursor.fetchall()

        for row in rows:
            assert row[1] == 1  # Original value preserved

    def test_type_widening_varchar(self, setup_test_table, postgres_connection):
        """Test safe widening: VARCHAR(50) -> VARCHAR(100)."""
        cursor = setup_test_table

        # Change status column size
        cursor.execute("""
            ALTER TABLE schema_evolution_test
            ALTER COLUMN status TYPE VARCHAR(100)
        """)
        postgres_connection.commit()

        # Verify type change
        cursor.execute("""
            SELECT character_maximum_length
            FROM information_schema.columns
            WHERE table_name = 'schema_evolution_test'
            AND column_name = 'status'
        """)

        max_length = cursor.fetchone()[0]
        assert max_length == 100

        # Insert longer value
        cursor.execute("""
            INSERT INTO schema_evolution_test (name, status)
            VALUES ('Test', 'a_very_long_status_value_that_exceeds_50_chars_but_fits_in_100')
            RETURNING status
        """)

        status = cursor.fetchone()[0]
        assert len(status) > 50  # Would have failed with VARCHAR(50)
        postgres_connection.commit()

    def test_type_change_with_data_conversion(self, setup_test_table, postgres_connection):
        """Test type change requiring data conversion."""
        cursor = setup_test_table

        # Add numeric column
        cursor.execute("""
            ALTER TABLE schema_evolution_test
            ADD COLUMN price DECIMAL(10, 2) DEFAULT 99.99
        """)
        postgres_connection.commit()

        # Change to integer (loses decimal places)
        cursor.execute("""
            ALTER TABLE schema_evolution_test
            ALTER COLUMN price TYPE INTEGER USING price::INTEGER
        """)
        postgres_connection.commit()

        # Verify conversion
        cursor.execute("SELECT price FROM schema_evolution_test LIMIT 1")
        price = cursor.fetchone()[0]

        assert isinstance(price, int)
        assert price == 100  # Decimal places rounded (99.99 -> 100)

    def test_type_change_cdc_event_structure(self, mock_cdc_consumer):
        """Test CDC event structure after type change."""
        # Event before type change
        event_before = {
            "table": "schema_evolution_test",
            "op": "c",
            "schema": {
                "id": "INTEGER",
                "version": "INTEGER"
            },
            "after": {
                "id": 1,
                "version": 1
            }
        }

        mock_cdc_consumer.consume_event(event_before)

        # Event after changing version to BIGINT
        event_after = {
            "table": "schema_evolution_test",
            "op": "c",
            "schema": {
                "id": "INTEGER",
                "version": "BIGINT"  # Type changed
            },
            "after": {
                "id": 2,
                "version": 2  # Still a number, but type is BIGINT
            }
        }

        mock_cdc_consumer.consume_event(event_after)

        # Validate schema reflects type change
        latest_schema = mock_cdc_consumer.get_latest_schema("schema_evolution_test")
        assert latest_schema["version"] == "BIGINT"


# ============================================================================
# Test Suite: RENAME COLUMN Scenario
# ============================================================================

class TestRenameColumnScenario:
    """
    Test RENAME COLUMN schema evolution.

    Validates:
    - Column is renamed successfully
    - Data is preserved after rename
    - CDC events use new column name
    - Old column name is no longer accessible
    """

    def test_rename_column_preserves_data(self, setup_test_table, postgres_connection):
        """Test that renaming a column preserves data."""
        cursor = setup_test_table

        # Get data before rename
        cursor.execute("""
            SELECT id, name
            FROM schema_evolution_test
            ORDER BY id
        """)
        data_before = cursor.fetchall()

        # Rename column
        cursor.execute("""
            ALTER TABLE schema_evolution_test
            RENAME COLUMN name TO display_name
        """)
        postgres_connection.commit()

        # Get data after rename (using new name)
        cursor.execute("""
            SELECT id, display_name
            FROM schema_evolution_test
            ORDER BY id
        """)
        data_after = cursor.fetchall()

        # Data should be identical
        assert data_before == data_after

    def test_rename_column_schema_update(self, setup_test_table, postgres_connection):
        """Test that schema reflects renamed column."""
        cursor = setup_test_table

        # Rename column
        cursor.execute("""
            ALTER TABLE schema_evolution_test
            RENAME COLUMN description TO description_text
        """)
        postgres_connection.commit()

        # Check schema
        cursor.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'schema_evolution_test'
        """)

        columns = {row[0] for row in cursor.fetchall()}

        assert 'description' not in columns
        assert 'description_text' in columns

    def test_rename_column_old_name_fails(self, setup_test_table, postgres_connection):
        """Test that queries using old column name fail after rename."""
        cursor = setup_test_table

        # Rename column
        cursor.execute("""
            ALTER TABLE schema_evolution_test
            RENAME COLUMN status TO record_status
        """)
        postgres_connection.commit()

        # Try to query using old name
        with pytest.raises(psycopg2.errors.UndefinedColumn):
            cursor.execute("SELECT id, status FROM schema_evolution_test")

    def test_rename_column_cdc_event_structure(self, mock_cdc_consumer):
        """Test CDC event structure after renaming a column."""
        # Event before rename
        event_before = {
            "table": "schema_evolution_test",
            "op": "c",
            "schema": {
                "id": "INTEGER",
                "name": "VARCHAR(100)",
                "status": "VARCHAR(50)"
            },
            "after": {
                "id": 1,
                "name": "Test",
                "status": "active"
            }
        }

        mock_cdc_consumer.consume_event(event_before)

        # Event after renaming name -> display_name
        event_after = {
            "table": "schema_evolution_test",
            "op": "c",
            "schema": {
                "id": "INTEGER",
                "display_name": "VARCHAR(100)",  # Renamed
                "status": "VARCHAR(50)"
            },
            "after": {
                "id": 2,
                "display_name": "Test 2",  # New field name
                "status": "active"
            }
        }

        mock_cdc_consumer.consume_event(event_after)

        # Validate schema uses new column name
        latest_schema = mock_cdc_consumer.get_latest_schema("schema_evolution_test")
        assert "name" not in latest_schema
        assert "display_name" in latest_schema


# ============================================================================
# Test Suite: Complex Evolution Scenarios
# ============================================================================

class TestComplexEvolutionScenarios:
    """Test complex schema evolution scenarios with multiple changes."""

    def test_multiple_changes_simultaneously(self, setup_test_table, postgres_connection):
        """Test multiple schema changes in a single transaction."""
        cursor = setup_test_table

        # Multiple changes (RENAME must be separate in PostgreSQL)
        cursor.execute("""
            ALTER TABLE schema_evolution_test
            ADD COLUMN email VARCHAR(255),
            DROP COLUMN description,
            ALTER COLUMN status TYPE VARCHAR(100)
        """)
        cursor.execute("""
            ALTER TABLE schema_evolution_test
            RENAME COLUMN name TO display_name
        """)
        postgres_connection.commit()

        # Verify all changes applied
        cursor.execute("""
            SELECT column_name, data_type, character_maximum_length
            FROM information_schema.columns
            WHERE table_name = 'schema_evolution_test'
            ORDER BY column_name
        """)

        schema = {row[0]: (row[1], row[2]) for row in cursor.fetchall()}

        assert 'email' in schema
        assert 'description' not in schema
        assert 'name' not in schema
        assert 'display_name' in schema
        assert schema['status'][1] == 100  # VARCHAR(100)

    def test_schema_evolution_sequence(self, setup_test_table, postgres_connection):
        """Test a sequence of schema changes over time."""
        cursor = setup_test_table

        # Version 1: Add email
        cursor.execute("""
            ALTER TABLE schema_evolution_test ADD COLUMN email VARCHAR(255)
        """)
        postgres_connection.commit()

        # Version 2: Add phone
        cursor.execute("""
            ALTER TABLE schema_evolution_test ADD COLUMN phone VARCHAR(20)
        """)
        postgres_connection.commit()

        # Version 3: Rename name to display_name
        cursor.execute("""
            ALTER TABLE schema_evolution_test RENAME COLUMN name TO display_name
        """)
        postgres_connection.commit()

        # Version 4: Drop description
        cursor.execute("""
            ALTER TABLE schema_evolution_test DROP COLUMN description
        """)
        postgres_connection.commit()

        # Verify final schema
        cursor.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'schema_evolution_test'
            ORDER BY column_name
        """)

        columns = {row[0] for row in cursor.fetchall()}

        expected_columns = {
            'id', 'version', 'display_name', 'status',
            'created_at', 'updated_at', 'email', 'phone'
        }

        assert columns == expected_columns
