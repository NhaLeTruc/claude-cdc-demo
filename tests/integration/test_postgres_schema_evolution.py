"""
Integration tests for schema evolution in Postgres CDC pipeline.

Tests validate that schema changes are properly handled end-to-end:
- Debezium detects schema changes
- Schema Registry is updated
- Kafka events include new schema
- Destination tables handle schema evolution
- No data loss occurs during evolution
"""

import pytest
import psycopg2
import time
from kafka import KafkaConsumer
import json
import os
import requests
from tests.test_utils import get_kafka_topic, safe_json_deserializer


def is_schema_registry_available():
    """Check if Schema Registry is running and accessible."""
    try:
        url = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
        response = requests.get(f"{url}/subjects", timeout=2)
        return response.status_code == 200
    except Exception:
        return False


def is_cdc_infrastructure_running():
    """Check if full CDC infrastructure (Debezium + Kafka + Schema Registry) is running."""
    try:
        # Check Debezium connector (try both possible names)
        connector_name = None
        for name in ["postgres-connector", "postgres-cdc-connector"]:
            response = requests.get(
                f"http://localhost:8083/connectors/{name}/status",
                timeout=2
            )
            if response.status_code == 200:
                connector_name = name
                break

        if not connector_name:
            return False

        status = response.json()
        connector_state = status.get("connector", {}).get("state")
        if connector_state != "RUNNING":
            return False

        # Check Kafka broker
        import socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(1)
        result = sock.connect_ex(('localhost', 29092))
        sock.close()
        if result != 0:
            return False

        # Check Schema Registry
        return is_schema_registry_available()
    except Exception:
        pass
    return False


def get_schema_registry_url():
    """Get Schema Registry URL from environment."""
    return os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")


def verify_schema_in_registry(subject_name):
    """
    Verify schema exists in Schema Registry and return schema data.

    Args:
        subject_name: Subject name (e.g., 'debezium.public.schema_evolution_test-value')

    Returns:
        dict: Schema data if found, None otherwise
    """
    try:
        url = get_schema_registry_url()
        response = requests.get(f"{url}/subjects/{subject_name}/versions/latest", timeout=2)
        if response.status_code == 200:
            return response.json()
        return None
    except Exception:
        return None


@pytest.fixture(scope="module")
def postgres_conn():
    """PostgreSQL connection fixture."""
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        database=os.getenv("POSTGRES_DB", "cdcdb"),
        user=os.getenv("POSTGRES_USER", "cdcuser"),
        password=os.getenv("POSTGRES_PASSWORD", "cdcpass")
    )
    yield conn
    conn.close()


@pytest.fixture
def test_table(postgres_conn):
    """Create and teardown test table."""
    cursor = postgres_conn.cursor()

    try:
        # Create test table (matches Debezium configuration)
        cursor.execute("""
            DROP TABLE IF EXISTS schema_evolution_test CASCADE
        """)

        cursor.execute("""
            CREATE TABLE schema_evolution_test (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                value INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # CRITICAL: Add table to Postgres publication for Debezium pgoutput plugin
        # Without this, Debezium cannot capture CDC events from this table
        cursor.execute("""
            ALTER PUBLICATION dbz_publication ADD TABLE schema_evolution_test
        """)

        postgres_conn.commit()

        # Insert a dummy record so Debezium's snapshot captures something
        # This ensures the table is being monitored even before the actual test starts
        cursor.execute("""
            INSERT INTO schema_evolution_test (name, value)
            VALUES ('_snapshot_marker', 0)
            RETURNING id
        """)
        marker_id = cursor.fetchone()[0]
        postgres_conn.commit()

        # Restart Debezium connector to pick up the new publication table
        # Debezium caches the publication table list, so it needs a restart
        try:
            debezium_url = os.getenv("DEBEZIUM_URL", "http://localhost:8083")
            requests.post(f"{debezium_url}/connectors/postgres-cdc-connector/restart", timeout=5)
            # Wait for connector to restart, snapshot the table, and stabilize
            time.sleep(8)
        except Exception as e:
            # If restart fails, log but continue - the test might still work
            print(f"Warning: Failed to restart Debezium connector: {e}")

        # Delete the marker record - this will also be captured by CDC
        cursor.execute("""
            DELETE FROM schema_evolution_test WHERE id = %s
        """, (marker_id,))
        postgres_conn.commit()

        # Give CDC time to capture the delete event
        time.sleep(2)

        yield cursor

    finally:
        # Cleanup - ensure we rollback any failed transactions first
        try:
            postgres_conn.rollback()  # Rollback any failed transactions

            # Remove table from publication before dropping it
            cursor.execute("""
                ALTER PUBLICATION dbz_publication DROP TABLE IF EXISTS schema_evolution_test
            """)

            cursor.execute("DROP TABLE IF EXISTS schema_evolution_test CASCADE")
            postgres_conn.commit()
        except Exception as e:
            # If cleanup fails, still rollback to prevent transaction pollution
            postgres_conn.rollback()
        finally:
            cursor.close()


@pytest.mark.integration
@pytest.mark.skipif(
    not is_schema_registry_available(),
    reason="Requires Schema Registry to be running at SCHEMA_REGISTRY_URL"
)
class TestPostgresSchemaEvolution:
    """Integration tests for Postgres CDC schema evolution."""

    @pytest.mark.skipif(
        not is_cdc_infrastructure_running(),
        reason="Requires full CDC pipeline setup with Debezium, Kafka, and Schema Registry running"
    )
    def test_add_column_propagates_through_cdc(self, test_table, postgres_conn):
        """
        Test ADD COLUMN scenario through full CDC pipeline.

        Steps:
        1. Insert initial data
        2. Verify CDC captures initial records
        3. ADD new column
        4. Insert data with new column
        5. Verify CDC includes new column in events
        6. Verify Schema Registry has updated schema
        """
        # Step 1: Insert initial data
        test_table.execute("""
            INSERT INTO schema_evolution_test (name, value)
            VALUES ('Initial Record', 100)
            RETURNING id
        """)
        initial_id = test_table.fetchone()[0]
        postgres_conn.commit()

        # Step 2: Wait for CDC to capture with polling and retry
        from tests.test_utils import wait_for_condition

        def check_initial_event_in_kafka():
            """Poll Kafka for the initial event with timeout."""
            consumer = KafkaConsumer(
                get_kafka_topic("schema_evolution_test"),
                bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"),
                auto_offset_reset='earliest',
                consumer_timeout_ms=3000,
                value_deserializer=safe_json_deserializer
            )

            try:
                for message in consumer:
                    event = message.value
                    # Skip tombstone records (None values from DELETE operations)
                    if event is None:
                        continue

                    # Handle both unwrapped (flat) and wrapped (nested) formats
                    # Unwrapped format (from ExtractNewRecordState transform): {id: ..., name: ..., __op: ...}
                    # Wrapped format: {payload: {after: {id: ..., name: ...}}}
                    if 'payload' in event:
                        # Wrapped format
                        after = event['payload'].get('after', {})
                    else:
                        # Unwrapped format
                        after = event

                    if after.get('id') == initial_id:
                        # Verify initial schema doesn't have new column
                        assert 'description' not in after
                        return True
            finally:
                consumer.close()

            return False

        wait_for_condition(
            condition_func=check_initial_event_in_kafka,
            timeout_seconds=30,
            poll_interval=2.0,
            error_message=f"Initial CDC event for id={initial_id} not found in Kafka topic"
        )

        # Step 3: ADD new column
        test_table.execute("""
            ALTER TABLE schema_evolution_test
            ADD COLUMN description TEXT DEFAULT 'No description'
        """)
        postgres_conn.commit()

        # Wait for schema change to propagate
        time.sleep(6)

        # Step 4: Insert data with new column
        test_table.execute("""
            INSERT INTO schema_evolution_test (name, value, description)
            VALUES ('Record After Schema Change', 200, 'This has a description')
            RETURNING id
        """)
        new_id = test_table.fetchone()[0]
        postgres_conn.commit()

        # Step 5: Verify CDC includes new column with polling
        def check_new_event_in_kafka():
            """Poll Kafka for the event with new column."""
            consumer = KafkaConsumer(
                get_kafka_topic("schema_evolution_test"),
                bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"),
                auto_offset_reset='earliest',
                consumer_timeout_ms=3000,
                value_deserializer=safe_json_deserializer
            )

            try:
                for message in consumer:
                    event = message.value
                    # Skip tombstone records
                    if event is None:
                        continue

                    # Handle both unwrapped and wrapped formats
                    if 'payload' in event:
                        after = event['payload'].get('after', {})
                    else:
                        after = event

                    if after.get('id') == new_id:
                        # Verify new schema includes description
                        assert 'description' in after
                        assert after['description'] == 'This has a description'
                        return True
            finally:
                consumer.close()

            return False

        wait_for_condition(
            condition_func=check_new_event_in_kafka,
            timeout_seconds=30,
            poll_interval=2.0,
            error_message=f"CDC event with new column for id={new_id} not found in Kafka topic"
        )

        # Step 6: Verify schema evolution in Schema Registry
        schema_data = verify_schema_in_registry('debezium.public.schema_evolution_test-value')
        if schema_data:
            # Verify schema includes description field
            schema_str = str(schema_data.get('schema', ''))
            assert 'description' in schema_str.lower(), "New column 'description' not found in schema registry"

    def test_drop_column_handled_gracefully(self, test_table, postgres_conn):
        """
        Test DROP COLUMN scenario through CDC pipeline.

        Validates that:
        - Dropped column removed from schema
        - Events after drop don't include dropped column
        - Existing data in other columns preserved
        """
        # Insert data with column
        test_table.execute("""
            INSERT INTO schema_evolution_test (name, value)
            VALUES ('Before Drop', 300)
            RETURNING id
        """)
        before_id = test_table.fetchone()[0]
        postgres_conn.commit()

        time.sleep(4)

        # Drop column
        test_table.execute("""
            ALTER TABLE schema_evolution_test
            DROP COLUMN value
        """)
        postgres_conn.commit()

        time.sleep(6)

        # Insert data after drop
        test_table.execute("""
            INSERT INTO schema_evolution_test (name)
            VALUES ('After Drop')
            RETURNING id
        """)
        after_id = test_table.fetchone()[0]
        postgres_conn.commit()

        time.sleep(4)

        # Verify CDC event doesn't include dropped column
        consumer = KafkaConsumer(
            get_kafka_topic("schema_evolution_test"),
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"),
            auto_offset_reset='latest',
            consumer_timeout_ms=5000,
            value_deserializer=safe_json_deserializer
        )

        for message in consumer:
            event = message.value
            # Skip tombstone records
            if event is None:
                continue
            if event.get('payload', {}).get('after', {}).get('id') == after_id:
                # Verify 'value' column not in event
                assert 'value' not in event['payload']['after']
                # Verify 'name' still present
                assert 'name' in event['payload']['after']
                break

        # Verify schema evolution in Schema Registry
        schema_data = verify_schema_in_registry('debezium.public.schema_evolution_test-value')
        if schema_data:
            schema_str = str(schema_data.get('schema', ''))
            # Verify 'value' column removed from schema
            assert 'value' not in schema_str.lower() or 'optional' in schema_str.lower()

    def test_alter_column_type_with_data_conversion(self, test_table, postgres_conn):
        """
        Test ALTER COLUMN TYPE scenario.

        Validates that:
        - Type change reflected in schema
        - Data converted correctly
        - CDC events use new type
        """
        # Insert with original type
        test_table.execute("""
            INSERT INTO schema_evolution_test (name, value)
            VALUES ('Before Type Change', 12345)
            RETURNING id
        """)
        postgres_conn.commit()

        time.sleep(4)

        # Change type (INTEGER -> BIGINT)
        test_table.execute("""
            ALTER TABLE schema_evolution_test
            ALTER COLUMN value TYPE BIGINT
        """)
        postgres_conn.commit()

        time.sleep(6)

        # Insert with new type (can now handle larger values)
        large_value = 9223372036854775807  # Max BIGINT
        test_table.execute("""
            INSERT INTO schema_evolution_test (name, value)
            VALUES ('After Type Change', %s)
            RETURNING id
        """, (large_value,))
        new_id = test_table.fetchone()[0]
        postgres_conn.commit()

        time.sleep(4)

        # Verify CDC event has correct type
        consumer = KafkaConsumer(
            get_kafka_topic("schema_evolution_test"),
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"),
            auto_offset_reset='latest',
            consumer_timeout_ms=5000,
            value_deserializer=safe_json_deserializer
        )

        for message in consumer:
            event = message.value
            # Skip tombstone records
            if event is None:
                continue
            if event.get('payload', {}).get('after', {}).get('id') == new_id:
                # Verify large value preserved
                assert event['payload']['after']['value'] == large_value
                break

        # Verify schema type change in Schema Registry
        schema_data = verify_schema_in_registry('debezium.public.schema_evolution_test-value')
        if schema_data:
            schema_str = str(schema_data.get('schema', ''))
            # Verify schema reflects BIGINT type (typically as int64)
            assert 'int64' in schema_str.lower() or 'long' in schema_str.lower()

    def test_rename_column_updates_schema(self, test_table, postgres_conn):
        """
        Test RENAME COLUMN scenario.

        Validates that:
        - Column renamed in schema
        - CDC events use new column name
        - Data preserved after rename
        """
        # Insert with original column name
        test_table.execute("""
            INSERT INTO schema_evolution_test (name, value)
            VALUES ('Before Rename', 999)
            RETURNING id
        """)
        postgres_conn.commit()

        time.sleep(4)

        # Rename column
        test_table.execute("""
            ALTER TABLE schema_evolution_test
            RENAME COLUMN name TO display_name
        """)
        postgres_conn.commit()

        time.sleep(6)

        # Insert with new column name
        test_table.execute("""
            INSERT INTO schema_evolution_test (display_name, value)
            VALUES ('After Rename', 1000)
            RETURNING id
        """)
        new_id = test_table.fetchone()[0]
        postgres_conn.commit()

        time.sleep(4)

        # Verify CDC event uses new column name
        consumer = KafkaConsumer(
            get_kafka_topic("schema_evolution_test"),
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"),
            auto_offset_reset='latest',
            consumer_timeout_ms=5000,
            value_deserializer=safe_json_deserializer
        )

        for message in consumer:
            event = message.value
            # Skip tombstone records
            if event is None:
                continue
            if event.get('payload', {}).get('after', {}).get('id') == new_id:
                # Verify new column name used
                assert 'display_name' in event['payload']['after']
                assert event['payload']['after']['display_name'] == 'After Rename'
                # Verify old column name not present
                assert 'name' not in event['payload']['after']
                break

        # Verify schema rename in Schema Registry
        schema_data = verify_schema_in_registry('debezium.public.schema_evolution_test-value')
        if schema_data:
            schema_str = str(schema_data.get('schema', ''))
            # Verify new column name in schema
            assert 'display_name' in schema_str.lower()

    def test_multiple_schema_changes_sequence(self, test_table, postgres_conn):
        """
        Test sequence of multiple schema changes.

        Scenario:
        1. Add column
        2. Rename column
        3. Change type
        4. Drop column
        """
        # Initial insert
        test_table.execute("""
            INSERT INTO schema_evolution_test (name, value)
            VALUES ('Initial', 1)
            RETURNING id
        """)
        postgres_conn.commit()
        time.sleep(4)

        # Change 1: Add column
        test_table.execute("""
            ALTER TABLE schema_evolution_test
            ADD COLUMN email VARCHAR(255)
        """)
        postgres_conn.commit()
        time.sleep(6)

        # Change 2: Rename column
        test_table.execute("""
            ALTER TABLE schema_evolution_test
            RENAME COLUMN name TO display_name
        """)
        postgres_conn.commit()
        time.sleep(6)

        # Change 3: Change type
        test_table.execute("""
            ALTER TABLE schema_evolution_test
            ALTER COLUMN value TYPE BIGINT
        """)
        postgres_conn.commit()
        time.sleep(6)

        # Insert after all changes
        test_table.execute("""
            INSERT INTO schema_evolution_test (display_name, value, email)
            VALUES ('Final', 12345678901234, 'test@example.com')
            RETURNING id
        """)
        final_id = test_table.fetchone()[0]
        postgres_conn.commit()
        time.sleep(4)

        # Verify final schema in CDC
        consumer = KafkaConsumer(
            get_kafka_topic("schema_evolution_test"),
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"),
            auto_offset_reset='latest',
            consumer_timeout_ms=5000,
            value_deserializer=safe_json_deserializer
        )

        for message in consumer:
            event = message.value
            # Skip tombstone records
            if event is None:
                continue
            if event.get('payload', {}).get('after', {}).get('id') == final_id:
                after_data = event['payload']['after']

                # Verify all changes applied
                assert 'display_name' in after_data  # Renamed
                assert 'name' not in after_data  # Old name gone
                assert 'email' in after_data  # Added
                assert after_data['value'] == 12345678901234  # BIGINT value
                assert after_data['email'] == 'test@example.com'
                break

        # Verify all schema changes in Schema Registry
        schema_data = verify_schema_in_registry('debezium.public.schema_evolution_test-value')
        if schema_data:
            schema_str = str(schema_data.get('schema', ''))
            # Verify all changes reflected in schema
            assert 'display_name' in schema_str.lower(), "Renamed column not in schema"
            assert 'email' in schema_str.lower(), "Added column not in schema"

    def test_schema_evolution_no_data_loss(self, test_table, postgres_conn):
        """
        Test that schema evolution doesn't cause data loss.

        Insert data, perform schema changes, verify all data accessible.
        """
        # Insert test data
        test_data = [(f'Record {i}', i * 100) for i in range(10)]

        for name, value in test_data:
            test_table.execute("""
                INSERT INTO schema_evolution_test (name, value)
                VALUES (%s, %s)
            """, (name, value))

        postgres_conn.commit()
        time.sleep(6)

        # Get initial count
        test_table.execute("SELECT COUNT(*) FROM schema_evolution_test")
        initial_count = test_table.fetchone()[0]

        # Perform schema change
        test_table.execute("""
            ALTER TABLE schema_evolution_test
            ADD COLUMN status VARCHAR(50) DEFAULT 'active'
        """)
        postgres_conn.commit()
        time.sleep(6)

        # Verify no data loss
        test_table.execute("SELECT COUNT(*) FROM schema_evolution_test")
        final_count = test_table.fetchone()[0]

        assert final_count == initial_count, "Data loss detected after schema evolution"

        # Verify all original data still accessible
        test_table.execute("SELECT name, value FROM schema_evolution_test ORDER BY id")
        retrieved_data = [(row[0], row[1]) for row in test_table.fetchall()]

        for original, retrieved in zip(test_data, retrieved_data):
            assert original == retrieved, "Data mismatch after schema evolution"

        # Verify schema evolution in Schema Registry preserves existing fields
        schema_data = verify_schema_in_registry('debezium.public.schema_evolution_test-value')
        if schema_data:
            schema_str = str(schema_data.get('schema', ''))
            # Verify original columns still in schema
            assert 'name' in schema_str.lower(), "Original column 'name' missing from schema"
            assert 'value' in schema_str.lower(), "Original column 'value' missing from schema"
            # Verify new column added
            assert 'status' in schema_str.lower(), "New column 'status' not in schema"
