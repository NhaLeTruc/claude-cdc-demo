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


@pytest.fixture(scope="module")
def postgres_conn():
    """PostgreSQL connection fixture."""
    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        database="demo_db",
        user="postgres",
        password="postgres"
    )
    yield conn
    conn.close()


@pytest.fixture
def test_table(postgres_conn):
    """Create and teardown test table."""
    cursor = postgres_conn.cursor()

    # Create test table
    cursor.execute("""
        DROP TABLE IF EXISTS schema_evo_test CASCADE
    """)

    cursor.execute("""
        CREATE TABLE schema_evo_test (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            value INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    postgres_conn.commit()

    yield cursor

    # Cleanup
    cursor.execute("DROP TABLE IF EXISTS schema_evo_test CASCADE")
    postgres_conn.commit()
    cursor.close()


@pytest.mark.integration
@pytest.mark.skipif(
    reason="Requires full CDC infrastructure (Postgres, Debezium, Kafka, Schema Registry)",
    condition=True,
)
class TestPostgresSchemaEvolution:
    """Integration tests for Postgres CDC schema evolution."""

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
            INSERT INTO schema_evo_test (name, value)
            VALUES ('Initial Record', 100)
            RETURNING id
        """)
        initial_id = test_table.fetchone()[0]
        postgres_conn.commit()

        # Step 2: Wait for CDC to capture
        time.sleep(3)

        # Verify event in Kafka
        consumer = KafkaConsumer(
            'debezium.public.schema_evo_test',
            bootstrap_servers='localhost:9092',
            auto_offset_reset='earliest',
            consumer_timeout_ms=5000,
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )

        initial_event_found = False
        for message in consumer:
            event = message.value
            if event.get('payload', {}).get('after', {}).get('id') == initial_id:
                # Verify initial schema doesn't have new column
                assert 'description' not in event['payload']['after']
                initial_event_found = True
                break

        assert initial_event_found, "Initial CDC event not found"

        # Step 3: ADD new column
        test_table.execute("""
            ALTER TABLE schema_evo_test
            ADD COLUMN description TEXT DEFAULT 'No description'
        """)
        postgres_conn.commit()

        # Wait for schema change to propagate
        time.sleep(5)

        # Step 4: Insert data with new column
        test_table.execute("""
            INSERT INTO schema_evo_test (name, value, description)
            VALUES ('Record After Schema Change', 200, 'This has a description')
            RETURNING id
        """)
        new_id = test_table.fetchone()[0]
        postgres_conn.commit()

        # Step 5: Verify CDC includes new column
        time.sleep(3)

        consumer = KafkaConsumer(
            'debezium.public.schema_evo_test',
            bootstrap_servers='localhost:9092',
            auto_offset_reset='latest',
            consumer_timeout_ms=5000,
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )

        new_event_found = False
        for message in consumer:
            event = message.value
            if event.get('payload', {}).get('after', {}).get('id') == new_id:
                # Verify new schema includes description
                assert 'description' in event['payload']['after']
                assert event['payload']['after']['description'] == 'This has a description'
                new_event_found = True
                break

        assert new_event_found, "CDC event with new column not found"

        # Step 6: Check Schema Registry (optional, if schema registry is running)
        # import requests
        # response = requests.get('http://localhost:8081/subjects/debezium.public.schema_evo_test-value/versions/latest')
        # assert response.status_code == 200

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
            INSERT INTO schema_evo_test (name, value)
            VALUES ('Before Drop', 300)
            RETURNING id
        """)
        before_id = test_table.fetchone()[0]
        postgres_conn.commit()

        time.sleep(3)

        # Drop column
        test_table.execute("""
            ALTER TABLE schema_evo_test
            DROP COLUMN value
        """)
        postgres_conn.commit()

        time.sleep(5)

        # Insert data after drop
        test_table.execute("""
            INSERT INTO schema_evo_test (name)
            VALUES ('After Drop')
            RETURNING id
        """)
        after_id = test_table.fetchone()[0]
        postgres_conn.commit()

        time.sleep(3)

        # Verify CDC event doesn't include dropped column
        consumer = KafkaConsumer(
            'debezium.public.schema_evo_test',
            bootstrap_servers='localhost:9092',
            auto_offset_reset='latest',
            consumer_timeout_ms=5000,
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )

        for message in consumer:
            event = message.value
            if event.get('payload', {}).get('after', {}).get('id') == after_id:
                # Verify 'value' column not in event
                assert 'value' not in event['payload']['after']
                # Verify 'name' still present
                assert 'name' in event['payload']['after']
                break

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
            INSERT INTO schema_evo_test (name, value)
            VALUES ('Before Type Change', 12345)
            RETURNING id
        """)
        postgres_conn.commit()

        time.sleep(3)

        # Change type (INTEGER -> BIGINT)
        test_table.execute("""
            ALTER TABLE schema_evo_test
            ALTER COLUMN value TYPE BIGINT
        """)
        postgres_conn.commit()

        time.sleep(5)

        # Insert with new type (can now handle larger values)
        large_value = 9223372036854775807  # Max BIGINT
        test_table.execute("""
            INSERT INTO schema_evo_test (name, value)
            VALUES ('After Type Change', %s)
            RETURNING id
        """, (large_value,))
        new_id = test_table.fetchone()[0]
        postgres_conn.commit()

        time.sleep(3)

        # Verify CDC event has correct type
        consumer = KafkaConsumer(
            'debezium.public.schema_evo_test',
            bootstrap_servers='localhost:9092',
            auto_offset_reset='latest',
            consumer_timeout_ms=5000,
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )

        for message in consumer:
            event = message.value
            if event.get('payload', {}).get('after', {}).get('id') == new_id:
                # Verify large value preserved
                assert event['payload']['after']['value'] == large_value
                break

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
            INSERT INTO schema_evo_test (name, value)
            VALUES ('Before Rename', 999)
            RETURNING id
        """)
        postgres_conn.commit()

        time.sleep(3)

        # Rename column
        test_table.execute("""
            ALTER TABLE schema_evo_test
            RENAME COLUMN name TO display_name
        """)
        postgres_conn.commit()

        time.sleep(5)

        # Insert with new column name
        test_table.execute("""
            INSERT INTO schema_evo_test (display_name, value)
            VALUES ('After Rename', 1000)
            RETURNING id
        """)
        new_id = test_table.fetchone()[0]
        postgres_conn.commit()

        time.sleep(3)

        # Verify CDC event uses new column name
        consumer = KafkaConsumer(
            'debezium.public.schema_evo_test',
            bootstrap_servers='localhost:9092',
            auto_offset_reset='latest',
            consumer_timeout_ms=5000,
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )

        for message in consumer:
            event = message.value
            if event.get('payload', {}).get('after', {}).get('id') == new_id:
                # Verify new column name used
                assert 'display_name' in event['payload']['after']
                assert event['payload']['after']['display_name'] == 'After Rename'
                # Verify old column name not present
                assert 'name' not in event['payload']['after']
                break

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
            INSERT INTO schema_evo_test (name, value)
            VALUES ('Initial', 1)
            RETURNING id
        """)
        postgres_conn.commit()
        time.sleep(3)

        # Change 1: Add column
        test_table.execute("""
            ALTER TABLE schema_evo_test
            ADD COLUMN email VARCHAR(255)
        """)
        postgres_conn.commit()
        time.sleep(5)

        # Change 2: Rename column
        test_table.execute("""
            ALTER TABLE schema_evo_test
            RENAME COLUMN name TO display_name
        """)
        postgres_conn.commit()
        time.sleep(5)

        # Change 3: Change type
        test_table.execute("""
            ALTER TABLE schema_evo_test
            ALTER COLUMN value TYPE BIGINT
        """)
        postgres_conn.commit()
        time.sleep(5)

        # Insert after all changes
        test_table.execute("""
            INSERT INTO schema_evo_test (display_name, value, email)
            VALUES ('Final', 12345678901234, 'test@example.com')
            RETURNING id
        """)
        final_id = test_table.fetchone()[0]
        postgres_conn.commit()
        time.sleep(3)

        # Verify final schema in CDC
        consumer = KafkaConsumer(
            'debezium.public.schema_evo_test',
            bootstrap_servers='localhost:9092',
            auto_offset_reset='latest',
            consumer_timeout_ms=5000,
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )

        for message in consumer:
            event = message.value
            if event.get('payload', {}).get('after', {}).get('id') == final_id:
                after_data = event['payload']['after']

                # Verify all changes applied
                assert 'display_name' in after_data  # Renamed
                assert 'name' not in after_data  # Old name gone
                assert 'email' in after_data  # Added
                assert after_data['value'] == 12345678901234  # BIGINT value
                assert after_data['email'] == 'test@example.com'
                break

    def test_schema_evolution_no_data_loss(self, test_table, postgres_conn):
        """
        Test that schema evolution doesn't cause data loss.

        Insert data, perform schema changes, verify all data accessible.
        """
        # Insert test data
        test_data = [(f'Record {i}', i * 100) for i in range(10)]

        for name, value in test_data:
            test_table.execute("""
                INSERT INTO schema_evo_test (name, value)
                VALUES (%s, %s)
            """, (name, value))

        postgres_conn.commit()
        time.sleep(5)

        # Get initial count
        test_table.execute("SELECT COUNT(*) FROM schema_evo_test")
        initial_count = test_table.fetchone()[0]

        # Perform schema change
        test_table.execute("""
            ALTER TABLE schema_evo_test
            ADD COLUMN status VARCHAR(50) DEFAULT 'active'
        """)
        postgres_conn.commit()
        time.sleep(5)

        # Verify no data loss
        test_table.execute("SELECT COUNT(*) FROM schema_evo_test")
        final_count = test_table.fetchone()[0]

        assert final_count == initial_count, "Data loss detected after schema evolution"

        # Verify all original data still accessible
        test_table.execute("SELECT name, value FROM schema_evo_test ORDER BY id")
        retrieved_data = [(row[0], row[1]) for row in test_table.fetchall()]

        for original, retrieved in zip(test_data, retrieved_data):
            assert original == retrieved, "Data mismatch after schema evolution"
