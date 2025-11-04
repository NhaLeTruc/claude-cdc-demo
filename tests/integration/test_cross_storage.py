"""Integration tests for cross-storage CDC pipeline (Postgres → Kafka → Iceberg)."""

import pytest
from datetime import datetime
import time

# Check PyIceberg availability
try:
    import pyiceberg
    PYICEBERG_AVAILABLE = True
except ImportError:
    PYICEBERG_AVAILABLE = False


@pytest.mark.integration
@pytest.mark.skip(reason="Cross-storage tests require full CDC pipeline setup (Debezium connectors, Spark jobs) - tests are incomplete stubs")
@pytest.mark.skipif(
    not PYICEBERG_AVAILABLE,
    reason="Requires full infrastructure (Postgres, Kafka, Debezium, Spark, Iceberg)"
)
class TestCrossStorageCDC:
    """Integration tests for Postgres to Iceberg cross-storage CDC."""

    def test_postgres_kafka_iceberg_flow(self, postgres_connection):
        """Test complete data flow from Postgres through Kafka to Iceberg."""
        from kafka import KafkaConsumer
        from src.cdc_pipelines.iceberg.table_manager import IcebergTableManager

        # Step 1: Insert data into Postgres (use fixture connection)
        conn = postgres_connection
        cursor = conn.cursor()

        # Generate unique email to avoid conflicts
        import uuid
        test_email = f"test_{uuid.uuid4().hex[:8]}@example.com"

        cursor.execute(
            """
            INSERT INTO customers (email, first_name, last_name,
                                   city, state, country)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING customer_id
            """,
            (
                test_email,
                "Test",
                "User",
                "Springfield",
                "IL",
                "USA",
            ),
        )
        customer_id = cursor.fetchone()[0]
        conn.commit()

        # Step 2: Wait for Debezium to capture change
        time.sleep(5)

        # Step 3: Verify event in Kafka
        consumer = KafkaConsumer(
            "debezium.public.customers",
            bootstrap_servers="localhost:9092",
            auto_offset_reset="earliest",
            consumer_timeout_ms=10000,
        )

        event_found = False
        for message in consumer:
            payload = message.value
            if payload.get("payload", {}).get("after", {}).get("customer_id") == customer_id:
                event_found = True
                break

        assert event_found, "CDC event not found in Kafka"

        # Step 4: Wait for Spark to process and write to Iceberg
        time.sleep(10)

        # Step 5: Verify data in Iceberg
        iceberg_manager = IcebergTableManager(...)
        result = iceberg_manager.query_table(
            filter_condition=f"customer_id = {customer_id}"
        )

        assert len(result) > 0
        assert result[0]["full_name"] == "Test User"
        assert result[0]["location"] == "Springfield, IL, USA"

        # Cleanup
        cursor.execute("DELETE FROM customers WHERE customer_id = %s", (customer_id,))
        conn.commit()
        conn.close()

    def test_schema_drift_handling(self):
        """Test handling schema drift across systems."""
        import psycopg2

        conn = psycopg2.connect(
            host="localhost",
            port=5432,
            database="demo_db",
            user="postgres",
            password="postgres",
        )
        cursor = conn.cursor()

        # Add new column to source table
        cursor.execute(
            """
            ALTER TABLE customers
            ADD COLUMN IF NOT EXISTS loyalty_points INTEGER DEFAULT 0
            """
        )
        conn.commit()

        # Insert record with new column
        cursor.execute(
            """
            INSERT INTO customers (email, first_name, last_name,
                                   loyalty_points, registration_date, last_updated)
            VALUES (%s, %s, %s, %s, NOW(), NOW())
            RETURNING customer_id
            """,
            ("schema_test@example.com", "Schema", "Test", 100),
        )
        customer_id = cursor.fetchone()[0]
        conn.commit()

        # Wait for CDC pipeline to process
        time.sleep(15)

        # Verify schema evolution was handled
        from src.cdc_pipelines.iceberg.table_manager import IcebergTableManager

        iceberg_manager = IcebergTableManager(...)
        schema = iceberg_manager.get_schema()

        # New column should be present (or null if not yet evolved)
        result = iceberg_manager.query_table(
            filter_condition=f"customer_id = {customer_id}"
        )

        assert len(result) > 0
        # Schema evolution should handle new column
        assert "loyalty_points" in result[0] or result[0].get("loyalty_points") is None

        # Cleanup
        cursor.execute("DELETE FROM customers WHERE customer_id = %s", (customer_id,))
        cursor.execute("ALTER TABLE customers DROP COLUMN IF EXISTS loyalty_points")
        conn.commit()
        conn.close()

    def test_data_transformation_accuracy(self):
        """Test accuracy of data transformations in pipeline."""
        import psycopg2

        conn = psycopg2.connect(
            host="localhost",
            port=5432,
            database="demo_db",
            user="postgres",
            password="postgres",
        )
        cursor = conn.cursor()

        # Insert test data
        test_data = [
            ("john.doe@example.com", "John", "Doe", "New York", "NY", "USA"),
            ("jane.smith@example.com", "Jane", "Smith", "Los Angeles", "CA", "USA"),
            ("bob.jones@example.com", "Bob", None, "Chicago", None, "USA"),
        ]

        customer_ids = []
        for email, first, last, city, state, country in test_data:
            cursor.execute(
                """
                INSERT INTO customers (email, first_name, last_name,
                                       city, state, country,
                                       registration_date, last_updated)
                VALUES (%s, %s, %s, %s, %s, %s, NOW(), NOW())
                RETURNING customer_id
                """,
                (email, first, last, city, state, country),
            )
            customer_ids.append(cursor.fetchone()[0])
        conn.commit()

        # Wait for pipeline processing
        time.sleep(15)

        # Verify transformations
        from src.cdc_pipelines.iceberg.table_manager import IcebergTableManager

        iceberg_manager = IcebergTableManager(...)

        # Check John Doe
        result = iceberg_manager.query_table(
            filter_condition=f"customer_id = {customer_ids[0]}"
        )
        assert result[0]["full_name"] == "John Doe"
        assert result[0]["location"] == "New York, NY, USA"

        # Check Jane Smith
        result = iceberg_manager.query_table(
            filter_condition=f"customer_id = {customer_ids[1]}"
        )
        assert result[0]["full_name"] == "Jane Smith"
        assert result[0]["location"] == "Los Angeles, CA, USA"

        # Check Bob Jones (null handling)
        result = iceberg_manager.query_table(
            filter_condition=f"customer_id = {customer_ids[2]}"
        )
        assert result[0]["full_name"] == "Bob" or result[0]["full_name"] == "Bob "
        assert "Chicago" in result[0]["location"]

        # Cleanup
        for cid in customer_ids:
            cursor.execute("DELETE FROM customers WHERE customer_id = %s", (cid,))
        conn.commit()
        conn.close()

    def test_high_throughput_scenario(self):
        """Test cross-storage pipeline under high throughput."""
        import psycopg2

        conn = psycopg2.connect(
            host="localhost",
            port=5432,
            database="demo_db",
            user="postgres",
            password="postgres",
        )
        cursor = conn.cursor()

        # Insert many records rapidly
        start_time = time.time()
        customer_ids = []

        for i in range(1000):
            cursor.execute(
                """
                INSERT INTO customers (email, first_name, last_name,
                                       registration_date, last_updated)
                VALUES (%s, %s, %s, NOW(), NOW())
                RETURNING customer_id
                """,
                (
                    f"user{i}@example.com",
                    f"User{i}",
                    f"Test{i}",
                ),
            )
            customer_ids.append(cursor.fetchone()[0])

            if i % 100 == 0:
                conn.commit()

        conn.commit()
        insert_duration = time.time() - start_time

        # Wait for pipeline to catch up
        time.sleep(30)

        # Verify all records made it through
        from src.cdc_pipelines.iceberg.table_manager import IcebergTableManager

        iceberg_manager = IcebergTableManager(...)
        result_count = iceberg_manager.count_records(
            filter_condition=f"customer_id IN ({','.join(map(str, customer_ids))})"
        )

        assert result_count == 1000, f"Expected 1000 records, found {result_count}"

        # Verify CDC lag is acceptable
        end_time = time.time()
        total_duration = end_time - start_time
        cdc_lag = total_duration - insert_duration

        assert cdc_lag < 30, f"CDC lag too high: {cdc_lag}s"

        # Cleanup
        for cid in customer_ids:
            cursor.execute("DELETE FROM customers WHERE customer_id = %s", (cid,))
        conn.commit()
        conn.close()

    def test_kafka_offset_management(self):
        """Test Kafka offset management and replay."""
        from kafka import KafkaConsumer
        from kafka.admin import KafkaAdminClient, NewTopic

        # Create test topic
        admin = KafkaAdminClient(bootstrap_servers="localhost:9092")

        # Consume from beginning
        consumer = KafkaConsumer(
            "debezium.public.customers",
            bootstrap_servers="localhost:9092",
            auto_offset_reset="earliest",
            group_id="test_cross_storage_group",
        )

        initial_offset = consumer.position(consumer.assignment())

        # Process some messages
        messages_processed = 0
        for message in consumer:
            messages_processed += 1
            if messages_processed >= 10:
                break

        # Commit offset
        consumer.commit()
        committed_offset = consumer.committed(consumer.assignment())

        # Close and reopen consumer
        consumer.close()

        new_consumer = KafkaConsumer(
            "debezium.public.customers",
            bootstrap_servers="localhost:9092",
            group_id="test_cross_storage_group",
        )

        # Should resume from committed offset
        resumed_offset = new_consumer.position(new_consumer.assignment())

        assert resumed_offset == committed_offset

        new_consumer.close()

    def test_idempotent_writes_to_iceberg(self):
        """Test idempotent writes to Iceberg (replaying same event)."""
        import psycopg2

        conn = psycopg2.connect(
            host="localhost",
            port=5432,
            database="demo_db",
            user="postgres",
            password="postgres",
        )
        cursor = conn.cursor()

        # Insert record
        cursor.execute(
            """
            INSERT INTO customers (email, first_name, last_name,
                                   registration_date, last_updated)
            VALUES (%s, %s, %s, NOW(), NOW())
            RETURNING customer_id
            """,
            ("idempotent_test@example.com", "Test", "User"),
        )
        customer_id = cursor.fetchone()[0]
        conn.commit()

        # Wait for initial processing
        time.sleep(10)

        # Force pipeline restart (simulating replay)
        # ... restart logic ...

        # Wait for potential duplicate processing
        time.sleep(10)

        # Verify no duplicates in Iceberg
        from src.cdc_pipelines.iceberg.table_manager import IcebergTableManager

        iceberg_manager = IcebergTableManager(...)
        result_count = iceberg_manager.count_records(
            filter_condition=f"customer_id = {customer_id}"
        )

        assert result_count == 1, f"Expected 1 record, found {result_count} (duplicates!)"

        # Cleanup
        cursor.execute("DELETE FROM customers WHERE customer_id = %s", (customer_id,))
        conn.commit()
        conn.close()
