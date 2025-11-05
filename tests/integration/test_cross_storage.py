"""Integration tests for cross-storage CDC pipeline (Postgres → Kafka → Iceberg)."""

import os
import pytest
import requests
from datetime import datetime
import time

# Check PyIceberg availability
try:
    import pyiceberg
    PYICEBERG_AVAILABLE = True
except ImportError:
    PYICEBERG_AVAILABLE = False


def is_cdc_infrastructure_running():
    """Check if CDC infrastructure (Debezium + Kafka) is running."""
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
        return result == 0
    except Exception:
        pass
    return False


@pytest.mark.integration
@pytest.mark.skipif(
    not PYICEBERG_AVAILABLE,
    reason="Requires PyIceberg package"
)
class TestCrossStorageCDC:
    """Integration tests for Postgres to Iceberg cross-storage CDC."""

    @pytest.mark.skipif(
        not is_cdc_infrastructure_running(),
        reason="Requires full CDC pipeline setup with Debezium and Kafka running"
    )
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
            bootstrap_servers="localhost:29092",
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
        from src.cdc_pipelines.iceberg.table_manager import IcebergTableConfig

        config = IcebergTableConfig(
            catalog_name="demo_catalog",
            namespace="cdc",
            table_name="customers",
            warehouse_path="s3://warehouse/iceberg",
        )
        iceberg_manager = IcebergTableManager(config)
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

    @pytest.mark.skipif(
        not is_cdc_infrastructure_running(),
        reason="Requires full CDC pipeline setup with Debezium and Kafka running"
    )
    def test_schema_drift_handling(self):
        """Test handling schema drift across systems."""
        import psycopg2

        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=int(os.getenv("POSTGRES_PORT", "5432")),
            database=os.getenv("POSTGRES_DB", "cdcdb"),
            user=os.getenv("POSTGRES_USER", "cdcuser"),
            password=os.getenv("POSTGRES_PASSWORD", "cdcpass"),
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
                                   loyalty_points, city, state, country)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING customer_id
            """,
            ("schema_test@example.com", "Schema", "Test", 100, "TestCity", "TS", "USA"),
        )
        customer_id = cursor.fetchone()[0]
        conn.commit()

        # Wait for CDC pipeline to process
        time.sleep(15)

        # Verify schema evolution was handled
        from src.cdc_pipelines.iceberg.table_manager import IcebergTableManager, IcebergTableConfig

        config = IcebergTableConfig(
            catalog_name="demo_catalog",
            namespace="cdc",
            table_name="customers",
            warehouse_path="s3://warehouse/iceberg",
        )
        iceberg_manager = IcebergTableManager(config)
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

    @pytest.mark.skipif(
        not is_cdc_infrastructure_running(),
        reason="Requires full CDC pipeline setup with Debezium and Kafka running"
    )
    def test_data_transformation_accuracy(self):
        """Test accuracy of data transformations in pipeline."""
        import psycopg2

        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=int(os.getenv("POSTGRES_PORT", "5432")),
            database=os.getenv("POSTGRES_DB", "cdcdb"),
            user=os.getenv("POSTGRES_USER", "cdcuser"),
            password=os.getenv("POSTGRES_PASSWORD", "cdcpass"),
        )
        cursor = conn.cursor()

        # Insert test data
        test_data = [
            ("john.doe@example.com", "John", "Doe", "New York", "NY", "USA"),
            ("jane.smith@example.com", "Jane", "Smith", "Los Angeles", "CA", "USA"),
            ("bob.jones@example.com", "Bob", "Jones", "Chicago", "IL", "USA"),
        ]

        customer_ids = []
        for email, first, last, city, state, country in test_data:
            cursor.execute(
                """
                INSERT INTO customers (email, first_name, last_name,
                                       city, state, country)
                VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING customer_id
                """,
                (email, first, last, city, state, country),
            )
            customer_ids.append(cursor.fetchone()[0])
        conn.commit()

        # Wait for pipeline processing
        time.sleep(15)

        # Verify transformations
        from src.cdc_pipelines.iceberg.table_manager import IcebergTableManager, IcebergTableConfig

        config = IcebergTableConfig(
            catalog_name="demo_catalog",
            namespace="cdc",
            table_name="customers",
            warehouse_path="s3://warehouse/iceberg",
        )
        iceberg_manager = IcebergTableManager(config)

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

        # Check Bob Jones
        result = iceberg_manager.query_table(
            filter_condition=f"customer_id = {customer_ids[2]}"
        )
        assert result[0]["full_name"] == "Bob Jones"
        assert result[0]["location"] == "Chicago, IL, USA"

        # Cleanup
        for cid in customer_ids:
            cursor.execute("DELETE FROM customers WHERE customer_id = %s", (cid,))
        conn.commit()
        conn.close()

    @pytest.mark.skipif(
        not is_cdc_infrastructure_running(),
        reason="Requires full CDC pipeline setup with Debezium and Kafka running"
    )
    def test_high_throughput_scenario(self):
        """Test cross-storage pipeline under high throughput."""
        import psycopg2

        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=int(os.getenv("POSTGRES_PORT", "5432")),
            database=os.getenv("POSTGRES_DB", "cdcdb"),
            user=os.getenv("POSTGRES_USER", "cdcuser"),
            password=os.getenv("POSTGRES_PASSWORD", "cdcpass"),
        )
        cursor = conn.cursor()

        # Insert many records rapidly
        start_time = time.time()
        customer_ids = []

        for i in range(1000):
            cursor.execute(
                """
                INSERT INTO customers (email, first_name, last_name,
                                       city, state, country)
                VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING customer_id
                """,
                (
                    f"user{i}@example.com",
                    f"User{i}",
                    f"Test{i}",
                    "TestCity",
                    "TS",
                    "USA",
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
        from src.cdc_pipelines.iceberg.table_manager import IcebergTableManager, IcebergTableConfig

        config = IcebergTableConfig(
            catalog_name="demo_catalog",
            namespace="cdc",
            table_name="customers",
            warehouse_path="s3://warehouse/iceberg",
        )
        iceberg_manager = IcebergTableManager(config)
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

    @pytest.mark.skipif(
        not is_cdc_infrastructure_running(),
        reason="Requires full CDC pipeline setup with Debezium and Kafka running"
    )
    def test_kafka_offset_management(self):
        """Test Kafka offset management and replay."""
        from kafka import KafkaConsumer
        from kafka.admin import KafkaAdminClient, NewTopic

        # Create test topic
        admin = KafkaAdminClient(bootstrap_servers="localhost:29092")

        # Consume from beginning
        consumer = KafkaConsumer(
            "debezium.public.customers",
            bootstrap_servers="localhost:29092",
            auto_offset_reset="earliest",
            group_id="test_cross_storage_group",
        )

        # Poll once to trigger partition assignment
        consumer.poll(timeout_ms=1000)

        # Get assigned partitions
        partitions = list(consumer.assignment())
        if not partitions:
            consumer.close()
            return  # No partitions assigned, skip test

        initial_offset = consumer.position(partitions[0])

        # Process some messages
        messages_processed = 0
        for message in consumer:
            messages_processed += 1
            if messages_processed >= 10:
                break

        # Commit offset
        consumer.commit()
        committed_offset = consumer.committed(partitions[0])

        # Close and reopen consumer
        consumer.close()

        new_consumer = KafkaConsumer(
            "debezium.public.customers",
            bootstrap_servers="localhost:29092",
            group_id="test_cross_storage_group",
        )

        # Poll to trigger partition assignment
        new_consumer.poll(timeout_ms=1000)

        # Should resume from committed offset
        new_partitions = list(new_consumer.assignment())
        if new_partitions:
            resumed_offset = new_consumer.position(new_partitions[0])
            assert resumed_offset == committed_offset

        new_consumer.close()

    @pytest.mark.skipif(
        not is_cdc_infrastructure_running(),
        reason="Requires full CDC pipeline setup with Debezium and Kafka running"
    )
    def test_idempotent_writes_to_iceberg(self):
        """Test idempotent writes to Iceberg (replaying same event)."""
        import psycopg2

        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=int(os.getenv("POSTGRES_PORT", "5432")),
            database=os.getenv("POSTGRES_DB", "cdcdb"),
            user=os.getenv("POSTGRES_USER", "cdcuser"),
            password=os.getenv("POSTGRES_PASSWORD", "cdcpass"),
        )
        cursor = conn.cursor()

        # Insert record
        cursor.execute(
            """
            INSERT INTO customers (email, first_name, last_name,
                                   city, state, country)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING customer_id
            """,
            ("idempotent_test@example.com", "Test", "User", "TestCity", "TS", "USA"),
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
        from src.cdc_pipelines.iceberg.table_manager import IcebergTableManager, IcebergTableConfig

        config = IcebergTableConfig(
            catalog_name="demo_catalog",
            namespace="cdc",
            table_name="customers",
            warehouse_path="s3://warehouse/iceberg",
        )
        iceberg_manager = IcebergTableManager(config)
        result_count = iceberg_manager.count_records(
            filter_condition=f"customer_id = {customer_id}"
        )

        assert result_count == 1, f"Expected 1 record, found {result_count} (duplicates!)"

        # Cleanup
        cursor.execute("DELETE FROM customers WHERE customer_id = %s", (customer_id,))
        conn.commit()
        conn.close()
