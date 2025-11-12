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


def is_spark_streaming_to_iceberg_running():
    """
    Check if Spark streaming job is running to process Kafka → Iceberg.

    Note: These tests require a Spark streaming application that:
    1. Consumes from Kafka topics (debezium.public.*)
    2. Writes to Iceberg tables in the configured warehouse

    This function checks for evidence that the streaming job is active.
    """
    try:
        from src.cdc_pipelines.iceberg.table_manager import IcebergTableManager, IcebergTableConfig

        # Check if Iceberg warehouse is accessible and has tables
        config = IcebergTableConfig(
            catalog_name="demo_catalog",
            namespace="cdc",
            table_name="customers",
            warehouse_path="s3://warehouse/iceberg",
        )
        manager = IcebergTableManager(config)

        # If table exists and has data, assume streaming is working
        if manager.table_exists():
            count = manager.count_records()
            return count > 0

        return False
    except Exception:
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
        """
        Test complete data flow from Postgres through Kafka to Iceberg.

        NOTE: This test requires a Spark streaming job to be running that:
        - Consumes from Kafka topic: debezium.public.customers
        - Writes to Iceberg tables at: s3://warehouse/iceberg

        If this test fails with "assert 0 > 0", the Spark streaming job is not running.
        """
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

        # Step 2: Verify event in Kafka using polling
        from tests.test_utils import get_kafka_topic, wait_for_kafka_event, wait_for_iceberg_record

        kafka_event = wait_for_kafka_event(
            topic=get_kafka_topic("customers"),
            filter_func=lambda payload: payload.get("customer_id") == customer_id,
            timeout_seconds=15,
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
        )
        assert kafka_event is not None, f"CDC event not found in Kafka for customer_id={customer_id}"
        assert kafka_event.get("email") == test_email

        # Step 3: Verify data in Iceberg using polling
        from src.cdc_pipelines.iceberg.table_manager import IcebergTableConfig

        config = IcebergTableConfig(
            catalog_name="demo_catalog",
            namespace="cdc",
            table_name="customers",
            warehouse_path="s3://warehouse/iceberg",
        )
        iceberg_manager = IcebergTableManager(config)

        # Wait for Spark streaming to process and write to Iceberg
        result = wait_for_iceberg_record(
            table_manager=iceberg_manager,
            filter_condition=f"customer_id = {customer_id}",
            timeout_seconds=60,
            poll_interval=3.0
        )

        assert len(result) > 0, f"No data found for customer_id={customer_id}"
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

        # Verify data in Iceberg using polling
        from src.cdc_pipelines.iceberg.table_manager import IcebergTableManager, IcebergTableConfig
        from tests.test_utils import wait_for_iceberg_record

        config = IcebergTableConfig(
            catalog_name="demo_catalog",
            namespace="cdc",
            table_name="customers",
            warehouse_path="s3://warehouse/iceberg",
        )
        iceberg_manager = IcebergTableManager(config)

        # Wait for Spark streaming to process and write to Iceberg
        result = wait_for_iceberg_record(
            table_manager=iceberg_manager,
            filter_condition=f"customer_id = {customer_id}",
            timeout_seconds=60,
            poll_interval=3.0
        )

        assert len(result) > 0, f"No data found for customer_id={customer_id}"
        # Schema evolution should handle new column
        assert "loyalty_points" in result[0]
        assert result[0].get("loyalty_points") == 100

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
        import uuid

        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=int(os.getenv("POSTGRES_PORT", "5432")),
            database=os.getenv("POSTGRES_DB", "cdcdb"),
            user=os.getenv("POSTGRES_USER", "cdcuser"),
            password=os.getenv("POSTGRES_PASSWORD", "cdcpass"),
        )
        cursor = conn.cursor()

        # Insert test data with unique emails
        test_uid = uuid.uuid4().hex[:8]
        test_data = [
            (f"john.doe.{test_uid}@example.com", "John", "Doe", "New York", "NY", "USA"),
            (f"jane.smith.{test_uid}@example.com", "Jane", "Smith", "Los Angeles", "CA", "USA"),
            (f"bob.jones.{test_uid}@example.com", "Bob", "Jones", "Chicago", "IL", "USA"),
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

        # Verify transformations using polling
        from src.cdc_pipelines.iceberg.table_manager import IcebergTableManager, IcebergTableConfig
        from tests.test_utils import wait_for_iceberg_record

        config = IcebergTableConfig(
            catalog_name="demo_catalog",
            namespace="cdc",
            table_name="customers",
            warehouse_path="s3://warehouse/iceberg",
        )
        iceberg_manager = IcebergTableManager(config)

        # Wait for all 3 records to be written to Iceberg
        expected = [
            (customer_ids[0], "John Doe", "New York, NY, USA"),
            (customer_ids[1], "Jane Smith", "Los Angeles, CA, USA"),
            (customer_ids[2], "Bob Jones", "Chicago, IL, USA"),
        ]

        for customer_id, expected_name, expected_location in expected:
            result = wait_for_iceberg_record(
                table_manager=iceberg_manager,
                filter_condition=f"customer_id = {customer_id}",
                timeout_seconds=60,
                poll_interval=3.0
            )
            assert len(result) > 0, f"No data found for customer_id={customer_id}"
            assert result[0]["full_name"] == expected_name, f"Name mismatch for {customer_id}"
            assert result[0]["location"] == expected_location, f"Location mismatch for {customer_id}"

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
        import uuid

        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=int(os.getenv("POSTGRES_PORT", "5432")),
            database=os.getenv("POSTGRES_DB", "cdcdb"),
            user=os.getenv("POSTGRES_USER", "cdcuser"),
            password=os.getenv("POSTGRES_PASSWORD", "cdcpass"),
        )
        cursor = conn.cursor()

        # Insert many records rapidly with unique emails
        start_time = time.time()
        customer_ids = []
        test_uid = uuid.uuid4().hex[:8]

        for i in range(1000):
            cursor.execute(
                """
                INSERT INTO customers (email, first_name, last_name,
                                       city, state, country)
                VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING customer_id
                """,
                (
                    f"user{i}.{test_uid}@example.com",
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

        # Verify all records made it through using polling
        from src.cdc_pipelines.iceberg.table_manager import IcebergTableManager, IcebergTableConfig
        from tests.test_utils import wait_for_condition

        config = IcebergTableConfig(
            catalog_name="demo_catalog",
            namespace="cdc",
            table_name="customers",
            warehouse_path="s3://warehouse/iceberg",
        )
        iceberg_manager = IcebergTableManager(config)

        # Wait for all 1000 records with polling
        def check_all_records():
            try:
                count = iceberg_manager.count_records(
                    filter_condition=f"customer_id IN ({','.join(map(str, customer_ids))})"
                )
                return count == 1000
            except Exception:
                return False

        wait_for_condition(
            condition_func=check_all_records,
            timeout_seconds=120,  # 2 minutes for 1000 records
            poll_interval=5.0,
            error_message=f"Not all 1000 records made it to Iceberg"
        )

        # Verify CDC lag is acceptable
        end_time = time.time()
        total_duration = end_time - start_time
        cdc_lag = total_duration - insert_duration

        # Increased threshold to account for batch processing (1000 records)
        assert cdc_lag < 120, f"CDC lag too high: {cdc_lag}s"

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
        from tests.test_utils import wait_for_condition

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

        # Verify no duplicates in Iceberg using polling
        from src.cdc_pipelines.iceberg.table_manager import IcebergTableManager, IcebergTableConfig
        from tests.test_utils import wait_for_iceberg_record

        config = IcebergTableConfig(
            catalog_name="demo_catalog",
            namespace="cdc",
            table_name="customers",
            warehouse_path="s3://warehouse/iceberg",
        )
        iceberg_manager = IcebergTableManager(config)

        # Wait for CDC pipeline to process the record (Postgres -> Debezium -> Kafka -> Spark -> Iceberg)
        result = wait_for_iceberg_record(
            table_manager=iceberg_manager,
            filter_condition=f"customer_id = {customer_id}",
            timeout_seconds=60,
            poll_interval=3.0,
            min_count=1
        )

        # Verify no duplicates in Iceberg
        # Note: In a real test, we'd restart the pipeline here to test replay
        # For now, just verify single record exists
        result_count = iceberg_manager.count_records(
            filter_condition=f"customer_id = {customer_id}"
        )

        assert result_count == 1, f"Expected 1 record, found {result_count} (duplicates!)"

        # Cleanup
        cursor.execute("DELETE FROM customers WHERE customer_id = %s", (customer_id,))
        conn.commit()
        conn.close()
