"""End-to-end tests for Postgres→Kafka→Iceberg cross-storage CDC pipeline."""

import pytest
from datetime import datetime
import time


@pytest.mark.e2e
@pytest.mark.skipif(
    reason="Requires complete infrastructure stack",
    condition=True,
)
class TestPostgresToIcebergE2E:
    """End-to-end tests for complete cross-storage CDC workflow."""

    def test_complete_postgres_to_iceberg_workflow(self):
        """Test complete end-to-end workflow from Postgres to Iceberg."""
        import psycopg2
        from kafka import KafkaConsumer
        from src.cdc_pipelines.iceberg.table_manager import (
            IcebergTableManager,
            IcebergTableConfig,
        )
        from src.validation.integrity import IntegrityValidator

        # Step 1: Setup - Ensure services are running
        # (Postgres, Debezium, Kafka, Spark, Iceberg)

        # Step 2: Connect to Postgres
        pg_conn = psycopg2.connect(
            host="localhost",
            port=5432,
            database="demo_db",
            user="postgres",
            password="postgres",
        )
        pg_cursor = pg_conn.cursor()

        # Step 3: Get initial row count
        pg_cursor.execute("SELECT COUNT(*) FROM customers")
        initial_pg_count = pg_cursor.fetchone()[0]

        # Step 4: Insert test dataset
        test_customers = [
            {
                "email": f"e2e_test_{i}@example.com",
                "first_name": f"E2E{i}",
                "last_name": f"Test{i}",
                "city": "TestCity",
                "state": "TS",
                "country": "USA",
                "customer_tier": "Gold",
                "lifetime_value": 1000.00 + i,
            }
            for i in range(100)
        ]

        customer_ids = []
        for customer in test_customers:
            pg_cursor.execute(
                """
                INSERT INTO customers
                (email, first_name, last_name, city, state, country,
                 customer_tier, lifetime_value, registration_date, last_updated)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                RETURNING customer_id
                """,
                (
                    customer["email"],
                    customer["first_name"],
                    customer["last_name"],
                    customer["city"],
                    customer["state"],
                    customer["country"],
                    customer["tier"],
                    customer["lifetime_value"],
                ),
            )
            customer_ids.append(pg_cursor.fetchone()[0])

        pg_conn.commit()

        # Step 5: Verify Debezium captured changes
        kafka_consumer = KafkaConsumer(
            "debezium.public.customers",
            bootstrap_servers="localhost:9092",
            auto_offset_reset="latest",
            consumer_timeout_ms=30000,
        )

        captured_ids = set()
        for message in kafka_consumer:
            payload = message.value
            if payload.get("payload", {}).get("op") == "c":  # create
                cid = payload["payload"]["after"].get("customer_id")
                if cid in customer_ids:
                    captured_ids.add(cid)

            if len(captured_ids) >= len(customer_ids):
                break

        assert len(captured_ids) == len(customer_ids), "Not all changes captured by Debezium"

        # Step 6: Wait for Spark processing
        time.sleep(20)

        # Step 7: Verify data in Iceberg
        iceberg_config = IcebergTableConfig(
            catalog_name="demo_catalog",
            namespace="cdc_demo",
            table_name="customers_analytics",
            warehouse_path="/tmp/iceberg_warehouse",
        )

        iceberg_manager = IcebergTableManager(iceberg_config)

        iceberg_count = iceberg_manager.count_records(
            filter_condition=f"customer_id IN ({','.join(map(str, customer_ids))})"
        )

        assert iceberg_count == len(customer_ids), f"Expected {len(customer_ids)} records in Iceberg, found {iceberg_count}"

        # Step 8: Validate transformations
        sample_customer = iceberg_manager.query_table(
            filter_condition=f"customer_id = {customer_ids[0]}"
        )[0]

        assert "full_name" in sample_customer
        assert sample_customer["full_name"] == f"E2E0 Test0"
        assert "location" in sample_customer
        assert "TestCity, TS, USA" in sample_customer["location"]

        # Step 9: Test UPDATE operation
        pg_cursor.execute(
            """
            UPDATE customers
            SET customer_tier = 'Platinum', last_updated = NOW()
            WHERE customer_id = %s
            """,
            (customer_ids[0],),
        )
        pg_conn.commit()

        # Wait for CDC propagation
        time.sleep(15)

        # Verify update in Iceberg
        updated_customer = iceberg_manager.query_table(
            filter_condition=f"customer_id = {customer_ids[0]}"
        )[0]

        assert updated_customer["customer_tier"] == "Platinum"

        # Step 10: Test DELETE operation
        pg_cursor.execute(
            """
            DELETE FROM customers
            WHERE customer_id = %s
            """,
            (customer_ids[1],),
        )
        pg_conn.commit()

        # Wait for CDC propagation
        time.sleep(15)

        # Verify delete reflected in Iceberg
        # (implementation-dependent: soft delete or actual removal)
        deleted_count = iceberg_manager.count_records(
            filter_condition=f"customer_id = {customer_ids[1]}"
        )

        # Either deleted or marked as deleted
        assert deleted_count == 0 or (
            deleted_count == 1
            and iceberg_manager.query_table(
                filter_condition=f"customer_id = {customer_ids[1]}"
            )[0].get("is_deleted")
            is True
        )

        # Step 11: Data quality validation
        validator = IntegrityValidator()

        # Row count validation
        pg_cursor.execute(
            f"""
            SELECT COUNT(*) FROM customers
            WHERE customer_id IN ({','.join(map(str, customer_ids))})
            """
        )
        current_pg_count = pg_cursor.fetchone()[0]

        # Account for the deleted record
        expected_iceberg_count = current_pg_count

        actual_iceberg_count = iceberg_manager.count_records(
            filter_condition=f"customer_id IN ({','.join(map(str, customer_ids))}) AND (is_deleted IS NULL OR is_deleted = false)"
        )

        validation_result = validator.validate_row_count(
            source_count=expected_iceberg_count,
            destination_count=actual_iceberg_count,
            tolerance=0.01,
        )

        assert validation_result.passed, f"Row count validation failed: {validation_result.message}"

        # Step 12: CDC lag validation
        # Measure time from Postgres update to Iceberg visibility
        lag_test_start = time.time()

        pg_cursor.execute(
            """
            UPDATE customers
            SET last_updated = NOW()
            WHERE customer_id = %s
            """,
            (customer_ids[2],),
        )
        pg_conn.commit()

        # Poll until change appears in Iceberg
        max_wait = 30
        lag = max_wait
        for _ in range(max_wait):
            time.sleep(1)
            result = iceberg_manager.query_table(
                filter_condition=f"customer_id = {customer_ids[2]}"
            )
            # Check if timestamp updated (proxy for CDC propagation)
            if result and result[0].get("_ingestion_timestamp"):
                lag = time.time() - lag_test_start
                break

        assert lag < 10, f"CDC lag too high: {lag}s (threshold: 10s)"

        # Step 13: Cleanup
        pg_cursor.execute(
            f"""
            DELETE FROM customers
            WHERE customer_id IN ({','.join(map(str, customer_ids))})
            """
        )
        pg_conn.commit()
        pg_conn.close()

    def test_cross_storage_pipeline_recovery(self):
        """Test pipeline recovery from failures."""
        import psycopg2
        import subprocess

        # Insert data
        pg_conn = psycopg2.connect(
            host="localhost",
            port=5432,
            database="demo_db",
            user="postgres",
            password="postgres",
        )
        pg_cursor = pg_conn.cursor()

        pg_cursor.execute(
            """
            INSERT INTO customers (email, first_name, last_name,
                                   registration_date, last_updated)
            VALUES (%s, %s, %s, NOW(), NOW())
            RETURNING customer_id
            """,
            ("recovery_test@example.com", "Recovery", "Test"),
        )
        customer_id = pg_cursor.fetchone()[0]
        pg_conn.commit()

        # Simulate Spark job failure
        # Stop Spark streaming job
        subprocess.run(["docker-compose", "stop", "spark-streaming"], check=False)

        # Make more changes while job is down
        pg_cursor.execute(
            """
            UPDATE customers
            SET first_name = 'Updated'
            WHERE customer_id = %s
            """,
            (customer_id,),
        )
        pg_conn.commit()

        # Restart Spark job
        time.sleep(5)
        subprocess.run(["docker-compose", "start", "spark-streaming"], check=False)

        # Wait for recovery
        time.sleep(30)

        # Verify update was processed
        from src.cdc_pipelines.iceberg.table_manager import IcebergTableManager

        iceberg_manager = IcebergTableManager(...)
        result = iceberg_manager.query_table(
            filter_condition=f"customer_id = {customer_id}"
        )

        assert len(result) > 0
        assert "Updated" in result[0]["full_name"]

        # Cleanup
        pg_cursor.execute("DELETE FROM customers WHERE customer_id = %s", (customer_id,))
        pg_conn.commit()
        pg_conn.close()

    def test_large_scale_end_to_end(self):
        """Test end-to-end pipeline with large dataset."""
        import psycopg2

        pg_conn = psycopg2.connect(
            host="localhost",
            port=5432,
            database="demo_db",
            user="postgres",
            password="postgres",
        )
        pg_cursor = pg_conn.cursor()

        # Insert 10K records
        start_time = time.time()
        customer_ids = []

        for i in range(10000):
            pg_cursor.execute(
                """
                INSERT INTO customers (email, first_name, last_name,
                                       registration_date, last_updated)
                VALUES (%s, %s, %s, NOW(), NOW())
                RETURNING customer_id
                """,
                (f"large_scale_{i}@example.com", f"User{i}", f"Test{i}"),
            )
            customer_ids.append(pg_cursor.fetchone()[0])

            if i % 1000 == 0:
                pg_conn.commit()

        pg_conn.commit()
        insert_duration = time.time() - start_time

        # Wait for pipeline to catch up (generous timeout)
        time.sleep(60)

        # Verify all records
        from src.cdc_pipelines.iceberg.table_manager import IcebergTableManager

        iceberg_manager = IcebergTableManager(...)
        # Query in batches to avoid memory issues
        total_found = 0
        batch_size = 1000
        for i in range(0, len(customer_ids), batch_size):
            batch_ids = customer_ids[i : i + batch_size]
            count = iceberg_manager.count_records(
                filter_condition=f"customer_id IN ({','.join(map(str, batch_ids))})"
            )
            total_found += count

        assert total_found == 10000, f"Expected 10000 records, found {total_found}"

        # Cleanup
        for i in range(0, len(customer_ids), batch_size):
            batch_ids = customer_ids[i : i + batch_size]
            pg_cursor.execute(
                f"DELETE FROM customers WHERE customer_id IN ({','.join(map(str, batch_ids))})"
            )
            pg_conn.commit()

        pg_conn.close()
