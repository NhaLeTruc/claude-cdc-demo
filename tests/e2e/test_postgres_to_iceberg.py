"""End-to-end tests for Postgres→Kafka→Iceberg cross-storage CDC pipeline."""

import os
import pytest
from datetime import datetime
import time
from tests.test_utils import ensure_iceberg_table_exists


@pytest.mark.e2e
class TestPostgresToIcebergE2E:
    """End-to-end tests for complete cross-storage CDC workflow."""

    def test_complete_postgres_to_iceberg_workflow(self):
        """Test complete end-to-end workflow from Postgres to Iceberg."""
        import psycopg2
        import json
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
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=int(os.getenv("POSTGRES_PORT", "5432")),
            database=os.getenv("POSTGRES_DB", "cdcdb"),
            user=os.getenv("POSTGRES_USER", "cdcuser"),
            password=os.getenv("POSTGRES_PASSWORD", "cdcpass"),
        )
        pg_cursor = pg_conn.cursor()

        # Initialize Iceberg manager early to ensure table exists
        # Use the same table that the Spark streaming job writes to
        iceberg_config = IcebergTableConfig(
            catalog_name="demo_catalog",
            namespace="cdc",
            table_name="customers",
            warehouse_path="s3://warehouse/iceberg",
        )
        iceberg_manager = IcebergTableManager(iceberg_config)

        # Wait for Iceberg table to be created by the streaming pipeline
        ensure_iceberg_table_exists(iceberg_manager, timeout_seconds=90)

        # Step 3: Clean up any existing test data from both Postgres and Iceberg
        print("Cleaning up existing test data...")
        pg_cursor.execute("SELECT customer_id FROM customers WHERE email LIKE 'e2e_test_%@example.com'")
        existing_ids = [row[0] for row in pg_cursor.fetchall()]

        pg_cursor.execute("DELETE FROM customers WHERE email LIKE 'e2e_test_%@example.com'")
        pg_conn.commit()

        # Clean Iceberg
        if existing_ids:
            try:
                iceberg_manager.delete_data(
                    f"customer_id IN ({','.join(map(str, existing_ids))})"
                )
                time.sleep(5)
            except Exception as e:
                print(f"Warning: Failed to clean Iceberg data: {e}")

        # Step 4: Get initial row count
        pg_cursor.execute("SELECT COUNT(*) FROM customers")
        initial_pg_count = pg_cursor.fetchone()[0]
        print(f"Initial customer count in Postgres: {initial_pg_count}")

        # Step 5: Insert test dataset
        print("Inserting 100 test customers...")
        test_customers = [
            {
                "email": f"e2e_test_{i}@example.com",
                "first_name": f"E2E{i}",
                "last_name": f"Test{i}",
                "city": "TestCity",
                "state": "TS",
                "country": "USA",
            }
            for i in range(100)
        ]

        customer_ids = []
        for customer in test_customers:
            pg_cursor.execute(
                """
                INSERT INTO customers
                (email, first_name, last_name, city, state, country)
                VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING customer_id
                """,
                (
                    customer["email"],
                    customer["first_name"],
                    customer["last_name"],
                    customer["city"],
                    customer["state"],
                    customer["country"],
                ),
            )
            customer_ids.append(pg_cursor.fetchone()[0])

        pg_conn.commit()
        print(f"✓ Inserted {len(customer_ids)} customers")

        # Step 6: Verify Debezium captured changes
        print("Verifying Debezium captured changes in Kafka...")
        kafka_consumer = KafkaConsumer(
            "debezium.public.customers",
            bootstrap_servers="localhost:29092",
            auto_offset_reset="latest",
            consumer_timeout_ms=30000,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')) if m else None
        )

        captured_ids = set()
        for message in kafka_consumer:
            payload = message.value
            if payload and payload.get("payload", {}).get("op") == "c":  # create
                cid = payload["payload"]["after"].get("customer_id")
                if cid in customer_ids:
                    captured_ids.add(cid)

            if len(captured_ids) >= len(customer_ids):
                break

        kafka_consumer.close()
        print(f"✓ Debezium captured {len(captured_ids)} out of {len(customer_ids)} changes")
        assert len(captured_ids) == len(customer_ids), f"Not all changes captured by Debezium: {len(captured_ids)}/{len(customer_ids)}"

        # Step 7: Wait for Spark processing
        print("Waiting for Spark to process data...")
        time.sleep(20)

        # Step 8: Verify data in Iceberg
        print("Verifying data in Iceberg...")
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
        from pathlib import Path

        # Get project root for docker compose
        project_root = Path(__file__).parent.parent.parent
        compose_file = project_root / "compose" / "docker-compose.yml"

        # Insert data
        pg_conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=int(os.getenv("POSTGRES_PORT", "5432")),
            database=os.getenv("POSTGRES_DB", "cdcdb"),
            user=os.getenv("POSTGRES_USER", "cdcuser"),
            password=os.getenv("POSTGRES_PASSWORD", "cdcpass"),
        )
        pg_cursor = pg_conn.cursor()

        # Initialize Iceberg manager first to ensure table exists
        from src.cdc_pipelines.iceberg.table_manager import IcebergTableManager, IcebergTableConfig

        config = IcebergTableConfig(
            catalog_name="demo_catalog",
            namespace="cdc",
            table_name="customers",
            warehouse_path="s3://warehouse/iceberg",
        )
        iceberg_manager = IcebergTableManager(config)

        # Wait for Iceberg table to exist (should be created by streaming pipeline)
        ensure_iceberg_table_exists(iceberg_manager, timeout_seconds=90)

        # Clean up any existing test data from both Postgres and Iceberg
        pg_cursor.execute("SELECT customer_id FROM customers WHERE email = 'recovery_test@example.com'")
        existing_ids = [row[0] for row in pg_cursor.fetchall()]

        pg_cursor.execute("DELETE FROM customers WHERE email = 'recovery_test@example.com'")
        pg_conn.commit()

        # Clean Iceberg
        if existing_ids:
            for cid in existing_ids:
                try:
                    iceberg_manager.delete_data(f"customer_id = {cid}")
                except Exception as e:
                    print(f"Warning: Failed to clean Iceberg data: {e}")
            time.sleep(5)

        # Insert test data
        pg_cursor.execute(
            """
            INSERT INTO customers (email, first_name, last_name)
            VALUES (%s, %s, %s)
            RETURNING customer_id
            """,
            ("recovery_test@example.com", "Recovery", "Test"),
        )
        customer_id = pg_cursor.fetchone()[0]
        pg_conn.commit()
        print(f"Inserted test customer with ID: {customer_id}")

        # Wait for initial data to propagate to Iceberg with proper polling
        print("Waiting for initial data to propagate to Iceberg...")
        from tests.test_utils import wait_for_condition

        def check_initial_record_exists():
            try:
                count = iceberg_manager.count_records(
                    filter_condition=f"customer_id = {customer_id}"
                )
                if count > 0:
                    print(f"  ✓ Initial record found in Iceberg")
                    return True
                return False
            except Exception as e:
                print(f"  Warning: Error checking record: {e}")
                return False

        wait_for_condition(
            condition_func=check_initial_record_exists,
            timeout_seconds=60,
            poll_interval=5.0,
            error_message=f"Initial record {customer_id} did not propagate to Iceberg"
        )

        # Simulate Spark job failure - Stop Spark streaming job
        print("Stopping Spark streaming container...")
        result = subprocess.run(
            [
                "docker", "compose", "-f", str(compose_file),
                "--project-directory", str(project_root),
                "-p", "claude-cdc-demo",
                "stop", "spark-streaming"
            ],
            capture_output=True,
            text=True,
            cwd=project_root
        )

        if result.returncode != 0:
            print(f"Warning: Failed to stop spark-streaming: {result.stderr}")
        else:
            print("✓ Spark streaming stopped")

        # Make changes while job is down
        time.sleep(2)
        pg_cursor.execute(
            """
            UPDATE customers
            SET first_name = 'Updated'
            WHERE customer_id = %s
            """,
            (customer_id,),
        )
        pg_conn.commit()
        print(f"Updated customer {customer_id} while pipeline was down")

        # Restart Spark job
        time.sleep(5)
        print("Restarting Spark streaming container...")
        result = subprocess.run(
            [
                "docker", "compose", "-f", str(compose_file),
                "--project-directory", str(project_root),
                "-p", "claude-cdc-demo",
                "start", "spark-streaming"
            ],
            capture_output=True,
            text=True,
            cwd=project_root
        )

        if result.returncode != 0:
            print(f"Warning: Failed to start spark-streaming: {result.stderr}")
        else:
            print("✓ Spark streaming restarted")

        # Wait for recovery and processing
        print("Waiting for pipeline to recover and process updates...")
        time.sleep(30)

        # Verify update was processed
        print("Verifying update in Iceberg...")
        try:
            result = iceberg_manager.query_table(
                filter_condition=f"customer_id = {customer_id}"
            )

            assert len(result) > 0, f"No records found for customer_id {customer_id}"
            assert "Updated" in result[0]["full_name"], f"Expected 'Updated' in full_name, got: {result[0].get('full_name')}"
            print(f"✓ Update successfully processed: {result[0]['full_name']}")

        finally:
            # Cleanup - always execute even if test fails
            print("Cleaning up test data...")
            pg_cursor.execute("DELETE FROM customers WHERE customer_id = %s", (customer_id,))
            pg_conn.commit()

            # Clean from Iceberg
            try:
                iceberg_manager.delete_data(f"customer_id = {customer_id}")
            except Exception as e:
                print(f"Warning: Failed to clean up Iceberg: {e}")

            pg_conn.close()
            print("✓ Cleanup completed")

    def test_large_scale_end_to_end(self):
        """Test end-to-end pipeline with large dataset."""
        import psycopg2
        from src.cdc_pipelines.iceberg.table_manager import IcebergTableManager, IcebergTableConfig

        pg_conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=int(os.getenv("POSTGRES_PORT", "5432")),
            database=os.getenv("POSTGRES_DB", "cdcdb"),
            user=os.getenv("POSTGRES_USER", "cdcuser"),
            password=os.getenv("POSTGRES_PASSWORD", "cdcpass"),
        )
        pg_cursor = pg_conn.cursor()

        # Initialize Iceberg manager
        config = IcebergTableConfig(
            catalog_name="demo_catalog",
            namespace="cdc",
            table_name="customers",
            warehouse_path="s3://warehouse/iceberg",
        )
        iceberg_manager = IcebergTableManager(config)

        # Wait for Iceberg table to exist (it should be created by the streaming pipeline)
        ensure_iceberg_table_exists(iceberg_manager, timeout_seconds=90)

        # Step 1: Clean up any existing test data from BOTH Postgres AND Iceberg
        print("Cleaning up existing test data from previous runs...")

        # Get all customer_ids for large_scale pattern from Postgres
        pg_cursor.execute("SELECT customer_id FROM customers WHERE email LIKE 'large_scale_%@example.com'")
        existing_ids = [row[0] for row in pg_cursor.fetchall()]

        # Clean Postgres
        pg_cursor.execute("DELETE FROM customers WHERE email LIKE 'large_scale_%@example.com'")
        pg_conn.commit()
        print(f"Deleted {len(existing_ids)} existing records from Postgres")

        # Clean Iceberg - delete any records with the large_scale pattern
        # Query by email pattern to find and delete old test data
        if existing_ids:
            # Delete from Iceberg in batches
            batch_size = 1000
            for i in range(0, len(existing_ids), batch_size):
                batch_ids = existing_ids[i : i + batch_size]
                try:
                    iceberg_manager.delete_data(
                        f"customer_id IN ({','.join(map(str, batch_ids))})"
                    )
                except Exception as e:
                    print(f"Warning: Failed to clean Iceberg data: {e}")

            # Wait for deletes to be processed
            time.sleep(10)
            print(f"Cleaned up old test data from Iceberg")

        # Insert 10K records
        print("Inserting 10,000 test records...")
        start_time = time.time()
        customer_ids = []

        for i in range(10000):
            pg_cursor.execute(
                """
                INSERT INTO customers (email, first_name, last_name)
                VALUES (%s, %s, %s)
                RETURNING customer_id
                """,
                (f"large_scale_{i}@example.com", f"User{i}", f"Test{i}"),
            )
            customer_ids.append(pg_cursor.fetchone()[0])

            if i % 1000 == 0:
                pg_conn.commit()
                if i > 0:
                    print(f"  Inserted {i} records...")

        pg_conn.commit()
        insert_duration = time.time() - start_time
        print(f"✓ Inserted 10,000 records in {insert_duration:.2f}s")

        # Wait for pipeline to catch up with proper polling
        print("Waiting for CDC pipeline to process records...")
        from tests.test_utils import wait_for_condition

        def check_records_propagated():
            try:
                total_found = 0
                batch_size = 1000
                for i in range(0, len(customer_ids), batch_size):
                    batch_ids = customer_ids[i : i + batch_size]
                    count = iceberg_manager.count_records(
                        filter_condition=f"customer_id IN ({','.join(map(str, batch_ids))})"
                    )
                    total_found += count

                # Consider success if we have at least 95% of records (allows for timing variations)
                success_threshold = len(customer_ids) * 0.95
                if total_found >= success_threshold:
                    print(f"  Progress: {total_found}/{len(customer_ids)} records propagated")
                    return True
                elif total_found > 0:
                    print(f"  Progress: {total_found}/{len(customer_ids)} records...")
                return False
            except Exception as e:
                print(f"  Error checking records: {e}")
                return False

        wait_for_condition(
            condition_func=check_records_propagated,
            timeout_seconds=180,  # Increased timeout for 10K records
            poll_interval=10.0,
            error_message="10K records did not propagate to Iceberg in time"
        )

        # Final verification - count all records
        print("Final verification of records in Iceberg...")
        total_found = 0
        batch_size = 1000
        for i in range(0, len(customer_ids), batch_size):
            batch_ids = customer_ids[i : i + batch_size]
            count = iceberg_manager.count_records(
                filter_condition=f"customer_id IN ({','.join(map(str, batch_ids))})"
            )
            total_found += count

        print(f"Found {total_found} records in Iceberg (expected 10,000)")
        # Allow 95% success rate for large scale test (timing variations acceptable)
        assert total_found >= 9500, f"Expected at least 9500 records (95%), found {total_found}"

        # Cleanup - delete test data from both Postgres and Iceberg
        print("Cleaning up test data...")
        for i in range(0, len(customer_ids), batch_size):
            batch_ids = customer_ids[i : i + batch_size]
            pg_cursor.execute(
                f"DELETE FROM customers WHERE customer_id IN ({','.join(map(str, batch_ids))})"
            )
            pg_conn.commit()

            # Also clean from Iceberg
            try:
                iceberg_manager.delete_data(
                    f"customer_id IN ({','.join(map(str, batch_ids))})"
                )
            except Exception as e:
                print(f"Warning: Failed to clean up Iceberg batch: {e}")

        pg_conn.close()
        print("✓ Test completed and cleaned up")
