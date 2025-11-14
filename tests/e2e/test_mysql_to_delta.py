"""End-to-end tests for MySQL to DeltaLake CDC pipeline."""

import pytest
import time
import os
from tests.test_utils import ensure_delta_table_exists, read_delta_table_with_retry, wait_for_condition


@pytest.fixture
def cdc_pipeline(baseline_mysql_products):
    """
    Provide MySQL CDC pipeline.

    Depends on baseline_mysql_products to ensure infrastructure is ready
    and Delta table is initialized before starting the pipeline.
    """
    from src.cdc_pipelines.mysql.pipeline import MySQLCDCPipeline

    delta_table_path = baseline_mysql_products['delta_table_path']

    pipeline = MySQLCDCPipeline(
        pipeline_name="mysql_e2e_test",
        kafka_bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"),
        kafka_topic="debezium.cdcdb.products",  # Default MySQL CDC topic
        delta_table_path=delta_table_path,
        primary_key="product_id",
    )
    yield pipeline
    pipeline.stop()


@pytest.mark.e2e
class TestMySQLToDeltaE2E:
    """End-to-end tests for complete MySQL to DeltaLake pipeline."""

    def test_complete_pipeline_insert(self, e2e_mysql_connection, baseline_mysql_products, cdc_pipeline, e2e_spark_session):
        """Test complete pipeline: MySQL INSERT → Kafka → DeltaLake."""
        delta_table_path = baseline_mysql_products['delta_table_path']
        cursor = e2e_mysql_connection.cursor()

        # Clean up test data
        cursor.execute("DELETE FROM products WHERE product_id >= 9000")
        e2e_mysql_connection.commit()

        # Start CDC pipeline in background
        import threading

        pipeline_thread = threading.Thread(
            target=cdc_pipeline.start, kwargs={"batch_size": 10, "poll_timeout_ms": 1000}
        )
        pipeline_thread.daemon = True
        pipeline_thread.start()

        time.sleep(5)  # Let pipeline initialize

        # Insert record in MySQL
        cursor.execute(
            """
            INSERT INTO products (product_id, product_name, category, price, stock_quantity)
            VALUES (9000, 'E2E Test Product', 'Test Category', 299.99, 150)
            """
        )
        e2e_mysql_connection.commit()

        # Wait for record to appear in Delta using proper polling
        def check_product_in_delta():
            try:
                df = e2e_spark_session.read.format("delta").load(delta_table_path)
                result = df.filter("product_id = 9000").collect()
                return len(result) > 0
            except Exception:
                return False

        wait_for_condition(
            condition_func=check_product_in_delta,
            timeout_seconds=60,
            poll_interval=2.0,
            error_message="Product 9000 did not appear in Delta Lake"
        )

        # Verify in DeltaLake
        delta_df = read_delta_table_with_retry(e2e_spark_session, delta_table_path)
        result = delta_df.filter("product_id = 9000").collect()

        assert len(result) == 1
        assert result[0]["product_name"] == "E2E Test Product"
        assert result[0]["price"] == 299.99
        assert result[0]["stock_quantity"] == 150
        assert result[0]["_cdc_operation"] == "INSERT"

        cursor.close()

    def test_baseline_data_exists(self, baseline_mysql_products, e2e_spark_session):
        """Test that baseline data was loaded successfully."""
        delta_table_path = baseline_mysql_products['delta_table_path']
        product_ids = baseline_mysql_products['product_ids']

        # Ensure table exists (it should from baseline fixture)
        ensure_delta_table_exists(e2e_spark_session, delta_table_path, timeout_seconds=30)

        # Read Delta table
        delta_df = read_delta_table_with_retry(e2e_spark_session, delta_table_path)

        # Check that some baseline products exist
        baseline_count = delta_df.filter("product_id >= 8000 AND product_id < 8100").count()

        # We should have at least some of the 100 baseline products
        assert baseline_count > 0, f"Expected baseline products but found {baseline_count}"
        print(f"✓ Found {baseline_count} baseline products in Delta Lake")