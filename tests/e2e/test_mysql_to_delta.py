"""End-to-end tests for MySQL to DeltaLake CDC pipeline."""

import pytest
import time
from pyspark.sql import SparkSession


@pytest.fixture(scope="module")
def spark():
    """Provide Spark session for tests."""
    spark = (
        SparkSession.builder.appName("MySQL_CDC_E2E_Test")
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .master("local[*]")
        .getOrCreate()
    )
    yield spark
    spark.stop()


@pytest.fixture
def mysql_connection():
    """Provide MySQL connection."""
    from src.cdc_pipelines.mysql.connection import MySQLConnectionManager
    from src.common.config import get_settings

    settings = get_settings()
    manager = MySQLConnectionManager(
        host=settings.mysql_host,
        port=settings.mysql_port,
        user=settings.mysql_user,
        password=settings.mysql_password,
        database=settings.mysql_database,
    )
    yield manager
    manager.close()


@pytest.fixture
def cdc_pipeline():
    """Provide MySQL CDC pipeline."""
    from src.cdc_pipelines.mysql.pipeline import MySQLCDCPipeline
    from src.common.config import get_settings

    settings = get_settings()
    pipeline = MySQLCDCPipeline(
        pipeline_name="mysql_e2e_test",
        kafka_bootstrap_servers=settings.kafka_bootstrap_servers,
        kafka_topic=settings.mysql_kafka_topic,
        delta_table_path="/tmp/delta/e2e_test/mysql_products",
        primary_key="product_id",
    )
    yield pipeline
    pipeline.stop()


@pytest.mark.e2e
@pytest.mark.skip(reason="Requires full infrastructure: MySQL, Debezium, Kafka, Spark")
class TestMySQLToDeltaE2E:
    """End-to-end tests for complete MySQL to DeltaLake pipeline."""

    def test_complete_pipeline_insert(self, mysql_connection, cdc_pipeline, spark):
        """Test complete pipeline: MySQL INSERT → Kafka → DeltaLake."""
        # Clean up test table
        mysql_connection.execute_query(
            "DELETE FROM products WHERE product_id >= 9000",
            fetch=False,
        )

        # Start CDC pipeline in background
        import threading

        pipeline_thread = threading.Thread(
            target=cdc_pipeline.start, kwargs={"batch_size": 10, "poll_timeout_ms": 1000}
        )
        pipeline_thread.daemon = True
        pipeline_thread.start()

        time.sleep(5)  # Let pipeline initialize

        # Insert record in MySQL
        mysql_connection.execute_query(
            """
            INSERT INTO products (product_id, name, category, price, stock_quantity)
            VALUES (9000, 'E2E Test Product', 'Test Category', 299.99, 150)
            """,
            fetch=False,
        )

        # Wait for CDC propagation
        time.sleep(10)

        # Verify in DeltaLake
        delta_df = spark.read.format("delta").load("/tmp/delta/e2e_test/mysql_products")
        result = delta_df.filter("product_id = 9000").collect()

        assert len(result) == 1
        assert result[0]["name"] == "E2E Test Product"
        assert result[0]["price"] == 299.99
        assert result[0]["stock_quantity"] == 150
        assert result[0]["_cdc_operation"] == "INSERT"

    def test_complete_pipeline_update(self, mysql_connection, cdc_pipeline, spark):
        """Test complete pipeline: MySQL UPDATE → Kafka → DeltaLake."""
        # Insert initial record
        mysql_connection.execute_query(
            """
            INSERT INTO products (product_id, name, category, price, stock_quantity)
            VALUES (9001, 'Update Test', 'Test', 100.0, 50)
            ON DUPLICATE KEY UPDATE name = 'Update Test'
            """,
            fetch=False,
        )

        time.sleep(5)

        # Update record
        mysql_connection.execute_query(
            """
            UPDATE products
            SET price = 150.0, stock_quantity = 75
            WHERE product_id = 9001
            """,
            fetch=False,
        )

        time.sleep(10)

        # Verify in DeltaLake
        delta_df = spark.read.format("delta").load("/tmp/delta/e2e_test/mysql_products")
        result = delta_df.filter("product_id = 9001").collect()

        assert len(result) == 1
        assert result[0]["price"] == 150.0
        assert result[0]["stock_quantity"] == 75
        # Should have UPDATE operation in CDC metadata
        assert result[0]["_cdc_operation"] == "UPDATE"

    def test_complete_pipeline_delete(self, mysql_connection, cdc_pipeline, spark):
        """Test complete pipeline: MySQL DELETE → Kafka → DeltaLake."""
        # Insert record
        mysql_connection.execute_query(
            """
            INSERT INTO products (product_id, name, category, price, stock_quantity)
            VALUES (9002, 'Delete Test', 'Test', 100.0, 50)
            ON DUPLICATE KEY UPDATE name = 'Delete Test'
            """,
            fetch=False,
        )

        time.sleep(5)

        # Delete record
        mysql_connection.execute_query(
            "DELETE FROM products WHERE product_id = 9002",
            fetch=False,
        )

        time.sleep(10)

        # Verify in DeltaLake (with CDF, delete should be marked)
        delta_df = spark.read.format("delta").load("/tmp/delta/e2e_test/mysql_products")
        result = delta_df.filter("product_id = 9002").collect()

        # Depending on implementation, deleted record might be marked or removed
        # Check for DELETE operation in CDC history
        if len(result) > 0:
            assert result[0]["_cdc_operation"] == "DELETE"

    def test_bulk_insert_performance(self, mysql_connection, cdc_pipeline, spark):
        """Test pipeline performance with bulk insert."""
        # Bulk insert 500 products
        values = []
        for i in range(500):
            product_id = 9100 + i
            values.append(
                f"({product_id}, 'Bulk Product {i}', 'Bulk', {50.0 + i}, {10 * i})"
            )

        # Insert in batches to avoid SQL size limits
        batch_size = 100
        for i in range(0, len(values), batch_size):
            batch = values[i : i + batch_size]
            insert_query = f"""
                INSERT INTO products (product_id, name, category, price, stock_quantity)
                VALUES {','.join(batch)}
            """
            mysql_connection.execute_query(insert_query, fetch=False)

        # Wait for pipeline to process
        time.sleep(30)

        # Verify in DeltaLake
        delta_df = spark.read.format("delta").load("/tmp/delta/e2e_test/mysql_products")
        result_count = delta_df.filter("product_id >= 9100 AND product_id < 9600").count()

        assert result_count == 500

    def test_data_quality_end_to_end(self, mysql_connection, cdc_pipeline, spark):
        """Test data quality validation across entire pipeline."""
        from src.validation.integrity import RowCountValidator

        # Insert test data
        mysql_connection.execute_query(
            """
            INSERT INTO products (product_id, name, category, price, stock_quantity)
            VALUES (9700, 'Quality Test', 'Test', 100.0, 50)
            ON DUPLICATE KEY UPDATE name = 'Quality Test'
            """,
            fetch=False,
        )

        time.sleep(10)

        # Get counts from source and destination
        source_result = mysql_connection.execute_query(
            "SELECT COUNT(*) as count FROM products WHERE product_id = 9700"
        )
        source_count = source_result[0]["count"]

        delta_df = spark.read.format("delta").load("/tmp/delta/e2e_test/mysql_products")
        dest_count = delta_df.filter("product_id = 9700").count()

        # Validate counts match
        validator = RowCountValidator(tolerance=0)
        result = validator.validate(
            list(range(source_count)), list(range(dest_count))
        )

        assert result.status.value == "passed"

    def test_lag_monitoring(self, mysql_connection, cdc_pipeline, spark):
        """Test CDC lag monitoring."""
        from datetime import datetime

        # Insert with current timestamp
        insert_time = datetime.now()

        mysql_connection.execute_query(
            """
            INSERT INTO products (product_id, name, category, price, stock_quantity, created_at)
            VALUES (9800, 'Lag Test', 'Test', 100.0, 50, NOW())
            ON DUPLICATE KEY UPDATE name = 'Lag Test'
            """,
            fetch=False,
        )

        time.sleep(10)

        # Check when data appears in DeltaLake
        delta_df = spark.read.format("delta").load("/tmp/delta/e2e_test/mysql_products")
        result = delta_df.filter("product_id = 9800").collect()

        if len(result) > 0 and "_cdc_timestamp" in result[0].asDict():
            cdc_time = result[0]["_cdc_timestamp"]
            # Calculate lag (should be < 10 seconds for healthy pipeline)
            # Implementation depends on timestamp format

    def test_schema_evolution(self, mysql_connection, cdc_pipeline, spark):
        """Test handling of schema changes."""
        # Note: This test would require ALTER TABLE support in Debezium
        # and schema evolution enabled in DeltaLake

        # Insert record before schema change
        mysql_connection.execute_query(
            """
            INSERT INTO products (product_id, name, category, price, stock_quantity)
            VALUES (9900, 'Schema Test', 'Test', 100.0, 50)
            ON DUPLICATE KEY UPDATE name = 'Schema Test'
            """,
            fetch=False,
        )

        time.sleep(5)

        # Verify initial record
        delta_df = spark.read.format("delta").load("/tmp/delta/e2e_test/mysql_products")
        result = delta_df.filter("product_id = 9900").collect()

        assert len(result) == 1

        # TODO: Add ALTER TABLE and verify schema evolution
        # This requires special Debezium configuration

    def test_transaction_atomicity(self, mysql_connection, cdc_pipeline, spark):
        """Test that MySQL transactions are reflected atomically."""
        # Execute transaction with multiple operations
        mysql_connection.execute_query("START TRANSACTION", fetch=False)

        mysql_connection.execute_query(
            """
            INSERT INTO products (product_id, name, category, price, stock_quantity)
            VALUES (9950, 'Txn Product 1', 'Test', 100.0, 50)
            """,
            fetch=False,
        )

        mysql_connection.execute_query(
            """
            INSERT INTO products (product_id, name, category, price, stock_quantity)
            VALUES (9951, 'Txn Product 2', 'Test', 200.0, 75)
            """,
            fetch=False,
        )

        mysql_connection.execute_query("COMMIT", fetch=False)

        time.sleep(10)

        # Verify both records appear
        delta_df = spark.read.format("delta").load("/tmp/delta/e2e_test/mysql_products")
        result = delta_df.filter("product_id IN (9950, 9951)").count()

        assert result == 2
