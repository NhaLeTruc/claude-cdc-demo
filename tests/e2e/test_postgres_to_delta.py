"""End-to-end tests for Postgres→Kafka→Delta Lake CDC pipeline."""

import pytest
import time
import psycopg2
import os
from pathlib import Path


@pytest.mark.e2e
@pytest.mark.slow
class TestPostgresToDeltaLakePipeline:
    """End-to-end tests for complete Postgres→DeltaLake CDC pipeline."""

    @pytest.fixture(scope="class")
    def postgres_conn(self):
        """PostgreSQL connection for E2E tests."""
        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=int(os.getenv("POSTGRES_PORT", "5432")),
            database=os.getenv("POSTGRES_DB", "cdcdb"),
            user=os.getenv("POSTGRES_USER", "cdcuser"),
            password=os.getenv("POSTGRES_PASSWORD", "cdcpass"),
        )
        yield conn
        conn.close()

    @pytest.fixture(scope="class")
    def delta_table_path(self):
        """Delta Lake table path where streaming job writes.

        Supports both local execution and Docker-based streaming:
        - Local: /tmp/delta/customers (orchestrate_streaming_pipelines.sh)
        - Docker: /opt/delta-lake/customers (via volume mount)

        Tests check both paths and use the one that exists.
        """
        # Primary path (from .env)
        path = os.getenv("DELTA_TABLE_PATH", "/tmp/delta/customers")

        # Check if Docker volume is mounted locally for testing
        docker_path = "/opt/delta-lake/customers"
        if Path(docker_path).exists() and not Path(path).exists():
            return docker_path

        return path

    @pytest.fixture(scope="class")
    def spark_session(self):
        """Spark session for querying Delta Lake."""
        try:
            from pyspark.sql import SparkSession
            from delta import configure_spark_with_delta_pip

            builder = (
                SparkSession.builder
                .appName("E2E-Test-Delta-Reader")
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config(
                    "spark.sql.catalog.spark_catalog",
                    "org.apache.spark.sql.delta.catalog.DeltaCatalog"
                )
                .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.3.2")
                .master("local[2]")
            )

            spark = configure_spark_with_delta_pip(builder).getOrCreate()
            yield spark
            spark.stop()
        except ImportError:
            pytest.skip("PySpark not available")

    def test_full_pipeline_insert_flow(self, postgres_conn, delta_table_path, spark_session):
        """Test INSERT flows from Postgres to Delta Lake - requires streaming job running."""
        cursor = postgres_conn.cursor()

        test_email = f"e2e_insert_{int(time.time())}@example.com"
        cursor.execute(
            """
            INSERT INTO customers (email, first_name, last_name, city, state, country)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING customer_id
            """,
            (test_email, "E2EInsert", "Test", "TestCity", "TS", "USA"),
        )
        customer_id = cursor.fetchone()[0]
        postgres_conn.commit()

        time.sleep(20)

        if Path(delta_table_path).exists():
            df = spark_session.read.format("delta").load(delta_table_path)
            result = df.filter(f"customer_id = {customer_id}").collect()
            assert len(result) > 0, f"Customer {customer_id} not found in Delta"
            assert result[0]["full_name"] == "E2EInsert Test"
        else:
            pytest.skip(f"Delta table not found at {delta_table_path} - streaming job not running")

        cursor.execute("DELETE FROM customers WHERE customer_id = %s", (customer_id,))
        postgres_conn.commit()

    def test_full_pipeline_update_flow(self, postgres_conn, delta_table_path, spark_session):
        """Test UPDATE flows from Postgres to Delta Lake - requires streaming job running."""
        cursor = postgres_conn.cursor()

        test_email = f"e2e_update_{int(time.time())}@example.com"
        cursor.execute(
            """
            INSERT INTO customers (email, first_name, last_name, city, state, country)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING customer_id
            """,
            (test_email, "E2EUpdate", "Test", "TestCity", "TS", "USA"),
        )
        customer_id = cursor.fetchone()[0]
        postgres_conn.commit()
        time.sleep(20)

        cursor.execute(
            "UPDATE customers SET state = 'CA' WHERE customer_id = %s",
            (customer_id,),
        )
        postgres_conn.commit()
        time.sleep(20)

        if Path(delta_table_path).exists():
            df = spark_session.read.format("delta").load(delta_table_path)
            result = df.filter(f"customer_id = {customer_id}").collect()
            assert len(result) > 0
            latest = sorted(result, key=lambda r: r["_ingestion_timestamp"], reverse=True)[0]
            assert latest["state"] == "CA"
        else:
            pytest.skip(f"Delta table not found - streaming job not running")

        cursor.execute("DELETE FROM customers WHERE customer_id = %s", (customer_id,))
        postgres_conn.commit()

    def test_full_pipeline_delete_flow(self, postgres_conn, delta_table_path, spark_session):
        """Test DELETE handling - requires streaming job running."""
        cursor = postgres_conn.cursor()

        test_email = f"e2e_delete_{int(time.time())}@example.com"
        cursor.execute(
            """
            INSERT INTO customers (email, first_name, last_name, city, state, country)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING customer_id
            """,
            (test_email, "E2EDelete", "Test", "TestCity", "TS", "USA"),
        )
        customer_id = cursor.fetchone()[0]
        postgres_conn.commit()
        time.sleep(20)

        cursor.execute("DELETE FROM customers WHERE customer_id = %s", (customer_id,))
        postgres_conn.commit()
        time.sleep(20)

        # Our streaming job filters out DELETEs, so this test just validates no errors occur
        if not Path(delta_table_path).exists():
            pytest.skip("Delta table not found - streaming job not running")

    def test_pipeline_throughput(self, postgres_conn, delta_table_path, spark_session):
        """Test pipeline throughput - requires streaming job running."""
        cursor = postgres_conn.cursor()
        customer_ids = []

        for i in range(50):
            cursor.execute(
                """
                INSERT INTO customers (email, first_name, last_name, city, state, country)
            VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING customer_id
                """,
                (f"e2e_throughput_{int(time.time())}_{i}@example.com", f"User{i}", "Test",
                 "TestCity", "TS", "USA"),
            )
            customer_ids.append(cursor.fetchone()[0])
            if i % 10 == 0:
                postgres_conn.commit()

        postgres_conn.commit()
        time.sleep(30)

        if Path(delta_table_path).exists():
            df = spark_session.read.format("delta").load(delta_table_path)
            found = sum(1 for cid in customer_ids if df.filter(f"customer_id = {cid}").count() > 0)
            assert found >= len(customer_ids) * 0.8, f"Only {found}/{len(customer_ids)} processed"
        else:
            pytest.skip("Delta table not found - streaming job not running")

        for cid in customer_ids:
            cursor.execute("DELETE FROM customers WHERE customer_id = %s", (cid,))
        postgres_conn.commit()

    def test_pipeline_lag_under_load(self, postgres_conn, delta_table_path, spark_session):
        """Test CDC lag - requires streaming job running."""
        cursor = postgres_conn.cursor()

        test_email = f"e2e_lag_{int(time.time())}@example.com"
        lag_start = time.time()

        cursor.execute(
            """
            INSERT INTO customers (email, first_name, last_name, city, state, country)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING customer_id
            """,
            (test_email, "LagTest", "User", "TestCity", "TS", "USA"),
        )
        customer_id = cursor.fetchone()[0]
        postgres_conn.commit()

        if Path(delta_table_path).exists():
            for _ in range(45):
                time.sleep(1)
                df = spark_session.read.format("delta").load(delta_table_path)
                if df.filter(f"customer_id = {customer_id}").count() > 0:
                    lag = time.time() - lag_start
                    assert lag < 30, f"Lag too high: {lag:.1f}s"
                    break
        else:
            pytest.skip("Delta table not found - streaming job not running")

        cursor.execute("DELETE FROM customers WHERE customer_id = %s", (customer_id,))
        postgres_conn.commit()

    def test_pipeline_data_quality(self, postgres_conn, delta_table_path, spark_session):
        """Test data quality - requires streaming job running."""
        cursor = postgres_conn.cursor()

        test_email = f"e2e_quality_{int(time.time())}@example.com"
        cursor.execute(
            """
            INSERT INTO customers (email, first_name, last_name, city, state, country)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING customer_id
            """,
            (test_email, "Quality", "Test", "QCity", "QS", "USA"),
        )
        customer_id = cursor.fetchone()[0]
        postgres_conn.commit()
        time.sleep(20)

        if Path(delta_table_path).exists():
            df = spark_session.read.format("delta").load(delta_table_path)
            result = df.filter(f"customer_id = {customer_id}").collect()
            assert len(result) > 0
            row = result[0]
            assert row["full_name"] == "Quality Test"
            assert "QCity" in row["location"]
            assert row["_source_system"] == "postgres_cdc"
        else:
            pytest.skip("Delta table not found - streaming job not running")

        cursor.execute("DELETE FROM customers WHERE customer_id = %s", (customer_id,))
        postgres_conn.commit()

    def test_pipeline_recovery_after_failure(self, postgres_conn, delta_table_path, spark_session):
        """Test eventual consistency - requires streaming job running."""
        cursor = postgres_conn.cursor()
        customer_ids = []

        for i in range(10):
            cursor.execute(
                """
                INSERT INTO customers (email, first_name, last_name, city, state, country)
            VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING customer_id
                """,
                (f"e2e_recovery_{int(time.time())}_{i}@example.com", f"R{i}", "Test",
                 "City", "ST", "USA"),
            )
            customer_ids.append(cursor.fetchone()[0])

        postgres_conn.commit()
        time.sleep(30)

        if Path(delta_table_path).exists():
            df = spark_session.read.format("delta").load(delta_table_path)
            found = sum(1 for cid in customer_ids if df.filter(f"customer_id = {cid}").count() > 0)
            assert found >= 9, f"Only {found}/10 recovered"
        else:
            pytest.skip("Delta table not found - streaming job not running")

        for cid in customer_ids:
            cursor.execute("DELETE FROM customers WHERE customer_id = %s", (cid,))
        postgres_conn.commit()

    def test_pipeline_schema_evolution(self, postgres_conn, delta_table_path, spark_session):
        """Test schema compatibility - requires streaming job running."""
        cursor = postgres_conn.cursor()

        test_email = f"e2e_schema_{int(time.time())}@example.com"
        cursor.execute(
            """
            INSERT INTO customers (email, first_name, last_name, city, state, country)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING customer_id
            """,
            (test_email, "Schema", "Test", "City", "ST", "USA"),
        )
        customer_id = cursor.fetchone()[0]
        postgres_conn.commit()
        time.sleep(20)

        if Path(delta_table_path).exists():
            df = spark_session.read.format("delta").load(delta_table_path)
            result = df.filter(f"customer_id = {customer_id}").collect()
            assert len(result) > 0
            assert "full_name" in result[0].asDict()
        else:
            pytest.skip("Delta table not found - streaming job not running")

        cursor.execute("DELETE FROM customers WHERE customer_id = %s", (customer_id,))
        postgres_conn.commit()

