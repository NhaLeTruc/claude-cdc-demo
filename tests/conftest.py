"""
Pytest configuration and shared fixtures for CDC demo tests.
"""

import os
import shutil
import uuid
from pathlib import Path
import pytest
import psycopg2
import pymysql
import httpx
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Try to import PySpark for Spark fixtures
try:
    from pyspark.sql import SparkSession
    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False

# Try to import PyIceberg for Iceberg fixtures
try:
    from pyiceberg.catalog import Catalog
    from tests.fixtures.iceberg_catalog import (
        IcebergCatalogManager,
        setup_test_namespace,
        cleanup_test_namespace,
    )
    ICEBERG_AVAILABLE = True
except ImportError:
    ICEBERG_AVAILABLE = False


@pytest.fixture(scope="session")
def postgres_credentials():
    """PostgreSQL connection credentials from environment."""
    return {
        "host": os.getenv("POSTGRES_HOST", "localhost"),
        "port": int(os.getenv("POSTGRES_PORT", "5432")),
        "database": os.getenv("POSTGRES_DB", "cdcdb"),
        "user": os.getenv("POSTGRES_USER", "cdcuser"),
        "password": os.getenv("POSTGRES_PASSWORD", "cdcpass"),
    }


@pytest.fixture(scope="session")
def mysql_credentials():
    """MySQL connection credentials from environment."""
    return {
        "host": os.getenv("MYSQL_HOST", "localhost"),
        "port": int(os.getenv("MYSQL_PORT", "3306")),
        "database": os.getenv("MYSQL_DB", "cdcdb"),
        "user": os.getenv("MYSQL_USER", "cdcuser"),
        "password": os.getenv("MYSQL_PASSWORD", "cdcpass"),
    }


@pytest.fixture
def postgres_connection(postgres_credentials):
    """Create PostgreSQL connection for testing."""
    conn = psycopg2.connect(**postgres_credentials)
    yield conn
    conn.close()


@pytest.fixture
def mysql_connection(mysql_credentials):
    """Create MySQL connection for testing."""
    conn = pymysql.connect(**mysql_credentials)
    yield conn
    conn.close()


# Spark and Delta Lake Fixtures
@pytest.fixture(scope="session")
def spark_session():
    """
    Create a Spark session for testing Delta Lake operations with CDF support.
    Session-scoped to reuse across tests for performance.
    Uses DeltaSparkManager for proper configuration.
    """
    if not SPARK_AVAILABLE:
        pytest.skip("PySpark not available")

    from tests.fixtures.delta_spark import DeltaSparkManager

    manager = DeltaSparkManager()
    spark = manager.get_spark_session()

    yield spark

    manager.stop_spark_session()


@pytest.fixture(scope="session")
def delta_spark():
    """
    Provide DeltaSparkManager for Delta Lake testing.
    Session-scoped for reuse across tests.
    """
    if not SPARK_AVAILABLE:
        pytest.skip("PySpark not available")

    from tests.fixtures.delta_spark import DeltaSparkManager

    manager = DeltaSparkManager()

    yield manager

    manager.stop_spark_session()


@pytest.fixture
def delta_table_path(tmp_path):
    """
    Provide a unique Delta table path for each test.
    Cleans up after test completes.
    """
    # Generate unique table name
    table_name = f"test_{uuid.uuid4().hex[:8]}"
    table_path = tmp_path / "delta-tables" / table_name
    table_path.mkdir(parents=True, exist_ok=True)

    yield str(table_path)

    # Cleanup
    if table_path.exists():
        shutil.rmtree(table_path, ignore_errors=True)


# AlertManager Fixtures
@pytest.fixture
def alertmanager_client():
    """
    HTTP client for AlertManager API.
    Provides helper methods for alert and silence operations.
    """
    base_url = os.getenv("ALERTMANAGER_URL", "http://localhost:9093")

    class AlertManagerClient:
        def __init__(self, base_url: str):
            self.base_url = base_url
            self.client = httpx.Client(base_url=base_url, timeout=10.0)
            self._test_alerts = []
            self._test_silences = []

        def post_alert(self, alert: dict):
            """Post an alert to AlertManager."""
            response = self.client.post("/api/v1/alerts", json=[alert])
            response.raise_for_status()
            self._test_alerts.append(alert)
            return response.json()

        def get_alerts(self):
            """Get all active alerts."""
            response = self.client.get("/api/v1/alerts")
            response.raise_for_status()
            return response.json()

        def post_silence(self, silence: dict):
            """Create a silence."""
            response = self.client.post("/api/v1/silences", json=silence)
            response.raise_for_status()
            data = response.json()
            if "silenceID" in data:
                self._test_silences.append(data["silenceID"])
            return data

        def delete_silence(self, silence_id: str):
            """Delete a silence."""
            response = self.client.delete(f"/api/v1/silence/{silence_id}")
            response.raise_for_status()
            return response.json()

        def get_status(self):
            """Get AlertManager status."""
            response = self.client.get("/api/v1/status")
            response.raise_for_status()
            return response.json()

        def cleanup(self):
            """Clean up test alerts and silences."""
            # Delete test silences
            for silence_id in self._test_silences:
                try:
                    self.delete_silence(silence_id)
                except Exception:
                    pass  # Ignore cleanup errors

            self.client.close()

    client = AlertManagerClient(base_url)

    yield client

    # Cleanup
    client.cleanup()


# Iceberg Catalog Fixtures
@pytest.fixture(scope="session")
def iceberg_catalog():
    """
    Create an Iceberg catalog for testing.
    Session-scoped to reuse across tests for performance.
    """
    if not ICEBERG_AVAILABLE:
        pytest.skip("PyIceberg not available")

    manager = IcebergCatalogManager()
    catalog = manager.get_catalog()

    yield catalog

    # No cleanup needed for session-scoped catalog


@pytest.fixture
def iceberg_namespace(iceberg_catalog):
    """
    Provide a clean Iceberg namespace for each test.
    Creates a unique namespace and cleans it up after the test.
    """
    if not ICEBERG_AVAILABLE:
        pytest.skip("PyIceberg not available")

    # Create unique namespace for this test
    namespace = f"test_{uuid.uuid4().hex[:8]}"
    manager = IcebergCatalogManager()

    # Create namespace
    manager.create_namespace(namespace, catalog=iceberg_catalog)

    yield namespace

    # Cleanup: drop all tables and namespace
    try:
        manager.cleanup_namespace(namespace, catalog=iceberg_catalog)
    except Exception:
        pass  # Ignore cleanup errors


@pytest.fixture(scope="session")
def iceberg_test_namespace():
    """
    Shared test namespace for Iceberg testing.
    Session-scoped for tests that can share a namespace.
    """
    if not ICEBERG_AVAILABLE:
        pytest.skip("PyIceberg not available")

    namespace = "cdc_test"
    catalog, ns = setup_test_namespace(namespace)

    yield ns

    # Cleanup after all tests
    cleanup_test_namespace(namespace)


@pytest.fixture(scope="session", autouse=True)
def clean_cdc_state():
    """
    Clean CDC pipeline state before test session starts.
    This ensures tests start with a clean slate:
    - Clears Postgres customers table and resets sequence
    - Clears Iceberg table
    - Resets Spark checkpoint to process from latest Kafka offset

    This prevents duplicate customer_ids from previous runs.
    """
    import subprocess
    import time

    try:
        # Step 1: Clear Postgres tables and reset sequences
        try:
            for table in ["customers", "schema_evolution_test"]:
                result = subprocess.run(
                    ["docker", "exec", "cdc-postgres", "psql", "-U", "cdcuser", "-d", "cdcdb", "-c",
                     f"TRUNCATE TABLE {table} RESTART IDENTITY CASCADE;"],
                    capture_output=True,
                    timeout=10,
                    text=True
                )
                if result.returncode == 0:
                    print(f"\n✓ Cleared Postgres {table} table and reset sequence")
                elif "does not exist" not in result.stderr:
                    print(f"\n⚠ Could not clear Postgres {table}: {result.stderr}")
        except Exception as e:
            print(f"\n⚠ Error clearing Postgres: {e}")

        # Step 2: Skip Iceberg table cleanup
        # NOTE: We don't delete the Iceberg table to preserve its metadata across test runs.
        # The Spark job uses CREATE TABLE IF NOT EXISTS, which will reuse existing table.
        # Postgres TRUNCATE will reset customer_id sequence, so tests won't interfere.
        print("✓ Skipping Iceberg table cleanup (table structure preserved)")

        # Step 3: Delete Kafka topics AND Kafka Connect offsets to remove CDC state
        try:
            # First delete the topics
            for topic in ["debezium.public.customers", "debezium.public.schema_evolution_test"]:
                result = subprocess.run(
                    ["docker", "exec", "cdc-kafka", "kafka-topics", "--bootstrap-server", "localhost:9092",
                     "--delete", "--topic", topic],
                    capture_output=True,
                    timeout=10,
                    text=True
                )
                if "marked for deletion" in result.stdout or result.returncode == 0:
                    print(f"✓ Deleted Kafka topic {topic}")
                elif "does not exist" not in result.stderr:
                    print(f"⚠ Could not delete Kafka topic {topic}: {result.stderr}")

            # Also delete Kafka Connect offset topic to reset Debezium's state
            subprocess.run(
                ["docker", "exec", "cdc-kafka", "kafka-topics", "--bootstrap-server", "localhost:9092",
                 "--delete", "--topic", "connect-offsets"],
                capture_output=True,
                timeout=10,
                check=False  # May not exist
            )
            subprocess.run(
                ["docker", "exec", "cdc-kafka", "kafka-topics", "--bootstrap-server", "localhost:9092",
                 "--delete", "--topic", "connect-status"],
                capture_output=True,
                timeout=10,
                check=False  # May not exist
            )

            time.sleep(3)  # Wait for topic deletion

            # Recreate data topics
            for topic in ["debezium.public.customers", "debezium.public.schema_evolution_test"]:
                subprocess.run(
                    ["docker", "exec", "cdc-kafka", "kafka-topics", "--bootstrap-server", "localhost:9092",
                     "--create", "--topic", topic, "--partitions", "1", "--replication-factor", "1"],
                    capture_output=True,
                    timeout=10,
                    check=False  # May already exist
                )
            print("✓ Recreated Kafka topics and reset Kafka Connect offsets")

            # Delete and recreate the Debezium connector to fully reset its state
            try:
                # Delete connector
                subprocess.run(
                    ["curl", "-X", "DELETE", "http://localhost:8083/connectors/postgres-cdc-connector"],
                    capture_output=True,
                    timeout=5,
                    check=False
                )
                time.sleep(2)

                # Recreate connector using the registration script
                # Use Docker container names instead of localhost
                connector_env = {
                    **os.environ,
                    "DEBEZIUM_HOST": "localhost",
                    "POSTGRES_HOST": "cdc-postgres",  # Docker container name
                    "POSTGRES_PORT": "5432",
                    "POSTGRES_USER": "cdcuser",
                    "POSTGRES_PASSWORD": "cdcpass",
                    "POSTGRES_DB": "cdcdb",
                }
                result = subprocess.run(
                    ["bash", "scripts/connectors/register-postgres-connector.sh"],
                    capture_output=True,
                    timeout=15,
                    text=True,
                    env=connector_env
                )
                if result.returncode == 0:
                    print("✓ Recreated Debezium connector with fresh state")
                else:
                    print(f"⚠ Could not recreate connector: {result.stderr}")

                time.sleep(5)  # Wait for connector to initialize
            except Exception as restart_err:
                print(f"⚠ Could not recreate Debezium connector: {restart_err}")
        except Exception as e:
            print(f"⚠ Error managing Kafka topics: {e}")

        # Step 4: Reset Spark checkpoint and restart
        try:
            subprocess.run(
                ["docker", "exec", "cdc-spark-streaming",
                 "rm", "-rf", "/tmp/spark-checkpoints/kafka-to-iceberg"],
                capture_output=True,
                timeout=5
            )

            subprocess.run(
                ["docker", "restart", "cdc-spark-streaming"],
                capture_output=True,
                timeout=30
            )

            print("✓ Reset Spark checkpoint and restarted container")
            print("⏳ Waiting for Spark to initialize...")

            # Wait for container to be healthy
            max_wait = 60
            wait_interval = 2
            elapsed = 0
            while elapsed < max_wait:
                result = subprocess.run(
                    ["docker", "inspect", "--format={{.State.Health.Status}}", "cdc-spark-streaming"],
                    capture_output=True,
                    text=True,
                    timeout=5
                )
                health_status = result.stdout.strip()
                if health_status == "healthy":
                    print(f"✓ Spark container healthy after {elapsed}s")
                    # Wait additional 10s for streaming query to fully start
                    time.sleep(10)
                    break
                time.sleep(wait_interval)
                elapsed += wait_interval
            else:
                print(f"⚠ Spark container not healthy after {max_wait}s, proceeding anyway...")
        except Exception as e:
            print(f"⚠ Could not reset Spark: {e}")

    except Exception as e:
        print(f"\n⚠ Error in CDC cleanup: {e}")

    yield

    # No cleanup needed after session
