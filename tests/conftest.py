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
    Create a Spark session for testing Delta Lake operations.
    Session-scoped to reuse across tests for performance.
    """
    if not SPARK_AVAILABLE:
        pytest.skip("PySpark not available")

    spark = (
        SparkSession.builder
        .appName("CDC-Demo-Tests")
        .master("spark://localhost:7077")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0")
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
        .config("spark.driver.memory", "1g")
        .config("spark.executor.memory", "1g")
        .getOrCreate()
    )

    yield spark

    spark.stop()


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
