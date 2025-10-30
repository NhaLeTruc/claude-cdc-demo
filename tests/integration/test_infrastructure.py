"""Integration tests for infrastructure services health and availability."""

import pytest
import httpx


class TestIcebergInfrastructure:
    """Test suite for Apache Iceberg REST Catalog infrastructure."""

    def test_iceberg_catalog_reachable(self):
        """Test that Iceberg REST Catalog is reachable and responding."""
        catalog_url = "http://localhost:8181"

        try:
            response = httpx.get(f"{catalog_url}/v1/config", timeout=10.0)
            assert response.status_code == 200, (
                f"Iceberg catalog returned status {response.status_code}"
            )

            # Verify response structure
            data = response.json()
            assert "defaults" in data or "overrides" in data, (
                "Iceberg catalog response missing expected fields"
            )

        except httpx.ConnectError as e:
            pytest.fail(
                f"Could not connect to Iceberg REST Catalog at {catalog_url}: {e}"
            )
        except httpx.TimeoutException as e:
            pytest.fail(
                f"Timeout connecting to Iceberg REST Catalog at {catalog_url}: {e}"
            )


class TestMinIOInfrastructure:
    """Test suite for MinIO S3-compatible storage infrastructure."""

    def test_minio_health_endpoint(self):
        """Test that MinIO health endpoint is accessible."""
        minio_url = "http://localhost:9000"

        try:
            response = httpx.get(
                f"{minio_url}/minio/health/live",
                timeout=10.0
            )
            assert response.status_code == 200, (
                f"MinIO health check returned status {response.status_code}"
            )

        except httpx.ConnectError as e:
            pytest.fail(f"Could not connect to MinIO at {minio_url}: {e}")
        except httpx.TimeoutException as e:
            pytest.fail(f"Timeout connecting to MinIO at {minio_url}: {e}")

    def test_minio_buckets_exist(self):
        """Test that required MinIO buckets were created."""
        from minio import Minio

        client = Minio(
            endpoint="localhost:9000",
            access_key="minioadmin",
            secret_key="minioadmin",
            secure=False
        )

        required_buckets = ["warehouse", "iceberg", "delta"]

        for bucket_name in required_buckets:
            assert client.bucket_exists(bucket_name), (
                f"Required bucket '{bucket_name}' does not exist"
            )


class TestSparkDeltaInfrastructure:
    """Test suite for Apache Spark with Delta Lake infrastructure."""

    def test_spark_master_reachable(self):
        """Test that Spark Master UI is accessible."""
        spark_url = "http://localhost:8080"

        try:
            response = httpx.get(spark_url, timeout=10.0)
            assert response.status_code == 200, (
                f"Spark Master UI returned status {response.status_code}"
            )

        except httpx.ConnectError as e:
            pytest.fail(f"Could not connect to Spark Master at {spark_url}: {e}")
        except httpx.TimeoutException as e:
            pytest.fail(f"Timeout connecting to Spark Master at {spark_url}: {e}")

    def test_spark_delta_packages_loaded(self):
        """Test that Spark session can load Delta Lake packages."""
        try:
            from pyspark.sql import SparkSession
        except ImportError:
            pytest.skip("PySpark not installed")

        from tests.fixtures.delta_spark import DeltaSparkManager

        # Create Spark session with Delta configuration
        manager = DeltaSparkManager()

        try:
            # This will fail if Delta packages can't be loaded
            spark = manager.get_spark_session()

            # Verify Delta Lake is available
            assert manager.is_delta_available(), (
                "Delta Lake packages not properly loaded in Spark"
            )

            # Verify Spark configuration
            conf = spark.sparkContext.getConf()
            assert "delta" in conf.get("spark.sql.extensions", "").lower(), (
                "Delta Lake extensions not configured"
            )

        finally:
            manager.stop_spark_session()
