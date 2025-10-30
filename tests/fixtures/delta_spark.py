"""Delta Lake with Spark test fixtures and utilities."""

import logging
from typing import Optional

import pytest

logger = logging.getLogger(__name__)

# Check PySpark availability
try:
    from pyspark.sql import SparkSession
    from delta import configure_spark_with_delta_pip
    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False
    SparkSession = None


from tests.fixtures.config import SparkConfig, DeltaLakeConfig


class DeltaSparkManager:
    """Manager for Delta Lake Spark sessions in testing.

    This class provides utilities for creating and managing Spark sessions
    configured for Delta Lake with Change Data Feed (CDF) support.
    """

    def __init__(
        self,
        spark_config: Optional[SparkConfig] = None,
        delta_config: Optional[DeltaLakeConfig] = None,
    ):
        """Initialize Delta Spark manager.

        Args:
            spark_config: Spark configuration (uses defaults if not provided)
            delta_config: Delta Lake configuration (uses defaults if not provided)
        """
        self.spark_config = spark_config or SparkConfig()
        self.delta_config = delta_config or DeltaLakeConfig()
        self._spark: Optional[SparkSession] = None

    def get_spark_session(self) -> SparkSession:
        """Get or create a Spark session configured for Delta Lake.

        Returns:
            SparkSession configured for Delta Lake with CDF
        """
        if not PYSPARK_AVAILABLE:
            raise ImportError("PySpark is not installed. Install with: poetry add pyspark")

        if self._spark is None:
            logger.info("Creating Spark session for Delta Lake testing")

            # Build Spark session with Delta Lake support
            builder = (
                SparkSession.builder.appName(self.spark_config.app_name)
                .master(self.spark_config.master_url)
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config(
                    "spark.sql.catalog.spark_catalog",
                    "org.apache.spark.sql.delta.catalog.DeltaCatalog",
                )
                .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.3.2")
                # Enable CDF by default
                .config(
                    "spark.databricks.delta.properties.defaults.enableChangeDataFeed",
                    "true",
                )
                # Memory configuration
                .config("spark.driver.memory", self.spark_config.driver_memory)
                .config("spark.executor.memory", self.spark_config.executor_memory)
                # S3A configuration for MinIO
                .config("spark.hadoop.fs.s3a.endpoint", self.spark_config.s3_endpoint)
                .config(
                    "spark.hadoop.fs.s3a.access.key",
                    self.spark_config.aws_access_key_id,
                )
                .config(
                    "spark.hadoop.fs.s3a.secret.key",
                    self.spark_config.aws_secret_access_key,
                )
                .config("spark.hadoop.fs.s3a.path.style.access", "true")
                .config(
                    "spark.hadoop.fs.s3a.impl",
                    "org.apache.hadoop.fs.s3a.S3AFileSystem",
                )
                .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
                .config(
                    "spark.hadoop.fs.s3a.aws.credentials.provider",
                    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
                )
                # Warehouse directory
                .config("spark.sql.warehouse.dir", self.spark_config.warehouse_dir)
            )

            # Configure Delta Lake extensions
            self._spark = configure_spark_with_delta_pip(builder).getOrCreate()

            logger.info(
                f"Spark session created: {self._spark.sparkContext.applicationId}"
            )

        return self._spark

    def stop_spark_session(self) -> None:
        """Stop the Spark session and clean up resources."""
        if self._spark is not None:
            logger.info("Stopping Spark session")
            self._spark.stop()
            self._spark = None

    def is_delta_available(self) -> bool:
        """Check if Delta Lake packages are available in Spark.

        Returns:
            True if Delta Lake is properly configured, False otherwise
        """
        if not PYSPARK_AVAILABLE:
            return False

        try:
            spark = self.get_spark_session()
            # Try to import Delta Lake
            spark.sparkContext._jvm.io.delta.VERSION  # type: ignore
            return True
        except Exception as e:
            logger.error(f"Delta Lake not available: {e}")
            return False

    def create_delta_table(
        self,
        df,
        table_path: str,
        mode: str = "overwrite",
        partition_by: Optional[list[str]] = None,
    ) -> None:
        """Create a Delta Lake table from a DataFrame.

        Args:
            df: Spark DataFrame to write
            table_path: S3A path for the Delta table
            mode: Write mode ('overwrite', 'append', 'error', 'ignore')
            partition_by: Optional list of columns to partition by
        """
        writer = df.write.format("delta").mode(mode)

        if partition_by:
            writer = writer.partitionBy(*partition_by)

        writer.save(table_path)
        logger.info(f"Created Delta table at {table_path}")

    def read_delta_table(self, table_path: str):
        """Read a Delta Lake table.

        Args:
            table_path: S3A path to the Delta table

        Returns:
            Spark DataFrame
        """
        spark = self.get_spark_session()
        return spark.read.format("delta").load(table_path)

    def read_delta_cdf(
        self,
        table_path: str,
        starting_version: Optional[int] = None,
        starting_timestamp: Optional[str] = None,
        ending_version: Optional[int] = None,
        ending_timestamp: Optional[str] = None,
    ):
        """Read Change Data Feed from a Delta table.

        Args:
            table_path: S3A path to the Delta table
            starting_version: Optional starting version for CDF
            starting_timestamp: Optional starting timestamp for CDF
            ending_version: Optional ending version for CDF
            ending_timestamp: Optional ending timestamp for CDF

        Returns:
            Spark DataFrame with CDF changes
        """
        spark = self.get_spark_session()
        reader = (
            spark.read.format("delta")
            .option("readChangeDataFeed", "true")
        )

        if starting_version is not None:
            reader = reader.option("startingVersion", starting_version)
        if starting_timestamp is not None:
            reader = reader.option("startingTimestamp", starting_timestamp)
        if ending_version is not None:
            reader = reader.option("endingVersion", ending_version)
        if ending_timestamp is not None:
            reader = reader.option("endingTimestamp", ending_timestamp)

        return reader.load(table_path)


def get_delta_spark_session(
    spark_config: Optional[SparkConfig] = None,
    delta_config: Optional[DeltaLakeConfig] = None,
) -> SparkSession:
    """Convenience function to get a Delta-enabled Spark session.

    Args:
        spark_config: Optional Spark configuration
        delta_config: Optional Delta Lake configuration

    Returns:
        SparkSession configured for Delta Lake
    """
    manager = DeltaSparkManager(spark_config, delta_config)
    return manager.get_spark_session()
