#!/usr/bin/env python3
"""
Spark Structured Streaming job for Kafka to Iceberg CDC pipeline.

This module implements a Spark Structured Streaming application that:
1. Consumes Debezium CDC events from Kafka
2. Applies transformations (name concatenation, location derivation)
3. Writes to Iceberg table with exactly-once semantics
"""

import logging
import os
import sys
from typing import Optional

try:
    from pyspark.sql import SparkSession, DataFrame
    from pyspark.sql.functions import (
        col, concat_ws, lit, current_timestamp, from_json, expr
    )
    from pyspark.sql.types import (
        StructType, StructField, StringType, LongType,
        DoubleType, BooleanType, TimestampType
    )
    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False


logger = logging.getLogger(__name__)


class KafkaToIcebergStreamingJob:
    """
    Spark Structured Streaming job for Kafka to Iceberg CDC.

    Implements end-to-end streaming from Kafka (Debezium) to Iceberg
    with transformations and exactly-once processing guarantees.
    """

    def __init__(
        self,
        kafka_bootstrap_servers: str,
        kafka_topic: str,
        iceberg_catalog: str,
        iceberg_warehouse: str,
        iceberg_namespace: str,
        iceberg_table: str,
        checkpoint_location: str,
        app_name: str = "CDC-Kafka-to-Iceberg",
        trigger_interval: str = "10 seconds",
    ):
        """
        Initialize Kafka to Iceberg Streaming Job.

        Args:
            kafka_bootstrap_servers: Kafka bootstrap servers
            kafka_topic: Kafka topic to consume
            iceberg_catalog: Iceberg catalog name
            iceberg_warehouse: Iceberg warehouse path
            iceberg_namespace: Iceberg namespace
            iceberg_table: Iceberg table name
            checkpoint_location: Checkpoint directory for streaming state
            app_name: Spark application name
            trigger_interval: Spark micro-batch trigger interval (default: "10 seconds")

        Raises:
            ImportError: If PySpark is not installed
        """
        if not SPARK_AVAILABLE:
            raise ImportError(
                "PySpark is not installed. "
                "Install it with: pip install pyspark"
            )

        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.kafka_topic = kafka_topic
        self.iceberg_catalog = iceberg_catalog
        self.iceberg_warehouse = iceberg_warehouse
        self.iceberg_namespace = iceberg_namespace
        self.iceberg_table = iceberg_table
        self.checkpoint_location = checkpoint_location
        self.app_name = app_name
        self.trigger_interval = trigger_interval

        self.spark: Optional[SparkSession] = None

        logger.info(
            f"Initialized KafkaToIcebergStreamingJob: "
            f"{kafka_topic} → {iceberg_namespace}.{iceberg_table} "
            f"(trigger: {trigger_interval})"
        )

    def create_spark_session(self) -> SparkSession:
        """
        Create Spark session with Iceberg and Kafka configurations.

        Returns:
            Configured SparkSession
        """
        # Get MinIO/S3 configuration from environment
        s3_endpoint = os.getenv("S3_ENDPOINT", "http://minio:9000")
        s3_access_key = os.getenv("S3_ACCESS_KEY", "minioadmin")
        s3_secret_key = os.getenv("S3_SECRET_KEY", "minioadmin")

        spark = (
            SparkSession.builder
            .appName(self.app_name)
            # Override Delta Lake extensions with Iceberg
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            # Keep spark_catalog as-is (don't override to avoid conflicts)
            # Instead, use a separate catalog for Iceberg - use REST catalog for integration tests
            .config(f"spark.sql.catalog.{self.iceberg_catalog}", "org.apache.iceberg.spark.SparkCatalog")
            .config(f"spark.sql.catalog.{self.iceberg_catalog}.catalog-impl", "org.apache.iceberg.rest.RESTCatalog")
            .config(f"spark.sql.catalog.{self.iceberg_catalog}.uri", "http://iceberg-rest:8181")
            .config(f"spark.sql.catalog.{self.iceberg_catalog}.warehouse", self.iceberg_warehouse)
            # S3/MinIO configuration for Iceberg
            .config(f"spark.sql.catalog.{self.iceberg_catalog}.s3.endpoint", s3_endpoint)
            .config(f"spark.sql.catalog.{self.iceberg_catalog}.s3.access-key-id", s3_access_key)
            .config(f"spark.sql.catalog.{self.iceberg_catalog}.s3.secret-access-key", s3_secret_key)
            .config(f"spark.sql.catalog.{self.iceberg_catalog}.s3.path-style-access", "true")
            # Hadoop S3A configuration (for underlying file operations)
            .config("spark.hadoop.fs.s3a.endpoint", s3_endpoint)
            .config("spark.hadoop.fs.s3a.access.key", s3_access_key)
            .config("spark.hadoop.fs.s3a.secret.key", s3_secret_key)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            # Don't override jars.packages if Delta is already loaded
            # .config("spark.jars.packages",
            #         "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,"
            #         "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
            # Streaming and performance
            .config("spark.sql.streaming.checkpointLocation", self.checkpoint_location)
            .config("spark.sql.adaptive.enabled", "true")
            .master("local[*]")
            .getOrCreate()
        )

        logger.info("Created Spark session with Iceberg and S3/MinIO support")
        return spark

    def define_debezium_schema(self) -> StructType:
        """
        Define Debezium CDC event schema for customers table (unwrapped format).

        This schema expects events from Debezium with ExtractNewRecordState transform,
        which produces flattened records with CDC metadata as additional fields.

        Returns:
            StructType representing unwrapped Debezium event structure
        """
        # Unwrapped Debezium CDC event schema (after ExtractNewRecordState transform)
        # The actual data fields + CDC metadata fields prefixed with __
        unwrapped_schema = StructType([
            # Customer table fields
            StructField("customer_id", LongType(), True),
            StructField("email", StringType(), True),
            StructField("first_name", StringType(), True),
            StructField("last_name", StringType(), True),
            StructField("phone", StringType(), True),
            StructField("address", StringType(), True),
            StructField("city", StringType(), True),
            StructField("state", StringType(), True),
            StructField("zip_code", StringType(), True),
            StructField("country", StringType(), True),
            StructField("created_at", LongType(), True),  # Microseconds since epoch
            StructField("updated_at", LongType(), True),  # Microseconds since epoch
            StructField("loyalty_points", LongType(), True),  # For schema evolution tests

            # CDC metadata fields (added by ExtractNewRecordState with add.fields)
            StructField("__deleted", StringType(), True),
            StructField("__op", StringType(), True),  # c=create, u=update, d=delete, r=read(snapshot)
            StructField("__db", StringType(), True),
            StructField("__table", StringType(), True),
            StructField("__ts_ms", LongType(), True),
            StructField("__lsn", LongType(), True),
        ])

        return unwrapped_schema

    def read_from_kafka(self) -> DataFrame:
        """
        Read CDC events from Kafka.

        Returns:
            Streaming DataFrame with Kafka messages
        """
        df = (
            self.spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers)
            .option("subscribe", self.kafka_topic)
            .option("startingOffsets", "earliest")
            .option("maxOffsetsPerTrigger", 1000)
            .option("failOnDataLoss", "false")
            .load()
        )

        logger.info(f"Reading from Kafka topic: {self.kafka_topic}")
        return df

    def parse_debezium_events(self, df: DataFrame) -> DataFrame:
        """
        Parse unwrapped Debezium JSON events.

        Args:
            df: Raw Kafka DataFrame

        Returns:
            Parsed DataFrame with Debezium fields
        """
        debezium_schema = self.define_debezium_schema()

        # Parse JSON value (already unwrapped by ExtractNewRecordState)
        parsed_df = df.select(
            col("key").cast("string").alias("kafka_key"),
            from_json(col("value").cast("string"), debezium_schema).alias("payload"),
            col("topic"),
            col("partition"),
            col("offset"),
            col("timestamp").alias("kafka_timestamp")
        )

        # Flatten payload - since it's already unwrapped, just extract fields directly
        flattened_df = parsed_df.select(
            col("kafka_key"),
            col("payload.*"),  # Expand all payload fields
            col("topic"),
            col("partition"),
            col("offset"),
            col("kafka_timestamp")
        )

        logger.info(f"Parsed Debezium events. Schema: {flattened_df.schema}")

        return flattened_df

    def apply_transformations(self, df: DataFrame) -> DataFrame:
        """
        Apply data transformations for analytics.

        Transformations:
        - Concatenate first_name + last_name → full_name
        - Derive location from city + state + country
        - Map operation codes
        - Add metadata columns

        Args:
            df: Parsed DataFrame (unwrapped format)

        Returns:
            Transformed DataFrame ready for Iceberg
        """
        # Check which CDC metadata fields are present
        has_deleted = "__deleted" in df.columns
        has_op = "__op" in df.columns

        logger.info(f"CDC metadata fields present - __deleted: {has_deleted}, __op: {has_op}")

        # Skip DELETE operations (retain only active records)
        # Only filter if CDC metadata fields are present
        if has_deleted and has_op:
            logger.info("Filtering out DELETE operations using __deleted and __op fields")
            df = df.filter((col("__deleted") == "false") | (col("__op") != "d"))
        elif has_op:
            logger.info("Filtering out DELETE operations using __op field only")
            df = df.filter(col("__op") != "d")
        else:
            logger.warning("No CDC metadata fields found - treating all records as INSERT/UPDATE")

        # Build select columns list dynamically based on available fields
        select_columns = [
            col("customer_id"),
            col("email"),

            # Transformation 1: Concatenate name
            concat_ws(" ",
                col("first_name"),
                col("last_name")
            ).alias("full_name"),

            # Transformation 2: Derive location
            concat_ws(", ",
                col("city"),
                col("state"),
                col("country")
            ).alias("location"),

            # Optional fields (may be null for older records)
            col("loyalty_points"),

            # Placeholder for join enrichment
            lit(0).alias("total_orders"),

            # Metadata columns
            current_timestamp().alias("_ingestion_timestamp"),
            lit("postgres_cdc").alias("_source_system"),
        ]

        # Add CDC operation field (conditional based on __op availability)
        if has_op:
            select_columns.append(
                expr("""
                    CASE
                        WHEN __op = 'c' THEN 'INSERT'
                        WHEN __op = 'u' THEN 'UPDATE'
                        WHEN __op = 'd' THEN 'DELETE'
                        WHEN __op = 'r' THEN 'SNAPSHOT'
                        ELSE 'UNKNOWN'
                    END
                """).alias("_cdc_operation")
            )
        else:
            # Default to INSERT if no operation metadata
            select_columns.append(lit("INSERT").alias("_cdc_operation"))

        # Pipeline metadata
        select_columns.extend([
            lit("postgres_to_iceberg").alias("_pipeline_id"),
            current_timestamp().alias("_processed_timestamp"),
        ])

        # Add optional CDC metadata fields if present
        has_ts_ms = "__ts_ms" in df.columns
        has_lsn = "__lsn" in df.columns

        if has_ts_ms:
            select_columns.append(col("__ts_ms").alias("_event_timestamp_ms"))
        else:
            select_columns.append(lit(None).cast("long").alias("_event_timestamp_ms"))

        if has_lsn:
            select_columns.append(col("__lsn").alias("_source_lsn"))
        else:
            select_columns.append(lit(None).cast("long").alias("_source_lsn"))

        # Kafka timestamp should always be present
        select_columns.append(col("kafka_timestamp").alias("_kafka_timestamp"))

        # Transform from unwrapped CDC event fields
        transformed_df = df.select(*select_columns)

        logger.info(
            f"Applied transformations. Output schema: {transformed_df.schema}. "
            f"CDC metadata preserved: op={has_op}, ts_ms={has_ts_ms}, lsn={has_lsn}"
        )

        return transformed_df

    def create_iceberg_table_if_not_exists(self) -> None:
        """
        Create Iceberg table if it doesn't exist.

        Creates the namespace and table with appropriate schema.
        Uses CREATE TABLE IF NOT EXISTS to avoid recreating existing tables.
        """
        table_identifier = f"{self.iceberg_catalog}.{self.iceberg_namespace}.{self.iceberg_table}"

        # Create namespace if it doesn't exist
        try:
            self.spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {self.iceberg_catalog}.{self.iceberg_namespace}")
            logger.info(f"Ensured namespace exists: {self.iceberg_catalog}.{self.iceberg_namespace}")
        except Exception as e:
            logger.warning(f"Could not create namespace: {e}")

        # Use CREATE TABLE IF NOT EXISTS instead of checking first
        # This is atomic and won't recreate if table already exists
        try:
            create_table_sql = f"""
                CREATE TABLE IF NOT EXISTS {table_identifier} (
                    customer_id BIGINT,
                    email STRING,
                    full_name STRING,
                    location STRING,
                    loyalty_points BIGINT,
                    total_orders INT,
                    _ingestion_timestamp TIMESTAMP,
                    _source_system STRING,
                    _cdc_operation STRING,
                    _pipeline_id STRING,
                    _processed_timestamp TIMESTAMP,
                    _event_timestamp_ms BIGINT,
                    _source_lsn BIGINT,
                    _kafka_timestamp TIMESTAMP
                )
                USING iceberg
            """
            self.spark.sql(create_table_sql)
            logger.info(f"Ensured table exists: {table_identifier}")

            # Verify table exists
            try:
                table_desc = self.spark.sql(f"DESC TABLE {table_identifier}").collect()
                logger.info(f"Table {table_identifier} verified with {len(table_desc)} columns")
            except Exception as e:
                logger.error(f"Failed to verify table after creation: {e}")
        except Exception as e:
            logger.error(f"Failed to create table {table_identifier}: {e}")
            raise

    def write_to_iceberg(self, df: DataFrame) -> None:
        """
        Write transformed data to Iceberg table.

        Uses Spark Structured Streaming with:
        - Append mode (insert only, no updates/deletes)
        - Checkpoint for exactly-once semantics
        - Iceberg format v2

        Args:
            df: Transformed DataFrame
        """
        table_identifier = f"{self.iceberg_catalog}.{self.iceberg_namespace}.{self.iceberg_table}"

        logger.info(f"Starting write to Iceberg table: {table_identifier}")
        logger.info(f"Trigger interval: {self.trigger_interval}")
        logger.info(f"Checkpoint location: {self.checkpoint_location}")

        query = (
            df.writeStream
            .format("iceberg")
            .outputMode("append")
            .option("checkpointLocation", self.checkpoint_location)
            .option("path", table_identifier)
            .option("fanout-enabled", "true")
            .trigger(processingTime=self.trigger_interval)
            .start()
        )

        logger.info(f"Streaming query started successfully for table: {table_identifier}")
        logger.info(f"Query ID: {query.id}")
        logger.info(f"Query name: {query.name}")

        # Wait for termination (blocking)
        query.awaitTermination()

    def run(self) -> None:
        """
        Run the streaming job end-to-end.

        This is the main entry point for the Spark job.
        """
        logger.info("Starting Kafka to Iceberg streaming job")

        # Create Spark session
        self.spark = self.create_spark_session()

        # Create Iceberg table if needed
        self.create_iceberg_table_if_not_exists()

        # Read from Kafka
        kafka_df = self.read_from_kafka()

        # Parse Debezium events
        parsed_df = self.parse_debezium_events(kafka_df)

        # Apply transformations
        transformed_df = self.apply_transformations(parsed_df)

        # Write to Iceberg
        self.write_to_iceberg(transformed_df)

        logger.info("Kafka to Iceberg streaming job completed")


def main():
    """
    Main entry point for Spark job submission.

    Configuration is read from environment variables or command-line args.
    """
    # Configuration from environment or defaults
    kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    kafka_topic = os.getenv("KAFKA_TOPIC", "debezium.public.customers")
    iceberg_catalog = os.getenv("ICEBERG_CATALOG", "demo_catalog")
    iceberg_warehouse = os.getenv("ICEBERG_WAREHOUSE", "s3a://warehouse/iceberg")
    iceberg_namespace = os.getenv("ICEBERG_NAMESPACE", "cdc")
    iceberg_table = os.getenv("ICEBERG_TABLE", "customers")
    checkpoint_location = os.getenv("CHECKPOINT_LOCATION", "/tmp/spark-checkpoints/kafka-to-iceberg")
    trigger_interval = os.getenv("SPARK_TRIGGER_INTERVAL", "10 seconds")

    # Allow command-line override
    if len(sys.argv) > 1:
        kafka_bootstrap_servers = sys.argv[1]
    if len(sys.argv) > 2:
        kafka_topic = sys.argv[2]
    if len(sys.argv) > 3:
        iceberg_warehouse = sys.argv[3]

    logger.info("=" * 80)
    logger.info("Kafka to Iceberg Streaming Job Configuration:")
    logger.info(f"  Kafka Bootstrap Servers: {kafka_bootstrap_servers}")
    logger.info(f"  Kafka Topic: {kafka_topic}")
    logger.info(f"  Iceberg Catalog: {iceberg_catalog}")
    logger.info(f"  Iceberg Warehouse: {iceberg_warehouse}")
    logger.info(f"  Iceberg Namespace: {iceberg_namespace}")
    logger.info(f"  Iceberg Table: {iceberg_table}")
    logger.info(f"  Checkpoint Location: {checkpoint_location}")
    logger.info(f"  Trigger Interval: {trigger_interval}")
    logger.info("=" * 80)

    # Create and run job
    job = KafkaToIcebergStreamingJob(
        kafka_bootstrap_servers=kafka_bootstrap_servers,
        kafka_topic=kafka_topic,
        iceberg_catalog=iceberg_catalog,
        iceberg_warehouse=iceberg_warehouse,
        iceberg_namespace=iceberg_namespace,
        iceberg_table=iceberg_table,
        checkpoint_location=checkpoint_location,
        trigger_interval=trigger_interval,
    )

    job.run()


if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    main()
