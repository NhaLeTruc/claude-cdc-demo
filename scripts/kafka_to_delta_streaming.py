#!/usr/bin/env python3
"""
Spark Structured Streaming job for Kafka to Delta Lake CDC pipeline.

This module implements a Spark Structured Streaming application that:
1. Consumes Debezium CDC events from Kafka
2. Applies transformations (name concatenation, location derivation)
3. Writes to Delta Lake table with Change Data Feed enabled
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
    from delta import configure_spark_with_delta_pip
    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False


logger = logging.getLogger(__name__)


class KafkaToDeltaStreamingJob:
    """
    Spark Structured Streaming job for Kafka to Delta Lake CDC.

    Implements end-to-end streaming from Kafka (Debezium) to Delta Lake
    with transformations and exactly-once processing guarantees.
    """

    def __init__(
        self,
        kafka_bootstrap_servers: str,
        kafka_topic: str,
        delta_table_path: str,
        checkpoint_location: str,
        app_name: str = "CDC-Kafka-to-Delta",
    ):
        """
        Initialize Kafka to Delta Streaming Job.

        Args:
            kafka_bootstrap_servers: Kafka bootstrap servers
            kafka_topic: Kafka topic to consume
            delta_table_path: Delta Lake table path
            checkpoint_location: Checkpoint directory for streaming state
            app_name: Spark application name

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
        self.delta_table_path = delta_table_path
        self.checkpoint_location = checkpoint_location
        self.app_name = app_name

        self.spark: Optional[SparkSession] = None

        logger.info(
            f"Initialized KafkaToDeltaStreamingJob: "
            f"{kafka_topic} → {delta_table_path}"
        )

    def create_spark_session(self) -> SparkSession:
        """
        Create Spark session with Delta Lake configurations.

        Returns:
            Configured SparkSession
        """
        builder = (
            SparkSession.builder
            .appName(self.app_name)
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog"
            )
            .config("spark.sql.streaming.checkpointLocation", self.checkpoint_location)
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
            .master("local[*]")
        )

        # configure_spark_with_delta_pip automatically adds Delta packages
        # We need to add Kafka package separately
        builder = configure_spark_with_delta_pip(builder)
        builder = builder.config("spark.jars.packages",
                                "io.delta:delta-spark_2.12:3.3.2,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0")
        spark = builder.getOrCreate()

        logger.info("Created Spark session with Delta Lake support")
        return spark

    def define_debezium_schema(self) -> StructType:
        """
        Define Debezium CDC event schema for customers table.

        Returns:
            StructType representing Debezium event structure
        """
        # Debezium payload schema for customers
        before_after_schema = StructType([
            StructField("customer_id", LongType(), True),
            StructField("email", StringType(), True),
            StructField("first_name", StringType(), True),
            StructField("last_name", StringType(), True),
            StructField("phone", StringType(), True),
            StructField("address_line1", StringType(), True),
            StructField("city", StringType(), True),
            StructField("state", StringType(), True),
            StructField("postal_code", StringType(), True),
            StructField("country", StringType(), True),
            StructField("registration_date", TimestampType(), True),
            StructField("last_updated", TimestampType(), True),
            StructField("is_active", BooleanType(), True),
            StructField("customer_tier", StringType(), True),
            StructField("lifetime_value", DoubleType(), True),
        ])

        # Source metadata schema
        source_schema = StructType([
            StructField("version", StringType(), True),
            StructField("connector", StringType(), True),
            StructField("name", StringType(), True),
            StructField("ts_ms", LongType(), True),
            StructField("db", StringType(), True),
            StructField("schema", StringType(), True),
            StructField("table", StringType(), True),
            StructField("lsn", LongType(), True),
        ])

        # Full Debezium envelope
        debezium_schema = StructType([
            StructField("before", before_after_schema, True),
            StructField("after", before_after_schema, True),
            StructField("source", source_schema, True),
            StructField("op", StringType(), True),
            StructField("ts_ms", LongType(), True),
        ])

        return debezium_schema

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
        Parse Debezium JSON events.

        Args:
            df: Raw Kafka DataFrame

        Returns:
            Parsed DataFrame with Debezium fields
        """
        debezium_schema = self.define_debezium_schema()

        # Parse JSON value
        parsed_df = df.select(
            col("key").cast("string").alias("kafka_key"),
            from_json(col("value").cast("string"), debezium_schema).alias("payload"),
            col("topic"),
            col("partition"),
            col("offset"),
            col("timestamp").alias("kafka_timestamp")
        )

        # Flatten payload
        flattened_df = parsed_df.select(
            col("kafka_key"),
            col("payload.before").alias("before"),
            col("payload.after").alias("after"),
            col("payload.source").alias("source"),
            col("payload.op").alias("operation"),
            col("payload.ts_ms").alias("event_timestamp_ms"),
            col("topic"),
            col("partition"),
            col("offset"),
            col("kafka_timestamp")
        )

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
            df: Parsed DataFrame

        Returns:
            Transformed DataFrame ready for Delta Lake
        """
        # Skip DELETE operations (retain only active records)
        df = df.filter(col("operation") != "d")

        # Transform from 'after' state
        transformed_df = df.select(
            col("after.customer_id").alias("customer_id"),
            col("after.email").alias("email"),

            # Transformation 1: Concatenate name
            concat_ws(" ",
                col("after.first_name"),
                col("after.last_name")
            ).alias("full_name"),

            # Transformation 2: Derive location
            concat_ws(", ",
                col("after.city"),
                col("after.state"),
                col("after.country")
            ).alias("location"),

            col("after.customer_tier").alias("customer_tier"),
            col("after.lifetime_value").alias("lifetime_value"),
            col("after.registration_date").alias("registration_date"),
            col("after.is_active").alias("is_active"),

            # Placeholder for join enrichment
            lit(0).alias("total_orders"),

            # Metadata columns
            current_timestamp().alias("_ingestion_timestamp"),
            lit("postgres_cdc").alias("_source_system"),

            # Map operation
            expr("""
                CASE
                    WHEN operation = 'c' THEN 'INSERT'
                    WHEN operation = 'u' THEN 'UPDATE'
                    WHEN operation = 'd' THEN 'DELETE'
                    WHEN operation = 'r' THEN 'SNAPSHOT'
                    ELSE 'UNKNOWN'
                END
            """).alias("_cdc_operation"),

            # Pipeline metadata
            lit("postgres_to_delta").alias("_pipeline_id"),
            current_timestamp().alias("_processed_timestamp"),

            # Original CDC metadata
            col("event_timestamp_ms").alias("_event_timestamp_ms"),
            col("source.lsn").alias("_source_lsn"),
            col("kafka_timestamp").alias("_kafka_timestamp"),
        )

        return transformed_df

    def write_to_delta(self, df: DataFrame) -> None:
        """
        Write transformed data to Delta Lake table with CDF enabled.

        Uses Spark Structured Streaming with:
        - Append mode (insert only, no updates/deletes)
        - Checkpoint for exactly-once semantics
        - Change Data Feed enabled

        Args:
            df: Transformed DataFrame
        """
        query = (
            df.writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", self.checkpoint_location)
            .option("delta.enableChangeDataFeed", "true")
            .trigger(processingTime="10 seconds")
            .start(self.delta_table_path)
        )

        logger.info(f"Started streaming to Delta Lake table: {self.delta_table_path}")
        logger.info(f"Checkpoint location: {self.checkpoint_location}")
        logger.info(f"Change Data Feed: ENABLED")

        # Wait for termination (blocking)
        query.awaitTermination()

    def run(self) -> None:
        """
        Run the streaming job end-to-end.

        This is the main entry point for the Spark job.
        """
        logger.info("Starting Kafka to Delta Lake streaming job")

        # Create Spark session
        self.spark = self.create_spark_session()

        # Read from Kafka
        kafka_df = self.read_from_kafka()

        # Parse Debezium events
        parsed_df = self.parse_debezium_events(kafka_df)

        # Apply transformations
        transformed_df = self.apply_transformations(parsed_df)

        # Write to Delta Lake
        self.write_to_delta(transformed_df)

        logger.info("Kafka to Delta Lake streaming job completed")


def main():
    """
    Main entry point for Spark job submission.

    Configuration is read from environment variables or command-line args.
    """
    # Configuration from environment or defaults
    kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    kafka_topic = os.getenv("KAFKA_TOPIC", "postgres.public.customers")
    delta_table_path = os.getenv("DELTA_TABLE_PATH", "/tmp/delta-cdc/customers")
    checkpoint_location = os.getenv("CHECKPOINT_LOCATION", "/tmp/spark-checkpoints/kafka-to-delta")

    # Allow command-line override
    if len(sys.argv) > 1:
        kafka_bootstrap_servers = sys.argv[1]
    if len(sys.argv) > 2:
        kafka_topic = sys.argv[2]
    if len(sys.argv) > 3:
        delta_table_path = sys.argv[3]

    logger.info("=" * 80)
    logger.info("Kafka to Delta Lake Streaming Job Configuration:")
    logger.info(f"  Kafka Bootstrap Servers: {kafka_bootstrap_servers}")
    logger.info(f"  Kafka Topic: {kafka_topic}")
    logger.info(f"  Delta Table Path: {delta_table_path}")
    logger.info(f"  Checkpoint Location: {checkpoint_location}")
    logger.info("=" * 80)

    # Create and run job
    job = KafkaToDeltaStreamingJob(
        kafka_bootstrap_servers=kafka_bootstrap_servers,
        kafka_topic=kafka_topic,
        delta_table_path=delta_table_path,
        checkpoint_location=checkpoint_location,
    )

    job.run()


if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    main()
