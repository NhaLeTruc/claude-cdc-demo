"""
Spark Structured Streaming job for Kafka to Iceberg CDC pipeline.

This module implements a Spark Structured Streaming application that:
1. Consumes Debezium CDC events from Kafka
2. Applies transformations (name concatenation, location derivation)
3. Writes to Iceberg table with exactly-once semantics
"""

import logging
from typing import Dict, Any, Optional

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


class SparkCDCStreamingJob:
    """
    Spark Structured Streaming job for cross-storage CDC.

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
    ):
        """
        Initialize Spark CDC Streaming Job.

        Args:
            kafka_bootstrap_servers: Kafka bootstrap servers
            kafka_topic: Kafka topic to consume
            iceberg_catalog: Iceberg catalog name
            iceberg_warehouse: Iceberg warehouse path
            iceberg_namespace: Iceberg namespace
            iceberg_table: Iceberg table name
            checkpoint_location: Checkpoint directory for streaming state

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

        self.spark: Optional[SparkSession] = None

        logger.info(
            f"Initialized SparkCDCStreamingJob: "
            f"{kafka_topic} → {iceberg_namespace}.{iceberg_table}"
        )

    def create_spark_session(self) -> SparkSession:
        """
        Create Spark session with Iceberg and Kafka configurations.

        Returns:
            Configured SparkSession
        """
        spark = (
            SparkSession.builder
            .appName("CDC-Kafka-to-Iceberg")
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.iceberg.type", "hadoop")
            .config("spark.sql.catalog.iceberg.warehouse", self.iceberg_warehouse)
            .config("spark.jars.packages",
                    "org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.4.0,"
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0")
            .config("spark.sql.streaming.checkpointLocation", self.checkpoint_location)
            .config("spark.sql.adaptive.enabled", "true")
            .getOrCreate()
        )

        logger.info("Created Spark session with Iceberg support")
        return spark

    def define_debezium_schema(self) -> StructType:
        """
        Define Debezium CDC event schema.

        Returns:
            StructType representing Debezium event structure
        """
        # Debezium payload schema
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

    def filter_customer_events(self, df: DataFrame) -> DataFrame:
        """
        Filter for customer table events only.

        Args:
            df: Parsed DataFrame

        Returns:
            Filtered DataFrame
        """
        return df.filter(col("source.table") == "customers")

    def apply_transformations(self, df: DataFrame) -> DataFrame:
        """
        Apply data transformations for analytics.

        Transformations:
        - Concatenate first_name + last_name → full_name
        - Derive location from city + state + country
        - Map operation codes
        - Add metadata columns

        Args:
            df: Filtered DataFrame

        Returns:
            Transformed DataFrame ready for Iceberg
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
            lit("postgres_to_iceberg").alias("_pipeline_id"),
            current_timestamp().alias("_processed_timestamp"),

            # Original CDC metadata
            col("event_timestamp_ms").alias("_event_timestamp_ms"),
            col("source.lsn").alias("_source_lsn"),
            col("kafka_timestamp").alias("_kafka_timestamp"),
        )

        return transformed_df

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

        query = (
            df.writeStream
            .format("iceberg")
            .outputMode("append")
            .option("checkpointLocation", self.checkpoint_location)
            .option("path", table_identifier)
            .option("fanout-enabled", "true")
            .trigger(processingTime="10 seconds")
            .start()
        )

        logger.info(f"Started streaming to Iceberg table: {table_identifier}")
        logger.info(f"Checkpoint location: {self.checkpoint_location}")

        # Wait for termination (blocking)
        query.awaitTermination()

    def run(self) -> None:
        """
        Run the streaming job end-to-end.

        This is the main entry point for the Spark job.
        """
        logger.info("Starting Spark CDC streaming job")

        # Create Spark session
        self.spark = self.create_spark_session()

        # Read from Kafka
        kafka_df = self.read_from_kafka()

        # Parse Debezium events
        parsed_df = self.parse_debezium_events(kafka_df)

        # Filter for customer events
        customer_df = self.filter_customer_events(parsed_df)

        # Apply transformations
        transformed_df = self.apply_transformations(customer_df)

        # Write to Iceberg
        self.write_to_iceberg(transformed_df)

        logger.info("Spark CDC streaming job completed")


def main():
    """
    Main entry point for Spark job submission.

    Configuration is read from environment variables or command-line args.
    """
    import os
    import sys

    # Configuration from environment or defaults
    kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    kafka_topic = os.getenv("KAFKA_TOPIC", "postgres.public.customers")
    iceberg_catalog = os.getenv("ICEBERG_CATALOG", "iceberg")
    iceberg_warehouse = os.getenv("ICEBERG_WAREHOUSE", "/data/iceberg/warehouse")
    iceberg_namespace = os.getenv("ICEBERG_NAMESPACE", "analytics")
    iceberg_table = os.getenv("ICEBERG_TABLE", "customers_analytics")
    checkpoint_location = os.getenv("CHECKPOINT_LOCATION", "/tmp/spark-checkpoints/postgres-to-iceberg")

    # Allow command-line override
    if len(sys.argv) > 1:
        kafka_bootstrap_servers = sys.argv[1]
    if len(sys.argv) > 2:
        kafka_topic = sys.argv[2]
    if len(sys.argv) > 3:
        iceberg_warehouse = sys.argv[3]

    logger.info("=" * 80)
    logger.info("Spark CDC Streaming Job Configuration:")
    logger.info(f"  Kafka Bootstrap Servers: {kafka_bootstrap_servers}")
    logger.info(f"  Kafka Topic: {kafka_topic}")
    logger.info(f"  Iceberg Catalog: {iceberg_catalog}")
    logger.info(f"  Iceberg Warehouse: {iceberg_warehouse}")
    logger.info(f"  Iceberg Namespace: {iceberg_namespace}")
    logger.info(f"  Iceberg Table: {iceberg_table}")
    logger.info(f"  Checkpoint Location: {checkpoint_location}")
    logger.info("=" * 80)

    # Create and run job
    job = SparkCDCStreamingJob(
        kafka_bootstrap_servers=kafka_bootstrap_servers,
        kafka_topic=kafka_topic,
        iceberg_catalog=iceberg_catalog,
        iceberg_warehouse=iceberg_warehouse,
        iceberg_namespace=iceberg_namespace,
        iceberg_table=iceberg_table,
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
