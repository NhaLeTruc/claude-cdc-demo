#!/usr/bin/env python3
"""
Lightweight Kafka to Iceberg consumer for CDC events.

This is a simple consumer that reads unwrapped Debezium events from Kafka
and writes them to Iceberg tables using PyIceberg. It avoids Spark configuration
conflicts and provides a straightforward CDC pipeline.
"""

import json
import logging
import os
import signal
import sys
import time
from datetime import datetime
from typing import Dict, Any, Optional

from kafka import KafkaConsumer
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField, LongType, StringType, TimestampType, IntegerType
)

logger = logging.getLogger(__name__)


class KafkaToIcebergConsumer:
    """
    Kafka consumer that writes CDC events to Iceberg tables.

    Handles unwrapped Debezium events and applies transformations.
    """

    def __init__(
        self,
        kafka_bootstrap_servers: str,
        kafka_topic: str,
        kafka_group_id: str,
        catalog_name: str,
        namespace: str,
        table_name: str,
        warehouse_path: str,
        s3_endpoint: str,
        s3_access_key: str,
        s3_secret_key: str,
    ):
        """Initialize Kafka to Iceberg consumer."""
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.kafka_topic = kafka_topic
        self.kafka_group_id = kafka_group_id
        self.catalog_name = catalog_name
        self.namespace = namespace
        self.table_name = table_name
        self.warehouse_path = warehouse_path
        self.s3_endpoint = s3_endpoint
        self.s3_access_key = s3_access_key
        self.s3_secret_key = s3_secret_key

        self.consumer: Optional[KafkaConsumer] = None
        self.catalog = None
        self.table = None
        self.running = True
        self.records_processed = 0

        # Setup signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully."""
        logger.info(f"Received signal {signum}, shutting down...")
        self.running = False

    def connect_kafka(self):
        """Connect to Kafka."""
        logger.info(f"Connecting to Kafka: {self.kafka_bootstrap_servers}")
        logger.info(f"Topic: {self.kafka_topic}, Group: {self.kafka_group_id}")

        self.consumer = KafkaConsumer(
            self.kafka_topic,
            bootstrap_servers=self.kafka_bootstrap_servers,
            group_id=self.kafka_group_id,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')) if m else None,
            max_poll_records=100,
        )

        logger.info("Connected to Kafka successfully")

    def connect_iceberg(self):
        """Connect to Iceberg catalog and get/create table."""
        logger.info(f"Connecting to Iceberg catalog: {self.catalog_name}")

        # Load catalog with S3 configuration (using filesystem/hadoop catalog)
        self.catalog = load_catalog(
            self.catalog_name,
            **{
                "type": "hadoop",
                "warehouse": self.warehouse_path,
                "s3.endpoint": self.s3_endpoint,
                "s3.access-key-id": self.s3_access_key,
                "s3.secret-access-key": self.s3_secret_key,
                "s3.path-style-access": "true",
            }
        )

        table_identifier = f"{self.namespace}.{self.table_name}"

        # Try to load existing table or create new one
        try:
            self.table = self.catalog.load_table(table_identifier)
            logger.info(f"Loaded existing Iceberg table: {table_identifier}")
        except Exception as e:
            logger.info(f"Table doesn't exist, creating: {table_identifier}")
            self._create_table()

    def _create_table(self):
        """Create Iceberg table with proper schema."""
        schema = Schema(
            NestedField(1, "customer_id", LongType(), required=True),
            NestedField(2, "email", StringType(), required=False),
            NestedField(3, "full_name", StringType(), required=False),
            NestedField(4, "location", StringType(), required=False),
            NestedField(5, "loyalty_points", LongType(), required=False),
            NestedField(6, "total_orders", IntegerType(), required=False),
            NestedField(7, "_ingestion_timestamp", TimestampType(), required=False),
            NestedField(8, "_source_system", StringType(), required=False),
            NestedField(9, "_cdc_operation", StringType(), required=False),
            NestedField(10, "_pipeline_id", StringType(), required=False),
            NestedField(11, "_processed_timestamp", TimestampType(), required=False),
            NestedField(12, "_event_timestamp_ms", LongType(), required=False),
            NestedField(13, "_source_lsn", LongType(), required=False),
            NestedField(14, "_kafka_timestamp", TimestampType(), required=False),
        )

        table_identifier = f"{self.namespace}.{self.table_name}"

        # Create namespace if it doesn't exist
        try:
            self.catalog.create_namespace(self.namespace)
        except Exception:
            pass  # Namespace might already exist

        self.table = self.catalog.create_table(
            table_identifier,
            schema=schema,
            location=f"{self.warehouse_path}/{self.namespace}/{self.table_name}",
        )

        logger.info(f"Created Iceberg table: {table_identifier}")

    def transform_event(self, event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Transform unwrapped Debezium event to Iceberg record.

        Args:
            event: Unwrapped Debezium CDC event

        Returns:
            Transformed record ready for Iceberg, or None if should be skipped
        """
        # Skip None events (tombstones)
        if event is None:
            return None

        # Skip delete operations
        if event.get("__deleted") == "true" or event.get("__op") == "d":
            return None

        # Extract fields
        customer_id = event.get("customer_id")
        if customer_id is None:
            logger.warning("Skipping event without customer_id")
            return None

        first_name = event.get("first_name", "")
        last_name = event.get("last_name", "")
        city = event.get("city", "")
        state = event.get("state", "")
        country = event.get("country", "")

        # Apply transformations
        full_name = f"{first_name} {last_name}".strip()
        location = ", ".join(filter(None, [city, state, country]))

        # Map CDC operation
        op = event.get("__op", "")
        cdc_operation = {
            "c": "INSERT",
            "u": "UPDATE",
            "d": "DELETE",
            "r": "SNAPSHOT",
        }.get(op, "UNKNOWN")

        # Get current timestamp
        now = datetime.utcnow()

        # Build Iceberg record
        record = {
            "customer_id": customer_id,
            "email": event.get("email"),
            "full_name": full_name,
            "location": location,
            "loyalty_points": event.get("loyalty_points"),
            "total_orders": 0,
            "_ingestion_timestamp": now,
            "_source_system": "postgres_cdc",
            "_cdc_operation": cdc_operation,
            "_pipeline_id": "kafka_to_iceberg_consumer",
            "_processed_timestamp": now,
            "_event_timestamp_ms": event.get("__ts_ms"),
            "_source_lsn": event.get("__lsn"),
            "_kafka_timestamp": now,  # Will be overridden with actual Kafka timestamp
        }

        return record

    def run(self):
        """Run the consumer loop."""
        logger.info("Starting Kafka to Iceberg consumer")

        # Connect to Kafka and Iceberg
        self.connect_kafka()
        self.connect_iceberg()

        batch = []
        batch_size = 100
        last_commit_time = time.time()
        commit_interval = 10  # seconds

        logger.info("Starting to consume messages...")

        try:
            for message in self.consumer:
                if not self.running:
                    break

                # Transform event
                event = message.value
                record = self.transform_event(event)

                if record is not None:
                    # Update Kafka timestamp
                    record["_kafka_timestamp"] = datetime.fromtimestamp(
                        message.timestamp / 1000.0
                    )
                    batch.append(record)

                # Write batch when size threshold or time interval reached
                if (len(batch) >= batch_size or
                    time.time() - last_commit_time >= commit_interval):

                    if batch:
                        self._write_batch(batch)
                        batch = []
                        last_commit_time = time.time()

        except Exception as e:
            logger.error(f"Error in consumer loop: {e}", exc_info=True)
            raise
        finally:
            # Write remaining records
            if batch:
                self._write_batch(batch)

            # Close consumer
            if self.consumer:
                self.consumer.close()

            logger.info(f"Consumer stopped. Total records processed: {self.records_processed}")

    def _write_batch(self, batch):
        """Write a batch of records to Iceberg."""
        try:
            # Append records to Iceberg table
            self.table.append(batch)
            self.records_processed += len(batch)
            logger.info(f"Wrote {len(batch)} records to Iceberg (total: {self.records_processed})")
        except Exception as e:
            logger.error(f"Error writing batch to Iceberg: {e}", exc_info=True)


def main():
    """Main entry point."""
    # Configuration from environment
    kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    kafka_topic = os.getenv("KAFKA_TOPIC", "debezium.public.customers")
    kafka_group_id = os.getenv("KAFKA_GROUP_ID", "iceberg-consumer-group")

    catalog_name = os.getenv("ICEBERG_CATALOG", "demo_catalog")
    namespace = os.getenv("ICEBERG_NAMESPACE", "cdc")
    table_name = os.getenv("ICEBERG_TABLE", "customers")
    warehouse_path = os.getenv("ICEBERG_WAREHOUSE", "s3a://warehouse/iceberg")

    s3_endpoint = os.getenv("S3_ENDPOINT", "http://minio:9000")
    s3_access_key = os.getenv("S3_ACCESS_KEY", "minioadmin")
    s3_secret_key = os.getenv("S3_SECRET_KEY", "minioadmin")

    logger.info("=" * 80)
    logger.info("Kafka to Iceberg Consumer Configuration:")
    logger.info(f"  Kafka Bootstrap Servers: {kafka_bootstrap_servers}")
    logger.info(f"  Kafka Topic: {kafka_topic}")
    logger.info(f"  Kafka Group ID: {kafka_group_id}")
    logger.info(f"  Iceberg Catalog: {catalog_name}")
    logger.info(f"  Iceberg Namespace: {namespace}")
    logger.info(f"  Iceberg Table: {table_name}")
    logger.info(f"  Warehouse Path: {warehouse_path}")
    logger.info(f"  S3 Endpoint: {s3_endpoint}")
    logger.info("=" * 80)

    # Create and run consumer
    consumer = KafkaToIcebergConsumer(
        kafka_bootstrap_servers=kafka_bootstrap_servers,
        kafka_topic=kafka_topic,
        kafka_group_id=kafka_group_id,
        catalog_name=catalog_name,
        namespace=namespace,
        table_name=table_name,
        warehouse_path=warehouse_path,
        s3_endpoint=s3_endpoint,
        s3_access_key=s3_access_key,
        s3_secret_key=s3_secret_key,
    )

    consumer.run()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    main()
