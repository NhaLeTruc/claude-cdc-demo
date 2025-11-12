"""Postgres CDC pipeline orchestrator."""

import time
from typing import Any, Dict, List, Optional

from kafka import KafkaConsumer

from src.cdc_pipelines.postgres.connection import PostgresConnectionManager
from src.cdc_pipelines.postgres.debezium_config import DebeziumConfigBuilder
from src.cdc_pipelines.postgres.delta_writer import DeltaLakeWriter
from src.cdc_pipelines.postgres.event_parser import CDCEventParser
from src.common.config import get_settings
from src.observability.logging_config import get_logger

logger = get_logger(__name__)


class PostgresCDCPipeline:
    """Orchestrates Postgres CDC pipeline from source to DeltaLake."""

    def __init__(
        self,
        pipeline_name: str = "postgres-cdc",
        kafka_bootstrap_servers: Optional[str] = None,
        kafka_topic: str = "cdc.postgres.public.customers",
        delta_table_path: str = "./delta-lake/customers",
        primary_key_field: str = "customer_id",
    ) -> None:
        """
        Initialize Postgres CDC pipeline.

        Args:
            pipeline_name: Pipeline name
            kafka_bootstrap_servers: Kafka bootstrap servers
            kafka_topic: Kafka topic to consume from
            delta_table_path: Path to DeltaLake table
            primary_key_field: Primary key field name
        """
        self.pipeline_name = pipeline_name
        settings = get_settings()

        self.kafka_bootstrap_servers = (
            kafka_bootstrap_servers or settings.kafka.bootstrap_servers
        )
        self.kafka_topic = kafka_topic
        self.delta_table_path = delta_table_path
        self.primary_key_field = primary_key_field

        # Initialize components
        self.event_parser = CDCEventParser(primary_key_field=primary_key_field)
        self.delta_writer = DeltaLakeWriter(
            table_path=delta_table_path,
            table_name=kafka_topic.split(".")[-1],
            enable_cdf=True,
            primary_key=primary_key_field,
        )

        self._consumer: Optional[KafkaConsumer] = None
        self._running = False
        self._events_processed = 0

        logger.info(
            f"Initialized {pipeline_name} pipeline: "
            f"topic={kafka_topic}, destination={delta_table_path}"
        )

    def start(self, batch_size: int = 100, poll_timeout_ms: int = 1000) -> None:
        """
        Start the CDC pipeline.

        Args:
            batch_size: Number of events to process in batch
            poll_timeout_ms: Kafka poll timeout in milliseconds
        """
        logger.info(f"Starting {self.pipeline_name} pipeline...")

        try:
            # Initialize Kafka consumer
            self._consumer = KafkaConsumer(
                self.kafka_topic,
                bootstrap_servers=self.kafka_bootstrap_servers,
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                group_id=f"{self.pipeline_name}-consumer-group",
                value_deserializer=lambda m: self._deserialize_json(m),
            )

            self._running = True
            logger.info(f"Pipeline started, consuming from {self.kafka_topic}")

            # Main processing loop
            while self._running:
                self._process_batch(batch_size, poll_timeout_ms)

        except KeyboardInterrupt:
            logger.info("Pipeline interrupted by user")
        except Exception as e:
            logger.error(f"Pipeline error: {e}", exc_info=True)
            raise
        finally:
            self.stop()

    def stop(self) -> None:
        """Stop the CDC pipeline."""
        logger.info(f"Stopping {self.pipeline_name} pipeline...")
        self._running = False

        if self._consumer:
            self._consumer.close()
            logger.info("Closed Kafka consumer")

        self.delta_writer.close()

        logger.info(
            f"Pipeline stopped. Total events processed: {self._events_processed}"
        )

    def _process_batch(self, batch_size: int, poll_timeout_ms: int) -> None:
        """
        Process batch of CDC events.

        Args:
            batch_size: Batch size
            poll_timeout_ms: Poll timeout
        """
        if not self._consumer:
            return

        # Poll for messages
        messages = self._consumer.poll(
            timeout_ms=poll_timeout_ms,
            max_records=batch_size,
        )

        if not messages:
            return

        # Process messages
        events_batch = []
        for topic_partition, records in messages.items():
            logger.debug(
                f"Received {len(records)} messages from {topic_partition.topic}"
            )

            for record in records:
                try:
                    # Skip tombstone records (None values from DELETE operations)
                    if record.value is None:
                        logger.debug("Skipping tombstone record")
                        continue

                    # Parse Debezium event
                    parsed_event = self.event_parser.parse(record.value)
                    events_batch.append(parsed_event)

                except Exception as e:
                    logger.error(f"Failed to parse event: {e}")
                    continue

        # Write batch to DeltaLake
        if events_batch:
            try:
                self.delta_writer.write_batch(events_batch)
                self._events_processed += len(events_batch)

                logger.info(
                    f"Processed batch of {len(events_batch)} events "
                    f"(total: {self._events_processed})"
                )

            except Exception as e:
                logger.error(f"Failed to write batch: {e}", exc_info=True)

    def _deserialize_json(self, message: Optional[bytes]) -> Optional[Dict[str, Any]]:
        """
        Deserialize JSON message from Kafka.

        Handles tombstone records (None values) which are sent for DELETE operations.

        Args:
            message: Raw message bytes (can be None for tombstone records)

        Returns:
            Deserialized dictionary, or None for tombstone records
        """
        import json

        try:
            if message is None:
                # Tombstone record (DELETE operation)
                return None

            return json.loads(message.decode("utf-8"))
        except json.JSONDecodeError as e:
            logger.error(f"Failed to deserialize JSON: {e}")
            logger.debug(f"Message bytes (first 100): {message[:100] if message else None}")
            return {}
        except (AttributeError, UnicodeDecodeError) as e:
            logger.error(f"Failed to decode message: {e}")
            logger.debug(f"Message bytes (first 100): {message[:100] if message else None}")
            return {}

    def get_status(self) -> Dict[str, Any]:
        """
        Get pipeline status.

        Returns:
            Status dictionary
        """
        return {
            "pipeline_name": self.pipeline_name,
            "running": self._running,
            "events_processed": self._events_processed,
            "kafka_topic": self.kafka_topic,
            "delta_table_path": self.delta_table_path,
        }

    def validate_connection(self) -> bool:
        """
        Validate pipeline connections.

        Returns:
            True if all connections are valid
        """
        try:
            # Test Kafka connection
            logger.info("Validating Kafka connection...")
            test_consumer = KafkaConsumer(
                bootstrap_servers=self.kafka_bootstrap_servers,
                request_timeout_ms=5000,
            )
            test_consumer.close()
            logger.info("Kafka connection valid")

            return True

        except Exception as e:
            logger.error(f"Connection validation failed: {e}")
            return False
