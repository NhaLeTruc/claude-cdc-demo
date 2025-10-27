"""MySQL CDC pipeline orchestrator."""

import json
import signal
import time
from typing import Any, Dict, Optional

from kafka import KafkaConsumer

from src.cdc_pipelines.mysql.delta_writer import MySQLDeltaLakeWriter
from src.cdc_pipelines.mysql.event_parser import BinlogEventParser
from src.observability.logging_config import get_logger
from src.observability.metrics import (
    mysql_cdc_batch_size,
    mysql_cdc_events_processed_total,
    mysql_cdc_lag_seconds,
)

logger = get_logger(__name__)


class MySQLCDCPipeline:
    """Orchestrates MySQL CDC pipeline from source to DeltaLake."""

    def __init__(
        self,
        pipeline_name: str,
        kafka_bootstrap_servers: str,
        kafka_topic: str,
        delta_table_path: str,
        primary_key: str,
        consumer_group: Optional[str] = None,
    ) -> None:
        """
        Initialize MySQL CDC pipeline.

        Args:
            pipeline_name: Name of the pipeline
            kafka_bootstrap_servers: Kafka bootstrap servers
            kafka_topic: Kafka topic to consume from
            delta_table_path: Path to DeltaLake table
            primary_key: Primary key column name
            consumer_group: Kafka consumer group (defaults to pipeline_name)
        """
        self.pipeline_name = pipeline_name
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.kafka_topic = kafka_topic
        self.delta_table_path = delta_table_path
        self.primary_key = primary_key
        self.consumer_group = consumer_group or f"{pipeline_name}-consumer-group"

        # Initialize components
        self.event_parser = BinlogEventParser()
        self.delta_writer = MySQLDeltaLakeWriter(
            table_path=delta_table_path,
            primary_key=primary_key,
        )

        # Runtime state
        self._consumer: Optional[KafkaConsumer] = None
        self._running = False
        self._events_processed = 0

        # Setup signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def start(self, batch_size: int = 100, poll_timeout_ms: int = 1000) -> None:
        """
        Start the CDC pipeline.

        Args:
            batch_size: Number of events to process per batch
            poll_timeout_ms: Kafka poll timeout in milliseconds
        """
        logger.info(f"Starting MySQL CDC pipeline: {self.pipeline_name}")
        logger.info(f"Kafka topic: {self.kafka_topic}")
        logger.info(f"Delta table: {self.delta_table_path}")

        # Create Kafka consumer
        self._consumer = KafkaConsumer(
            self.kafka_topic,
            bootstrap_servers=self.kafka_bootstrap_servers,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            group_id=self.consumer_group,
            value_deserializer=lambda m: self._deserialize_json(m),
        )

        logger.info("MySQL CDC pipeline started successfully")
        self._running = True

        # Main processing loop
        while self._running:
            try:
                self._process_batch(batch_size, poll_timeout_ms)
            except Exception as e:
                logger.error(f"Error in processing loop: {e}", exc_info=True)
                time.sleep(5)  # Brief pause before retrying

    def stop(self) -> None:
        """Stop the CDC pipeline."""
        logger.info("Stopping MySQL CDC pipeline...")
        self._running = False

        if self._consumer:
            self._consumer.close()
            logger.info("Kafka consumer closed")

        logger.info(f"MySQL CDC pipeline stopped. Total events processed: {self._events_processed}")

    def _process_batch(self, batch_size: int, poll_timeout_ms: int) -> None:
        """
        Process batch of CDC events.

        Args:
            batch_size: Maximum number of events to process
            poll_timeout_ms: Poll timeout in milliseconds
        """
        if not self._consumer:
            return

        # Poll for messages
        messages = self._consumer.poll(
            timeout_ms=poll_timeout_ms, max_records=batch_size
        )

        if not messages:
            return

        # Parse and collect events
        events_batch = []
        for topic_partition, records in messages.items():
            for record in records:
                try:
                    # Parse Debezium event
                    parsed_event = self.event_parser.parse(record.value)

                    # Track metrics
                    table = parsed_event.get("table", "unknown")
                    operation = parsed_event.get("operation", "UNKNOWN")
                    mysql_cdc_events_processed_total.labels(
                        table=table, operation=operation
                    ).inc()

                    # Calculate lag
                    self._update_lag_metric(parsed_event, table)

                    events_batch.append(parsed_event)

                except Exception as e:
                    logger.error(f"Failed to parse event: {e}")
                    logger.debug(f"Problem record: {record.value}")
                    continue

        # Write batch to DeltaLake
        if events_batch:
            mysql_cdc_batch_size.observe(len(events_batch))
            self.delta_writer.write_batch(events_batch)
            self._events_processed += len(events_batch)

            logger.info(
                f"Processed batch: {len(events_batch)} events "
                f"(total: {self._events_processed})"
            )

    def _deserialize_json(self, message: bytes) -> Dict[str, Any]:
        """
        Deserialize JSON message from Kafka.

        Args:
            message: Raw message bytes

        Returns:
            Deserialized dictionary
        """
        try:
            return json.loads(message.decode("utf-8"))
        except json.JSONDecodeError as e:
            logger.error(f"Failed to deserialize JSON: {e}")
            return {}

    def _update_lag_metric(self, event: Dict[str, Any], table: str) -> None:
        """
        Update CDC lag metric.

        Args:
            event: Parsed CDC event
            table: Table name
        """
        try:
            from datetime import datetime

            event_timestamp = event.get("timestamp")
            if event_timestamp:
                event_time = datetime.fromisoformat(event_timestamp)
                now = datetime.now()
                lag_seconds = (now - event_time).total_seconds()

                mysql_cdc_lag_seconds.labels(
                    pipeline=self.pipeline_name, table=table
                ).set(lag_seconds)

        except Exception as e:
            logger.debug(f"Failed to update lag metric: {e}")

    def _signal_handler(self, signum: int, frame: Any) -> None:
        """
        Handle shutdown signals.

        Args:
            signum: Signal number
            frame: Current stack frame
        """
        logger.info(f"Received signal {signum}, shutting down...")
        self.stop()

    def get_stats(self) -> Dict[str, Any]:
        """
        Get pipeline statistics.

        Returns:
            Dictionary with pipeline stats
        """
        return {
            "pipeline_name": self.pipeline_name,
            "kafka_topic": self.kafka_topic,
            "delta_table_path": self.delta_table_path,
            "events_processed": self._events_processed,
            "running": self._running,
        }

    def health_check(self) -> bool:
        """
        Check if pipeline is healthy.

        Returns:
            True if healthy, False otherwise
        """
        # Check if running
        if not self._running:
            return False

        # Check if consumer is connected
        if not self._consumer:
            return False

        # Could add more checks here (Kafka connectivity, Delta table accessibility, etc.)

        return True
