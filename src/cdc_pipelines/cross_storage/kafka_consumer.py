"""
Kafka consumer for Debezium CDC events.

This module provides a consumer for reading CDC events from Kafka topics
produced by Debezium connectors.
"""

import logging
import json
from typing import List, Dict, Any, Optional, Callable
from dataclasses import dataclass

try:
    from kafka import KafkaConsumer
    from kafka.errors import KafkaError
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False


logger = logging.getLogger(__name__)


@dataclass
class KafkaConfig:
    """Configuration for Kafka consumer"""

    bootstrap_servers: List[str]
    topic: str
    group_id: str
    auto_offset_reset: str = "earliest"
    enable_auto_commit: bool = True
    max_poll_records: int = 500
    session_timeout_ms: int = 30000
    consumer_timeout_ms: int = 1000


class DebeziumKafkaConsumer:
    """
    Consumes Debezium CDC events from Kafka.

    Provides methods to poll events, parse Debezium format,
    and handle offsets for reliable delivery.
    """

    def __init__(self, config: KafkaConfig):
        """
        Initialize Debezium Kafka Consumer.

        Args:
            config: Kafka configuration

        Raises:
            ImportError: If kafka-python is not installed
        """
        if not KAFKA_AVAILABLE:
            raise ImportError(
                "kafka-python is not installed. "
                "Install it with: pip install kafka-python"
            )

        self.config = config
        self.consumer: Optional[KafkaConsumer] = None
        self.total_consumed = 0

        logger.info(
            f"Initialized DebeziumKafkaConsumer for topic {config.topic}"
        )

    def connect(self) -> None:
        """Connect to Kafka and subscribe to topic"""
        try:
            self.consumer = KafkaConsumer(
                self.config.topic,
                bootstrap_servers=self.config.bootstrap_servers,
                group_id=self.config.group_id,
                auto_offset_reset=self.config.auto_offset_reset,
                enable_auto_commit=self.config.enable_auto_commit,
                max_poll_records=self.config.max_poll_records,
                session_timeout_ms=self.config.session_timeout_ms,
                consumer_timeout_ms=self.config.consumer_timeout_ms,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                key_deserializer=lambda m: json.loads(m.decode("utf-8")) if m else None,
            )

            logger.info(f"Connected to Kafka topic: {self.config.topic}")

        except Exception as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            raise

    def disconnect(self) -> None:
        """Disconnect from Kafka"""
        if self.consumer:
            self.consumer.close()
            logger.info("Disconnected from Kafka")

    def poll(self, timeout_ms: int = 1000) -> List[Dict[str, Any]]:
        """
        Poll for CDC events.

        Args:
            timeout_ms: Poll timeout in milliseconds

        Returns:
            List of Debezium CDC events
        """
        if not self.consumer:
            raise RuntimeError("Consumer not connected. Call connect() first.")

        events = []

        try:
            message_batch = self.consumer.poll(
                timeout_ms=timeout_ms,
                max_records=self.config.max_poll_records
            )

            for topic_partition, messages in message_batch.items():
                for message in messages:
                    events.append({
                        "key": message.key,
                        "value": message.value,
                        "topic": message.topic,
                        "partition": message.partition,
                        "offset": message.offset,
                        "timestamp": message.timestamp,
                    })

            self.total_consumed += len(events)

            if events:
                logger.debug(f"Polled {len(events)} events from Kafka")

        except KafkaError as e:
            logger.error(f"Kafka error during poll: {e}")
            raise

        return events

    def parse_debezium_event(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parse Debezium event format.

        Args:
            message: Raw Kafka message

        Returns:
            Parsed CDC event with standard fields
        """
        value = message.get("value", {})

        if not value:
            return {}

        # Extract Debezium envelope
        payload = value.get("payload", {})

        return {
            "before": payload.get("before"),
            "after": payload.get("after"),
            "source": payload.get("source", {}),
            "op": payload.get("op"),  # c=create, u=update, d=delete, r=read
            "ts_ms": payload.get("ts_ms"),
            "transaction": payload.get("transaction"),
            "_metadata": {
                "topic": message.get("topic"),
                "partition": message.get("partition"),
                "offset": message.get("offset"),
                "timestamp": message.get("timestamp"),
                "key": message.get("key"),
            },
        }

    def consume_batch(
        self,
        batch_size: int = 100,
        timeout_ms: int = 1000
    ) -> List[Dict[str, Any]]:
        """
        Consume a batch of parsed CDC events.

        Args:
            batch_size: Number of events to consume
            timeout_ms: Timeout for each poll

        Returns:
            List of parsed CDC events
        """
        events = []
        remaining = batch_size

        while remaining > 0:
            polled = self.poll(timeout_ms=timeout_ms)

            if not polled:
                break

            for message in polled:
                parsed = self.parse_debezium_event(message)
                if parsed:
                    events.append(parsed)
                    remaining -= 1

                    if remaining <= 0:
                        break

        logger.info(f"Consumed batch of {len(events)} CDC events")
        return events

    def consume_with_callback(
        self,
        callback: Callable[[Dict[str, Any]], bool],
        max_events: Optional[int] = None
    ) -> int:
        """
        Consume events and process with callback.

        Args:
            callback: Function to process each event (return False to stop)
            max_events: Maximum events to process (unlimited if None)

        Returns:
            Number of events processed
        """
        processed = 0

        try:
            while max_events is None or processed < max_events:
                events = self.poll(timeout_ms=1000)

                if not events:
                    continue

                for message in events:
                    parsed = self.parse_debezium_event(message)
                    if not parsed:
                        continue

                    # Process with callback
                    continue_processing = callback(parsed)

                    processed += 1

                    if not continue_processing:
                        logger.info("Callback requested stop")
                        return processed

                    if max_events and processed >= max_events:
                        break

        except KeyboardInterrupt:
            logger.info("Consumer interrupted by user")
        except Exception as e:
            logger.error(f"Error during consume: {e}")
            raise

        return processed

    def seek_to_beginning(self) -> None:
        """Seek to the beginning of the topic"""
        if not self.consumer:
            raise RuntimeError("Consumer not connected")

        self.consumer.seek_to_beginning()
        logger.info("Seeked to beginning of topic")

    def seek_to_end(self) -> None:
        """Seek to the end of the topic"""
        if not self.consumer:
            raise RuntimeError("Consumer not connected")

        self.consumer.seek_to_end()
        logger.info("Seeked to end of topic")

    def commit(self) -> None:
        """Manually commit current offsets"""
        if not self.consumer:
            raise RuntimeError("Consumer not connected")

        self.consumer.commit()
        logger.debug("Committed offsets")

    def get_consumer_stats(self) -> Dict[str, Any]:
        """
        Get consumer statistics.

        Returns:
            Dictionary with consumer stats
        """
        if not self.consumer:
            return {"connected": False}

        try:
            # Get partition assignments
            assignments = self.consumer.assignment()

            # Get current positions
            positions = {}
            for partition in assignments:
                positions[str(partition)] = self.consumer.position(partition)

            return {
                "connected": True,
                "topic": self.config.topic,
                "group_id": self.config.group_id,
                "total_consumed": self.total_consumed,
                "assigned_partitions": len(assignments),
                "positions": positions,
            }

        except Exception as e:
            logger.error(f"Failed to get consumer stats: {e}")
            return {"connected": True, "error": str(e)}

    def get_lag(self) -> Dict[str, int]:
        """
        Get consumer lag per partition.

        Returns:
            Dictionary of partition -> lag
        """
        if not self.consumer:
            return {}

        lag = {}

        try:
            assignments = self.consumer.assignment()

            for partition in assignments:
                # Get current position
                current_position = self.consumer.position(partition)

                # Get end offset
                end_offsets = self.consumer.end_offsets([partition])
                end_offset = end_offsets.get(partition, current_position)

                # Calculate lag
                lag[str(partition)] = end_offset - current_position

        except Exception as e:
            logger.error(f"Failed to calculate lag: {e}")

        return lag

    def filter_events_by_table(
        self,
        events: List[Dict[str, Any]],
        table_name: str
    ) -> List[Dict[str, Any]]:
        """
        Filter events for a specific table.

        Args:
            events: List of CDC events
            table_name: Table name to filter

        Returns:
            Filtered list of events
        """
        filtered = []

        for event in events:
            source = event.get("source", {})
            if source.get("table") == table_name:
                filtered.append(event)

        logger.debug(
            f"Filtered {len(filtered)} events for table {table_name} "
            f"out of {len(events)} total"
        )

        return filtered

    def filter_events_by_operation(
        self,
        events: List[Dict[str, Any]],
        operations: List[str]
    ) -> List[Dict[str, Any]]:
        """
        Filter events by operation type.

        Args:
            events: List of CDC events
            operations: List of operations to include (c/u/d/r)

        Returns:
            Filtered list of events
        """
        filtered = [
            event for event in events
            if event.get("op") in operations
        ]

        logger.debug(
            f"Filtered {len(filtered)} events for operations {operations}"
        )

        return filtered
