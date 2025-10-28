"""
Cross-Storage CDC Pipeline Orchestrator.

This module orchestrates end-to-end CDC from Postgres to Iceberg
via Kafka, with transformations applied during streaming.
"""

import logging
from typing import Dict, Any, Optional
from datetime import datetime
from dataclasses import dataclass

from src.cdc_pipelines.cross_storage.transformations import DataTransformer
from src.cdc_pipelines.cross_storage.kafka_consumer import (
    DebeziumKafkaConsumer,
    KafkaConfig,
)


logger = logging.getLogger(__name__)


@dataclass
class CrossStorageConfig:
    """Configuration for cross-storage CDC pipeline"""

    # Source (Kafka/Debezium)
    kafka_bootstrap_servers: list[str]
    kafka_topic: str
    kafka_group_id: str

    # Destination (Iceberg)
    iceberg_catalog: str
    iceberg_namespace: str
    iceberg_table: str
    iceberg_warehouse: str

    # Processing
    batch_size: int = 1000
    poll_interval_ms: int = 1000
    checkpoint_location: Optional[str] = None


class CrossStorageCDCPipeline:
    """
    Cross-storage CDC pipeline from Postgres to Iceberg.

    Orchestrates the end-to-end flow:
    1. Consume CDC events from Kafka (Debezium)
    2. Transform data (name concatenation, location derivation)
    3. Write to Iceberg table
    4. Monitor and report metrics
    """

    def __init__(self, config: CrossStorageConfig):
        """
        Initialize cross-storage CDC pipeline.

        Args:
            config: Pipeline configuration
        """
        self.config = config

        # Initialize components
        self.transformer = DataTransformer()

        kafka_config = KafkaConfig(
            bootstrap_servers=config.kafka_bootstrap_servers,
            topic=config.kafka_topic,
            group_id=config.kafka_group_id,
            max_poll_records=config.batch_size,
            consumer_timeout_ms=config.poll_interval_ms,
        )
        self.kafka_consumer = DebeziumKafkaConsumer(kafka_config)

        self.pipeline_start_time = datetime.now()
        self.total_processed = 0
        self.total_errors = 0

        logger.info(
            f"Initialized CrossStorageCDCPipeline: "
            f"{config.kafka_topic} → {config.iceberg_namespace}.{config.iceberg_table}"
        )

    def start(self) -> Dict[str, Any]:
        """
        Start the CDC pipeline.

        Returns:
            Dictionary with startup status
        """
        logger.info("Starting cross-storage CDC pipeline")

        try:
            # Connect to Kafka
            self.kafka_consumer.connect()

            status = {
                "status": "started",
                "kafka_topic": self.config.kafka_topic,
                "iceberg_table": f"{self.config.iceberg_namespace}.{self.config.iceberg_table}",
                "start_time": self.pipeline_start_time,
                "batch_size": self.config.batch_size,
            }

            logger.info("Cross-storage CDC pipeline started successfully")
            return status

        except Exception as e:
            logger.error(f"Failed to start pipeline: {e}")
            return {
                "status": "error",
                "error": str(e),
            }

    def process_batch(self) -> Dict[str, Any]:
        """
        Process one batch of CDC events.

        Returns:
            Dictionary with processing results
        """
        try:
            # Consume batch from Kafka
            events = self.kafka_consumer.consume_batch(
                batch_size=self.config.batch_size
            )

            if not events:
                return {
                    "status": "no_data",
                    "events_consumed": 0,
                }

            # Filter for customer table events
            customer_events = self.kafka_consumer.filter_events_by_table(
                events, "customers"
            )

            if not customer_events:
                logger.debug("No customer events in batch")
                return {
                    "status": "no_customer_events",
                    "events_consumed": len(events),
                    "customer_events": 0,
                }

            # Transform events
            transformed_data = self.transformer.transform_batch(customer_events)

            # Apply business rules
            validated_data = self.transformer.apply_business_rules(transformed_data)

            # Add metadata
            batch_id = f"{self.pipeline_start_time.strftime('%Y%m%d_%H%M%S')}_{self.total_processed}"
            enriched_data = self.transformer.add_metadata_columns(
                validated_data,
                pipeline_id="postgres_to_iceberg",
                batch_id=batch_id
            )

            # Write to Iceberg (simulated - would use Spark/PyIceberg in production)
            self._write_to_iceberg(enriched_data)

            # Update counters
            self.total_processed += enriched_data.num_rows

            # Commit Kafka offsets
            self.kafka_consumer.commit()

            result = {
                "status": "success",
                "events_consumed": len(events),
                "customer_events": len(customer_events),
                "rows_transformed": transformed_data.num_rows,
                "rows_validated": validated_data.num_rows,
                "rows_written": enriched_data.num_rows,
                "total_processed": self.total_processed,
            }

            logger.info(
                f"Processed batch: {result['rows_written']} rows written "
                f"(total: {self.total_processed})"
            )

            return result

        except Exception as e:
            self.total_errors += 1
            logger.error(f"Error processing batch: {e}")
            return {
                "status": "error",
                "error": str(e),
                "total_errors": self.total_errors,
            }

    def _write_to_iceberg(self, data) -> None:
        """
        Write data to Iceberg table.

        Note: This is a placeholder. In production, this would use:
        - PyIceberg for direct writes
        - Spark Structured Streaming for scalable streaming writes
        - Flink Iceberg connector for real-time streaming

        Args:
            data: PyArrow Table to write
        """
        logger.info(f"Writing {data.num_rows} rows to Iceberg (simulated)")

        # In production, this would be:
        # from pyiceberg.catalog import load_catalog
        # catalog = load_catalog(...)
        # table = catalog.load_table(f"{namespace}.{table_name}")
        # table.append(data)

    def run_continuous(self, max_batches: Optional[int] = None) -> Dict[str, Any]:
        """
        Run pipeline continuously.

        Args:
            max_batches: Maximum number of batches to process (unlimited if None)

        Returns:
            Dictionary with final statistics
        """
        logger.info("Starting continuous processing")

        batches_processed = 0

        try:
            while max_batches is None or batches_processed < max_batches:
                result = self.process_batch()

                if result["status"] == "no_data":
                    # No data available, short sleep
                    import time
                    time.sleep(self.config.poll_interval_ms / 1000)
                    continue

                batches_processed += 1

                if batches_processed % 10 == 0:
                    logger.info(
                        f"Progress: {batches_processed} batches, "
                        f"{self.total_processed} rows processed"
                    )

        except KeyboardInterrupt:
            logger.info("Pipeline stopped by user")
        except Exception as e:
            logger.error(f"Pipeline error: {e}")

        return self.get_statistics()

    def get_pipeline_status(self) -> Dict[str, Any]:
        """
        Get current pipeline status.

        Returns:
            Dictionary with pipeline status
        """
        uptime = (datetime.now() - self.pipeline_start_time).total_seconds()

        consumer_stats = self.kafka_consumer.get_consumer_stats()
        lag = self.kafka_consumer.get_lag()

        return {
            "pipeline_id": "postgres_to_iceberg",
            "status": "running",
            "uptime_seconds": uptime,
            "total_processed": self.total_processed,
            "total_errors": self.total_errors,
            "throughput_rows_per_second": (
                self.total_processed / uptime if uptime > 0 else 0
            ),
            "kafka_consumer": consumer_stats,
            "kafka_lag": lag,
        }

    def get_statistics(self) -> Dict[str, Any]:
        """
        Get comprehensive pipeline statistics.

        Returns:
            Dictionary with statistics
        """
        uptime = (datetime.now() - self.pipeline_start_time).total_seconds()

        return {
            "pipeline_id": "postgres_to_iceberg",
            "total_processed": self.total_processed,
            "total_errors": self.total_errors,
            "error_rate": (
                self.total_errors / max(self.total_processed, 1)
            ),
            "uptime_seconds": uptime,
            "average_throughput": (
                self.total_processed / uptime if uptime > 0 else 0
            ),
            "start_time": self.pipeline_start_time.isoformat(),
            "end_time": datetime.now().isoformat(),
        }

    def stop(self) -> Dict[str, Any]:
        """
        Stop the CDC pipeline.

        Returns:
            Dictionary with shutdown status
        """
        logger.info("Stopping cross-storage CDC pipeline")

        # Disconnect from Kafka
        self.kafka_consumer.disconnect()

        stats = self.get_statistics()
        stats["status"] = "stopped"

        logger.info(
            f"Pipeline stopped. Processed {self.total_processed} rows "
            f"in {stats['uptime_seconds']:.2f} seconds"
        )

        return stats

    def validate_pipeline(self) -> Dict[str, Any]:
        """
        Validate pipeline configuration and connectivity.

        Returns:
            Dictionary with validation results
        """
        validations = {
            "kafka_configured": bool(self.config.kafka_bootstrap_servers),
            "iceberg_configured": bool(self.config.iceberg_warehouse),
        }

        # Check Kafka connectivity
        try:
            consumer_stats = self.kafka_consumer.get_consumer_stats()
            validations["kafka_connected"] = consumer_stats.get("connected", False)
        except Exception:
            validations["kafka_connected"] = False

        validations["all_valid"] = all(validations.values())

        return validations
