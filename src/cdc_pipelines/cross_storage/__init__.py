"""
Cross-Storage CDC Pipeline Module.

This module implements end-to-end CDC from Postgres to Iceberg
via Kafka, demonstrating data transformation and cross-storage replication.
"""

from src.cdc_pipelines.cross_storage.transformations import DataTransformer
from src.cdc_pipelines.cross_storage.kafka_consumer import DebeziumKafkaConsumer
from src.cdc_pipelines.cross_storage.pipeline import CrossStorageCDCPipeline

__all__ = [
    "DataTransformer",
    "DebeziumKafkaConsumer",
    "CrossStorageCDCPipeline",
]
