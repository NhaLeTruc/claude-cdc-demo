"""
Cross-Storage CDC Pipeline Module.

This module implements end-to-end CDC from Postgres to Iceberg
via Kafka, demonstrating data transformation and cross-storage replication.
"""

# Lazy imports to avoid import-time dependencies
def __getattr__(name):
    if name == "DataTransformer":
        from src.cdc_pipelines.cross_storage.transformations import DataTransformer
        return DataTransformer
    elif name == "DebeziumKafkaConsumer":
        from src.cdc_pipelines.cross_storage.kafka_consumer import DebeziumKafkaConsumer
        return DebeziumKafkaConsumer
    elif name == "CrossStorageCDCPipeline":
        from src.cdc_pipelines.cross_storage.pipeline import CrossStorageCDCPipeline
        return CrossStorageCDCPipeline
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

__all__ = [
    "DataTransformer",
    "DebeziumKafkaConsumer",
    "CrossStorageCDCPipeline",
]
