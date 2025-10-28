"""
Apache Iceberg CDC Pipeline Module.

This module implements Change Data Capture using Apache Iceberg's
snapshot-based incremental read capabilities.
"""

from src.cdc_pipelines.iceberg.table_manager import IcebergTableManager
from src.cdc_pipelines.iceberg.snapshot_tracker import SnapshotTracker
from src.cdc_pipelines.iceberg.incremental_reader import IncrementalReader
from src.cdc_pipelines.iceberg.pipeline import IcebergCDCPipeline

__all__ = [
    "IcebergTableManager",
    "SnapshotTracker",
    "IncrementalReader",
    "IcebergCDCPipeline",
]
