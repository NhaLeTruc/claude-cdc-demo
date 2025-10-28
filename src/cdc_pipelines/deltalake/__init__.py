"""
DeltaLake CDC Pipeline Module.

This module implements Change Data Capture using DeltaLake's native
Change Data Feed (CDF) feature.
"""

from src.cdc_pipelines.deltalake.table_manager import DeltaTableManager
from src.cdc_pipelines.deltalake.cdf_reader import CDFReader
from src.cdc_pipelines.deltalake.version_tracker import VersionTracker
from src.cdc_pipelines.deltalake.pipeline import DeltaCDCPipeline

__all__ = [
    "DeltaTableManager",
    "CDFReader",
    "VersionTracker",
    "DeltaCDCPipeline",
]
