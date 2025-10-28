"""
Apache Iceberg Table Manager.

This module provides functionality to create and manage Apache Iceberg tables
for CDC operations using snapshot-based change tracking.
"""

import logging
from pathlib import Path
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from datetime import datetime

try:
    from pyiceberg.catalog import load_catalog
    from pyiceberg.schema import Schema
    from pyiceberg.table import Table
    from pyiceberg.types import (
        NestedField,
        StringType,
        LongType,
        TimestampType,
        BooleanType,
        DoubleType,
    )
    from pyiceberg.partitioning import PartitionSpec, PartitionField
    from pyiceberg.transforms import MonthTransform, DayTransform
    PYICEBERG_AVAILABLE = True
except ImportError:
    PYICEBERG_AVAILABLE = False


logger = logging.getLogger(__name__)


@dataclass
class IcebergTableConfig:
    """Configuration for Iceberg table"""

    catalog_name: str
    namespace: str
    table_name: str
    warehouse_path: str
    partition_spec: Optional[List[tuple[str, str]]] = None  # [(field, transform)]
    sort_order: Optional[List[str]] = None


class IcebergTableManager:
    """
    Manages Apache Iceberg tables for CDC operations.

    Provides methods to create, configure, and manage Iceberg tables
    with snapshot tracking for incremental read capabilities.
    """

    def __init__(self, config: IcebergTableConfig):
        """
        Initialize Iceberg Table Manager.

        Args:
            config: Table configuration

        Raises:
            ImportError: If PyIceberg is not installed
        """
        if not PYICEBERG_AVAILABLE:
            raise ImportError(
                "PyIceberg is not installed. "
                "Install it with: pip install pyiceberg"
            )

        self.config = config
        self.catalog = None
        self.table = None

        logger.info(
            f"Initialized IcebergTableManager for "
            f"{config.namespace}.{config.table_name}"
        )

    def _get_catalog(self):
        """Get or create catalog instance"""
        if self.catalog is None:
            self.catalog = load_catalog(
                self.config.catalog_name,
                **{
                    "type": "rest",
                    "uri": "http://localhost:8181",
                    "warehouse": self.config.warehouse_path,
                }
            )
        return self.catalog

    def create_table(self, schema: Schema, properties: Optional[Dict[str, str]] = None) -> Table:
        """
        Create a new Iceberg table.

        Args:
            schema: Table schema definition
            properties: Optional table properties

        Returns:
            Created Iceberg table
        """
        catalog = self._get_catalog()

        # Build partition spec
        partition_spec = None
        if self.config.partition_spec:
            partition_fields = []
            for i, (field_name, transform) in enumerate(self.config.partition_spec):
                field_id = 1000 + i
                if transform == "month":
                    partition_fields.append(
                        PartitionField(
                            source_id=schema.find_field(field_name).field_id,
                            field_id=field_id,
                            transform=MonthTransform(),
                            name=f"{field_name}_month",
                        )
                    )
                elif transform == "day":
                    partition_fields.append(
                        PartitionField(
                            source_id=schema.find_field(field_name).field_id,
                            field_id=field_id,
                            transform=DayTransform(),
                            name=f"{field_name}_day",
                        )
                    )

            if partition_fields:
                partition_spec = PartitionSpec(*partition_fields)

        # Create table
        table_identifier = f"{self.config.namespace}.{self.config.table_name}"

        try:
            self.table = catalog.create_table(
                identifier=table_identifier,
                schema=schema,
                partition_spec=partition_spec,
                properties=properties or {},
            )
            logger.info(f"Created Iceberg table: {table_identifier}")
            return self.table
        except Exception as e:
            logger.error(f"Failed to create table: {e}")
            raise

    def load_table(self) -> Table:
        """
        Load an existing Iceberg table.

        Returns:
            Loaded Iceberg table
        """
        if self.table is None:
            catalog = self._get_catalog()
            table_identifier = f"{self.config.namespace}.{self.config.table_name}"

            try:
                self.table = catalog.load_table(table_identifier)
                logger.info(f"Loaded Iceberg table: {table_identifier}")
            except Exception as e:
                logger.error(f"Failed to load table: {e}")
                raise

        return self.table

    def table_exists(self) -> bool:
        """
        Check if table exists.

        Returns:
            True if table exists
        """
        try:
            self.load_table()
            return True
        except Exception:
            return False

    def get_table_metadata(self) -> Dict[str, Any]:
        """
        Get table metadata.

        Returns:
            Dictionary with table metadata
        """
        if not self.table_exists():
            return {
                "exists": False,
                "namespace": self.config.namespace,
                "table_name": self.config.table_name,
            }

        table = self.load_table()
        metadata = table.metadata

        return {
            "exists": True,
            "namespace": self.config.namespace,
            "table_name": self.config.table_name,
            "location": metadata.location,
            "format_version": metadata.format_version,
            "current_snapshot_id": metadata.current_snapshot_id,
            "last_updated_ms": metadata.last_updated_ms,
            "properties": metadata.properties,
            "partition_spec": [
                {
                    "source_id": field.source_id,
                    "field_id": field.field_id,
                    "name": field.name,
                    "transform": str(field.transform),
                }
                for field in metadata.partition_spec.fields
            ] if metadata.partition_spec else [],
        }

    def get_current_snapshot_id(self) -> Optional[int]:
        """
        Get current snapshot ID.

        Returns:
            Current snapshot ID or None
        """
        if not self.table_exists():
            return None

        table = self.load_table()
        return table.metadata.current_snapshot_id

    def get_snapshots(self) -> List[Dict[str, Any]]:
        """
        Get all table snapshots.

        Returns:
            List of snapshot metadata
        """
        if not self.table_exists():
            return []

        table = self.load_table()
        snapshots = []

        for snapshot in table.metadata.snapshots:
            snapshots.append({
                "snapshot_id": snapshot.snapshot_id,
                "parent_snapshot_id": snapshot.parent_snapshot_id,
                "timestamp_ms": snapshot.timestamp_ms,
                "manifest_list": snapshot.manifest_list,
                "summary": snapshot.summary.additional_properties if snapshot.summary else {},
            })

        return snapshots

    def add_snapshot(self, data_files: List[str]) -> int:
        """
        Add a new snapshot (simplified for demo).

        Args:
            data_files: List of data file paths

        Returns:
            New snapshot ID
        """
        # Note: In production, use proper Iceberg write operations
        table = self.load_table()

        # This is a placeholder - actual implementation would use
        # table.append() or table.overwrite() methods
        logger.info(f"Adding snapshot with {len(data_files)} files")

        return table.metadata.current_snapshot_id

    def get_schema(self) -> Optional[Schema]:
        """
        Get table schema.

        Returns:
            Iceberg schema or None
        """
        if not self.table_exists():
            return None

        table = self.load_table()
        return table.schema()

    def validate_configuration(self) -> bool:
        """
        Validate table configuration.

        Returns:
            True if configuration is valid
        """
        if not self.config.catalog_name:
            return False

        if not self.config.namespace or not self.config.table_name:
            return False

        if not self.config.warehouse_path:
            return False

        return True

    def get_table_properties(self) -> Dict[str, str]:
        """
        Get table properties.

        Returns:
            Dictionary of table properties
        """
        if not self.table_exists():
            return {}

        table = self.load_table()
        return dict(table.metadata.properties)

    def set_table_property(self, key: str, value: str) -> None:
        """
        Set a table property.

        Args:
            key: Property key
            value: Property value
        """
        table = self.load_table()

        # Note: PyIceberg API for updating properties
        # This would use table.update_properties() in production
        logger.info(f"Setting property {key}={value}")

    def get_table_statistics(self) -> Dict[str, Any]:
        """
        Get table statistics.

        Returns:
            Dictionary with statistics
        """
        if not self.table_exists():
            return {
                "exists": False,
                "total_snapshots": 0,
            }

        table = self.load_table()
        snapshots = self.get_snapshots()

        current_snapshot = None
        if table.metadata.current_snapshot_id:
            current_snapshot = next(
                (s for s in snapshots if s["snapshot_id"] == table.metadata.current_snapshot_id),
                None
            )

        return {
            "exists": True,
            "total_snapshots": len(snapshots),
            "current_snapshot_id": table.metadata.current_snapshot_id,
            "format_version": table.metadata.format_version,
            "last_updated": datetime.fromtimestamp(
                table.metadata.last_updated_ms / 1000
            ) if table.metadata.last_updated_ms else None,
            "current_snapshot_summary": current_snapshot.get("summary", {}) if current_snapshot else {},
        }

    def drop_table(self) -> None:
        """Drop the table"""
        catalog = self._get_catalog()
        table_identifier = f"{self.config.namespace}.{self.config.table_name}"

        try:
            catalog.drop_table(table_identifier)
            self.table = None
            logger.info(f"Dropped Iceberg table: {table_identifier}")
        except Exception as e:
            logger.error(f"Failed to drop table: {e}")
            raise
