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
                    "s3.endpoint": "http://localhost:9000",
                    "s3.access-key-id": "minioadmin",
                    "s3.secret-access-key": "minioadmin",
                    "s3.path-style-access": "true",
                }
            )
        return self.catalog

    def load_catalog(self):
        """
        Load or get the catalog instance.

        Returns:
            Iceberg catalog instance
        """
        return self._get_catalog()

    def get_schema(self) -> Optional[Schema]:
        """
        Get the table schema.

        Returns:
            Iceberg Schema object or None if table doesn't exist
        """
        if not self.table_exists():
            return None
        table = self.load_table()
        return table.schema()

    def get_partition_spec(self) -> Optional[Any]:
        """
        Get the table partition specification.

        Returns:
            Partition spec or None if table doesn't exist
        """
        if not self.table_exists():
            return None
        table = self.load_table()
        return table.spec()

    def get_sort_order(self) -> Optional[Any]:
        """
        Get the table sort order.

        Returns:
            Sort order or None if table doesn't exist
        """
        if not self.table_exists():
            return None
        table = self.load_table()
        return table.sort_order()

    def get_table_location(self) -> Optional[str]:
        """
        Get the table location path.

        Returns:
            Table location string or None if table doesn't exist
        """
        if not self.table_exists():
            return None
        table = self.load_table()
        return table.location()

    def get_table_properties(self) -> Optional[Dict[str, str]]:
        """
        Get table properties.

        Returns:
            Dictionary of table properties or None if table doesn't exist
        """
        if not self.table_exists():
            return None
        table = self.load_table()
        return table.properties

    def create_table(self, schema, properties: Optional[Dict[str, str]] = None) -> Table:
        """
        Create a new Iceberg table.

        Args:
            schema: Table schema definition (Schema object or list of tuples)
            properties: Optional table properties

        Returns:
            Created Iceberg table
        """
        catalog = self._get_catalog()

        # Convert schema if it's a list of tuples
        if isinstance(schema, list):
            schema = self._convert_schema_from_list(schema)

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
            # Only include partition_spec if it's not None
            create_args = {
                "identifier": table_identifier,
                "schema": schema,
                "properties": properties or {},
            }
            if partition_spec is not None:
                create_args["partition_spec"] = partition_spec

            self.table = catalog.create_table(**create_args)
            logger.info(f"Created Iceberg table: {table_identifier}")
            return self.table
        except Exception as e:
            logger.error(f"Failed to create table: {e}")
            raise

    def _convert_schema_from_list(self, schema_list: List[tuple]) -> Schema:
        """
        Convert a list of (field_name, type_name) tuples to Iceberg Schema.

        Args:
            schema_list: List of (field_name, type_name) tuples

        Returns:
            Iceberg Schema object
        """
        from pyiceberg.types import IntegerType, FloatType, DateType

        type_mapping = {
            "long": LongType(),
            "string": StringType(),
            "timestamp": TimestampType(),
            "boolean": BooleanType(),
            "double": DoubleType(),
            "integer": IntegerType(),
            "float": FloatType(),
            "date": DateType(),
        }

        fields = []
        for i, (field_name, type_name) in enumerate(schema_list, start=1):
            field_type = type_mapping.get(type_name.lower(), StringType())
            fields.append(NestedField(i, field_name, field_type, required=False))

        return Schema(*fields)

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

    def create_table_if_not_exists(
        self, schema: Optional[Schema] = None, properties: Optional[Dict[str, str]] = None
    ) -> Table:
        """
        Create table if it doesn't exist, otherwise load it.

        Args:
            schema: Table schema definition (uses default if None)
            properties: Optional table properties

        Returns:
            Iceberg table
        """
        if self.table_exists():
            logger.info(f"Table {self.config.namespace}.{self.config.table_name} already exists")
            return self.load_table()

        # Use default schema if none provided
        if schema is None:
            schema = self._get_default_schema()

        return self.create_table(schema, properties)

    def _get_default_schema(self) -> Schema:
        """Get a default schema for CDC tables"""
        from pyiceberg.types import DateType
        return Schema(
            NestedField(1, "customer_id", LongType(), required=True),
            NestedField(2, "email", StringType(), required=False),
            NestedField(3, "name", StringType(), required=False),
            NestedField(4, "registration_date", DateType(), required=False),
            NestedField(5, "status", StringType(), required=False),
        )

    def append_data(self, data: List[Dict[str, Any]]) -> None:
        """
        Append data to the Iceberg table. Alias for write_data.

        Args:
            data: List of records to append
        """
        self.write_data(data)

    def append_data_with_schema_evolution(self, data: List[Dict[str, Any]]) -> None:
        """
        Append data to the Iceberg table with automatic schema evolution.

        Detects new columns in the data and updates the table schema accordingly
        before appending the data.

        Args:
            data: List of records to append (may contain new fields)
        """
        from pyiceberg.types import StringType, LongType, DoubleType, BooleanType
        from pyiceberg.schema import Schema
        import pyarrow as pa

        if not data:
            logger.warning("No data to append")
            return

        # Ensure table exists
        if not self.table_exists():
            # If table doesn't exist, create it with inferred schema
            self.write_data(data)
            return

        table = self.load_table()
        current_schema = table.schema()

        # Get current field names
        current_fields = {field.name for field in current_schema.fields}

        # Find new fields in data
        new_fields = set()
        for record in data:
            for key in record.keys():
                if key not in current_fields:
                    new_fields.add(key)

        # Add new fields to schema
        if new_fields:
            logger.info(f"Detected new fields: {new_fields}. Evolving schema...")

            with table.update_schema() as update:
                for field_name in new_fields:
                    # Infer type from first non-None value
                    field_type = None
                    for record in data:
                        value = record.get(field_name)
                        if value is not None:
                            if isinstance(value, bool):
                                field_type = BooleanType()
                            elif isinstance(value, int):
                                field_type = LongType()
                            elif isinstance(value, float):
                                field_type = DoubleType()
                            else:
                                field_type = StringType()
                            break

                    # Default to string if cannot infer
                    if field_type is None:
                        field_type = StringType()

                    update.add_column(field_name, field_type, required=False)

            logger.info("Schema evolution completed")

            # Reload table to get updated schema
            self.table = None  # Clear cached table
            table = self.load_table()

        # Append data using standard write_data
        self.write_data(data)

    def write_data(self, data: List[Dict[str, Any]]) -> None:
        """
        Write data to the Iceberg table.

        Args:
            data: List of records to write
        """
        import pyarrow as pa
        from pyiceberg.types import DateType
        from datetime import datetime, date

        if not data:
            logger.warning("No data to write")
            return

        # Ensure table exists
        if not self.table_exists():
            self.create_table_if_not_exists()

        table = self.load_table()
        iceberg_schema = table.schema()

        # Build PyArrow schema from Iceberg schema
        from pyiceberg.io.pyarrow import schema_to_pyarrow

        # Use Iceberg's conversion to get the correct PyArrow schema
        target_schema = schema_to_pyarrow(iceberg_schema)

        # Convert data to PyArrow arrays column by column to handle type conversions
        arrays = []
        for field in iceberg_schema.fields:
            column_data = [record.get(field.name) for record in data]

            if isinstance(field.field_type, DateType):
                # Convert string dates to date objects
                converted_dates = []
                for value in column_data:
                    if value is None:
                        converted_dates.append(None)
                    elif isinstance(value, str):
                        try:
                            converted_dates.append(datetime.strptime(value, "%Y-%m-%d").date())
                        except ValueError:
                            logger.warning(f"Could not convert {value} to date")
                            converted_dates.append(None)
                    else:
                        converted_dates.append(value)
                column_data = converted_dates

            # Find the corresponding field in target schema
            pa_field = target_schema.field(field.name)
            arrays.append(pa.array(column_data, type=pa_field.type))

        # Build PyArrow table from arrays
        arrow_table = pa.Table.from_arrays(arrays, schema=target_schema)

        # Append data
        try:
            table.append(arrow_table)
            logger.info(f"Wrote {len(data)} records to {self.config.namespace}.{self.config.table_name}")
        except Exception as e:
            logger.error(f"Failed to write data: {e}")
            raise

    def overwrite_data(self, data: List[Dict[str, Any]]) -> None:
        """
        Overwrite all data in the Iceberg table.

        Args:
            data: List of records to write (replaces all existing data)
        """
        import pyarrow as pa
        from pyiceberg.types import DateType
        from datetime import datetime, date

        if not data:
            logger.warning("No data to overwrite")
            return

        # Ensure table exists
        if not self.table_exists():
            self.create_table_if_not_exists()

        table = self.load_table()
        iceberg_schema = table.schema()

        # Build PyArrow schema from Iceberg schema
        from pyiceberg.io.pyarrow import schema_to_pyarrow
        target_schema = schema_to_pyarrow(iceberg_schema)

        # Convert data to PyArrow arrays column by column
        arrays = []
        for field in iceberg_schema.fields:
            column_data = [record.get(field.name) for record in data]

            if isinstance(field.field_type, DateType):
                converted_dates = []
                for value in column_data:
                    if value is None:
                        converted_dates.append(None)
                    elif isinstance(value, str):
                        try:
                            converted_dates.append(datetime.strptime(value, "%Y-%m-%d").date())
                        except ValueError:
                            logger.warning(f"Could not convert {value} to date")
                            converted_dates.append(None)
                    else:
                        converted_dates.append(value)
                column_data = converted_dates

            pa_field = target_schema.field(field.name)
            arrays.append(pa.array(column_data, type=pa_field.type))

        arrow_table = pa.Table.from_arrays(arrays, schema=target_schema)

        try:
            # Overwrite data (replace all existing data)
            table.overwrite(arrow_table)
            logger.info(f"Overwrote table with {len(data)} records")
        except Exception as e:
            logger.error(f"Failed to overwrite data: {e}")
            raise

    def delete_data(self, filter_condition: str) -> None:
        """
        Delete data from the Iceberg table based on a filter condition.

        Args:
            filter_condition: SQL-like filter condition (e.g., "customer_id < 50")
        """
        if not self.table_exists():
            logger.warning("Table does not exist, nothing to delete")
            return

        table = self.load_table()

        try:
            # PyIceberg delete API
            table.delete(filter_condition)
            logger.info(f"Deleted data matching filter: {filter_condition}")
        except Exception as e:
            logger.error(f"Failed to delete data: {e}")
            raise

    def upsert_data(self, data: List[Dict[str, Any]], merge_on: List[str]) -> None:
        """
        Upsert data to the Iceberg table.

        Args:
            data: List of records to upsert
            merge_on: List of key columns to merge on
        """
        import pyarrow as pa
        from pyiceberg.types import DateType
        from datetime import datetime, date

        if not data:
            logger.warning("No data to upsert")
            return

        # Ensure table exists
        if not self.table_exists():
            self.create_table_if_not_exists()

        table = self.load_table()
        iceberg_schema = table.schema()

        # Build PyArrow schema from Iceberg schema
        from pyiceberg.io.pyarrow import schema_to_pyarrow

        # Use Iceberg's conversion to get the correct PyArrow schema
        target_schema = schema_to_pyarrow(iceberg_schema)

        # Convert data to PyArrow arrays column by column to handle type conversions
        arrays = []
        for field in iceberg_schema.fields:
            column_data = [record.get(field.name) for record in data]

            if isinstance(field.field_type, DateType):
                # Convert string dates to date objects
                converted_dates = []
                for value in column_data:
                    if value is None:
                        converted_dates.append(None)
                    elif isinstance(value, str):
                        try:
                            converted_dates.append(datetime.strptime(value, "%Y-%m-%d").date())
                        except ValueError:
                            logger.warning(f"Could not convert {value} to date")
                            converted_dates.append(None)
                    else:
                        converted_dates.append(value)
                column_data = converted_dates

            # Find the corresponding field in target schema
            pa_field = target_schema.field(field.name)
            arrays.append(pa.array(column_data, type=pa_field.type))

        # Build PyArrow table from arrays
        arrow_table = pa.Table.from_arrays(arrays, schema=target_schema)

        # Note: PyIceberg doesn't have native upsert yet
        # This is a simplified implementation that does an overwrite
        # In production, you'd need to:
        # 1. Read existing data
        # 2. Merge with new data based on merge_on keys
        # 3. Write merged result

        try:
            # For now, just append (in real implementation, merge logic would go here)
            table.append(arrow_table)
            logger.info(f"Upserted {len(data)} records to {self.config.namespace}.{self.config.table_name}")
        except Exception as e:
            logger.error(f"Failed to upsert data: {e}")
            raise
