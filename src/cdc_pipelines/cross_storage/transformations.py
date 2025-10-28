"""
Data transformation module for cross-storage CDC pipeline.

This module provides transformation functions for converting data
from Postgres CDC format to Iceberg-optimized format.
"""

import logging
from typing import Dict, Any, Optional, List
from datetime import datetime

try:
    import pyarrow as pa
    import pyarrow.compute as pc
    PYARROW_AVAILABLE = True
except ImportError:
    PYARROW_AVAILABLE = False


logger = logging.getLogger(__name__)


class DataTransformer:
    """
    Transforms CDC data for cross-storage replication.

    Provides transformation functions for data enrichment,
    field derivation, and format conversion.
    """

    def __init__(self):
        """Initialize DataTransformer"""
        if not PYARROW_AVAILABLE:
            raise ImportError("PyArrow is required for data transformations")

        logger.info("Initialized DataTransformer")

    def transform_customer_data(
        self, cdc_event: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """
        Transform customer CDC event to analytics format.

        Transformations applied:
        - Concatenate first_name + last_name → full_name
        - Derive location from city + state + country
        - Add ingestion_timestamp
        - Extract source system metadata

        Args:
            cdc_event: Debezium CDC event

        Returns:
            Transformed record or None if event should be filtered
        """
        try:
            # Extract operation type
            operation = cdc_event.get("op", "u")

            # Skip delete operations for analytics (retain only active records)
            if operation == "d":
                logger.debug("Skipping DELETE operation")
                return None

            # Get after state (for INSERT and UPDATE)
            after = cdc_event.get("after", {})
            if not after:
                logger.warning("No 'after' state in CDC event")
                return None

            # Build transformed record
            transformed = {
                "customer_id": after.get("customer_id"),
                "email": after.get("email"),
                "full_name": self._concatenate_name(
                    after.get("first_name"), after.get("last_name")
                ),
                "location": self._derive_location(
                    after.get("city"),
                    after.get("state"),
                    after.get("country")
                ),
                "customer_tier": after.get("customer_tier"),
                "lifetime_value": after.get("lifetime_value", 0.0),
                "registration_date": self._parse_timestamp(
                    after.get("registration_date")
                ),
                "is_active": after.get("is_active", True),
                "total_orders": 0,  # Placeholder for join enrichment
                "_ingestion_timestamp": datetime.now().isoformat(),
                "_source_system": "postgres_cdc",
                "_cdc_operation": self._map_operation(operation),
            }

            return transformed

        except Exception as e:
            logger.error(f"Error transforming customer data: {e}")
            return None

    def _concatenate_name(
        self, first_name: Optional[str], last_name: Optional[str]
    ) -> str:
        """Concatenate first and last name"""
        if first_name and last_name:
            return f"{first_name} {last_name}"
        elif first_name:
            return first_name
        elif last_name:
            return last_name
        else:
            return "Unknown"

    def _derive_location(
        self,
        city: Optional[str],
        state: Optional[str],
        country: Optional[str]
    ) -> str:
        """Derive location string from components"""
        parts = []

        if city:
            parts.append(city)
        if state:
            parts.append(state)
        if country:
            parts.append(country)

        return ", ".join(parts) if parts else "Unknown"

    def _parse_timestamp(self, timestamp_value: Any) -> Optional[str]:
        """Parse timestamp to ISO format"""
        if not timestamp_value:
            return None

        try:
            if isinstance(timestamp_value, (int, float)):
                # Assume milliseconds since epoch
                dt = datetime.fromtimestamp(timestamp_value / 1000)
                return dt.isoformat()
            elif isinstance(timestamp_value, str):
                # Already a string, return as-is
                return timestamp_value
            elif isinstance(timestamp_value, datetime):
                return timestamp_value.isoformat()
            else:
                return str(timestamp_value)
        except Exception as e:
            logger.warning(f"Failed to parse timestamp {timestamp_value}: {e}")
            return None

    def _map_operation(self, debezium_op: str) -> str:
        """Map Debezium operation to standard CDC operation"""
        mapping = {
            "c": "INSERT",
            "u": "UPDATE",
            "d": "DELETE",
            "r": "SNAPSHOT",
        }
        return mapping.get(debezium_op, "UNKNOWN")

    def transform_batch(
        self, cdc_events: List[Dict[str, Any]]
    ) -> pa.Table:
        """
        Transform a batch of CDC events to PyArrow Table.

        Args:
            cdc_events: List of Debezium CDC events

        Returns:
            PyArrow Table with transformed data
        """
        transformed_records = []

        for event in cdc_events:
            transformed = self.transform_customer_data(event)
            if transformed:
                transformed_records.append(transformed)

        if not transformed_records:
            # Return empty table with schema
            schema = pa.schema([
                ("customer_id", pa.int64()),
                ("email", pa.string()),
                ("full_name", pa.string()),
                ("location", pa.string()),
                ("customer_tier", pa.string()),
                ("lifetime_value", pa.float64()),
                ("registration_date", pa.string()),
                ("is_active", pa.bool_()),
                ("total_orders", pa.int64()),
                ("_ingestion_timestamp", pa.string()),
                ("_source_system", pa.string()),
                ("_cdc_operation", pa.string()),
            ])
            return pa.table({col: [] for col in schema.names}, schema=schema)

        # Convert to PyArrow Table
        return pa.table(transformed_records)

    def enrich_with_aggregates(
        self,
        customer_data: pa.Table,
        orders_data: Optional[pa.Table] = None
    ) -> pa.Table:
        """
        Enrich customer data with aggregated metrics.

        Args:
            customer_data: Customer data table
            orders_data: Optional orders data for aggregation

        Returns:
            Enriched PyArrow Table
        """
        if orders_data is None or orders_data.num_rows == 0:
            return customer_data

        # Count orders per customer
        # Note: This is a simplified example - production would use Spark/DuckDB
        logger.info("Enriching customer data with order counts")

        # In production, this would be a proper join operation
        # For now, just return the customer data
        return customer_data

    def apply_business_rules(
        self, data: pa.Table
    ) -> pa.Table:
        """
        Apply business rules and data quality checks.

        Args:
            data: Input data table

        Returns:
            Filtered and validated data table
        """
        # Filter out invalid records
        valid_data = data

        # Rule 1: Email must be present
        valid_data = valid_data.filter(
            pc.is_valid(pc.field("email"))
        )

        # Rule 2: Lifetime value must be non-negative
        valid_data = valid_data.filter(
            pc.field("lifetime_value") >= 0
        )

        # Rule 3: Customer ID must be present
        valid_data = valid_data.filter(
            pc.is_valid(pc.field("customer_id"))
        )

        rows_filtered = data.num_rows - valid_data.num_rows
        if rows_filtered > 0:
            logger.info(f"Filtered {rows_filtered} invalid records")

        return valid_data

    def anonymize_pii(
        self, data: pa.Table, fields: List[str]
    ) -> pa.Table:
        """
        Anonymize PII fields for privacy compliance.

        Args:
            data: Input data table
            fields: List of field names to anonymize

        Returns:
            Data table with anonymized fields
        """
        import hashlib

        # Note: This is a basic example - production would use proper anonymization
        logger.info(f"Anonymizing PII fields: {fields}")

        # For demo, just mask email domains
        if "email" in fields and "email" in data.column_names:
            emails = data["email"].to_pylist()
            anonymized = [
                f"user_{hashlib.md5(email.encode()).hexdigest()[:8]}@masked.com"
                if email else None
                for email in emails
            ]

            # Replace email column
            data = data.drop(["email"])
            data = data.append_column("email", pa.array(anonymized))

        return data

    def add_metadata_columns(
        self,
        data: pa.Table,
        pipeline_id: str,
        batch_id: str
    ) -> pa.Table:
        """
        Add pipeline metadata columns.

        Args:
            data: Input data table
            pipeline_id: Pipeline identifier
            batch_id: Batch identifier

        Returns:
            Data table with metadata columns
        """
        num_rows = data.num_rows

        # Add metadata
        data = data.append_column(
            "_pipeline_id",
            pa.array([pipeline_id] * num_rows)
        )
        data = data.append_column(
            "_batch_id",
            pa.array([batch_id] * num_rows)
        )
        data = data.append_column(
            "_processed_timestamp",
            pa.array([datetime.now().isoformat()] * num_rows)
        )

        return data

    def validate_schema(
        self,
        data: pa.Table,
        expected_schema: pa.Schema
    ) -> bool:
        """
        Validate data schema against expected schema.

        Args:
            data: Data table to validate
            expected_schema: Expected schema

        Returns:
            True if schema is valid
        """
        try:
            # Check if all required fields are present
            for field in expected_schema:
                if field.name not in data.column_names:
                    logger.error(f"Missing required field: {field.name}")
                    return False

            # Check data types match
            for field in expected_schema:
                if field.name in data.column_names:
                    actual_type = data.schema.field(field.name).type
                    if actual_type != field.type:
                        logger.warning(
                            f"Type mismatch for {field.name}: "
                            f"expected {field.type}, got {actual_type}"
                        )
                        # Allow type mismatches with warning
                        # Production might be stricter

            return True

        except Exception as e:
            logger.error(f"Schema validation failed: {e}")
            return False
