"""Schema validation for CDC pipelines."""

from typing import Any, Dict, List, Optional, Set

from src.observability.logging_config import get_logger
from src.validation import ValidationResult, ValidationStatus

logger = get_logger(__name__)


class SchemaValidator:
    """Validates schema compatibility and evolution."""

    def __init__(self, name: str = "SchemaValidator") -> None:
        """
        Initialize schema validator.

        Args:
            name: Validator name
        """
        self.name = name

    def validate_schema_compatibility(
        self, source_schema: Dict[str, str], destination_schema: Dict[str, str]
    ) -> ValidationResult:
        """
        Validate that destination schema is compatible with source.

        Args:
            source_schema: Source schema (field_name -> data_type)
            destination_schema: Destination schema (field_name -> data_type)

        Returns:
            Validation result
        """
        missing_fields = set(source_schema.keys()) - set(destination_schema.keys())
        extra_fields = set(destination_schema.keys()) - set(source_schema.keys())
        type_mismatches = []

        # Check for type mismatches in common fields
        common_fields = set(source_schema.keys()) & set(destination_schema.keys())
        for field in common_fields:
            source_type = source_schema[field]
            dest_type = destination_schema[field]
            if not self._are_types_compatible(source_type, dest_type):
                type_mismatches.append(
                    {
                        "field": field,
                        "source_type": source_type,
                        "destination_type": dest_type,
                    }
                )

        # Determine overall status
        if not missing_fields and not type_mismatches:
            return ValidationResult(
                validator=self.name,
                status=ValidationStatus.PASSED,
                message="Schema is fully compatible",
                details={
                    "common_fields": len(common_fields),
                    "extra_fields": list(extra_fields),
                },
            )
        elif missing_fields:
            return ValidationResult(
                validator=self.name,
                status=ValidationStatus.FAILED,
                message=f"Schema incompatible: {len(missing_fields)} missing fields",
                details={
                    "missing_fields": list(missing_fields),
                    "extra_fields": list(extra_fields),
                    "type_mismatches": type_mismatches,
                },
            )
        else:
            return ValidationResult(
                validator=self.name,
                status=ValidationStatus.FAILED,
                message=f"Schema incompatible: {len(type_mismatches)} type mismatches",
                details={
                    "missing_fields": list(missing_fields),
                    "extra_fields": list(extra_fields),
                    "type_mismatches": type_mismatches,
                },
            )

    def validate_schema_evolution(
        self, old_schema: Dict[str, str], new_schema: Dict[str, str]
    ) -> ValidationResult:
        """
        Validate schema evolution (detecting changes).

        Args:
            old_schema: Previous schema version
            new_schema: New schema version

        Returns:
            Validation result with evolution details
        """
        added_fields = set(new_schema.keys()) - set(old_schema.keys())
        removed_fields = set(old_schema.keys()) - set(new_schema.keys())
        modified_fields = []

        common_fields = set(old_schema.keys()) & set(new_schema.keys())
        for field in common_fields:
            if old_schema[field] != new_schema[field]:
                modified_fields.append(
                    {
                        "field": field,
                        "old_type": old_schema[field],
                        "new_type": new_schema[field],
                    }
                )

        has_changes = bool(added_fields or removed_fields or modified_fields)

        if not has_changes:
            return ValidationResult(
                validator=self.name,
                status=ValidationStatus.PASSED,
                message="No schema changes detected",
                details={"schema_fields": len(old_schema)},
            )
        else:
            # Schema evolution detected
            changes = {
                "added_fields": list(added_fields),
                "removed_fields": list(removed_fields),
                "modified_fields": modified_fields,
            }

            # Determine if evolution is safe (backward compatible)
            is_safe = not removed_fields and all(
                self._is_safe_type_change(m["old_type"], m["new_type"])
                for m in modified_fields
            )

            status = ValidationStatus.WARNING if is_safe else ValidationStatus.FAILED
            message = (
                "Safe schema evolution detected"
                if is_safe
                else "Breaking schema evolution detected"
            )

            return ValidationResult(
                validator=self.name, status=status, message=message, details=changes
            )

    def validate_row_schema(
        self, row: Dict[str, Any], expected_schema: Dict[str, str]
    ) -> ValidationResult:
        """
        Validate that a row matches expected schema.

        Args:
            row: Data row
            expected_schema: Expected schema

        Returns:
            Validation result
        """
        row_fields = set(row.keys())
        schema_fields = set(expected_schema.keys())

        missing_fields = schema_fields - row_fields
        extra_fields = row_fields - schema_fields
        type_errors = []

        # Validate types for common fields
        for field in row_fields & schema_fields:
            expected_type = expected_schema[field]
            actual_value = row[field]

            if actual_value is not None and not self._validate_type(
                actual_value, expected_type
            ):
                type_errors.append(
                    {
                        "field": field,
                        "expected_type": expected_type,
                        "actual_type": type(actual_value).__name__,
                    }
                )

        if not missing_fields and not type_errors:
            return ValidationResult(
                validator=self.name,
                status=ValidationStatus.PASSED,
                message="Row schema is valid",
                details={"validated_fields": len(row_fields)},
            )
        else:
            return ValidationResult(
                validator=self.name,
                status=ValidationStatus.FAILED,
                message="Row schema validation failed",
                details={
                    "missing_fields": list(missing_fields),
                    "extra_fields": list(extra_fields),
                    "type_errors": type_errors,
                },
            )

    def _are_types_compatible(self, source_type: str, dest_type: str) -> bool:
        """
        Check if two types are compatible.

        Args:
            source_type: Source data type
            dest_type: Destination data type

        Returns:
            True if compatible
        """
        # Exact match
        if source_type == dest_type:
            return True

        # Define compatible type mappings
        compatible_types = {
            "INTEGER": ["BIGINT", "INT", "SMALLINT", "NUMBER"],
            "BIGINT": ["INTEGER", "NUMBER"],
            "VARCHAR": ["TEXT", "STRING", "CHAR"],
            "TEXT": ["VARCHAR", "STRING"],
            "DECIMAL": ["NUMERIC", "FLOAT", "DOUBLE", "NUMBER"],
            "TIMESTAMP": ["DATETIME", "DATE"],
        }

        source_normalized = source_type.upper().split("(")[0]
        dest_normalized = dest_type.upper().split("(")[0]

        return dest_normalized in compatible_types.get(source_normalized, [])

    def _is_safe_type_change(self, old_type: str, new_type: str) -> bool:
        """
        Check if type change is safe (backward compatible).

        Args:
            old_type: Old data type
            new_type: New data type

        Returns:
            True if change is safe
        """
        # Widening conversions are generally safe
        safe_changes = {
            "INTEGER": ["BIGINT"],
            "SMALLINT": ["INTEGER", "BIGINT"],
            "VARCHAR": ["TEXT"],
            "CHAR": ["VARCHAR", "TEXT"],
            "DECIMAL": ["NUMERIC"],
        }

        old_normalized = old_type.upper().split("(")[0]
        new_normalized = new_type.upper().split("(")[0]

        return new_normalized in safe_changes.get(old_normalized, [])

    def _validate_type(self, value: Any, expected_type: str) -> bool:
        """
        Validate value matches expected type.

        Args:
            value: Value to check
            expected_type: Expected type string

        Returns:
            True if type matches
        """
        type_mapping = {
            "INTEGER": (int,),
            "BIGINT": (int,),
            "SMALLINT": (int,),
            "VARCHAR": (str,),
            "TEXT": (str,),
            "CHAR": (str,),
            "DECIMAL": (float, int),
            "NUMERIC": (float, int),
            "FLOAT": (float, int),
            "DOUBLE": (float, int),
            "BOOLEAN": (bool,),
            "TIMESTAMP": (str,),  # Usually ISO string
            "DATE": (str,),
            "JSON": (dict, list, str),
        }

        expected_normalized = expected_type.upper().split("(")[0]
        expected_python_types = type_mapping.get(expected_normalized, (object,))

        return isinstance(value, expected_python_types)
