"""Data integrity validators for CDC pipelines."""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

from src.common.utils import calculate_checksum, calculate_row_checksum
from src.observability.logging_config import get_logger
from src.validation import ValidationResult, ValidationStatus

logger = get_logger(__name__)


class IntegrityValidator(ABC):
    """Base class for integrity validators."""

    def __init__(self, name: str) -> None:
        """
        Initialize integrity validator.

        Args:
            name: Validator name
        """
        self.name = name

    @abstractmethod
    def validate(self, source_data: Any, destination_data: Any) -> ValidationResult:
        """
        Validate data integrity.

        Args:
            source_data: Source data
            destination_data: Destination data

        Returns:
            Validation result
        """
        pass


class RowCountValidator(IntegrityValidator):
    """Validates row count consistency between source and destination."""

    def __init__(self, tolerance: int = 0) -> None:
        """
        Initialize row count validator.

        Args:
            tolerance: Allowed difference in row counts
        """
        super().__init__("RowCountValidator")
        self.tolerance = tolerance

    def validate(
        self, source_data: List[Dict[str, Any]], destination_data: List[Dict[str, Any]]
    ) -> ValidationResult:
        """Validate row counts match within tolerance."""
        source_count = len(source_data) if isinstance(source_data, list) else source_data
        dest_count = (
            len(destination_data) if isinstance(destination_data, list) else destination_data
        )

        difference = abs(source_count - dest_count)

        if difference <= self.tolerance:
            return ValidationResult(
                validator=self.name,
                status=ValidationStatus.PASSED,
                message=f"Row counts match: source={source_count}, dest={dest_count}",
                details={
                    "source_count": source_count,
                    "destination_count": dest_count,
                    "difference": difference,
                },
            )
        else:
            return ValidationResult(
                validator=self.name,
                status=ValidationStatus.FAILED,
                message=f"Row count mismatch: source={source_count}, dest={dest_count}, diff={difference}",
                details={
                    "source_count": source_count,
                    "destination_count": dest_count,
                    "difference": difference,
                    "tolerance": self.tolerance,
                },
            )


class ChecksumValidator(IntegrityValidator):
    """Validates data integrity using checksums."""

    def __init__(self, exclude_fields: Optional[List[str]] = None) -> None:
        """
        Initialize checksum validator.

        Args:
            exclude_fields: Fields to exclude from checksum calculation
        """
        super().__init__("ChecksumValidator")
        self.exclude_fields = exclude_fields or ["created_at", "updated_at"]

    def validate(
        self, source_data: List[Dict[str, Any]], destination_data: List[Dict[str, Any]]
    ) -> ValidationResult:
        """Validate data using checksums."""
        if len(source_data) != len(destination_data):
            return ValidationResult(
                validator=self.name,
                status=ValidationStatus.FAILED,
                message="Cannot compare checksums: row counts differ",
                details={
                    "source_count": len(source_data),
                    "destination_count": len(destination_data),
                },
            )

        mismatches = []
        for i, (source_row, dest_row) in enumerate(zip(source_data, destination_data)):
            source_checksum = calculate_row_checksum(source_row, self.exclude_fields)
            dest_checksum = calculate_row_checksum(dest_row, self.exclude_fields)

            if source_checksum != dest_checksum:
                mismatches.append(
                    {
                        "row_index": i,
                        "source_checksum": source_checksum,
                        "dest_checksum": dest_checksum,
                    }
                )

        if not mismatches:
            return ValidationResult(
                validator=self.name,
                status=ValidationStatus.PASSED,
                message=f"All {len(source_data)} row checksums match",
                details={"validated_rows": len(source_data)},
            )
        else:
            return ValidationResult(
                validator=self.name,
                status=ValidationStatus.FAILED,
                message=f"Found {len(mismatches)} checksum mismatches",
                details={
                    "total_rows": len(source_data),
                    "mismatches": len(mismatches),
                    "first_mismatch": mismatches[0] if mismatches else None,
                },
            )


class FieldValidator(IntegrityValidator):
    """Validates specific field values and types."""

    def __init__(self, field_name: str, required: bool = True) -> None:
        """
        Initialize field validator.

        Args:
            field_name: Field to validate
            required: Whether field is required
        """
        super().__init__(f"FieldValidator({field_name})")
        self.field_name = field_name
        self.required = required

    def validate(
        self, source_data: List[Dict[str, Any]], destination_data: List[Dict[str, Any]]
    ) -> ValidationResult:
        """Validate field presence and consistency."""
        issues = []

        for i, (source_row, dest_row) in enumerate(zip(source_data, destination_data)):
            # Check field presence
            source_has_field = self.field_name in source_row
            dest_has_field = self.field_name in dest_row

            if self.required:
                if not source_has_field:
                    issues.append(
                        {"row": i, "issue": "field_missing_in_source", "field": self.field_name}
                    )
                if not dest_has_field:
                    issues.append(
                        {
                            "row": i,
                            "issue": "field_missing_in_destination",
                            "field": self.field_name,
                        }
                    )

            # Check value equality (if field exists in both)
            if source_has_field and dest_has_field:
                source_value = source_row[self.field_name]
                dest_value = dest_row[self.field_name]

                if source_value != dest_value:
                    issues.append(
                        {
                            "row": i,
                            "issue": "field_value_mismatch",
                            "field": self.field_name,
                            "source_value": source_value,
                            "dest_value": dest_value,
                        }
                    )

        if not issues:
            return ValidationResult(
                validator=self.name,
                status=ValidationStatus.PASSED,
                message=f"Field '{self.field_name}' validated successfully",
                details={"validated_rows": len(source_data)},
            )
        else:
            return ValidationResult(
                validator=self.name,
                status=ValidationStatus.FAILED,
                message=f"Found {len(issues)} issues with field '{self.field_name}'",
                details={"issues": issues[:10]},  # Limit to first 10 issues
            )


class ReferentialIntegrityValidator(IntegrityValidator):
    """Validates referential integrity between tables."""

    def __init__(
        self, child_table: str, parent_table: str, foreign_key: str, parent_key: str = "id"
    ) -> None:
        """
        Initialize referential integrity validator.

        Args:
            child_table: Child table name
            parent_table: Parent table name
            foreign_key: Foreign key field in child table
            parent_key: Primary key field in parent table
        """
        super().__init__(f"ReferentialIntegrity({child_table}->{parent_table})")
        self.child_table = child_table
        self.parent_table = parent_table
        self.foreign_key = foreign_key
        self.parent_key = parent_key

    def validate(
        self, child_data: List[Dict[str, Any]], parent_data: List[Dict[str, Any]]
    ) -> ValidationResult:
        """Validate referential integrity."""
        # Build set of valid parent keys
        parent_keys = {row.get(self.parent_key) for row in parent_data if self.parent_key in row}

        # Check for orphaned child records
        orphans = []
        for i, child_row in enumerate(child_data):
            if self.foreign_key in child_row:
                fk_value = child_row[self.foreign_key]
                if fk_value not in parent_keys:
                    orphans.append(
                        {
                            "row_index": i,
                            "foreign_key_value": fk_value,
                            "child_data": child_row,
                        }
                    )

        if not orphans:
            return ValidationResult(
                validator=self.name,
                status=ValidationStatus.PASSED,
                message=f"All {len(child_data)} child records have valid parent references",
                details={
                    "child_records": len(child_data),
                    "parent_records": len(parent_data),
                },
            )
        else:
            return ValidationResult(
                validator=self.name,
                status=ValidationStatus.FAILED,
                message=f"Found {len(orphans)} orphaned records",
                details={
                    "child_records": len(child_data),
                    "parent_records": len(parent_data),
                    "orphaned_count": len(orphans),
                    "first_orphan": orphans[0] if orphans else None,
                },
            )
