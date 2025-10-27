"""Data validation module for CDC pipelines."""

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional


class ValidationStatus(str, Enum):
    """Validation status enum."""

    PASSED = "passed"
    FAILED = "failed"
    WARNING = "warning"
    SKIPPED = "skipped"


@dataclass
class ValidationResult:
    """Result of a single validation check."""

    validator: str
    status: ValidationStatus
    message: str
    details: Optional[Dict[str, Any]] = None
    checked_at: Optional[datetime] = None

    def __post_init__(self) -> None:
        """Set default checked_at if not provided."""
        if self.checked_at is None:
            self.checked_at = datetime.now()


@dataclass
class ValidationReport:
    """Comprehensive validation report."""

    pipeline: str
    results: List[ValidationResult]
    overall_status: ValidationStatus
    started_at: datetime
    completed_at: Optional[datetime] = None

    @property
    def passed_count(self) -> int:
        """Count of passed validations."""
        return sum(1 for r in self.results if r.status == ValidationStatus.PASSED)

    @property
    def failed_count(self) -> int:
        """Count of failed validations."""
        return sum(1 for r in self.results if r.status == ValidationStatus.FAILED)

    @property
    def warning_count(self) -> int:
        """Count of warning validations."""
        return sum(1 for r in self.results if r.status == ValidationStatus.WARNING)

    @property
    def skipped_count(self) -> int:
        """Count of skipped validations."""
        return sum(1 for r in self.results if r.status == ValidationStatus.SKIPPED)

    @property
    def total_count(self) -> int:
        """Total validation count."""
        return len(self.results)

    def to_dict(self) -> Dict[str, Any]:
        """Convert report to dictionary."""
        return {
            "pipeline": self.pipeline,
            "overall_status": self.overall_status.value,
            "started_at": self.started_at.isoformat(),
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "summary": {
                "total": self.total_count,
                "passed": self.passed_count,
                "failed": self.failed_count,
                "warning": self.warning_count,
                "skipped": self.skipped_count,
            },
            "results": [
                {
                    "validator": r.validator,
                    "status": r.status.value,
                    "message": r.message,
                    "details": r.details,
                    "checked_at": r.checked_at.isoformat() if r.checked_at else None,
                }
                for r in self.results
            ],
        }


__all__ = ["ValidationStatus", "ValidationResult", "ValidationReport"]
