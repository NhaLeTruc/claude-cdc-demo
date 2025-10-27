"""Validation orchestrator for running multiple validators."""

from datetime import datetime
from typing import List, Optional

from src.observability.logging_config import get_logger
from src.observability.metrics import MetricsExporter
from src.validation import ValidationReport, ValidationResult, ValidationStatus
from src.validation.integrity import IntegrityValidator

logger = get_logger(__name__)


class ValidationOrchestrator:
    """Orchestrates multiple validators for a CDC pipeline."""

    def __init__(
        self,
        pipeline_name: str,
        validators: Optional[List[IntegrityValidator]] = None,
        metrics_exporter: Optional[MetricsExporter] = None,
    ) -> None:
        """
        Initialize validation orchestrator.

        Args:
            pipeline_name: Name of the pipeline being validated
            validators: List of validators to run
            metrics_exporter: Optional metrics exporter
        """
        self.pipeline_name = pipeline_name
        self.validators = validators or []
        self.metrics_exporter = metrics_exporter
        logger.info(
            f"Initialized ValidationOrchestrator for {pipeline_name} with {len(self.validators)} validators"
        )

    def add_validator(self, validator: IntegrityValidator) -> None:
        """
        Add a validator to the orchestrator.

        Args:
            validator: Validator to add
        """
        self.validators.append(validator)
        logger.debug(f"Added validator: {validator.name}")

    def validate_all(self, source_data: any, destination_data: any) -> ValidationReport:
        """
        Run all registered validators.

        Args:
            source_data: Source data to validate
            destination_data: Destination data to validate

        Returns:
            Comprehensive validation report
        """
        started_at = datetime.now()
        results: List[ValidationResult] = []

        logger.info(f"Starting validation for pipeline: {self.pipeline_name}")

        for validator in self.validators:
            try:
                logger.debug(f"Running validator: {validator.name}")
                result = validator.validate(source_data, destination_data)
                results.append(result)

                # Record metrics
                if self.metrics_exporter:
                    self.metrics_exporter.record_validation(
                        validator=validator.name,
                        success=(result.status == ValidationStatus.PASSED),
                        failure_type=(
                            result.status.value
                            if result.status != ValidationStatus.PASSED
                            else None
                        ),
                    )

                logger.info(f"{validator.name}: {result.status.value} - {result.message}")

            except Exception as e:
                logger.error(f"Validator {validator.name} failed with error: {e}", exc_info=True)
                results.append(
                    ValidationResult(
                        validator=validator.name,
                        status=ValidationStatus.FAILED,
                        message=f"Validator error: {str(e)}",
                        details={"error_type": type(e).__name__, "error_message": str(e)},
                    )
                )

                if self.metrics_exporter:
                    self.metrics_exporter.record_validation(
                        validator=validator.name, success=False, failure_type="exception"
                    )

        # Determine overall status
        overall_status = self._determine_overall_status(results)

        completed_at = datetime.now()
        report = ValidationReport(
            pipeline=self.pipeline_name,
            results=results,
            overall_status=overall_status,
            started_at=started_at,
            completed_at=completed_at,
        )

        duration = (completed_at - started_at).total_seconds()
        logger.info(
            f"Validation complete for {self.pipeline_name}: "
            f"{overall_status.value} ({report.passed_count}/{report.total_count} passed, "
            f"duration: {duration:.2f}s)"
        )

        return report

    def validate_pipeline_health(self) -> ValidationStatus:
        """
        Quick health check validation.

        Returns:
            Overall health status
        """
        # This is a simplified check - can be extended
        if not self.validators:
            logger.warning(f"No validators registered for pipeline: {self.pipeline_name}")
            return ValidationStatus.WARNING

        return ValidationStatus.PASSED

    def _determine_overall_status(self, results: List[ValidationResult]) -> ValidationStatus:
        """
        Determine overall validation status from individual results.

        Args:
            results: List of validation results

        Returns:
            Overall status
        """
        if not results:
            return ValidationStatus.SKIPPED

        # If any failed, overall is failed
        if any(r.status == ValidationStatus.FAILED for r in results):
            return ValidationStatus.FAILED

        # If any warning, overall is warning
        if any(r.status == ValidationStatus.WARNING for r in results):
            return ValidationStatus.WARNING

        # If any skipped and none failed/warning
        if any(r.status == ValidationStatus.SKIPPED for r in results):
            # If all skipped
            if all(r.status == ValidationStatus.SKIPPED for r in results):
                return ValidationStatus.SKIPPED
            # Some passed, some skipped
            return ValidationStatus.PASSED

        # All passed
        return ValidationStatus.PASSED

    def get_validator_count(self) -> int:
        """Get number of registered validators."""
        return len(self.validators)

    def clear_validators(self) -> None:
        """Remove all validators."""
        self.validators.clear()
        logger.info(f"Cleared all validators for pipeline: {self.pipeline_name}")
