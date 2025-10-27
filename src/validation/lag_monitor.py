"""CDC lag monitoring and validation."""

from datetime import datetime, timedelta
from typing import Optional

from src.common.config import get_settings
from src.observability.logging_config import get_logger
from src.validation import ValidationResult, ValidationStatus

logger = get_logger(__name__)


class LagMonitor:
    """Monitors CDC lag and validates against thresholds."""

    def __init__(self, threshold_seconds: Optional[int] = None) -> None:
        """
        Initialize lag monitor.

        Args:
            threshold_seconds: Maximum allowed lag in seconds (default from config)
        """
        settings = get_settings()
        self.threshold_seconds = (
            threshold_seconds or settings.app.cdc_lag_threshold_seconds
        )
        self.name = "LagMonitor"

    def calculate_lag(
        self, event_timestamp: datetime, processing_timestamp: Optional[datetime] = None
    ) -> float:
        """
        Calculate lag in seconds.

        Args:
            event_timestamp: When the event occurred
            processing_timestamp: When the event was processed (default: now)

        Returns:
            Lag in seconds
        """
        if processing_timestamp is None:
            processing_timestamp = datetime.now()

        lag = (processing_timestamp - event_timestamp).total_seconds()
        return max(0, lag)  # Ensure non-negative

    def validate_lag(
        self, event_timestamp: datetime, processing_timestamp: Optional[datetime] = None
    ) -> ValidationResult:
        """
        Validate CDC lag against threshold.

        Args:
            event_timestamp: Event timestamp
            processing_timestamp: Processing timestamp

        Returns:
            Validation result
        """
        lag = self.calculate_lag(event_timestamp, processing_timestamp)

        if lag <= self.threshold_seconds:
            return ValidationResult(
                validator=self.name,
                status=ValidationStatus.PASSED,
                message=f"CDC lag is acceptable: {lag:.2f}s (threshold: {self.threshold_seconds}s)",
                details={
                    "lag_seconds": lag,
                    "threshold_seconds": self.threshold_seconds,
                    "event_timestamp": event_timestamp.isoformat(),
                },
            )
        else:
            return ValidationResult(
                validator=self.name,
                status=ValidationStatus.FAILED,
                message=f"CDC lag exceeds threshold: {lag:.2f}s > {self.threshold_seconds}s",
                details={
                    "lag_seconds": lag,
                    "threshold_seconds": self.threshold_seconds,
                    "event_timestamp": event_timestamp.isoformat(),
                },
            )

    def validate_batch_lag(
        self, event_timestamps: list[datetime], processing_timestamp: Optional[datetime] = None
    ) -> ValidationResult:
        """
        Validate lag for a batch of events.

        Args:
            event_timestamps: List of event timestamps
            processing_timestamp: Processing timestamp

        Returns:
            Validation result with average, min, max lag
        """
        if not event_timestamps:
            return ValidationResult(
                validator=self.name,
                status=ValidationStatus.SKIPPED,
                message="No events to validate",
            )

        lags = [self.calculate_lag(ts, processing_timestamp) for ts in event_timestamps]
        avg_lag = sum(lags) / len(lags)
        min_lag = min(lags)
        max_lag = max(lags)

        if max_lag <= self.threshold_seconds:
            return ValidationResult(
                validator=self.name,
                status=ValidationStatus.PASSED,
                message=f"Batch CDC lag acceptable: avg={avg_lag:.2f}s, max={max_lag:.2f}s",
                details={
                    "event_count": len(event_timestamps),
                    "avg_lag_seconds": avg_lag,
                    "min_lag_seconds": min_lag,
                    "max_lag_seconds": max_lag,
                    "threshold_seconds": self.threshold_seconds,
                },
            )
        else:
            return ValidationResult(
                validator=self.name,
                status=ValidationStatus.FAILED,
                message=f"Batch CDC lag exceeds threshold: max={max_lag:.2f}s > {self.threshold_seconds}s",
                details={
                    "event_count": len(event_timestamps),
                    "avg_lag_seconds": avg_lag,
                    "min_lag_seconds": min_lag,
                    "max_lag_seconds": max_lag,
                    "threshold_seconds": self.threshold_seconds,
                },
            )

    def check_staleness(
        self, last_event_timestamp: datetime, staleness_threshold_minutes: int = 5
    ) -> ValidationResult:
        """
        Check if pipeline has gone stale (no recent events).

        Args:
            last_event_timestamp: Timestamp of last processed event
            staleness_threshold_minutes: Minutes without events before considering stale

        Returns:
            Validation result
        """
        time_since_last_event = (datetime.now() - last_event_timestamp).total_seconds()
        threshold_seconds = staleness_threshold_minutes * 60

        if time_since_last_event <= threshold_seconds:
            return ValidationResult(
                validator=self.name,
                status=ValidationStatus.PASSED,
                message=f"Pipeline is active: last event {time_since_last_event:.0f}s ago",
                details={
                    "time_since_last_event_seconds": time_since_last_event,
                    "threshold_seconds": threshold_seconds,
                    "last_event_timestamp": last_event_timestamp.isoformat(),
                },
            )
        else:
            return ValidationResult(
                validator=self.name,
                status=ValidationStatus.WARNING,
                message=f"Pipeline may be stale: no events for {time_since_last_event:.0f}s",
                details={
                    "time_since_last_event_seconds": time_since_last_event,
                    "threshold_seconds": threshold_seconds,
                    "last_event_timestamp": last_event_timestamp.isoformat(),
                },
            )
