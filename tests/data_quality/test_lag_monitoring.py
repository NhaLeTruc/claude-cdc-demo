"""Data quality tests for CDC lag monitoring."""

import pytest
from datetime import datetime, timedelta


@pytest.mark.data_quality
class TestCDCLagMonitoring:
    """Test CDC lag monitoring and validation."""

    def test_lag_within_threshold(self):
        """Test lag validation passes when within threshold."""
        from src.validation.lag_monitor import LagMonitor

        monitor = LagMonitor(threshold_seconds=5)

        event_timestamp = datetime.now() - timedelta(seconds=2)
        result = monitor.validate_lag(event_timestamp)

        assert result.status.value == "passed"
        assert result.details["lag_seconds"] < 5

    def test_lag_exceeds_threshold(self):
        """Test lag validation fails when exceeding threshold."""
        from src.validation.lag_monitor import LagMonitor

        monitor = LagMonitor(threshold_seconds=5)

        event_timestamp = datetime.now() - timedelta(seconds=10)
        result = monitor.validate_lag(event_timestamp)

        assert result.status.value == "failed"
        assert result.details["lag_seconds"] > 5

    def test_batch_lag_validation(self):
        """Test lag validation for batch of events."""
        from src.validation.lag_monitor import LagMonitor

        monitor = LagMonitor(threshold_seconds=5)

        event_timestamps = [
            datetime.now() - timedelta(seconds=1),
            datetime.now() - timedelta(seconds=2),
            datetime.now() - timedelta(seconds=3),
        ]

        result = monitor.validate_batch_lag(event_timestamps)

        assert result.status.value == "passed"
        assert result.details["max_lag_seconds"] < 5

    def test_batch_lag_with_outlier(self):
        """Test batch lag validation with one slow event."""
        from src.validation.lag_monitor import LagMonitor

        monitor = LagMonitor(threshold_seconds=5)

        event_timestamps = [
            datetime.now() - timedelta(seconds=1),
            datetime.now() - timedelta(seconds=2),
            datetime.now() - timedelta(seconds=10),  # Outlier
        ]

        result = monitor.validate_batch_lag(event_timestamps)

        assert result.status.value == "failed"
        assert result.details["max_lag_seconds"] > 5

    def test_staleness_check_active_pipeline(self):
        """Test staleness check for active pipeline."""
        from src.validation.lag_monitor import LagMonitor

        monitor = LagMonitor()

        last_event_timestamp = datetime.now() - timedelta(seconds=30)
        result = monitor.check_staleness(last_event_timestamp, staleness_threshold_minutes=5)

        assert result.status.value == "passed"

    def test_staleness_check_stale_pipeline(self):
        """Test staleness check detects stale pipeline."""
        from src.validation.lag_monitor import LagMonitor

        monitor = LagMonitor()

        last_event_timestamp = datetime.now() - timedelta(minutes=10)
        result = monitor.check_staleness(last_event_timestamp, staleness_threshold_minutes=5)

        assert result.status.value == "warning"

    def test_lag_calculation_accuracy(self):
        """Test lag calculation is accurate."""
        from src.validation.lag_monitor import LagMonitor

        monitor = LagMonitor()

        event_timestamp = datetime.now() - timedelta(seconds=3)
        processing_timestamp = datetime.now()

        lag = monitor.calculate_lag(event_timestamp, processing_timestamp)

        # Lag should be approximately 3 seconds (with small tolerance)
        assert 2.9 <= lag <= 3.1
