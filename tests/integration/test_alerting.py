"""Integration tests for alert delivery mechanisms."""

import pytest
import requests
import time
import json
from typing import Dict, Any


@pytest.fixture
def alertmanager_url():
    """Alertmanager URL."""
    return "http://localhost:9093"


@pytest.fixture
def prometheus_url():
    """Prometheus URL."""
    return "http://localhost:9090"


@pytest.mark.integration
class TestAlertDelivery:
    """Integration tests for alert delivery mechanisms."""

    def test_alertmanager_is_reachable(self, alertmanager_url):
        """Test that Alertmanager is running and reachable."""
        response = requests.get(f"{alertmanager_url}/api/v1/status")

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "success"

    def test_alertmanager_config_loaded(self, alertmanager_url):
        """Test that Alertmanager configuration is loaded."""
        response = requests.get(f"{alertmanager_url}/api/v1/status")

        assert response.status_code == 200
        data = response.json()

        # Verify config has receivers
        assert "config" in data["data"]
        config = data["data"]["config"]
        assert "receivers" in config
        assert len(config["receivers"]) > 0

        # Verify default receiver exists
        receiver_names = [r["name"] for r in config["receivers"]]
        assert "default-receiver" in receiver_names

    def test_prometheus_alerting_rules_loaded(self, prometheus_url):
        """Test that Prometheus alert rules are loaded."""
        response = requests.get(f"{prometheus_url}/api/v1/rules")

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "success"

        # Verify alert groups exist
        groups = data["data"]["groups"]
        assert len(groups) > 0

        # Look for CDC alerts group
        cdc_groups = [g for g in groups if "cdc" in g["name"].lower()]
        assert len(cdc_groups) > 0

    def test_send_test_alert_to_alertmanager(self, alertmanager_url):
        """Test sending a test alert to Alertmanager."""
        # Create test alert
        test_alert = [
            {
                "labels": {
                    "alertname": "TestAlert",
                    "severity": "info",
                    "component": "test",
                },
                "annotations": {
                    "summary": "Test alert for integration testing",
                    "description": "This is a test alert to verify alert delivery",
                },
                "startsAt": time.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
            }
        ]

        # Send alert
        response = requests.post(
            f"{alertmanager_url}/api/v1/alerts",
            json=test_alert,
            headers={"Content-Type": "application/json"},
        )

        assert response.status_code == 200

        # Wait for alert to be processed
        time.sleep(2)

        # Verify alert was received
        response = requests.get(f"{alertmanager_url}/api/v1/alerts")
        assert response.status_code == 200

        data = response.json()
        alerts = data["data"]

        # Check if our test alert is in the list
        test_alerts = [a for a in alerts if a["labels"]["alertname"] == "TestAlert"]
        assert len(test_alerts) > 0

    def test_alert_routing(self, alertmanager_url):
        """Test alert routing to correct receivers."""
        # Test alert with critical severity
        critical_alert = [
            {
                "labels": {
                    "alertname": "CriticalTest",
                    "severity": "critical",
                    "component": "cdc_pipeline",
                },
                "annotations": {
                    "summary": "Critical alert routing test",
                    "description": "Testing critical alert routing",
                },
            }
        ]

        response = requests.post(
            f"{alertmanager_url}/api/v1/alerts",
            json=critical_alert,
        )

        assert response.status_code == 200
        time.sleep(2)

        # Verify alert was routed (check alertmanager status)
        response = requests.get(f"{alertmanager_url}/api/v1/alerts")
        data = response.json()
        alerts = data["data"]

        critical_alerts = [
            a for a in alerts if a["labels"]["alertname"] == "CriticalTest"
        ]
        assert len(critical_alerts) > 0

        # Verify it has correct receiver
        alert = critical_alerts[0]
        assert "receivers" in alert or "receiver" in alert["status"]

    def test_alert_inhibition(self, alertmanager_url):
        """Test alert inhibition rules."""
        # Send a critical alert that should inhibit warnings
        critical_alert = [
            {
                "labels": {
                    "alertname": "ConnectorFailure",
                    "severity": "critical",
                    "component": "debezium",
                    "pipeline": "test_pipeline",
                },
                "annotations": {
                    "summary": "Connector failed",
                    "description": "Test connector failure",
                },
            }
        ]

        # Send a warning alert that should be inhibited
        warning_alert = [
            {
                "labels": {
                    "alertname": "HighCDCLag",
                    "severity": "warning",
                    "component": "cdc_pipeline",
                    "pipeline": "test_pipeline",
                },
                "annotations": {
                    "summary": "High CDC lag",
                    "description": "Test lag alert",
                },
            }
        ]

        # Send both alerts
        requests.post(f"{alertmanager_url}/api/v1/alerts", json=critical_alert)
        time.sleep(1)
        requests.post(f"{alertmanager_url}/api/v1/alerts", json=warning_alert)
        time.sleep(2)

        # Check alert status
        response = requests.get(f"{alertmanager_url}/api/v1/alerts")
        data = response.json()
        alerts = data["data"]

        # Verify critical alert is active
        critical_alerts = [
            a for a in alerts if a["labels"]["alertname"] == "ConnectorFailure"
        ]
        assert len(critical_alerts) > 0

        # Check if warning is inhibited
        warning_alerts = [
            a for a in alerts if a["labels"]["alertname"] == "HighCDCLag"
        ]
        if len(warning_alerts) > 0:
            warning_alert = warning_alerts[0]
            # Check if it's inhibited
            assert (
                warning_alert["status"]["state"] == "suppressed"
                or "inhibitedBy" in warning_alert["status"]
            )

    def test_alert_silencing(self, alertmanager_url):
        """Test creating and removing alert silences."""
        # Create a silence
        silence = {
            "matchers": [
                {
                    "name": "alertname",
                    "value": "TestAlert",
                    "isRegex": False,
                }
            ],
            "startsAt": time.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
            "endsAt": time.strftime(
                "%Y-%m-%dT%H:%M:%S.000Z",
                time.localtime(time.time() + 3600),  # 1 hour from now
            ),
            "createdBy": "integration_test",
            "comment": "Test silence for integration testing",
        }

        # Create silence
        response = requests.post(
            f"{alertmanager_url}/api/v1/silences",
            json=silence,
        )

        assert response.status_code == 200
        data = response.json()
        silence_id = data["data"]["silenceID"]

        # Verify silence was created
        response = requests.get(f"{alertmanager_url}/api/v1/silences")
        data = response.json()
        silences = data["data"]

        our_silence = [s for s in silences if s["id"] == silence_id]
        assert len(our_silence) == 1

        # Delete silence
        response = requests.delete(f"{alertmanager_url}/api/v1/silence/{silence_id}")
        assert response.status_code == 200

    def test_webhook_delivery(self):
        """Test webhook alert delivery."""
        # This would require a test webhook receiver
        # For now, we'll just verify the configuration
        pytest.skip("Requires test webhook receiver setup")

    def test_email_delivery_configuration(self, alertmanager_url):
        """Test that email delivery is configured."""
        response = requests.get(f"{alertmanager_url}/api/v1/status")
        data = response.json()

        config = data["data"]["config"]
        receivers = config["receivers"]

        # Check for email configurations
        email_receivers = [
            r for r in receivers if "email_configs" in r and len(r["email_configs"]) > 0
        ]

        assert len(email_receivers) > 0, "No email receivers configured"

        # Verify email config has required fields
        for receiver in email_receivers:
            for email_config in receiver["email_configs"]:
                assert "to" in email_config
                assert "@" in email_config["to"]

    def test_slack_delivery_configuration(self, alertmanager_url):
        """Test that Slack delivery is configured."""
        response = requests.get(f"{alertmanager_url}/api/v1/status")
        data = response.json()

        config = data["data"]["config"]
        receivers = config["receivers"]

        # Check for Slack configurations
        slack_receivers = [
            r
            for r in receivers
            if "slack_configs" in r and len(r["slack_configs"]) > 0
        ]

        assert len(slack_receivers) > 0, "No Slack receivers configured"

        # Note: api_url should be configured but may be placeholder
        for receiver in slack_receivers:
            for slack_config in receiver["slack_configs"]:
                assert "api_url" in slack_config or "webhook_url" in slack_config

    def test_alert_group_by_configuration(self, alertmanager_url):
        """Test alert grouping configuration."""
        response = requests.get(f"{alertmanager_url}/api/v1/status")
        data = response.json()

        config = data["data"]["config"]
        route = config["route"]

        # Verify grouping is configured
        assert "group_by" in route
        assert len(route["group_by"]) > 0

        # Verify timing configuration
        assert "group_wait" in route
        assert "group_interval" in route
        assert "repeat_interval" in route

    def test_alert_repeat_interval(self, alertmanager_url):
        """Test that repeat interval is configured appropriately."""
        response = requests.get(f"{alertmanager_url}/api/v1/status")
        data = response.json()

        config = data["data"]["config"]
        route = config["route"]

        # Verify repeat interval is reasonable (not too frequent)
        repeat_interval = route["repeat_interval"]

        # Should be at least 1 hour for production
        # Format is like "3h" or "1h30m"
        import re

        hours = re.findall(r"(\d+)h", repeat_interval)
        if hours:
            assert int(hours[0]) >= 1, "Repeat interval should be at least 1 hour"

    def test_prometheus_alertmanager_integration(self, prometheus_url, alertmanager_url):
        """Test integration between Prometheus and Alertmanager."""
        # Check Prometheus knows about Alertmanager
        response = requests.get(f"{prometheus_url}/api/v1/alertmanagers")

        assert response.status_code == 200
        data = response.json()

        alertmanagers = data["data"]["activeAlertmanagers"]
        assert len(alertmanagers) > 0, "Prometheus not connected to any Alertmanagers"

        # Verify at least one is active
        active = [am for am in alertmanagers if am["url"]]
        assert len(active) > 0

    def test_cdc_specific_alert_rules_exist(self, prometheus_url):
        """Test that CDC-specific alert rules are defined."""
        response = requests.get(f"{prometheus_url}/api/v1/rules")
        data = response.json()

        groups = data["data"]["groups"]

        # Find all alert rules
        all_rules = []
        for group in groups:
            all_rules.extend(group["rules"])

        # Filter for actual alerts (not just recording rules)
        alerts = [r for r in all_rules if r["type"] == "alerting"]

        # Verify CDC-specific alerts exist
        cdc_alerts = [
            a
            for a in alerts
            if any(
                keyword in a["name"].lower()
                for keyword in ["cdc", "lag", "connector", "debezium"]
            )
        ]

        assert len(cdc_alerts) > 0, "No CDC-specific alert rules found"

        # Verify critical alerts exist
        expected_alerts = [
            "HighCDCLag",
            "ConnectorFailure",
            "DataIntegrityFailure",
        ]

        alert_names = [a["name"] for a in alerts]
        for expected in expected_alerts:
            matching = [name for name in alert_names if expected.lower() in name.lower()]
            assert len(matching) > 0, f"Expected alert '{expected}' not found"


@pytest.mark.integration
@pytest.mark.skipif(
    reason="Requires actual notification endpoints configured",
    condition=True,
)
class TestActualAlertDelivery:
    """Tests that require actual notification delivery (email, Slack, etc.)."""

    def test_email_delivery_end_to_end(self):
        """Test actual email delivery."""
        # Would require SMTP server or email service integration
        pytest.skip("Requires SMTP server configuration")

    def test_slack_delivery_end_to_end(self):
        """Test actual Slack delivery."""
        # Would require valid Slack webhook
        pytest.skip("Requires valid Slack webhook configuration")

    def test_pagerduty_delivery_end_to_end(self):
        """Test actual PagerDuty delivery."""
        # Would require PagerDuty integration
        pytest.skip("Requires PagerDuty integration configuration")
