"""Health check endpoints and monitoring."""

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from http.server import BaseHTTPRequestHandler, HTTPServer
import json
from threading import Thread
from typing import Dict, List, Optional

from src.common.config import get_settings


class HealthStatus(str, Enum):
    """Health status enum."""

    HEALTHY = "healthy"
    UNHEALTHY = "unhealthy"
    DEGRADED = "degraded"


@dataclass
class ComponentHealth:
    """Health status for a component."""

    name: str
    status: HealthStatus
    message: str
    checked_at: datetime


class HealthChecker:
    """Health check orchestrator."""

    def __init__(self) -> None:
        """Initialize health checker."""
        self.settings = get_settings()
        self._components: Dict[str, ComponentHealth] = {}

    def register_component(self, name: str) -> None:
        """
        Register a component for health checking.

        Args:
            name: Component name
        """
        self._components[name] = ComponentHealth(
            name=name,
            status=HealthStatus.HEALTHY,
            message="Component registered",
            checked_at=datetime.now(),
        )

    def update_component_health(
        self, name: str, status: HealthStatus, message: str = ""
    ) -> None:
        """
        Update component health status.

        Args:
            name: Component name
            status: Health status
            message: Status message
        """
        self._components[name] = ComponentHealth(
            name=name, status=status, message=message, checked_at=datetime.now()
        )

    def get_component_health(self, name: str) -> Optional[ComponentHealth]:
        """
        Get health status for a component.

        Args:
            name: Component name

        Returns:
            Component health or None if not registered
        """
        return self._components.get(name)

    def get_overall_health(self) -> HealthStatus:
        """
        Get overall system health.

        Returns:
            Overall health status
        """
        if not self._components:
            return HealthStatus.HEALTHY

        statuses = [comp.status for comp in self._components.values()]

        if all(s == HealthStatus.HEALTHY for s in statuses):
            return HealthStatus.HEALTHY
        elif any(s == HealthStatus.UNHEALTHY for s in statuses):
            return HealthStatus.UNHEALTHY
        else:
            return HealthStatus.DEGRADED

    def get_health_report(self) -> Dict:
        """
        Get full health report.

        Returns:
            Health report dictionary
        """
        overall_status = self.get_overall_health()

        return {
            "status": overall_status.value,
            "timestamp": datetime.now().isoformat(),
            "components": [
                {
                    "name": comp.name,
                    "status": comp.status.value,
                    "message": comp.message,
                    "checked_at": comp.checked_at.isoformat(),
                }
                for comp in self._components.values()
            ],
        }


class HealthCheckHandler(BaseHTTPRequestHandler):
    """HTTP handler for health check endpoint."""

    health_checker: HealthChecker

    def do_GET(self) -> None:
        """Handle GET requests."""
        if self.path == "/health":
            report = self.health_checker.get_health_report()
            overall_status = report["status"]

            # Return 200 for healthy, 503 for unhealthy/degraded
            status_code = 200 if overall_status == "healthy" else 503

            self.send_response(status_code)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(report).encode())

        elif self.path == "/health/ready":
            # Readiness check - are we ready to accept traffic?
            report = self.health_checker.get_health_report()
            status_code = 200 if report["status"] != "unhealthy" else 503

            self.send_response(status_code)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"ready": status_code == 200}).encode())

        elif self.path == "/health/live":
            # Liveness check - is the service alive?
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"alive": True}).encode())

        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format: str, *args) -> None:  # type: ignore
        """Suppress default logging."""
        pass


class HealthCheckServer:
    """HTTP server for health check endpoints."""

    def __init__(self, health_checker: HealthChecker, port: Optional[int] = None) -> None:
        """
        Initialize health check server.

        Args:
            health_checker: Health checker instance
            port: Port to listen on (default from config)
        """
        settings = get_settings()
        self.port = port or settings.observability.health_check_port
        self.health_checker = health_checker

        # Set health checker on handler class
        HealthCheckHandler.health_checker = health_checker

        self.server = HTTPServer(("0.0.0.0", self.port), HealthCheckHandler)
        self._thread: Optional[Thread] = None

    def start(self) -> None:
        """Start health check server in background thread."""

        def run_server() -> None:
            self.server.serve_forever()

        self._thread = Thread(target=run_server, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        """Stop health check server."""
        self.server.shutdown()
        if self._thread:
            self._thread.join()
