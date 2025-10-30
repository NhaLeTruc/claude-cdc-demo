"""Service health check utilities for integration testing."""

import logging
import time
from dataclasses import dataclass
from typing import Callable, Optional

import httpx

logger = logging.getLogger(__name__)


@dataclass
class HealthCheckConfig:
    """Configuration for service health checks."""

    max_retries: int = 30
    initial_delay: float = 1.0
    max_delay: float = 10.0
    backoff_factor: float = 1.5
    timeout: float = 5.0


class HealthChecker:
    """Utility class for checking service health with exponential backoff."""

    def __init__(self, config: Optional[HealthCheckConfig] = None):
        """Initialize health checker.

        Args:
            config: Health check configuration (uses defaults if not provided)
        """
        self.config = config or HealthCheckConfig()

    def wait_for_service(
        self,
        check_func: Callable[[], bool],
        service_name: str,
        config: Optional[HealthCheckConfig] = None,
    ) -> bool:
        """Wait for a service to become healthy using exponential backoff.

        Args:
            check_func: Function that returns True when service is healthy
            service_name: Name of the service (for logging)
            config: Optional override for health check configuration

        Returns:
            True if service became healthy, False if max retries exceeded
        """
        cfg = config or self.config
        delay = cfg.initial_delay

        for attempt in range(1, cfg.max_retries + 1):
            try:
                if check_func():
                    logger.info(
                        f"{service_name} is healthy (attempt {attempt}/{cfg.max_retries})"
                    )
                    return True
            except Exception as e:
                logger.debug(
                    f"{service_name} health check failed (attempt {attempt}/{cfg.max_retries}): {e}"
                )

            if attempt < cfg.max_retries:
                logger.debug(f"Waiting {delay:.1f}s before next attempt...")
                time.sleep(delay)
                delay = min(delay * cfg.backoff_factor, cfg.max_delay)

        logger.error(
            f"{service_name} did not become healthy after {cfg.max_retries} attempts"
        )
        return False

    def check_http_endpoint(
        self,
        url: str,
        service_name: str,
        expected_status: int = 200,
        config: Optional[HealthCheckConfig] = None,
    ) -> bool:
        """Wait for an HTTP endpoint to become healthy.

        Args:
            url: HTTP endpoint URL
            service_name: Name of the service (for logging)
            expected_status: Expected HTTP status code for healthy service
            config: Optional override for health check configuration

        Returns:
            True if endpoint became healthy, False otherwise
        """
        cfg = config or self.config

        def check() -> bool:
            try:
                response = httpx.get(url, timeout=cfg.timeout)
                return response.status_code == expected_status
            except Exception:
                return False

        return self.wait_for_service(check, service_name, config)

    def check_iceberg_catalog(
        self,
        catalog_url: str = "http://localhost:8181",
        config: Optional[HealthCheckConfig] = None,
    ) -> bool:
        """Check if Iceberg REST Catalog is healthy.

        Args:
            catalog_url: Iceberg REST Catalog base URL
            config: Optional override for health check configuration

        Returns:
            True if catalog is healthy, False otherwise
        """
        return self.check_http_endpoint(
            f"{catalog_url}/v1/config",
            "Iceberg REST Catalog",
            expected_status=200,
            config=config,
        )

    def check_schema_registry(
        self,
        registry_url: str = "http://localhost:8081",
        config: Optional[HealthCheckConfig] = None,
    ) -> bool:
        """Check if Schema Registry is healthy.

        Args:
            registry_url: Schema Registry base URL
            config: Optional override for health check configuration

        Returns:
            True if registry is healthy, False otherwise
        """
        return self.check_http_endpoint(
            f"{registry_url}/subjects",
            "Schema Registry",
            expected_status=200,
            config=config,
        )

    def check_debezium(
        self,
        debezium_url: str = "http://localhost:8083",
        config: Optional[HealthCheckConfig] = None,
    ) -> bool:
        """Check if Debezium Kafka Connect is healthy.

        Args:
            debezium_url: Debezium Connect base URL
            config: Optional override for health check configuration

        Returns:
            True if Debezium is healthy, False otherwise
        """
        return self.check_http_endpoint(
            debezium_url,
            "Debezium Kafka Connect",
            expected_status=200,
            config=config,
        )

    def check_spark(
        self,
        spark_url: str = "http://localhost:8080",
        config: Optional[HealthCheckConfig] = None,
    ) -> bool:
        """Check if Spark Master is healthy.

        Args:
            spark_url: Spark Master web UI URL
            config: Optional override for health check configuration

        Returns:
            True if Spark is healthy, False otherwise
        """
        return self.check_http_endpoint(
            spark_url,
            "Spark Master",
            expected_status=200,
            config=config,
        )

    def check_minio(
        self,
        minio_url: str = "http://localhost:9000",
        config: Optional[HealthCheckConfig] = None,
    ) -> bool:
        """Check if MinIO is healthy.

        Args:
            minio_url: MinIO server URL
            config: Optional override for health check configuration

        Returns:
            True if MinIO is healthy, False otherwise
        """
        return self.check_http_endpoint(
            f"{minio_url}/minio/health/live",
            "MinIO",
            expected_status=200,
            config=config,
        )

    def check_prometheus(
        self,
        prometheus_url: str = "http://localhost:9090",
        config: Optional[HealthCheckConfig] = None,
    ) -> bool:
        """Check if Prometheus is healthy.

        Args:
            prometheus_url: Prometheus server URL
            config: Optional override for health check configuration

        Returns:
            True if Prometheus is healthy, False otherwise
        """
        return self.check_http_endpoint(
            f"{prometheus_url}/-/healthy",
            "Prometheus",
            expected_status=200,
            config=config,
        )

    def check_alertmanager(
        self,
        alertmanager_url: str = "http://localhost:9093",
        config: Optional[HealthCheckConfig] = None,
    ) -> bool:
        """Check if Alertmanager is healthy.

        Args:
            alertmanager_url: Alertmanager server URL
            config: Optional override for health check configuration

        Returns:
            True if Alertmanager is healthy, False otherwise
        """
        return self.check_http_endpoint(
            f"{alertmanager_url}/-/healthy",
            "Alertmanager",
            expected_status=200,
            config=config,
        )


def wait_for_all_services(
    services: Optional[list[str]] = None,
    config: Optional[HealthCheckConfig] = None,
) -> dict[str, bool]:
    """Wait for multiple services to become healthy.

    Args:
        services: List of service names to check (defaults to all)
        config: Optional override for health check configuration

    Returns:
        Dictionary mapping service names to health status (True/False)
    """
    checker = HealthChecker(config)

    # Default to all services if not specified
    if services is None:
        services = [
            "minio",
            "debezium",
            "prometheus",
            "alertmanager",
        ]

    results = {}

    service_checks = {
        "minio": checker.check_minio,
        "debezium": checker.check_debezium,
        "prometheus": checker.check_prometheus,
        "alertmanager": checker.check_alertmanager,
        "iceberg": checker.check_iceberg_catalog,
        "schema-registry": checker.check_schema_registry,
        "spark": checker.check_spark,
    }

    for service in services:
        if service in service_checks:
            logger.info(f"Checking {service}...")
            results[service] = service_checks[service]()
        else:
            logger.warning(f"Unknown service: {service}")
            results[service] = False

    return results
