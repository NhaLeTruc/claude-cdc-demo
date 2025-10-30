"""Test fixtures and utilities for CDC testing."""

from tests.fixtures.config import (
    AlertmanagerConfig,
    DebeziumConfig,
    DeltaLakeConfig,
    IcebergCatalogConfig,
    KafkaConfig,
    MinIOConfig,
    MySQLConfig,
    PostgresConfig,
    PrometheusConfig,
    SchemaRegistryConfig,
    SparkConfig,
    TestInfrastructureConfig,
    get_test_config,
)
from tests.fixtures.health_checks import HealthCheckConfig, HealthChecker, wait_for_all_services
from tests.fixtures.minio_setup import (
    MinIOSetup,
    cleanup_test_buckets,
    setup_test_buckets,
)
from tests.fixtures.iceberg_catalog import (
    IcebergCatalogManager,
    cleanup_test_namespace,
    get_iceberg_catalog,
    setup_test_namespace,
)
from tests.fixtures.delta_spark import (
    DeltaSparkManager,
    get_delta_spark_session,
)

__all__ = [
    # Configuration
    "AlertmanagerConfig",
    "DebeziumConfig",
    "DeltaLakeConfig",
    "IcebergCatalogConfig",
    "KafkaConfig",
    "MinIOConfig",
    "MySQLConfig",
    "PostgresConfig",
    "PrometheusConfig",
    "SchemaRegistryConfig",
    "SparkConfig",
    "TestInfrastructureConfig",
    "get_test_config",
    # Health Checks
    "HealthCheckConfig",
    "HealthChecker",
    "wait_for_all_services",
    # MinIO Setup
    "MinIOSetup",
    "setup_test_buckets",
    "cleanup_test_buckets",
    # Iceberg Catalog
    "IcebergCatalogManager",
    "get_iceberg_catalog",
    "setup_test_namespace",
    "cleanup_test_namespace",
    # Delta Lake with Spark
    "DeltaSparkManager",
    "get_delta_spark_session",
]
