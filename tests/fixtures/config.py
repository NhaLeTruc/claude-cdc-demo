"""Shared test configuration and fixtures for CDC testing."""

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class MinIOConfig:
    """MinIO S3-compatible storage configuration."""

    endpoint: str = "localhost:9000"
    access_key: str = "minioadmin"
    secret_key: str = "minioadmin"
    secure: bool = False
    warehouse_bucket: str = "warehouse"
    iceberg_bucket: str = "iceberg"
    delta_bucket: str = "delta"


@dataclass
class IcebergCatalogConfig:
    """Apache Iceberg REST Catalog configuration."""

    catalog_uri: str = "http://localhost:8181"
    warehouse_path: str = "s3://warehouse/"
    catalog_name: str = "demo"
    s3_endpoint: str = "http://minio:9000"
    aws_access_key_id: str = "minioadmin"
    aws_secret_access_key: str = "minioadmin"
    aws_region: str = "us-east-1"


@dataclass
class SchemaRegistryConfig:
    """Confluent Schema Registry configuration."""

    url: str = "http://localhost:8081"
    timeout: float = 10.0


@dataclass
class SparkConfig:
    """Apache Spark configuration for Delta Lake."""

    master_url: str = "spark://localhost:7077"
    app_name: str = "cdc-test"
    driver_memory: str = "1g"
    executor_memory: str = "1g"
    warehouse_dir: str = "s3a://warehouse/"
    s3_endpoint: str = "http://localhost:9000"
    aws_access_key_id: str = "minioadmin"
    aws_secret_access_key: str = "minioadmin"


@dataclass
class DeltaLakeConfig:
    """Delta Lake configuration."""

    table_path: str = "s3a://delta/"
    warehouse_path: str = "s3a://warehouse/"
    enable_cdf: bool = True
    s3_endpoint: str = "http://localhost:9000"
    aws_access_key_id: str = "minioadmin"
    aws_secret_access_key: str = "minioadmin"


@dataclass
class DebeziumConfig:
    """Debezium Kafka Connect configuration."""

    url: str = "http://localhost:8083"
    timeout: float = 10.0
    postgres_connector_name: str = "postgres-cdc-connector"
    mysql_connector_name: str = "mysql-cdc-connector"


@dataclass
class KafkaConfig:
    """Kafka broker configuration."""

    bootstrap_servers: str = "localhost:29092"
    group_id: str = "test-consumer-group"
    auto_offset_reset: str = "earliest"
    enable_auto_commit: bool = True


@dataclass
class PostgresConfig:
    """PostgreSQL database configuration."""

    host: str = "localhost"
    port: int = 5432
    user: str = "cdcuser"
    password: str = "cdcpass"
    database: str = "cdcdb"
    slot_name: str = "test_slot"


@dataclass
class MySQLConfig:
    """MySQL database configuration."""

    host: str = "localhost"
    port: int = 3306
    user: str = "cdcuser"
    password: str = "cdcpass"
    database: str = "cdcdb"
    server_id: int = 1


@dataclass
class PrometheusConfig:
    """Prometheus monitoring configuration."""

    url: str = "http://localhost:9090"
    timeout: float = 10.0


@dataclass
class AlertmanagerConfig:
    """Alertmanager configuration."""

    url: str = "http://localhost:9093"
    timeout: float = 10.0


@dataclass
class TestInfrastructureConfig:
    """Complete test infrastructure configuration.

    This dataclass aggregates all service configurations needed for
    comprehensive CDC testing across all components.
    """

    minio: MinIOConfig = field(default_factory=MinIOConfig)
    iceberg: IcebergCatalogConfig = field(default_factory=IcebergCatalogConfig)
    schema_registry: SchemaRegistryConfig = field(default_factory=SchemaRegistryConfig)
    spark: SparkConfig = field(default_factory=SparkConfig)
    delta: DeltaLakeConfig = field(default_factory=DeltaLakeConfig)
    debezium: DebeziumConfig = field(default_factory=DebeziumConfig)
    kafka: KafkaConfig = field(default_factory=KafkaConfig)
    postgres: PostgresConfig = field(default_factory=PostgresConfig)
    mysql: MySQLConfig = field(default_factory=MySQLConfig)
    prometheus: PrometheusConfig = field(default_factory=PrometheusConfig)
    alertmanager: AlertmanagerConfig = field(default_factory=AlertmanagerConfig)

    @classmethod
    def from_env(cls) -> "TestInfrastructureConfig":
        """Create configuration from environment variables.

        This method allows overriding default values with environment variables
        for CI/CD environments or custom deployments.

        Returns:
            TestInfrastructureConfig with values from environment or defaults
        """
        import os

        return cls(
            minio=MinIOConfig(
                endpoint=os.getenv("MINIO_ENDPOINT", "localhost:9000"),
                access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
                secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
            ),
            iceberg=IcebergCatalogConfig(
                catalog_uri=os.getenv("ICEBERG_CATALOG_URI", "http://localhost:8181"),
                s3_endpoint=os.getenv("ICEBERG_S3_ENDPOINT", "http://minio:9000"),
            ),
            schema_registry=SchemaRegistryConfig(
                url=os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081"),
            ),
            debezium=DebeziumConfig(
                url=os.getenv("DEBEZIUM_URL", "http://localhost:8083"),
            ),
            kafka=KafkaConfig(
                bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"),
            ),
            postgres=PostgresConfig(
                host=os.getenv("POSTGRES_HOST", "localhost"),
                port=int(os.getenv("POSTGRES_PORT", "5432")),
                user=os.getenv("POSTGRES_USER", "cdcuser"),
                password=os.getenv("POSTGRES_PASSWORD", "cdcpass"),
                database=os.getenv("POSTGRES_DB", "cdcdb"),
            ),
            mysql=MySQLConfig(
                host=os.getenv("MYSQL_HOST", "localhost"),
                port=int(os.getenv("MYSQL_PORT", "3306")),
                user=os.getenv("MYSQL_USER", "cdcuser"),
                password=os.getenv("MYSQL_PASSWORD", "cdcpass"),
                database=os.getenv("MYSQL_DB", "cdcdb"),
            ),
            prometheus=PrometheusConfig(
                url=os.getenv("PROMETHEUS_URL", "http://localhost:9090"),
            ),
            alertmanager=AlertmanagerConfig(
                url=os.getenv("ALERTMANAGER_URL", "http://localhost:9093"),
            ),
        )


def get_test_config() -> TestInfrastructureConfig:
    """Get the default test infrastructure configuration.

    This is a convenience function for tests to easily access configuration.

    Returns:
        TestInfrastructureConfig with default or environment-based values
    """
    return TestInfrastructureConfig.from_env()
