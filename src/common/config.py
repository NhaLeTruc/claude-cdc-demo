"""Configuration management for CDC Demo."""

from functools import lru_cache
from typing import Optional

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class DatabaseConfig(BaseSettings):
    """Database configuration."""

    model_config = SettingsConfigDict(env_prefix="POSTGRES_")

    user: str = "cdcuser"
    password: str = "cdcpass"
    db: str = "cdcdb"
    host: str = "localhost"
    port: int = 5432


class MySQLConfig(BaseSettings):
    """MySQL configuration."""

    model_config = SettingsConfigDict(env_prefix="MYSQL_")

    user: str = "cdcuser"
    password: str = "cdcpass"
    db: str = "cdcdb"
    host: str = "localhost"
    port: int = 3306


class KafkaConfig(BaseSettings):
    """Kafka configuration."""

    model_config = SettingsConfigDict(env_prefix="KAFKA_")

    bootstrap_servers: str = "localhost:29092"
    topic_prefix: str = "cdc"


class DebeziumConfig(BaseSettings):
    """Debezium configuration."""

    model_config = SettingsConfigDict(env_prefix="DEBEZIUM_")

    host: str = "localhost"
    port: int = 8083


class MinIOConfig(BaseSettings):
    """MinIO configuration."""

    model_config = SettingsConfigDict(env_prefix="MINIO_")

    root_user: str = "minioadmin"
    root_password: str = "minioadmin"
    endpoint: str = "localhost:9000"
    bucket_deltalake: str = "deltalake"
    bucket_iceberg: str = "iceberg"


class ObservabilityConfig(BaseSettings):
    """Observability configuration."""

    model_config = SettingsConfigDict(env_prefix="")

    prometheus_port: int = Field(default=9090, alias="PROMETHEUS_PORT")
    grafana_port: int = Field(default=3000, alias="GRAFANA_PORT")
    loki_port: int = Field(default=3100, alias="LOKI_PORT")
    metrics_port: int = Field(default=8000, alias="METRICS_PORT")
    health_check_port: int = Field(default=8001, alias="HEALTH_CHECK_PORT")


class ApplicationConfig(BaseSettings):
    """Application configuration."""

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    log_level: str = "INFO"
    cdc_lag_threshold_seconds: int = 5
    cdc_batch_size: int = 100
    cdc_poll_interval_ms: int = 1000


class Settings(BaseSettings):
    """Main settings container."""

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    postgres: DatabaseConfig = Field(default_factory=DatabaseConfig)
    mysql: MySQLConfig = Field(default_factory=MySQLConfig)
    kafka: KafkaConfig = Field(default_factory=KafkaConfig)
    debezium: DebeziumConfig = Field(default_factory=DebeziumConfig)
    minio: MinIOConfig = Field(default_factory=MinIOConfig)
    observability: ObservabilityConfig = Field(default_factory=ObservabilityConfig)
    app: ApplicationConfig = Field(default_factory=ApplicationConfig)


@lru_cache()
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()
