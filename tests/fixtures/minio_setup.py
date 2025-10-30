"""MinIO bucket initialization and management utilities for testing."""

import logging
from typing import Optional

import httpx
from minio import Minio
from minio.error import S3Error

logger = logging.getLogger(__name__)


class MinIOSetup:
    """Utility class for setting up MinIO buckets for testing.

    This class provides methods to initialize and manage MinIO buckets
    required for Iceberg and Delta Lake testing.
    """

    def __init__(
        self,
        endpoint: str = "localhost:9000",
        access_key: str = "minioadmin",
        secret_key: str = "minioadmin",
        secure: bool = False,
    ):
        """Initialize MinIO setup utility.

        Args:
            endpoint: MinIO server endpoint (host:port)
            access_key: MinIO access key
            secret_key: MinIO secret key
            secure: Whether to use HTTPS
        """
        self.client = Minio(
            endpoint=endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure,
        )
        self.endpoint = endpoint
        self.access_key = access_key
        self.secret_key = secret_key

    def ensure_bucket(self, bucket_name: str, region: Optional[str] = None) -> bool:
        """Ensure a bucket exists, creating it if necessary.

        Args:
            bucket_name: Name of the bucket to ensure exists
            region: Optional region for the bucket (default: us-east-1)

        Returns:
            True if bucket exists or was created successfully, False otherwise
        """
        try:
            if self.client.bucket_exists(bucket_name):
                logger.info(f"Bucket '{bucket_name}' already exists")
                return True

            self.client.make_bucket(bucket_name, location=region or "us-east-1")
            logger.info(f"Created bucket '{bucket_name}'")
            return True

        except S3Error as e:
            logger.error(f"Failed to ensure bucket '{bucket_name}': {e}")
            return False

    def cleanup_bucket(self, bucket_name: str) -> bool:
        """Remove all objects from a bucket (but keep the bucket).

        Args:
            bucket_name: Name of the bucket to cleanup

        Returns:
            True if cleanup was successful, False otherwise
        """
        try:
            if not self.client.bucket_exists(bucket_name):
                logger.warning(f"Bucket '{bucket_name}' does not exist")
                return True

            # List and delete all objects
            objects = self.client.list_objects(bucket_name, recursive=True)
            for obj in objects:
                self.client.remove_object(bucket_name, obj.object_name)
                logger.debug(f"Removed object: {obj.object_name}")

            logger.info(f"Cleaned up bucket '{bucket_name}'")
            return True

        except S3Error as e:
            logger.error(f"Failed to cleanup bucket '{bucket_name}': {e}")
            return False

    def remove_bucket(self, bucket_name: str) -> bool:
        """Remove a bucket and all its contents.

        Args:
            bucket_name: Name of the bucket to remove

        Returns:
            True if removal was successful, False otherwise
        """
        try:
            if not self.client.bucket_exists(bucket_name):
                logger.warning(f"Bucket '{bucket_name}' does not exist")
                return True

            # First cleanup all objects
            self.cleanup_bucket(bucket_name)

            # Then remove the bucket
            self.client.remove_bucket(bucket_name)
            logger.info(f"Removed bucket '{bucket_name}'")
            return True

        except S3Error as e:
            logger.error(f"Failed to remove bucket '{bucket_name}': {e}")
            return False

    def list_buckets(self) -> list[str]:
        """List all buckets in MinIO.

        Returns:
            List of bucket names
        """
        try:
            buckets = self.client.list_buckets()
            return [bucket.name for bucket in buckets]
        except S3Error as e:
            logger.error(f"Failed to list buckets: {e}")
            return []

    def is_healthy(self, timeout: float = 5.0) -> bool:
        """Check if MinIO service is healthy and accessible.

        Args:
            timeout: Request timeout in seconds

        Returns:
            True if service is healthy, False otherwise
        """
        try:
            # Try to list buckets as a health check
            self.client.list_buckets()
            return True
        except Exception as e:
            logger.error(f"MinIO health check failed: {e}")
            return False


def setup_test_buckets(
    warehouse_bucket: str = "warehouse",
    iceberg_bucket: str = "iceberg",
    delta_bucket: str = "delta",
) -> MinIOSetup:
    """Setup standard test buckets for CDC testing.

    This is a convenience function that creates the standard set of buckets
    needed for Iceberg and Delta Lake testing.

    Args:
        warehouse_bucket: General warehouse bucket name
        iceberg_bucket: Iceberg-specific bucket name
        delta_bucket: Delta Lake-specific bucket name

    Returns:
        MinIOSetup instance with buckets created
    """
    minio_setup = MinIOSetup()

    # Ensure all standard buckets exist
    minio_setup.ensure_bucket(warehouse_bucket)
    minio_setup.ensure_bucket(iceberg_bucket)
    minio_setup.ensure_bucket(delta_bucket)

    return minio_setup


def cleanup_test_buckets(
    warehouse_bucket: str = "warehouse",
    iceberg_bucket: str = "iceberg",
    delta_bucket: str = "delta",
) -> None:
    """Cleanup all test buckets after testing.

    Args:
        warehouse_bucket: General warehouse bucket name
        iceberg_bucket: Iceberg-specific bucket name
        delta_bucket: Delta Lake-specific bucket name
    """
    minio_setup = MinIOSetup()

    # Cleanup all standard buckets
    minio_setup.cleanup_bucket(warehouse_bucket)
    minio_setup.cleanup_bucket(iceberg_bucket)
    minio_setup.cleanup_bucket(delta_bucket)
