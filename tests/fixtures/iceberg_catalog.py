"""PyIceberg catalog configuration and utilities for testing."""

import logging
from typing import Any, Dict, Optional

from pyiceberg.catalog import Catalog, load_catalog

from tests.fixtures.config import IcebergCatalogConfig

logger = logging.getLogger(__name__)


class IcebergCatalogManager:
    """Manager for PyIceberg catalog connections in testing.

    This class provides utilities for creating and managing Iceberg catalog
    connections using PyIceberg library, configured for MinIO S3 storage.
    """

    def __init__(self, config: Optional[IcebergCatalogConfig] = None):
        """Initialize Iceberg catalog manager.

        Args:
            config: Iceberg catalog configuration (uses defaults if not provided)
        """
        self.config = config or IcebergCatalogConfig()
        self._catalog: Optional[Catalog] = None

    def get_catalog_properties(self) -> Dict[str, Any]:
        """Get catalog properties for PyIceberg.

        Returns:
            Dictionary of catalog properties
        """
        return {
            "type": "rest",
            "uri": self.config.catalog_uri,
            "s3.endpoint": self.config.s3_endpoint,
            "s3.access-key-id": self.config.aws_access_key_id,
            "s3.secret-access-key": self.config.aws_secret_access_key,
            "s3.region": self.config.aws_region,
            "s3.path-style-access": "true",
        }

    def get_catalog(self, name: Optional[str] = None) -> Catalog:
        """Get or create an Iceberg catalog instance.

        Args:
            name: Optional catalog name (uses config default if not provided)

        Returns:
            PyIceberg Catalog instance
        """
        if self._catalog is None:
            catalog_name = name or self.config.catalog_name
            properties = self.get_catalog_properties()

            logger.info(f"Loading Iceberg catalog '{catalog_name}' from {self.config.catalog_uri}")
            self._catalog = load_catalog(catalog_name, **properties)

        return self._catalog

    def reset_catalog(self) -> None:
        """Reset the catalog instance, forcing reconnection on next get_catalog()."""
        self._catalog = None
        logger.info("Iceberg catalog instance reset")

    def list_namespaces(self, catalog: Optional[Catalog] = None) -> list[tuple]:
        """List all namespaces in the catalog.

        Args:
            catalog: Optional catalog instance (uses default if not provided)

        Returns:
            List of namespace tuples
        """
        cat = catalog or self.get_catalog()
        try:
            namespaces = list(cat.list_namespaces())
            logger.debug(f"Found {len(namespaces)} namespaces")
            return namespaces
        except Exception as e:
            logger.error(f"Failed to list namespaces: {e}")
            return []

    def create_namespace(
        self,
        namespace: str,
        properties: Optional[Dict[str, str]] = None,
        catalog: Optional[Catalog] = None,
    ) -> bool:
        """Create a namespace in the catalog.

        Args:
            namespace: Namespace name (e.g., "cdc_test")
            properties: Optional namespace properties
            catalog: Optional catalog instance (uses default if not provided)

        Returns:
            True if namespace was created, False otherwise
        """
        cat = catalog or self.get_catalog()
        try:
            cat.create_namespace(namespace, properties=properties or {})
            logger.info(f"Created namespace '{namespace}'")
            return True
        except Exception as e:
            logger.error(f"Failed to create namespace '{namespace}': {e}")
            return False

    def namespace_exists(self, namespace: str, catalog: Optional[Catalog] = None) -> bool:
        """Check if a namespace exists.

        Args:
            namespace: Namespace name to check
            catalog: Optional catalog instance (uses default if not provided)

        Returns:
            True if namespace exists, False otherwise
        """
        cat = catalog or self.get_catalog()
        namespaces = self.list_namespaces(cat)
        return (namespace,) in namespaces

    def list_tables(self, namespace: str, catalog: Optional[Catalog] = None) -> list[tuple]:
        """List all tables in a namespace.

        Args:
            namespace: Namespace name
            catalog: Optional catalog instance (uses default if not provided)

        Returns:
            List of table identifier tuples
        """
        cat = catalog or self.get_catalog()
        try:
            tables = list(cat.list_tables(namespace))
            logger.debug(f"Found {len(tables)} tables in namespace '{namespace}'")
            return tables
        except Exception as e:
            logger.error(f"Failed to list tables in namespace '{namespace}': {e}")
            return []

    def drop_table(
        self,
        table_identifier: str,
        catalog: Optional[Catalog] = None,
    ) -> bool:
        """Drop a table from the catalog.

        Args:
            table_identifier: Table identifier (e.g., "namespace.table_name")
            catalog: Optional catalog instance (uses default if not provided)

        Returns:
            True if table was dropped, False otherwise
        """
        cat = catalog or self.get_catalog()
        try:
            cat.drop_table(table_identifier)
            logger.info(f"Dropped table '{table_identifier}'")
            return True
        except Exception as e:
            logger.error(f"Failed to drop table '{table_identifier}': {e}")
            return False

    def cleanup_namespace(
        self,
        namespace: str,
        catalog: Optional[Catalog] = None,
    ) -> bool:
        """Clean up a namespace by dropping all tables and the namespace itself.

        Args:
            namespace: Namespace name to cleanup
            catalog: Optional catalog instance (uses default if not provided)

        Returns:
            True if cleanup was successful, False otherwise
        """
        cat = catalog or self.get_catalog()

        try:
            # Drop all tables in the namespace
            tables = self.list_tables(namespace, cat)
            for table_id in tables:
                table_name = f"{namespace}.{table_id[-1]}"
                self.drop_table(table_name, cat)

            # Drop the namespace
            cat.drop_namespace(namespace)
            logger.info(f"Cleaned up namespace '{namespace}'")
            return True

        except Exception as e:
            logger.error(f"Failed to cleanup namespace '{namespace}': {e}")
            return False


def get_iceberg_catalog(config: Optional[IcebergCatalogConfig] = None) -> Catalog:
    """Convenience function to get an Iceberg catalog.

    Args:
        config: Optional Iceberg catalog configuration

    Returns:
        PyIceberg Catalog instance
    """
    manager = IcebergCatalogManager(config)
    return manager.get_catalog()


def setup_test_namespace(
    namespace: str = "cdc_test",
    config: Optional[IcebergCatalogConfig] = None,
) -> tuple[Catalog, str]:
    """Setup a test namespace for Iceberg testing.

    Args:
        namespace: Namespace name to create
        config: Optional Iceberg catalog configuration

    Returns:
        Tuple of (catalog instance, namespace name)
    """
    manager = IcebergCatalogManager(config)
    catalog = manager.get_catalog()

    # Create namespace if it doesn't exist
    if not manager.namespace_exists(namespace, catalog):
        manager.create_namespace(namespace, catalog=catalog)

    return catalog, namespace


def cleanup_test_namespace(
    namespace: str = "cdc_test",
    config: Optional[IcebergCatalogConfig] = None,
) -> None:
    """Cleanup a test namespace after testing.

    Args:
        namespace: Namespace name to cleanup
        config: Optional Iceberg catalog configuration
    """
    manager = IcebergCatalogManager(config)
    catalog = manager.get_catalog()

    if manager.namespace_exists(namespace, catalog):
        manager.cleanup_namespace(namespace, catalog)
