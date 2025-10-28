"""Postgres connection manager for CDC operations."""

import time
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
from psycopg2.extras import RealDictCursor

from src.common.config import get_settings
from src.observability.logging_config import get_logger

logger = get_logger(__name__)


class PostgresConnectionManager:
    """Manages PostgreSQL connections for CDC operations."""

    def __init__(
        self,
        host: str,
        port: int,
        user: str,
        password: str,
        database: str,
        max_retries: int = 3,
        retry_delay: float = 1.0,
    ) -> None:
        """
        Initialize Postgres connection manager.

        Args:
            host: Database host
            port: Database port
            user: Database user
            password: Database password
            database: Database name
            max_retries: Maximum connection retry attempts
            retry_delay: Delay between retries in seconds
        """
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self._connection: Optional[psycopg2.extensions.connection] = None

    def get_connection(self) -> psycopg2.extensions.connection:
        """
        Get database connection with retry logic.

        Returns:
            PostgreSQL connection

        Raises:
            Exception: If connection fails after all retries
        """
        if self._connection and not self._connection.closed:
            return self._connection

        last_exception = None
        for attempt in range(self.max_retries):
            try:
                logger.info(
                    f"Connecting to Postgres at {self.host}:{self.port}/{self.database} "
                    f"(attempt {attempt + 1}/{self.max_retries})"
                )
                self._connection = psycopg2.connect(
                    host=self.host,
                    port=self.port,
                    user=self.user,
                    password=self.password,
                    database=self.database,
                    cursor_factory=RealDictCursor,
                )
                logger.info("Successfully connected to Postgres")
                return self._connection
            except Exception as e:
                last_exception = e
                logger.warning(
                    f"Connection attempt {attempt + 1} failed: {e}"
                )
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay)

        logger.error(f"Failed to connect after {self.max_retries} attempts")
        raise last_exception  # type: ignore

    def is_connected(self) -> bool:
        """
        Check if connection is active.

        Returns:
            True if connected, False otherwise
        """
        return self._connection is not None and not self._connection.closed

    def close(self) -> None:
        """Close database connection."""
        if self._connection and not self._connection.closed:
            self._connection.close()
            logger.info("Closed Postgres connection")

    def execute_query(
        self, query: str, params: Optional[Tuple] = None, fetch: bool = True
    ) -> Optional[List[Dict[str, Any]]]:
        """
        Execute SQL query.

        Args:
            query: SQL query to execute
            params: Query parameters
            fetch: Whether to fetch results

        Returns:
            Query results if fetch=True, None otherwise
        """
        conn = self.get_connection()
        with conn.cursor() as cursor:
            cursor.execute(query, params)
            # Check if query returns results (SELECT, RETURNING, etc.)
            if fetch and cursor.description:
                return cursor.fetchall()  # type: ignore
            conn.commit()
            return None

    def get_table_schema(self, table_name: str, schema: str = "public") -> Dict[str, str]:
        """
        Get table schema.

        Args:
            table_name: Table name
            schema: Schema name (default: public)

        Returns:
            Dictionary mapping column names to data types
        """
        query = """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
        """
        results = self.execute_query(query, (schema, table_name))
        return {row["column_name"]: row["data_type"] for row in results} if results else {}

    def get_table_count(self, table_name: str, schema: str = "public") -> int:
        """
        Get row count for table.

        Args:
            table_name: Table name
            schema: Schema name (default: public)

        Returns:
            Row count
        """
        query = f"SELECT COUNT(*) as count FROM {schema}.{table_name}"
        result = self.execute_query(query)
        return result[0]["count"] if result else 0

    def check_cdc_enabled(self) -> bool:
        """
        Check if CDC is enabled on Postgres.

        Returns:
            True if wal_level is set to 'logical', False otherwise
        """
        query = "SHOW wal_level"
        result = self.execute_query(query)
        if result:
            wal_level = result[0].get("wal_level")
            logger.info(f"Current wal_level: {wal_level}")
            return wal_level == "logical"
        return False

    def __enter__(self) -> "PostgresConnectionManager":
        """Context manager entry."""
        self.get_connection()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Context manager exit."""
        self.close()
