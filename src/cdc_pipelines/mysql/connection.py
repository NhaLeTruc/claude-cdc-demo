"""MySQL connection manager for CDC operations."""

import time
from typing import Any, Dict, List, Optional, Tuple

import mysql.connector
from mysql.connector import Error as MySQLError

from src.observability.logging_config import get_logger

logger = get_logger(__name__)


class MySQLConnectionManager:
    """Manages MySQL connections for CDC operations."""

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
        Initialize MySQL connection manager.

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
        self._connection: Optional[mysql.connector.MySQLConnection] = None

    def get_connection(self) -> mysql.connector.MySQLConnection:
        """
        Get database connection with retry logic.

        Returns:
            MySQL connection

        Raises:
            Exception: If connection fails after all retries
        """
        if self._connection and self._connection.is_connected():
            return self._connection

        last_exception = None
        for attempt in range(self.max_retries):
            try:
                logger.info(
                    f"Connecting to MySQL at {self.host}:{self.port}/{self.database} "
                    f"(attempt {attempt + 1}/{self.max_retries})"
                )
                self._connection = mysql.connector.connect(
                    host=self.host,
                    port=self.port,
                    user=self.user,
                    password=self.password,
                    database=self.database,
                    autocommit=True,
                )
                logger.info("Successfully connected to MySQL")
                return self._connection
            except MySQLError as e:
                last_exception = e
                logger.warning(f"Connection attempt {attempt + 1} failed: {e}")
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
        return self._connection is not None and self._connection.is_connected()

    def close(self) -> None:
        """Close database connection."""
        if self._connection and self._connection.is_connected():
            self._connection.close()
            logger.info("Closed MySQL connection")

    def execute_query(
        self,
        query: str,
        params: Optional[Tuple] = None,
        fetch: bool = True,
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
        cursor = conn.cursor(dictionary=True)
        try:
            cursor.execute(query, params)
            # Check if query returns results (SELECT, SHOW, etc.)
            if fetch and cursor.description:
                return cursor.fetchall()  # type: ignore
            conn.commit()
            return None
        finally:
            cursor.close()

    def get_table_schema(
        self, table_name: str, schema: Optional[str] = None
    ) -> Dict[str, str]:
        """
        Get table schema.

        Args:
            table_name: Table name
            schema: Schema name (database name in MySQL, optional)

        Returns:
            Dictionary mapping column names to data types
        """
        db = schema or self.database
        query = """
            SELECT COLUMN_NAME as column_name, DATA_TYPE as data_type
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
            ORDER BY ORDINAL_POSITION
        """
        results = self.execute_query(query, (db, table_name))
        return (
            {row["column_name"]: row["data_type"] for row in results}
            if results
            else {}
        )

    def get_table_count(
        self, table_name: str, schema: Optional[str] = None
    ) -> int:
        """
        Get row count for table.

        Args:
            table_name: Table name
            schema: Schema name (database name, optional)

        Returns:
            Row count
        """
        db = schema or self.database
        query = f"SELECT COUNT(*) as count FROM `{db}`.`{table_name}`"
        result = self.execute_query(query)
        return result[0]["count"] if result else 0

    def check_binlog_enabled(self) -> bool:
        """
        Check if binary logging is enabled on MySQL.

        Returns:
            True if log_bin is ON, False otherwise
        """
        query = "SHOW VARIABLES LIKE 'log_bin'"
        result = self.execute_query(query)
        if result:
            log_bin_status = result[0].get("Value", "OFF")
            logger.info(f"Binary logging status: {log_bin_status}")
            return log_bin_status == "ON"
        return False

    def get_binlog_position(self) -> Dict[str, Any]:
        """
        Get current binlog position.

        Returns:
            Dictionary with binlog file and position
        """
        query = "SHOW MASTER STATUS"
        result = self.execute_query(query)
        if result and len(result) > 0:
            return {
                "file": result[0].get("File"),
                "position": result[0].get("Position"),
            }
        return {"file": None, "position": None}

    def __enter__(self) -> "MySQLConnectionManager":
        """Context manager entry."""
        self.get_connection()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Context manager exit."""
        self.close()
