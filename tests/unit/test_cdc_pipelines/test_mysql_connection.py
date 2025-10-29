"""Unit tests for MySQL connection manager."""

import time
from unittest.mock import MagicMock, Mock, patch

import pytest

# Try to import MySQL connector, skip tests if not available
try:
    import mysql.connector
    MYSQL_AVAILABLE = True
except ImportError:
    MYSQL_AVAILABLE = False

pytestmark = pytest.mark.skipif(
    not MYSQL_AVAILABLE,
    reason="MySQL connector not available for MySQL tests"
)


class TestMySQLConnectionManager:
    """Test suite for MySQL connection manager."""

    @patch("mysql.connector.connect")
    def test_connection_creation(self, mock_mysql_connection, mysql_credentials):
        """Test connection is created with correct parameters."""
        from src.cdc_pipelines.mysql.connection import MySQLConnectionManager

        # Setup mock
        mock_conn = MagicMock()
        mock_mysql_connection.return_value = mock_conn

        manager = MySQLConnectionManager(**mysql_credentials)
        conn = manager.get_connection()

        assert conn is not None
        assert manager.is_connected()
        mock_mysql_connection.assert_called_once()

    @patch("mysql.connector.connect")
    def test_connection_retry_on_failure(self, mock_mysql_connection, mysql_credentials):
        """Test connection retry logic on failure."""
        from src.cdc_pipelines.mysql.connection import MySQLConnectionManager
        import mysql.connector

        # First call raises exception, second succeeds
        mock_conn = MagicMock()
        mock_conn.is_connected.return_value = True
        mock_mysql_connection.side_effect = [
            mysql.connector.Error("Connection failed"),
            mock_conn,
        ]

        config = {**mysql_credentials, "max_retries": 3, "retry_delay": 0.1}
        manager = MySQLConnectionManager(**config)

        conn = manager.get_connection()
        assert conn is not None
        assert mock_mysql_connection.call_count == 2

    @patch("mysql.connector.connect")
    def test_connection_failure_after_max_retries(self, mock_mysql_connection, mysql_credentials):
        """Test connection fails after max retries."""
        from src.cdc_pipelines.mysql.connection import MySQLConnectionManager

        mock_mysql_connection.side_effect = Exception("Connection failed")

        config = {**mysql_credentials, "max_retries": 2, "retry_delay": 0.1}
        manager = MySQLConnectionManager(**config)

        with pytest.raises(Exception, match="Connection failed"):
            manager.get_connection()

    @patch("mysql.connector.connect")
    def test_connection_reuse(self, mock_mysql_connection, mysql_credentials):
        """Test existing connection is reused."""
        from src.cdc_pipelines.mysql.connection import MySQLConnectionManager

        mock_conn = MagicMock()
        mock_conn.is_connected.return_value = True
        mock_mysql_connection.return_value = mock_conn

        manager = MySQLConnectionManager(**mysql_credentials)

        conn1 = manager.get_connection()
        conn2 = manager.get_connection()

        assert conn1 is conn2
        assert mock_mysql_connection.call_count == 1

    @patch("mysql.connector.connect")
    def test_execute_query_with_fetch(self, mock_mysql_connection, mysql_credentials):
        """Test execute query with fetch."""
        from src.cdc_pipelines.mysql.connection import MySQLConnectionManager

        # Setup mock
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [{"count": 5}]
        mock_cursor.description = [("count",)]  # Needed for fetch check
        mock_conn = MagicMock()
        mock_conn.is_connected.return_value = True
        mock_conn.cursor.return_value = mock_cursor
        mock_mysql_connection.return_value = mock_conn

        manager = MySQLConnectionManager(**mysql_credentials)

        result = manager.execute_query("SELECT COUNT(*) FROM test", fetch=True)
        assert result == [{"count": 5}]
        mock_cursor.execute.assert_called_once()

    @patch("mysql.connector.connect")
    def test_get_table_schema(self, mock_mysql_connection, mysql_credentials):
        """Test get table schema."""
        from src.cdc_pipelines.mysql.connection import MySQLConnectionManager

        # Setup mock
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            {"column_name": "product_id", "data_type": "int"},
            {"column_name": "name", "data_type": "varchar"},
            {"column_name": "price", "data_type": "decimal"},
        ]
        mock_cursor.description = [("column_name",), ("data_type",)]
        mock_conn = MagicMock()
        mock_conn.is_connected.return_value = True
        mock_conn.cursor.return_value = mock_cursor
        mock_mysql_connection.return_value = mock_conn

        manager = MySQLConnectionManager(**mysql_credentials)

        schema = manager.get_table_schema("products")
        assert schema == {
            "product_id": "int",
            "name": "varchar",
            "price": "decimal",
        }

    @patch("mysql.connector.connect")
    def test_check_binlog_enabled(self, mock_mysql_connection, mysql_credentials):
        """Test check if binlog is enabled."""
        from src.cdc_pipelines.mysql.connection import MySQLConnectionManager

        # Setup mock
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [{"Variable_name": "log_bin", "Value": "ON"}]
        mock_cursor.description = [("Variable_name",), ("Value",)]
        mock_conn = MagicMock()
        mock_conn.is_connected.return_value = True
        mock_conn.cursor.return_value = mock_cursor
        mock_mysql_connection.return_value = mock_conn

        manager = MySQLConnectionManager(**mysql_credentials)

        is_enabled = manager.check_binlog_enabled()
        assert is_enabled is True

    @patch("mysql.connector.connect")
    def test_context_manager(self, mock_mysql_connection, mysql_credentials):
        """Test context manager usage."""
        from src.cdc_pipelines.mysql.connection import MySQLConnectionManager

        mock_conn = MagicMock()
        mock_conn.is_connected.return_value = True
        mock_mysql_connection.return_value = mock_conn

        with MySQLConnectionManager(**mysql_credentials) as manager:
            assert manager.is_connected()

        mock_conn.close.assert_called_once()
