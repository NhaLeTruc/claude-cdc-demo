"""Unit tests for MySQL connection manager."""

import time
from unittest.mock import MagicMock, Mock, patch

import pytest


class TestMySQLConnectionManager:
    """Test suite for MySQL connection manager."""

    @patch("mysql.connector.connect")
    def test_connection_creation(self, mock_mysql_connection):
        """Test connection is created with correct parameters."""
        from src.cdc_pipelines.mysql.connection import MySQLConnectionManager

        config = {
            "host": "localhost",
            "port": 3306,
            "user": "cdcuser",
            "password": "cdcpass",
            "database": "cdcdb",
        }

        # Setup mock
        mock_conn = MagicMock()
        mock_mysql_connection.return_value = mock_conn

        manager = MySQLConnectionManager(**config)
        conn = manager.get_connection()

        assert conn is not None
        assert manager.is_connected()
        mock_mysql_connection.assert_called_once()

    @patch("mysql.connector.connect")
    def test_connection_retry_on_failure(self, mock_mysql_connection):
        """Test connection retry logic on failure."""
        from src.cdc_pipelines.mysql.connection import MySQLConnectionManager

        # First call raises exception, second succeeds
        mock_conn = MagicMock()
        mock_mysql_connection.side_effect = [
            Exception("Connection failed"),
            mock_conn,
        ]

        manager = MySQLConnectionManager(
            host="localhost",
            port=3306,
            user="cdcuser",
            password="cdcpass",
            database="cdcdb",
            max_retries=3,
            retry_delay=0.1,
        )

        conn = manager.get_connection()
        assert conn is not None
        assert mock_mysql_connection.call_count == 2

    @patch("mysql.connector.connect")
    def test_connection_failure_after_max_retries(self, mock_mysql_connection):
        """Test connection fails after max retries."""
        from src.cdc_pipelines.mysql.connection import MySQLConnectionManager

        mock_mysql_connection.side_effect = Exception("Connection failed")

        manager = MySQLConnectionManager(
            host="localhost",
            port=3306,
            user="cdcuser",
            password="cdcpass",
            database="cdcdb",
            max_retries=2,
            retry_delay=0.1,
        )

        with pytest.raises(Exception, match="Connection failed"):
            manager.get_connection()

    @patch("mysql.connector.connect")
    def test_connection_reuse(self, mock_mysql_connection):
        """Test existing connection is reused."""
        from src.cdc_pipelines.mysql.connection import MySQLConnectionManager

        mock_conn = MagicMock()
        mock_conn.is_connected.return_value = True
        mock_mysql_connection.return_value = mock_conn

        manager = MySQLConnectionManager(
            host="localhost",
            port=3306,
            user="cdcuser",
            password="cdcpass",
            database="cdcdb",
        )

        conn1 = manager.get_connection()
        conn2 = manager.get_connection()

        assert conn1 is conn2
        assert mock_mysql_connection.call_count == 1

    @patch("mysql.connector.connect")
    def test_execute_query_with_fetch(self, mock_mysql_connection):
        """Test execute query with fetch."""
        from src.cdc_pipelines.mysql.connection import MySQLConnectionManager

        # Setup mock
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [{"count": 5}]
        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_mysql_connection.return_value = mock_conn

        manager = MySQLConnectionManager(
            host="localhost",
            port=3306,
            user="cdcuser",
            password="cdcpass",
            database="cdcdb",
        )

        result = manager.execute_query("SELECT COUNT(*) FROM test", fetch=True)
        assert result == [{"count": 5}]
        mock_cursor.execute.assert_called_once()

    @patch("mysql.connector.connect")
    def test_get_table_schema(self, mock_mysql_connection):
        """Test get table schema."""
        from src.cdc_pipelines.mysql.connection import MySQLConnectionManager

        # Setup mock
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            {"column_name": "product_id", "data_type": "int"},
            {"column_name": "name", "data_type": "varchar"},
            {"column_name": "price", "data_type": "decimal"},
        ]
        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_mysql_connection.return_value = mock_conn

        manager = MySQLConnectionManager(
            host="localhost",
            port=3306,
            user="cdcuser",
            password="cdcpass",
            database="cdcdb",
        )

        schema = manager.get_table_schema("products")
        assert schema == {
            "product_id": "int",
            "name": "varchar",
            "price": "decimal",
        }

    @patch("mysql.connector.connect")
    def test_check_binlog_enabled(self, mock_mysql_connection):
        """Test check if binlog is enabled."""
        from src.cdc_pipelines.mysql.connection import MySQLConnectionManager

        # Setup mock
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [{"Variable_name": "log_bin", "Value": "ON"}]
        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_mysql_connection.return_value = mock_conn

        manager = MySQLConnectionManager(
            host="localhost",
            port=3306,
            user="cdcuser",
            password="cdcpass",
            database="cdcdb",
        )

        is_enabled = manager.check_binlog_enabled()
        assert is_enabled is True

    @patch("mysql.connector.connect")
    def test_context_manager(self, mock_mysql_connection):
        """Test context manager usage."""
        from src.cdc_pipelines.mysql.connection import MySQLConnectionManager

        mock_conn = MagicMock()
        mock_mysql_connection.return_value = mock_conn

        with MySQLConnectionManager(
            host="localhost",
            port=3306,
            user="cdcuser",
            password="cdcpass",
            database="cdcdb",
        ) as manager:
            assert manager.is_connected()

        mock_conn.close.assert_called_once()
