"""Unit tests for Postgres connection manager."""

import pytest
from unittest.mock import Mock, patch, MagicMock


@pytest.fixture
def mock_pg_connection():
    """Mock PostgreSQL connection."""
    with patch("src.cdc_pipelines.postgres.connection.psycopg2.connect") as mock_connect:
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.description = [("column1",), ("column2",)]  # Simulate result columns
        mock_cursor.__enter__ = Mock(return_value=mock_cursor)
        mock_cursor.__exit__ = Mock(return_value=False)
        mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = Mock(return_value=False)
        mock_conn.closed = 0  # psycopg2 uses 0 for open, non-zero for closed
        mock_connect.return_value = mock_conn
        yield mock_conn


@pytest.mark.unit
class TestPostgresConnectionManager:
    """Test Postgres connection manager."""

    def test_connection_creation(self, mock_pg_connection):
        """Test connection is created with correct parameters."""
        from src.cdc_pipelines.postgres.connection import PostgresConnectionManager

        config = {
            "host": "localhost",
            "port": 5432,
            "user": "cdcuser",
            "password": "cdcpass",
            "database": "cdcdb",
        }

        manager = PostgresConnectionManager(**config)
        conn = manager.get_connection()

        assert conn is not None
        assert manager.is_connected()

    def test_connection_close(self, mock_pg_connection):
        """Test connection can be closed."""
        from src.cdc_pipelines.postgres.connection import PostgresConnectionManager

        config = {
            "host": "localhost",
            "port": 5432,
            "user": "cdcuser",
            "password": "cdcpass",
            "database": "cdcdb",
        }

        manager = PostgresConnectionManager(**config)
        manager.get_connection()
        manager.close()

        mock_pg_connection.close.assert_called_once()

    def test_execute_query(self, mock_pg_connection):
        """Test query execution."""
        from src.cdc_pipelines.postgres.connection import PostgresConnectionManager

        config = {
            "host": "localhost",
            "port": 5432,
            "user": "cdcuser",
            "password": "cdcpass",
            "database": "cdcdb",
        }

        manager = PostgresConnectionManager(**config)
        mock_cursor = mock_pg_connection.cursor.return_value
        mock_cursor.fetchall.return_value = [(1, "test")]

        result = manager.execute_query("SELECT * FROM customers")

        assert result == [(1, "test")]
        mock_cursor.execute.assert_called_once()

    def test_connection_context_manager(self, mock_pg_connection):
        """Test connection manager can be used as context manager."""
        from src.cdc_pipelines.postgres.connection import PostgresConnectionManager

        config = {
            "host": "localhost",
            "port": 5432,
            "user": "cdcuser",
            "password": "cdcpass",
            "database": "cdcdb",
        }

        with PostgresConnectionManager(**config) as manager:
            assert manager.is_connected()

        mock_pg_connection.close.assert_called_once()

    def test_connection_retry_on_failure(self, mock_pg_connection):
        """Test connection retries on failure."""
        from src.cdc_pipelines.postgres.connection import PostgresConnectionManager

        config = {
            "host": "localhost",
            "port": 5432,
            "user": "cdcuser",
            "password": "cdcpass",
            "database": "cdcdb",
            "max_retries": 3,
        }

        with patch("psycopg2.connect") as mock_connect:
            # First two attempts fail, third succeeds
            mock_connect.side_effect = [
                Exception("Connection failed"),
                Exception("Connection failed"),
                mock_pg_connection,
            ]

            manager = PostgresConnectionManager(**config)
            conn = manager.get_connection()

            assert conn is not None
            assert mock_connect.call_count == 3

    def test_get_table_schema(self, mock_pg_connection):
        """Test retrieving table schema."""
        from src.cdc_pipelines.postgres.connection import PostgresConnectionManager

        config = {
            "host": "localhost",
            "port": 5432,
            "user": "cdcuser",
            "password": "cdcpass",
            "database": "cdcdb",
        }

        manager = PostgresConnectionManager(**config)
        mock_cursor = mock_pg_connection.cursor.return_value
        mock_cursor.fetchall.return_value = [
            ("customer_id", "integer"),
            ("email", "varchar"),
        ]

        schema = manager.get_table_schema("customers")

        assert "customer_id" in schema
        assert schema["customer_id"] == "integer"
        assert "email" in schema

    def test_check_cdc_enabled(self, mock_pg_connection):
        """Test checking if CDC is enabled on Postgres."""
        from src.cdc_pipelines.postgres.connection import PostgresConnectionManager

        config = {
            "host": "localhost",
            "port": 5432,
            "user": "cdcuser",
            "password": "cdcpass",
            "database": "cdcdb",
        }

        manager = PostgresConnectionManager(**config)
        mock_cursor = mock_pg_connection.cursor.return_value
        mock_cursor.fetchone.return_value = ("logical",)

        is_enabled = manager.check_cdc_enabled()

        assert is_enabled is True

    def test_get_table_count(self, mock_pg_connection):
        """Test getting row count for a table."""
        from src.cdc_pipelines.postgres.connection import PostgresConnectionManager

        config = {
            "host": "localhost",
            "port": 5432,
            "user": "cdcuser",
            "password": "cdcpass",
            "database": "cdcdb",
        }

        manager = PostgresConnectionManager(**config)
        mock_cursor = mock_pg_connection.cursor.return_value
        mock_cursor.fetchone.return_value = (100,)

        count = manager.get_table_count("customers")

        assert count == 100
