"""Unit tests for Postgres connection manager."""

import pytest
from unittest.mock import Mock, patch, MagicMock


@pytest.fixture
def mock_pg_connection():
    """Mock PostgreSQL connection with RealDictCursor."""
    with patch("src.cdc_pipelines.postgres.connection.psycopg2.connect") as mock_connect:
        mock_conn = MagicMock()
        mock_cursor = MagicMock()

        # RealDictCursor returns dictionaries, not tuples
        mock_cursor.description = [("column1",), ("column2",)]
        mock_cursor.fetchall.return_value = []
        mock_cursor.fetchone.return_value = None
        mock_cursor.__enter__ = Mock(return_value=mock_cursor)
        mock_cursor.__exit__ = Mock(return_value=False)
        mock_cursor.execute = Mock()

        # Setup cursor context manager
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = Mock(return_value=False)
        mock_conn.closed = 0  # psycopg2 uses 0 for open, non-zero for closed
        mock_conn.commit = Mock()

        mock_connect.return_value = mock_conn
        yield mock_conn


@pytest.mark.unit
class TestPostgresConnectionManager:
    """Test Postgres connection manager."""

    def test_connection_creation(self, mock_pg_connection, postgres_credentials):
        """Test connection is created with correct parameters."""
        from src.cdc_pipelines.postgres.connection import PostgresConnectionManager

        manager = PostgresConnectionManager(**postgres_credentials)
        conn = manager.get_connection()

        assert conn is not None
        assert manager.is_connected()

    def test_connection_close(self, mock_pg_connection, postgres_credentials):
        """Test connection can be closed."""
        from src.cdc_pipelines.postgres.connection import PostgresConnectionManager

        manager = PostgresConnectionManager(**postgres_credentials)
        manager.get_connection()
        manager.close()

        mock_pg_connection.close.assert_called_once()

    def test_execute_query(self, mock_pg_connection, postgres_credentials):
        """Test query execution."""
        from src.cdc_pipelines.postgres.connection import PostgresConnectionManager

        manager = PostgresConnectionManager(**postgres_credentials)

        # Configure the cursor mock to return dict-like results (RealDictCursor behavior)
        mock_cursor = mock_pg_connection.cursor.return_value.__enter__.return_value
        mock_cursor.fetchall.return_value = [{"id": 1, "name": "test"}]
        mock_cursor.description = [("id",), ("name",)]

        result = manager.execute_query("SELECT * FROM customers")

        assert result == [{"id": 1, "name": "test"}]
        assert mock_cursor.execute.called

    def test_connection_context_manager(self, mock_pg_connection, postgres_credentials):
        """Test connection manager can be used as context manager."""
        from src.cdc_pipelines.postgres.connection import PostgresConnectionManager

        with PostgresConnectionManager(**postgres_credentials) as manager:
            assert manager.is_connected()

        mock_pg_connection.close.assert_called_once()

    def test_connection_retry_on_failure(self, mock_pg_connection, postgres_credentials):
        """Test connection retries on failure."""
        from src.cdc_pipelines.postgres.connection import PostgresConnectionManager

        config = {**postgres_credentials, "max_retries": 3}

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

    def test_get_table_schema(self, mock_pg_connection, postgres_credentials):
        """Test retrieving table schema."""
        from src.cdc_pipelines.postgres.connection import PostgresConnectionManager

        manager = PostgresConnectionManager(**postgres_credentials)
        mock_cursor = mock_pg_connection.cursor.return_value.__enter__.return_value
        mock_cursor.fetchall.return_value = [
            {"column_name": "customer_id", "data_type": "integer"},
            {"column_name": "email", "data_type": "varchar"},
        ]
        mock_cursor.description = [("column_name",), ("data_type",)]

        schema = manager.get_table_schema("customers")

        assert "customer_id" in schema
        assert schema["customer_id"] == "integer"
        assert "email" in schema

    def test_check_cdc_enabled(self, mock_pg_connection, postgres_credentials):
        """Test checking if CDC is enabled on Postgres."""
        from src.cdc_pipelines.postgres.connection import PostgresConnectionManager

        manager = PostgresConnectionManager(**postgres_credentials)
        mock_cursor = mock_pg_connection.cursor.return_value.__enter__.return_value
        mock_cursor.fetchall.return_value = [{"wal_level": "logical"}]
        mock_cursor.description = [("wal_level",)]

        is_enabled = manager.check_cdc_enabled()

        assert is_enabled is True

    def test_get_table_count(self, mock_pg_connection, postgres_credentials):
        """Test getting row count for a table."""
        from src.cdc_pipelines.postgres.connection import PostgresConnectionManager

        manager = PostgresConnectionManager(**postgres_credentials)
        mock_cursor = mock_pg_connection.cursor.return_value.__enter__.return_value
        mock_cursor.fetchall.return_value = [{"count": 100}]
        mock_cursor.description = [("count",)]

        count = manager.get_table_count("customers")

        assert count == 100
