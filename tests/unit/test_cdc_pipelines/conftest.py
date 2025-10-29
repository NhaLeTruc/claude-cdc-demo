"""Pytest configuration for CDC pipeline unit tests."""

import pytest
from unittest.mock import MagicMock, Mock, patch


@pytest.fixture
def mock_postgres_connection():
    """Mock PostgreSQL connection with RealDictCursor."""
    with patch("src.cdc_pipelines.postgres.connection.psycopg2.connect") as mock_connect:
        mock_conn = MagicMock()
        mock_cursor = MagicMock()

        # Setup cursor to return dict results (RealDictCursor behavior)
        mock_cursor.description = [("col1",), ("col2",)]
        mock_cursor.fetchall.return_value = []
        mock_cursor.fetchone.return_value = None
        mock_cursor.__enter__ = Mock(return_value=mock_cursor)
        mock_cursor.__exit__ = Mock(return_value=False)

        # Setup connection
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.closed = 0
        mock_conn.__enter__ = Mock(return_value=mock_conn)
        mock_conn.__exit__ = Mock(return_value=False)

        mock_connect.return_value = mock_conn

        yield {"connection": mock_conn, "cursor": mock_cursor, "connect": mock_connect}


@pytest.fixture
def mock_mysql_connection():
    """Mock MySQL connection."""
    with patch("src.cdc_pipelines.mysql.connection.mysql.connector.connect") as mock_connect:
        mock_conn = MagicMock()
        mock_cursor = MagicMock()

        # Setup cursor
        mock_cursor.description = [("col1",), ("col2",)]
        mock_cursor.fetchall.return_value = []
        mock_cursor.fetchone.return_value = None
        mock_cursor.__enter__ = Mock(return_value=mock_cursor)
        mock_cursor.__exit__ = Mock(return_value=False)

        # Setup connection
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.is_connected.return_value = True
        mock_conn.__enter__ = Mock(return_value=mock_conn)
        mock_conn.__exit__ = Mock(return_value=False)

        mock_connect.return_value = mock_conn

        yield {"connection": mock_conn, "cursor": mock_cursor, "connect": mock_connect}


@pytest.fixture
def mock_spark_session():
    """Mock Spark session for Delta/Iceberg tests."""
    with patch("pyspark.sql.SparkSession") as mock_spark_class:
        mock_spark = MagicMock()
        mock_builder = MagicMock()

        # Setup builder chain
        mock_builder.appName.return_value = mock_builder
        mock_builder.config.return_value = mock_builder
        mock_builder.master.return_value = mock_builder
        mock_builder.getOrCreate.return_value = mock_spark

        mock_spark_class.builder = mock_builder

        # Setup Spark session methods
        mock_df = MagicMock()
        mock_df.write = MagicMock()
        mock_spark.createDataFrame.return_value = mock_df
        mock_spark.read = MagicMock()
        mock_spark.sql = MagicMock()

        yield {"spark": mock_spark, "builder": mock_builder, "class": mock_spark_class}
