"""
Pytest configuration and shared fixtures for CDC demo tests.
"""

import os
import pytest
import psycopg2
import pymysql
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


@pytest.fixture(scope="session")
def postgres_credentials():
    """PostgreSQL connection credentials from environment."""
    return {
        "host": os.getenv("POSTGRES_HOST", "localhost"),
        "port": int(os.getenv("POSTGRES_PORT", "5432")),
        "database": os.getenv("POSTGRES_DB", "cdcdb"),
        "user": os.getenv("POSTGRES_USER", "cdcuser"),
        "password": os.getenv("POSTGRES_PASSWORD", "cdcpass"),
    }


@pytest.fixture(scope="session")
def mysql_credentials():
    """MySQL connection credentials from environment."""
    return {
        "host": os.getenv("MYSQL_HOST", "localhost"),
        "port": int(os.getenv("MYSQL_PORT", "3306")),
        "database": os.getenv("MYSQL_DB", "cdcdb"),
        "user": os.getenv("MYSQL_USER", "cdcuser"),
        "password": os.getenv("MYSQL_PASSWORD", "cdcpass"),
    }


@pytest.fixture
def postgres_connection(postgres_credentials):
    """Create PostgreSQL connection for testing."""
    conn = psycopg2.connect(**postgres_credentials)
    yield conn
    conn.close()


@pytest.fixture
def mysql_connection(mysql_credentials):
    """Create MySQL connection for testing."""
    conn = pymysql.connect(**mysql_credentials)
    yield conn
    conn.close()
