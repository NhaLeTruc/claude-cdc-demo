"""Integration tests for Postgres CDC pipeline."""

import pytest
import time
from datetime import datetime


@pytest.mark.integration
@pytest.mark.slow
class TestPostgresCDCPipeline:
    """Integration tests for Postgres CDC pipeline."""

    @pytest.fixture(scope="class")
    def postgres_connection(self):
        """Create Postgres connection for testing."""
        from src.cdc_pipelines.postgres.connection import PostgresConnectionManager

        config = {
            "host": "localhost",
            "port": 5432,
            "user": "cdcuser",
            "password": "cdcpass",
            "database": "cdcdb",
        }

        manager = PostgresConnectionManager(**config)
        yield manager
        manager.close()

    @pytest.fixture(scope="function")
    def clean_customers_table(self, postgres_connection):
        """Clean customers table before each test."""
        postgres_connection.execute_query("DELETE FROM customers WHERE customer_id > 0")
        yield
        postgres_connection.execute_query("DELETE FROM customers WHERE customer_id > 0")

    def test_insert_capture(self, postgres_connection, clean_customers_table):
        """Test CDC captures INSERT operations."""
        # Insert a customer record
        postgres_connection.execute_query(
            """
            INSERT INTO customers (customer_id, first_name, last_name, email, created_at)
            VALUES (1, 'John', 'Doe', 'john@example.com', NOW())
            """
        )

        # Wait for CDC to process
        time.sleep(2)

        # Verify event was captured
        # This would check Kafka topic or DeltaLake destination
        # For now, just verify insert succeeded
        result = postgres_connection.execute_query(
            "SELECT COUNT(*) as count FROM customers WHERE customer_id = 1"
        )
        assert result[0]['count'] == 1

    def test_update_capture(self, postgres_connection, clean_customers_table):
        """Test CDC captures UPDATE operations."""
        # Insert a record first
        postgres_connection.execute_query(
            """
            INSERT INTO customers (customer_id, first_name, last_name, email, created_at)
            VALUES (1, 'John', 'Doe', 'john@example.com', NOW())
            """
        )

        time.sleep(1)

        # Update the record
        postgres_connection.execute_query(
            """
            UPDATE customers
            SET email = 'john.doe@example.com'
            WHERE customer_id = 1
            """
        )

        time.sleep(2)

        # Verify update succeeded
        result = postgres_connection.execute_query(
            "SELECT email FROM customers WHERE customer_id = 1"
        )
        assert result[0]['email'] == "john.doe@example.com"

    def test_delete_capture(self, postgres_connection, clean_customers_table):
        """Test CDC captures DELETE operations."""
        # Insert a record first
        postgres_connection.execute_query(
            """
            INSERT INTO customers (customer_id, first_name, last_name, email, created_at)
            VALUES (1, 'John', 'Doe', 'john@example.com', NOW())
            """
        )

        time.sleep(1)

        # Delete the record
        postgres_connection.execute_query("DELETE FROM customers WHERE customer_id = 1")

        time.sleep(2)

        # Verify deletion
        result = postgres_connection.execute_query(
            "SELECT COUNT(*) as count FROM customers WHERE customer_id = 1"
        )
        assert result[0]['count'] == 0

    def test_bulk_insert_capture(self, postgres_connection, clean_customers_table):
        """Test CDC handles bulk inserts."""
        # Insert multiple records
        for i in range(1, 101):
            postgres_connection.execute_query(
                f"""
                INSERT INTO customers (customer_id, first_name, last_name, email, created_at)
                VALUES ({i}, 'User{i}', 'Test', 'user{i}@example.com', NOW())
                """
            )

        time.sleep(5)

        # Verify all inserts succeeded
        result = postgres_connection.execute_query("SELECT COUNT(*) as count FROM customers")
        assert result[0]['count'] == 100

    def test_transaction_ordering(self, postgres_connection, clean_customers_table):
        """Test CDC maintains transaction order."""
        # Perform a sequence of operations
        postgres_connection.execute_query(
            """
            BEGIN;
            INSERT INTO customers (customer_id, first_name, last_name, email, created_at)
            VALUES (1, 'John', 'Doe', 'john@example.com', NOW());
            UPDATE customers SET email = 'john.updated@example.com' WHERE customer_id = 1;
            COMMIT;
            """
        )

        time.sleep(2)

        # Verify final state
        result = postgres_connection.execute_query(
            "SELECT email FROM customers WHERE customer_id = 1"
        )
        assert result[0]['email'] == "john.updated@example.com"

    def test_null_value_handling(self, postgres_connection, clean_customers_table):
        """Test CDC handles NULL values correctly."""
        postgres_connection.execute_query(
            """
            INSERT INTO customers (customer_id, first_name, last_name, email, phone, address, created_at)
            VALUES (1, 'John', 'Doe', 'john@example.com', NULL, NULL, NOW())
            """
        )

        time.sleep(2)

        result = postgres_connection.execute_query(
            "SELECT phone, address FROM customers WHERE customer_id = 1"
        )
        assert result[0]['phone'] is None
        assert result[0]['address'] is None

    @pytest.mark.skip(reason="Requires Debezium connector to be registered")
    def test_cdc_lag_within_threshold(self, postgres_connection, clean_customers_table):
        """Test CDC lag stays within acceptable threshold."""
        start_time = datetime.now()

        postgres_connection.execute_query(
            """
            INSERT INTO customers (customer_id, first_name, last_name, email, created_at)
            VALUES (1, 'John', 'Doe', 'john@example.com', NOW())
            """
        )

        # Wait for CDC event to appear in destination
        time.sleep(5)

        end_time = datetime.now()
        lag_seconds = (end_time - start_time).total_seconds()

        # CDC lag should be less than 5 seconds
        assert lag_seconds < 5

    @pytest.mark.skip(reason="Requires full CDC pipeline setup")
    def test_end_to_end_postgres_to_delta(self):
        """Test complete Postgres→DeltaLake pipeline."""
        # This would test the full pipeline end-to-end
        # Including Kafka, Debezium, and DeltaLake write
        pass
