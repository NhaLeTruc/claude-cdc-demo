"""Integration tests for MySQL CDC pipeline."""

import time
import pytest
from typing import Generator


@pytest.fixture
def mysql_connection():
    """Provide MySQL connection for tests."""
    from src.cdc_pipelines.mysql.connection import MySQLConnectionManager
    from src.common.config import get_settings

    settings = get_settings()
    manager = MySQLConnectionManager(
        host=settings.mysql_host,
        port=settings.mysql_port,
        user=settings.mysql_user,
        password=settings.mysql_password,
        database=settings.mysql_database,
    )

    try:
        conn = manager.get_connection()
        yield manager
    finally:
        manager.close()


@pytest.fixture
def clean_products_table(mysql_connection):
    """Clean products table before each test."""
    mysql_connection.execute_query(
        "DELETE FROM products WHERE product_id >= 1000",
        fetch=False,
    )
    yield
    # Cleanup after test
    mysql_connection.execute_query(
        "DELETE FROM products WHERE product_id >= 1000",
        fetch=False,
    )


@pytest.mark.integration
class TestMySQLCDCPipeline:
    """Integration tests for MySQL CDC pipeline."""

    def test_transaction_ordering(self, mysql_connection, clean_products_table):
        """Test CDC maintains transaction order."""
        # Execute multiple operations in a transaction
        queries = [
            "START TRANSACTION",
            """
            INSERT INTO products (product_id, name, category, price, stock_quantity)
            VALUES (1000, 'Product A', 'Electronics', 99.99, 100)
            """,
            """
            INSERT INTO products (product_id, name, category, price, stock_quantity)
            VALUES (1001, 'Product B', 'Electronics', 149.99, 50)
            """,
            """
            UPDATE products SET price = 89.99 WHERE product_id = 1000
            """,
            "COMMIT",
        ]

        for query in queries:
            mysql_connection.execute_query(query, fetch=False)

        # Give CDC time to process
        time.sleep(3)

        # Verify final state
        result = mysql_connection.execute_query(
            "SELECT * FROM products WHERE product_id IN (1000, 1001) ORDER BY product_id"
        )

        assert len(result) == 2
        assert result[0]["product_id"] == 1000
        assert result[0]["price"] == 89.99  # Updated price
        assert result[1]["product_id"] == 1001
        assert result[1]["price"] == 149.99

    def test_bulk_insert_100_records(self, mysql_connection, clean_products_table):
        """Test CDC captures bulk insert of 100+ records."""
        # Generate bulk insert
        values = []
        for i in range(150):
            product_id = 1000 + i
            values.append(
                f"({product_id}, 'Product {i}', 'Category', {50.0 + i}, {10 * i})"
            )

        insert_query = f"""
            INSERT INTO products (product_id, name, category, price, stock_quantity)
            VALUES {','.join(values)}
        """

        mysql_connection.execute_query(insert_query, fetch=False)

        # Give CDC time to process
        time.sleep(5)

        # Verify all records inserted
        result = mysql_connection.execute_query(
            "SELECT COUNT(*) as count FROM products WHERE product_id >= 1000"
        )

        assert result[0]["count"] == 150

    def test_mixed_operations_in_transaction(self, mysql_connection, clean_products_table):
        """Test transaction with INSERT, UPDATE, DELETE."""
        queries = [
            "START TRANSACTION",
            # Insert
            """
            INSERT INTO products (product_id, name, category, price, stock_quantity)
            VALUES (1000, 'Test Product', 'Test', 100.0, 50)
            """,
            # Update same record
            """
            UPDATE products SET price = 120.0, stock_quantity = 45
            WHERE product_id = 1000
            """,
            # Insert another
            """
            INSERT INTO products (product_id, name, category, price, stock_quantity)
            VALUES (1001, 'Test Product 2', 'Test', 200.0, 30)
            """,
            # Delete first one
            """
            DELETE FROM products WHERE product_id = 1000
            """,
            "COMMIT",
        ]

        for query in queries:
            mysql_connection.execute_query(query, fetch=False)

        time.sleep(3)

        # Verify final state
        result = mysql_connection.execute_query(
            "SELECT * FROM products WHERE product_id IN (1000, 1001)"
        )

        # Only product 1001 should exist
        assert len(result) == 1
        assert result[0]["product_id"] == 1001

    def test_rollback_handling(self, mysql_connection, clean_products_table):
        """Test CDC handles rolled back transactions."""
        queries = [
            "START TRANSACTION",
            """
            INSERT INTO products (product_id, name, category, price, stock_quantity)
            VALUES (1000, 'Rollback Test', 'Test', 100.0, 50)
            """,
            "ROLLBACK",
        ]

        for query in queries:
            mysql_connection.execute_query(query, fetch=False)

        time.sleep(2)

        # Verify record was NOT inserted
        result = mysql_connection.execute_query(
            "SELECT COUNT(*) as count FROM products WHERE product_id = 1000"
        )

        assert result[0]["count"] == 0

    def test_null_value_handling(self, mysql_connection, clean_products_table):
        """Test CDC handles NULL values correctly."""
        mysql_connection.execute_query(
            """
            INSERT INTO products (product_id, name, category, price, stock_quantity, description)
            VALUES (1000, 'Null Test', NULL, 100.0, 50, NULL)
            """,
            fetch=False,
        )

        time.sleep(2)

        result = mysql_connection.execute_query(
            "SELECT * FROM products WHERE product_id = 1000"
        )

        assert result[0]["category"] is None
        assert result[0]["description"] is None
        assert result[0]["price"] == 100.0  # Non-null value

    def test_concurrent_updates(self, mysql_connection, clean_products_table):
        """Test CDC handles concurrent updates to same record."""
        # Insert initial record
        mysql_connection.execute_query(
            """
            INSERT INTO products (product_id, name, category, price, stock_quantity)
            VALUES (1000, 'Concurrent Test', 'Test', 100.0, 100)
            """,
            fetch=False,
        )

        time.sleep(1)

        # Multiple rapid updates
        for i in range(10):
            mysql_connection.execute_query(
                f"""
                UPDATE products SET stock_quantity = {100 - i}
                WHERE product_id = 1000
                """,
                fetch=False,
            )

        time.sleep(3)

        # Verify final state
        result = mysql_connection.execute_query(
            "SELECT stock_quantity FROM products WHERE product_id = 1000"
        )

        assert result[0]["stock_quantity"] == 91  # 100 - 9

    def test_large_text_field(self, mysql_connection, clean_products_table):
        """Test CDC handles large TEXT fields."""
        large_description = "Lorem ipsum dolor sit amet. " * 1000  # ~28KB

        mysql_connection.execute_query(
            """
            INSERT INTO products (product_id, name, category, price, stock_quantity, description)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            params=(1000, "Large Text Test", "Test", 100.0, 50, large_description),
            fetch=False,
        )

        time.sleep(2)

        result = mysql_connection.execute_query(
            "SELECT description FROM products WHERE product_id = 1000"
        )

        assert len(result[0]["description"]) > 25000

    def test_special_characters_in_data(self, mysql_connection, clean_products_table):
        """Test CDC handles special characters correctly."""
        special_name = "Test 'quotes' \"double\" & <html> émojis: 🎉"

        mysql_connection.execute_query(
            """
            INSERT INTO products (product_id, name, category, price, stock_quantity)
            VALUES (%s, %s, %s, %s, %s)
            """,
            params=(1000, special_name, "Test", 100.0, 50),
            fetch=False,
        )

        time.sleep(2)

        result = mysql_connection.execute_query(
            "SELECT name FROM products WHERE product_id = 1000"
        )

        assert result[0]["name"] == special_name
        assert "🎉" in result[0]["name"]

    def test_update_all_fields(self, mysql_connection, clean_products_table):
        """Test UPDATE that changes all fields."""
        # Insert
        mysql_connection.execute_query(
            """
            INSERT INTO products (product_id, name, category, price, stock_quantity)
            VALUES (1000, 'Before', 'Cat1', 100.0, 50)
            """,
            fetch=False,
        )

        time.sleep(1)

        # Update all fields
        mysql_connection.execute_query(
            """
            UPDATE products
            SET name = 'After', category = 'Cat2', price = 200.0, stock_quantity = 100
            WHERE product_id = 1000
            """,
            fetch=False,
        )

        time.sleep(2)

        result = mysql_connection.execute_query(
            "SELECT * FROM products WHERE product_id = 1000"
        )

        assert result[0]["name"] == "After"
        assert result[0]["category"] == "Cat2"
        assert result[0]["price"] == 200.0
        assert result[0]["stock_quantity"] == 100
