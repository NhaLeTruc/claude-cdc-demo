"""Data quality tests for integrity validation."""

import pytest


@pytest.mark.data_quality
class TestIntegrityValidation:
    """Test data integrity validation for CDC pipelines."""

    def test_row_count_validation_postgres(self):
        """Test row count matches between Postgres and DeltaLake."""
        from src.validation.integrity import RowCountValidator

        # Mock source and destination data
        source_data = list(range(100))  # 100 records
        destination_data = list(range(100))  # 100 records

        validator = RowCountValidator(tolerance=0)
        result = validator.validate(source_data, destination_data)

        assert result.status.value == "passed"

    def test_row_count_mismatch_detection(self):
        """Test detecting row count mismatches."""
        from src.validation.integrity import RowCountValidator

        source_data = list(range(100))  # 100 records
        destination_data = list(range(95))  # 95 records

        validator = RowCountValidator(tolerance=0)
        result = validator.validate(source_data, destination_data)

        assert result.status.value == "failed"
        assert result.details["difference"] == 5

    def test_row_count_within_tolerance(self):
        """Test row count validation with tolerance."""
        from src.validation.integrity import RowCountValidator

        source_data = list(range(100))  # 100 records
        destination_data = list(range(98))  # 98 records

        validator = RowCountValidator(tolerance=2)
        result = validator.validate(source_data, destination_data)

        assert result.status.value == "passed"

    def test_checksum_validation_matching(self):
        """Test checksum validation for matching data."""
        from src.validation.integrity import ChecksumValidator

        source_data = [
            {"id": 1, "email": "test1@example.com", "created_at": "2024-01-01"},
            {"id": 2, "email": "test2@example.com", "created_at": "2024-01-02"},
        ]
        destination_data = [
            {"id": 1, "email": "test1@example.com", "created_at": "2024-01-01"},
            {"id": 2, "email": "test2@example.com", "created_at": "2024-01-02"},
        ]

        validator = ChecksumValidator(exclude_fields=["created_at"])
        result = validator.validate(source_data, destination_data)

        assert result.status.value == "passed"

    def test_checksum_mismatch_detection(self):
        """Test detecting checksum mismatches."""
        from src.validation.integrity import ChecksumValidator

        source_data = [
            {"id": 1, "email": "test1@example.com"},
            {"id": 2, "email": "test2@example.com"},
        ]
        destination_data = [
            {"id": 1, "email": "test1@example.com"},
            {"id": 2, "email": "different@example.com"},  # Mismatch
        ]

        validator = ChecksumValidator(exclude_fields=[])
        result = validator.validate(source_data, destination_data)

        assert result.status.value == "failed"
        assert result.details["mismatches"] == 1

    def test_field_validation(self):
        """Test field-level validation."""
        from src.validation.integrity import FieldValidator

        source_data = [
            {"customer_id": 1, "email": "test@example.com"},
            {"customer_id": 2, "email": "test2@example.com"},
        ]
        destination_data = [
            {"customer_id": 1, "email": "test@example.com"},
            {"customer_id": 2, "email": "test2@example.com"},
        ]

        validator = FieldValidator(field_name="email", required=True)
        result = validator.validate(source_data, destination_data)

        assert result.status.value == "passed"

    def test_referential_integrity_validation(self):
        """Test referential integrity between tables."""
        from src.validation.integrity import ReferentialIntegrityValidator

        # Orders reference customers
        orders_data = [
            {"order_id": 1, "customer_id": 1, "amount": 100},
            {"order_id": 2, "customer_id": 2, "amount": 200},
        ]
        customers_data = [
            {"customer_id": 1, "email": "test1@example.com"},
            {"customer_id": 2, "email": "test2@example.com"},
        ]

        validator = ReferentialIntegrityValidator(
            child_table="orders",
            parent_table="customers",
            foreign_key="customer_id",
            parent_key="customer_id",
        )
        result = validator.validate(orders_data, customers_data)

        assert result.status.value == "passed"

    def test_orphaned_records_detection(self):
        """Test detecting orphaned records."""
        from src.validation.integrity import ReferentialIntegrityValidator

        # Order references non-existent customer
        orders_data = [
            {"order_id": 1, "customer_id": 1, "amount": 100},
            {"order_id": 2, "customer_id": 999, "amount": 200},  # Orphaned
        ]
        customers_data = [
            {"customer_id": 1, "email": "test1@example.com"},
        ]

        validator = ReferentialIntegrityValidator(
            child_table="orders",
            parent_table="customers",
            foreign_key="customer_id",
            parent_key="customer_id",
        )
        result = validator.validate(orders_data, customers_data)

        assert result.status.value == "failed"
        assert result.details["orphaned_count"] == 1

    def test_mysql_data_integrity(self):
        """Test MySQL-specific data integrity validation."""
        from src.validation.integrity import RowCountValidator

        # Simulate MySQL products data
        source_data = list(range(200))  # 200 products
        destination_data = list(range(200))  # 200 products

        validator = RowCountValidator(tolerance=0)
        result = validator.validate(source_data, destination_data)

        assert result.status.value == "passed"

    def test_mysql_decimal_precision(self):
        """Test MySQL decimal precision is maintained."""
        from src.validation.integrity import ChecksumValidator

        source_data = [
            {"product_id": 1, "price": 99.99, "discount": 0.15},
            {"product_id": 2, "price": 149.99, "discount": 0.20},
        ]
        destination_data = [
            {"product_id": 1, "price": 99.99, "discount": 0.15},
            {"product_id": 2, "price": 149.99, "discount": 0.20},
        ]

        validator = ChecksumValidator(exclude_fields=[])
        result = validator.validate(source_data, destination_data)

        assert result.status.value == "passed"

    def test_mysql_varchar_length_preservation(self):
        """Test MySQL VARCHAR length is preserved."""
        from src.validation.integrity import FieldValidator

        long_description = "A" * 1000

        source_data = [
            {"product_id": 1, "description": long_description},
        ]
        destination_data = [
            {"product_id": 1, "description": long_description},
        ]

        validator = FieldValidator(field_name="description", required=True)
        result = validator.validate(source_data, destination_data)

        assert result.status.value == "passed"

    def test_delta_cdf_data_integrity(self):
        """Test DeltaLake CDF data integrity validation."""
        from src.validation.integrity import RowCountValidator

        # Simulate Delta CDF changes
        original_data = list(range(100))
        cdf_changes = list(range(100))  # All records should match

        validator = RowCountValidator(tolerance=0)
        result = validator.validate(original_data, cdf_changes)

        assert result.status.value == "passed"

    def test_delta_cdf_change_tracking(self):
        """Test Delta CDF tracks all changes correctly."""
        from src.validation.integrity import ChecksumValidator

        # Original data
        source_data = [
            {"id": 1, "value": "A", "_change_type": "insert"},
            {"id": 1, "value": "B", "_change_type": "update_postimage"},
        ]

        # CDF should have both changes
        cdf_data = [
            {"id": 1, "value": "A", "_change_type": "insert"},
            {"id": 1, "value": "B", "_change_type": "update_postimage"},
        ]

        validator = ChecksumValidator(exclude_fields=["_commit_timestamp"])
        result = validator.validate(source_data, cdf_data)

        assert result.status.value == "passed"
