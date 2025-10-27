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
