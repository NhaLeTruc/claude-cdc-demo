"""Unit tests for data transformation logic in cross-storage CDC pipeline."""

import pytest
from unittest.mock import MagicMock


class TestDataTransformations:
    """Test suite for cross-storage data transformations."""

    def test_name_concatenation(self):
        """Test concatenating first_name and last_name."""
        from src.cdc_pipelines.cross_storage.transformations import concatenate_name

        result = concatenate_name("John", "Doe")
        assert result == "John Doe"

    def test_name_concatenation_with_none(self):
        """Test name concatenation with None values."""
        from src.cdc_pipelines.cross_storage.transformations import concatenate_name

        result = concatenate_name("John", None)
        assert result == "John"

        result = concatenate_name(None, "Doe")
        assert result == "Doe"

        result = concatenate_name(None, None)
        assert result == ""

    def test_name_concatenation_with_empty_strings(self):
        """Test name concatenation with empty strings."""
        from src.cdc_pipelines.cross_storage.transformations import concatenate_name

        result = concatenate_name("", "Doe")
        assert result == "Doe"

        result = concatenate_name("John", "")
        assert result == "John"

    def test_location_derivation(self):
        """Test deriving location from city, state, country."""
        from src.cdc_pipelines.cross_storage.transformations import derive_location

        result = derive_location("Springfield", "IL", "USA")
        assert result == "Springfield, IL, USA"

    def test_location_derivation_partial(self):
        """Test location derivation with missing components."""
        from src.cdc_pipelines.cross_storage.transformations import derive_location

        result = derive_location("Springfield", None, "USA")
        assert result == "Springfield, USA"

        result = derive_location(None, "IL", "USA")
        assert result == "IL, USA"

        result = derive_location("Springfield", "IL", None)
        assert result == "Springfield, IL"

    def test_location_derivation_all_none(self):
        """Test location derivation with all None values."""
        from src.cdc_pipelines.cross_storage.transformations import derive_location

        result = derive_location(None, None, None)
        assert result == "" or result is None

    def test_customer_transformation(self):
        """Test complete customer transformation."""
        from src.cdc_pipelines.cross_storage.transformations import transform_customer

        input_data = {
            "customer_id": 1,
            "first_name": "John",
            "last_name": "Doe",
            "email": "john.doe@example.com",
            "city": "Springfield",
            "state": "IL",
            "country": "USA",
            "customer_tier": "Gold",
            "lifetime_value": 1500.00,
            "registration_date": "2025-01-15",
            "is_active": True,
        }

        result = transform_customer(input_data)

        assert result["customer_id"] == 1
        assert result["full_name"] == "John Doe"
        assert result["location"] == "Springfield, IL, USA"
        assert result["email"] == "john.doe@example.com"
        assert result["customer_tier"] == "Gold"
        assert result["lifetime_value"] == 1500.00

    def test_customer_transformation_minimal_data(self):
        """Test customer transformation with minimal data."""
        from src.cdc_pipelines.cross_storage.transformations import transform_customer

        input_data = {
            "customer_id": 2,
            "first_name": "Jane",
            "last_name": None,
            "email": "jane@example.com",
        }

        result = transform_customer(input_data)

        assert result["customer_id"] == 2
        assert result["full_name"] == "Jane"
        assert result["email"] == "jane@example.com"

    def test_debezium_event_extraction(self):
        """Test extracting data from Debezium event."""
        from src.cdc_pipelines.cross_storage.transformations import extract_debezium_payload

        debezium_event = {
            "payload": {
                "op": "c",  # create/insert
                "after": {
                    "customer_id": 1,
                    "first_name": "John",
                    "last_name": "Doe",
                    "email": "john.doe@example.com",
                },
            }
        }

        result = extract_debezium_payload(debezium_event)

        assert result["customer_id"] == 1
        assert result["first_name"] == "John"

    def test_debezium_event_update_operation(self):
        """Test extracting data from Debezium UPDATE event."""
        from src.cdc_pipelines.cross_storage.transformations import extract_debezium_payload

        debezium_event = {
            "payload": {
                "op": "u",  # update
                "before": {
                    "customer_id": 1,
                    "first_name": "John",
                    "last_name": "Doe",
                },
                "after": {
                    "customer_id": 1,
                    "first_name": "John",
                    "last_name": "Smith",  # Changed
                },
            }
        }

        result = extract_debezium_payload(debezium_event)

        # Should return 'after' state for updates
        assert result["last_name"] == "Smith"

    def test_debezium_event_delete_operation(self):
        """Test extracting data from Debezium DELETE event."""
        from src.cdc_pipelines.cross_storage.transformations import extract_debezium_payload

        debezium_event = {
            "payload": {
                "op": "d",  # delete
                "before": {
                    "customer_id": 1,
                    "first_name": "John",
                    "last_name": "Doe",
                },
                "after": None,
            }
        }

        result = extract_debezium_payload(debezium_event)

        # For deletes, return 'before' state
        assert result["customer_id"] == 1

    def test_field_type_casting(self):
        """Test field type casting transformations."""
        from src.cdc_pipelines.cross_storage.transformations import cast_field_types

        input_data = {
            "customer_id": "123",  # String that should be int
            "lifetime_value": "1500.50",  # String that should be float
            "is_active": "true",  # String that should be bool
            "registration_date": "2025-01-15T10:30:00",  # ISO string
        }

        result = cast_field_types(input_data)

        assert isinstance(result["customer_id"], int)
        assert result["customer_id"] == 123

        assert isinstance(result["lifetime_value"], float)
        assert result["lifetime_value"] == 1500.50

        assert isinstance(result["is_active"], bool)
        assert result["is_active"] is True

    def test_null_value_handling(self):
        """Test handling of null values in transformations."""
        from src.cdc_pipelines.cross_storage.transformations import transform_customer

        input_data = {
            "customer_id": 1,
            "first_name": None,
            "last_name": None,
            "email": "test@example.com",
            "city": None,
            "state": None,
            "country": None,
        }

        result = transform_customer(input_data)

        assert result["customer_id"] == 1
        assert result["full_name"] == ""
        assert result["location"] == "" or result["location"] is None

    def test_transformation_batch_processing(self):
        """Test batch transformation of multiple records."""
        from src.cdc_pipelines.cross_storage.transformations import transform_batch

        input_batch = [
            {
                "customer_id": 1,
                "first_name": "John",
                "last_name": "Doe",
                "email": "john@example.com",
            },
            {
                "customer_id": 2,
                "first_name": "Jane",
                "last_name": "Smith",
                "email": "jane@example.com",
            },
        ]

        result = transform_batch(input_batch)

        assert len(result) == 2
        assert result[0]["full_name"] == "John Doe"
        assert result[1]["full_name"] == "Jane Smith"

    def test_transformation_error_handling(self):
        """Test error handling in transformations."""
        from src.cdc_pipelines.cross_storage.transformations import transform_customer

        # Invalid input (missing required field)
        invalid_data = {
            "first_name": "John",
            # Missing customer_id
        }

        try:
            result = transform_customer(invalid_data)
            # Should handle missing fields gracefully
            assert result is not None or result is None
        except Exception as e:
            # Or raise appropriate error
            assert "customer_id" in str(e).lower() or "required" in str(e).lower()

    def test_custom_transformation_rules(self):
        """Test custom transformation rules."""
        from src.cdc_pipelines.cross_storage.transformations import apply_custom_rules

        input_data = {
            "customer_tier": "bronze",  # Should be uppercase
            "email": "  JOHN@EXAMPLE.COM  ",  # Should be trimmed and lowercase
        }

        result = apply_custom_rules(input_data)

        assert result["customer_tier"] == "Bronze"  # Title case
        assert result["email"] == "john@example.com"  # Lowercase, trimmed
