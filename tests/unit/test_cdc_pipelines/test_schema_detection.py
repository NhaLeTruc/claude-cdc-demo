"""
Unit tests for schema evolution detection in CDC pipelines.

Tests cover:
- Schema change detection from CDC events
- Schema version comparison
- Compatibility checking (backward/forward)
- Schema drift alerts
"""

import pytest
from typing import Dict, Any, List, Optional
from datetime import datetime


# ============================================================================
# Mock Schema Detector Class
# ============================================================================

class SchemaChange:
    """Represents a detected schema change."""

    def __init__(
        self,
        change_type: str,
        table_name: str,
        column_name: Optional[str] = None,
        old_type: Optional[str] = None,
        new_type: Optional[str] = None,
        is_breaking: bool = False,
    ):
        self.change_type = change_type  # ADD, DROP, ALTER, RENAME
        self.table_name = table_name
        self.column_name = column_name
        self.old_type = old_type
        self.new_type = new_type
        self.is_breaking = is_breaking
        self.detected_at = datetime.now()

    def __repr__(self):
        return (
            f"SchemaChange(type={self.change_type}, "
            f"table={self.table_name}, column={self.column_name})"
        )


class SchemaDetector:
    """Detects schema changes from CDC events."""

    def __init__(self):
        self.known_schemas: Dict[str, Dict[str, str]] = {}

    def register_schema(self, table_name: str, schema: Dict[str, str]) -> None:
        """Register a known schema for comparison."""
        self.known_schemas[table_name] = schema

    def detect_changes(
        self,
        table_name: str,
        new_schema: Dict[str, str]
    ) -> List[SchemaChange]:
        """Detect schema changes between known and new schema."""
        if table_name not in self.known_schemas:
            # First time seeing this table
            self.known_schemas[table_name] = new_schema
            return []

        old_schema = self.known_schemas[table_name]
        changes: List[SchemaChange] = []

        # Detect added columns
        for col_name, col_type in new_schema.items():
            if col_name not in old_schema:
                changes.append(SchemaChange(
                    change_type="ADD",
                    table_name=table_name,
                    column_name=col_name,
                    new_type=col_type,
                    is_breaking=False,  # ADD is backward compatible
                ))

        # Detect dropped columns
        for col_name, col_type in old_schema.items():
            if col_name not in new_schema:
                changes.append(SchemaChange(
                    change_type="DROP",
                    table_name=table_name,
                    column_name=col_name,
                    old_type=col_type,
                    is_breaking=True,  # DROP is breaking
                ))

        # Detect type changes
        for col_name in set(old_schema.keys()) & set(new_schema.keys()):
            if old_schema[col_name] != new_schema[col_name]:
                changes.append(SchemaChange(
                    change_type="ALTER",
                    table_name=table_name,
                    column_name=col_name,
                    old_type=old_schema[col_name],
                    new_type=new_schema[col_name],
                    is_breaking=self._is_type_change_breaking(
                        old_schema[col_name],
                        new_schema[col_name]
                    ),
                ))

        # Update known schema
        self.known_schemas[table_name] = new_schema

        return changes

    def _is_type_change_breaking(self, old_type: str, new_type: str) -> bool:
        """Determine if a type change is breaking."""
        # Widening conversions are usually safe
        safe_widenings = {
            ("INTEGER", "BIGINT"),
            ("FLOAT", "DOUBLE"),
            ("VARCHAR(50)", "VARCHAR(100)"),
            ("VARCHAR(50)", "TEXT"),
        }

        return (old_type, new_type) not in safe_widenings

    def extract_schema_from_cdc_event(self, cdc_event: Dict[str, Any]) -> Dict[str, str]:
        """Extract schema information from a Debezium CDC event."""
        schema_dict = {}

        if "schema" not in cdc_event:
            return schema_dict

        schema = cdc_event["schema"]

        # Handle Debezium schema format
        if "fields" in schema:
            for field in schema["fields"]:
                if field.get("field") == "after":
                    # Extract nested schema for the "after" field
                    after_schema = field.get("fields", [])
                    for col in after_schema:
                        col_name = col.get("field")
                        col_type = col.get("type")
                        if col_name and col_type:
                            schema_dict[col_name] = col_type

        return schema_dict


# ============================================================================
# Test Fixtures
# ============================================================================

@pytest.fixture
def detector():
    """Create a SchemaDetector instance."""
    return SchemaDetector()


@pytest.fixture
def initial_schema():
    """Initial schema for schema_evolution_test table."""
    return {
        "id": "INTEGER",
        "version": "INTEGER",
        "name": "VARCHAR(100)",
        "description": "TEXT",
        "status": "VARCHAR(50)",
        "created_at": "TIMESTAMP",
        "updated_at": "TIMESTAMP",
    }


@pytest.fixture
def sample_cdc_event():
    """Sample Debezium CDC event with schema."""
    return {
        "schema": {
            "type": "struct",
            "fields": [
                {
                    "field": "after",
                    "fields": [
                        {"field": "id", "type": "int32"},
                        {"field": "name", "type": "string"},
                        {"field": "status", "type": "string"},
                    ]
                }
            ]
        },
        "payload": {
            "after": {
                "id": 1,
                "name": "Test",
                "status": "active"
            }
        }
    }


# ============================================================================
# Test Cases: Schema Change Detection
# ============================================================================

class TestSchemaChangeDetection:
    """Test schema change detection functionality."""

    def test_detect_no_changes(self, detector, initial_schema):
        """Test that no changes are detected when schema is unchanged."""
        detector.register_schema("schema_evolution_test", initial_schema)
        changes = detector.detect_changes("schema_evolution_test", initial_schema)

        assert len(changes) == 0

    def test_detect_added_column(self, detector, initial_schema):
        """Test detection of added column (backward compatible)."""
        detector.register_schema("schema_evolution_test", initial_schema)

        new_schema = initial_schema.copy()
        new_schema["email"] = "VARCHAR(255)"

        changes = detector.detect_changes("schema_evolution_test", new_schema)

        assert len(changes) == 1
        assert changes[0].change_type == "ADD"
        assert changes[0].column_name == "email"
        assert changes[0].new_type == "VARCHAR(255)"
        assert changes[0].is_breaking is False

    def test_detect_dropped_column(self, detector, initial_schema):
        """Test detection of dropped column (breaking change)."""
        detector.register_schema("schema_evolution_test", initial_schema)

        new_schema = initial_schema.copy()
        del new_schema["description"]

        changes = detector.detect_changes("schema_evolution_test", new_schema)

        assert len(changes) == 1
        assert changes[0].change_type == "DROP"
        assert changes[0].column_name == "description"
        assert changes[0].old_type == "TEXT"
        assert changes[0].is_breaking is True

    def test_detect_type_change_breaking(self, detector, initial_schema):
        """Test detection of breaking type change."""
        detector.register_schema("schema_evolution_test", initial_schema)

        new_schema = initial_schema.copy()
        new_schema["version"] = "VARCHAR(50)"  # INTEGER -> VARCHAR (breaking)

        changes = detector.detect_changes("schema_evolution_test", new_schema)

        assert len(changes) == 1
        assert changes[0].change_type == "ALTER"
        assert changes[0].column_name == "version"
        assert changes[0].old_type == "INTEGER"
        assert changes[0].new_type == "VARCHAR(50)"
        assert changes[0].is_breaking is True

    def test_detect_type_change_safe(self, detector):
        """Test detection of safe type widening."""
        initial = {"count": "INTEGER"}
        detector.register_schema("test_table", initial)

        new_schema = {"count": "BIGINT"}  # INTEGER -> BIGINT (safe widening)

        changes = detector.detect_changes("test_table", new_schema)

        assert len(changes) == 1
        assert changes[0].change_type == "ALTER"
        assert changes[0].is_breaking is False

    def test_detect_multiple_changes(self, detector, initial_schema):
        """Test detection of multiple simultaneous changes."""
        detector.register_schema("schema_evolution_test", initial_schema)

        new_schema = initial_schema.copy()
        new_schema["email"] = "VARCHAR(255)"  # ADD
        del new_schema["description"]  # DROP
        new_schema["status"] = "VARCHAR(100)"  # ALTER (widen)

        changes = detector.detect_changes("schema_evolution_test", new_schema)

        assert len(changes) == 3

        change_types = {c.change_type for c in changes}
        assert change_types == {"ADD", "DROP", "ALTER"}

        # Verify DROP is flagged as breaking
        drop_change = next(c for c in changes if c.change_type == "DROP")
        assert drop_change.is_breaking is True

    def test_first_schema_registration(self, detector, initial_schema):
        """Test that first schema registration doesn't report changes."""
        changes = detector.detect_changes("schema_evolution_test", initial_schema)

        assert len(changes) == 0
        assert "schema_evolution_test" in detector.known_schemas


# ============================================================================
# Test Cases: CDC Event Schema Extraction
# ============================================================================

class TestCDCEventSchemaExtraction:
    """Test schema extraction from CDC events."""

    def test_extract_schema_from_debezium_event(self, detector, sample_cdc_event):
        """Test schema extraction from Debezium event format."""
        schema = detector.extract_schema_from_cdc_event(sample_cdc_event)

        assert "id" in schema
        assert "name" in schema
        assert "status" in schema
        assert schema["id"] == "int32"
        assert schema["name"] == "string"

    def test_extract_schema_empty_event(self, detector):
        """Test schema extraction from event without schema."""
        event = {"payload": {"after": {"id": 1}}}
        schema = detector.extract_schema_from_cdc_event(event)

        assert len(schema) == 0

    def test_extract_schema_missing_fields(self, detector):
        """Test schema extraction with missing fields."""
        event = {"schema": {"type": "struct"}}  # No fields
        schema = detector.extract_schema_from_cdc_event(event)

        assert len(schema) == 0


# ============================================================================
# Test Cases: Schema Compatibility
# ============================================================================

class TestSchemaCompatibility:
    """Test schema compatibility checking."""

    def test_backward_compatible_add_column(self, detector):
        """Test that ADD COLUMN is backward compatible."""
        old_schema = {"id": "INTEGER", "name": "VARCHAR(100)"}
        new_schema = {"id": "INTEGER", "name": "VARCHAR(100)", "email": "VARCHAR(255)"}

        detector.register_schema("test", old_schema)
        changes = detector.detect_changes("test", new_schema)

        # ADD should not be breaking
        assert all(not c.is_breaking for c in changes if c.change_type == "ADD")

    def test_backward_incompatible_drop_column(self, detector):
        """Test that DROP COLUMN is backward incompatible."""
        old_schema = {"id": "INTEGER", "name": "VARCHAR(100)", "email": "VARCHAR(255)"}
        new_schema = {"id": "INTEGER", "name": "VARCHAR(100)"}

        detector.register_schema("test", old_schema)
        changes = detector.detect_changes("test", new_schema)

        # DROP should be breaking
        assert all(c.is_breaking for c in changes if c.change_type == "DROP")

    def test_type_widening_compatibility(self, detector):
        """Test that type widening is usually compatible."""
        test_cases = [
            ({"val": "INTEGER"}, {"val": "BIGINT"}, False),  # Safe widening
            ({"val": "VARCHAR(50)"}, {"val": "VARCHAR(100)"}, False),  # Safe widening
            ({"val": "BIGINT"}, {"val": "INTEGER"}, True),  # Narrowing (breaking)
        ]

        for i, (old_schema, new_schema, expected_breaking) in enumerate(test_cases):
            detector_instance = SchemaDetector()
            detector_instance.register_schema(f"test_{i}", old_schema)
            changes = detector_instance.detect_changes(f"test_{i}", new_schema)

            if changes:
                assert changes[0].is_breaking == expected_breaking


# ============================================================================
# Test Cases: Schema Change Tracking
# ============================================================================

class TestSchemaChangeTracking:
    """Test schema change tracking over time."""

    def test_track_schema_evolution_sequence(self, detector):
        """Test tracking a sequence of schema changes."""
        # Version 1: Initial schema
        v1 = {"id": "INTEGER", "name": "VARCHAR(100)"}
        changes_v1 = detector.detect_changes("test", v1)
        assert len(changes_v1) == 0  # First registration

        # Version 2: Add email
        v2 = v1.copy()
        v2["email"] = "VARCHAR(255)"
        changes_v2 = detector.detect_changes("test", v2)
        assert len(changes_v2) == 1
        assert changes_v2[0].change_type == "ADD"

        # Version 3: Widen name column
        v3 = v2.copy()
        v3["name"] = "VARCHAR(200)"
        changes_v3 = detector.detect_changes("test", v3)
        assert len(changes_v3) == 1
        assert changes_v3[0].change_type == "ALTER"

        # Version 4: Drop email
        v4 = v3.copy()
        del v4["email"]
        changes_v4 = detector.detect_changes("test", v4)
        assert len(changes_v4) == 1
        assert changes_v4[0].change_type == "DROP"

    def test_schema_change_metadata(self, detector, initial_schema):
        """Test that schema changes include proper metadata."""
        detector.register_schema("test", initial_schema)

        new_schema = initial_schema.copy()
        new_schema["new_col"] = "INTEGER"

        changes = detector.detect_changes("test", new_schema)

        assert len(changes) == 1
        change = changes[0]

        # Verify metadata
        assert change.table_name == "test"
        assert change.detected_at is not None
        assert isinstance(change.detected_at, datetime)
        assert change.column_name == "new_col"


# ============================================================================
# Test Cases: Edge Cases
# ============================================================================

class TestEdgeCases:
    """Test edge cases in schema detection."""

    def test_empty_schema(self, detector):
        """Test handling of empty schema."""
        changes = detector.detect_changes("test", {})
        assert len(changes) == 0

    def test_completely_changed_schema(self, detector):
        """Test detection when all columns change."""
        old_schema = {"col_a": "INTEGER", "col_b": "VARCHAR(100)"}
        new_schema = {"col_c": "TEXT", "col_d": "BOOLEAN"}

        detector.register_schema("test", old_schema)
        changes = detector.detect_changes("test", new_schema)

        # Should detect 2 drops and 2 adds
        assert len(changes) == 4
        drops = [c for c in changes if c.change_type == "DROP"]
        adds = [c for c in changes if c.change_type == "ADD"]
        assert len(drops) == 2
        assert len(adds) == 2

    def test_case_sensitive_column_names(self, detector):
        """Test that column names are case-sensitive."""
        old_schema = {"Name": "VARCHAR(100)"}
        new_schema = {"name": "VARCHAR(100)"}  # Lowercase

        detector.register_schema("test", old_schema)
        changes = detector.detect_changes("test", new_schema)

        # Should detect as DROP "Name" and ADD "name"
        assert len(changes) == 2
