"""
Unit tests for catalog_utils module.

Tests the utility functions for handling catalog.schema.table three-part names.
"""

import pytest
from dbt.adapters.watsonx_spark.catalog_utils import CatalogUtils


class TestHasCatalogPrefix:
    """Test has_catalog_prefix function."""

    def test_with_catalog_prefix(self):
        """Test schema with catalog prefix."""
        assert CatalogUtils.has_catalog_prefix("iceberg_data.uk0429") is True
        assert CatalogUtils.has_catalog_prefix("spark_catalog.default") is True
        assert CatalogUtils.has_catalog_prefix("my_catalog.my_schema") is True

    def test_without_catalog_prefix(self):
        """Test schema without catalog prefix."""
        assert CatalogUtils.has_catalog_prefix("uk0429") is False
        assert CatalogUtils.has_catalog_prefix("default") is False
        assert CatalogUtils.has_catalog_prefix("my_schema") is False

    def test_empty_string(self):
        """Test empty string."""
        assert CatalogUtils.has_catalog_prefix("") is False

    def test_multiple_dots(self):
        """Test schema with multiple dots (should still return True)."""
        assert CatalogUtils.has_catalog_prefix("catalog.schema.extra") is True


class TestSplitCatalogSchema:
    """Test split_catalog_schema function."""

    def test_with_catalog_prefix(self):
        """Test splitting catalog.schema."""
        assert CatalogUtils.split_catalog_schema("iceberg_data.uk0429") == ("iceberg_data", "uk0429")
        assert CatalogUtils.split_catalog_schema("spark_catalog.default") == ("spark_catalog", "default")

    def test_without_catalog_prefix(self):
        """Test splitting schema without catalog."""
        assert CatalogUtils.split_catalog_schema("uk0429") == ("", "uk0429")
        assert CatalogUtils.split_catalog_schema("default") == ("", "default")

    def test_multiple_dots(self):
        """Test schema with multiple dots (splits on first dot only)."""
        assert CatalogUtils.split_catalog_schema("catalog.schema.extra") == ("catalog", "schema.extra")


class TestGetSchemaOnly:
    """Test get_schema_only function."""

    def test_with_catalog_prefix(self):
        """Test extracting schema from catalog.schema."""
        assert CatalogUtils.get_schema_only("iceberg_data.uk0429") == "uk0429"
        assert CatalogUtils.get_schema_only("spark_catalog.default") == "default"

    def test_without_catalog_prefix(self):
        """Test extracting schema when no catalog."""
        assert CatalogUtils.get_schema_only("uk0429") == "uk0429"
        assert CatalogUtils.get_schema_only("default") == "default"

    def test_empty_string(self):
        """Test empty string."""
        assert CatalogUtils.get_schema_only("") == ""

    def test_multiple_dots(self):
        """Test schema with multiple dots (returns everything after first dot)."""
        assert CatalogUtils.get_schema_only("catalog.schema.extra") == "schema.extra"


class TestGetCatalogOnly:
    """Test get_catalog_only function."""

    def test_with_catalog_prefix(self):
        """Test extracting catalog from catalog.schema."""
        assert CatalogUtils.get_catalog_only("iceberg_data.uk0429") == "iceberg_data"
        assert CatalogUtils.get_catalog_only("spark_catalog.default") == "spark_catalog"

    def test_without_catalog_prefix(self):
        """Test extracting catalog when none present."""
        assert CatalogUtils.get_catalog_only("uk0429") == ""
        assert CatalogUtils.get_catalog_only("default") == ""

    def test_empty_string(self):
        """Test empty string."""
        assert CatalogUtils.get_catalog_only("") == ""

    def test_multiple_dots(self):
        """Test schema with multiple dots (returns only first part)."""
        assert CatalogUtils.get_catalog_only("catalog.schema.extra") == "catalog"


class TestEnsureCatalogPrefix:
    """Test ensure_catalog_prefix function."""

    def test_add_catalog_prefix(self):
        """Test adding catalog prefix when missing."""
        assert CatalogUtils.ensure_catalog_prefix("uk0429", "iceberg_data") == "iceberg_data.uk0429"
        assert CatalogUtils.ensure_catalog_prefix("default", "spark_catalog") == "spark_catalog.default"

    def test_preserve_existing_prefix(self):
        """Test preserving existing catalog prefix."""
        assert CatalogUtils.ensure_catalog_prefix("iceberg_data.uk0429", "iceberg_data") == "iceberg_data.uk0429"
        assert CatalogUtils.ensure_catalog_prefix("spark_catalog.default", "spark_catalog") == "spark_catalog.default"

    def test_empty_catalog(self):
        """Test with empty catalog (should return schema unchanged)."""
        assert CatalogUtils.ensure_catalog_prefix("uk0429", "") == "uk0429"
        assert CatalogUtils.ensure_catalog_prefix("iceberg_data.uk0429", "") == "iceberg_data.uk0429"


class TestQuoteIdentifier:
    """Test quote_identifier function."""

    def test_simple_identifier(self):
        """Test quoting simple identifier."""
        assert CatalogUtils.quote_identifier("my_table") == "`my_table`"
        assert CatalogUtils.quote_identifier("schema") == "`schema`"

    def test_identifier_with_special_chars(self):
        """Test quoting identifier with special characters."""
        assert CatalogUtils.quote_identifier("test-special-chars") == "`test-special-chars`"
        assert CatalogUtils.quote_identifier("table@name") == "`table@name`"
        assert CatalogUtils.quote_identifier("my.table") == "`my.table`"

    def test_empty_identifier(self):
        """Test quoting empty identifier."""
        assert CatalogUtils.quote_identifier("") == "``"


class TestQuoteSchemaTable:
    """Test quote_schema_table function."""

    def test_simple_schema_table(self):
        """Test quoting simple schema.table."""
        assert CatalogUtils.quote_schema_table("uk0429", "my_table") == "`uk0429`.`my_table`"
        assert CatalogUtils.quote_schema_table("default", "users") == "`default`.`users`"

    def test_catalog_schema_table(self):
        """Test quoting catalog.schema.table - each part quoted separately."""
        assert CatalogUtils.quote_schema_table("iceberg_data.uk0429", "my_table") == "`iceberg_data`.`uk0429`.`my_table`"
        assert CatalogUtils.quote_schema_table("spark_catalog.default", "users") == "`spark_catalog`.`default`.`users`"

    def test_with_special_characters(self):
        """Test quoting with special characters."""
        assert CatalogUtils.quote_schema_table("uk0429", "test-special-chars") == "`uk0429`.`test-special-chars`"
        assert CatalogUtils.quote_schema_table("iceberg_data.uk0429", "test-special-chars") == "`iceberg_data`.`uk0429`.`test-special-chars`"


class TestBuildQualifiedName:
    """Test build_qualified_name function."""

    def test_with_quoting(self):
        """Test building qualified name with quoting."""
        assert CatalogUtils.build_qualified_name("uk0429", "my_table", quote=True) == "`uk0429`.`my_table`"
        assert CatalogUtils.build_qualified_name("iceberg_data.uk0429", "my_table", quote=True) == "`iceberg_data`.`uk0429`.`my_table`"

    def test_without_quoting(self):
        """Test building qualified name without quoting."""
        assert CatalogUtils.build_qualified_name("uk0429", "my_table", quote=False) == "uk0429.my_table"
        assert CatalogUtils.build_qualified_name("iceberg_data.uk0429", "my_table", quote=False) == "iceberg_data.uk0429.my_table"

    def test_default_quoting(self):
        """Test default quoting behavior (should be True)."""
        assert CatalogUtils.build_qualified_name("uk0429", "my_table") == "`uk0429`.`my_table`"


class TestNormalizeSchemaFromShowTables:
    """Test normalize_schema_from_show_tables function."""

    def test_restore_catalog_prefix(self):
        """Test restoring catalog prefix when SHOW TABLES loses it."""
        # SHOW TABLES returned "uk0429", context was "iceberg_data.uk0429"
        assert CatalogUtils.normalize_schema_from_show_tables("uk0429", "iceberg_data.uk0429") == "iceberg_data.uk0429"
        assert CatalogUtils.normalize_schema_from_show_tables("default", "spark_catalog.default") == "spark_catalog.default"

    def test_preserve_when_already_has_catalog(self):
        """Test preserving schema when it already has catalog prefix."""
        assert CatalogUtils.normalize_schema_from_show_tables("iceberg_data.uk0429", "iceberg_data.uk0429") == "iceberg_data.uk0429"
        assert CatalogUtils.normalize_schema_from_show_tables("spark_catalog.default", "spark_catalog.default") == "spark_catalog.default"

    def test_no_context(self):
        """Test when no context is provided."""
        assert CatalogUtils.normalize_schema_from_show_tables("uk0429", None) == "uk0429"
        assert CatalogUtils.normalize_schema_from_show_tables("iceberg_data.uk0429", None) == "iceberg_data.uk0429"

    def test_context_without_catalog(self):
        """Test when context also doesn't have catalog."""
        assert CatalogUtils.normalize_schema_from_show_tables("uk0429", "uk0429") == "uk0429"
        assert CatalogUtils.normalize_schema_from_show_tables("default", "default") == "default"


class TestIntegrationScenarios:
    """Integration tests for common usage scenarios."""

    def test_iceberg_table_workflow(self):
        """Test typical Iceberg table workflow."""
        # User configures catalog.schema
        schema = "iceberg_data.uk0429"
        table = "test-special-chars"
        
        # Check if has catalog
        assert CatalogUtils.has_catalog_prefix(schema) is True
        
        # Build qualified name
        qualified = CatalogUtils.build_qualified_name(schema, table, quote=True)
        assert qualified == "`iceberg_data.uk0429`.`test-special-chars`"
        
        # Extract components
        catalog, schema_only = CatalogUtils.split_catalog_schema(schema)
        assert catalog == "iceberg_data"
        assert schema_only == "uk0429"

    def test_show_tables_bug_workflow(self):
        """Test workflow for handling SHOW TABLES bug."""
        # Original context
        context_schema = "iceberg_data.uk0429"
        
        # SHOW TABLES returns only schema (bug)
        returned_schema = "uk0429"
        
        # Normalize to restore catalog
        normalized = CatalogUtils.normalize_schema_from_show_tables(returned_schema, context_schema)
        assert normalized == "iceberg_data.uk0429"
        
        # Build table name with restored catalog
        table = "my_table"
        qualified = CatalogUtils.quote_schema_table(normalized, table)
        assert qualified == "`iceberg_data.uk0429`.`my_table`"

    def test_s3_location_workflow(self):
        """Test workflow for building S3 location."""
        # Schema may have catalog prefix
        schema = "iceberg_data.uk0429"
        catalog = "iceberg_data"
        bucket = "my-bucket"
        
        # Extract schema only for S3 path
        schema_only = CatalogUtils.get_schema_only(schema)
        s3_path = f"s3a://{bucket}/{catalog}/{schema_only}"
        assert s3_path == "s3a://my-bucket/iceberg_data/uk0429"

    def test_delta_table_workflow(self):
        """Test typical Delta table workflow."""
        # Delta uses spark_catalog prefix
        schema = "spark_catalog.default"
        table = "my_table"
        
        # Check if has catalog
        assert CatalogUtils.has_catalog_prefix(schema) is True
        
        # Get catalog
        catalog = CatalogUtils.get_catalog_only(schema)
        assert catalog == "spark_catalog"
        
        # Build qualified name
        qualified = CatalogUtils.build_qualified_name(schema, table, quote=True)
        assert qualified == "`spark_catalog.default`.`my_table`"

# Made with Bob
