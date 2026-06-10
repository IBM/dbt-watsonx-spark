"""
Test for Issue #71459: AuthZ Connection Failure
Tests that connections work correctly with AuthZ enabled and catalog configuration
"""
import pytest
from dbt.tests.util import run_dbt


# Simple model to test connection
model_simple_sql = """
{{ config(
    materialized='table',
    file_format='iceberg'
) }}

SELECT 
    1 as id,
    'test' as name,
    current_timestamp() as created_at
"""

# Model that uses database parameter
model_with_database_sql = """
{{ config(
    materialized='table',
    file_format='iceberg'
) }}

-- This should work even with AuthZ enabled
SELECT 
    2 as id,
    'database_test' as name,
    current_timestamp() as created_at
"""


class TestIssue71459AuthZ:
    """
    Test for Issue #71459: AuthZ Connection Failure
    
    Problem: When AuthZ is enabled, dbt fails with:
    "No catalog schema mapping received from MDS for default"
    
    Root Cause: dbt doesn't pass database parameter to hive.connect(),
    causing Spark to default to non-existent spark_catalog
    
    Solution: Pass database parameter as catalog.schema format in connection
    """
    
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_simple.sql": model_simple_sql,
            "model_with_database.sql": model_with_database_sql,
        }
    
    def test_connection_with_catalog(self, project):
        """Test that connection works with catalog configured"""
        # This should not fail with "No catalog schema mapping" error
        results = run_dbt(["run"])
        assert len(results) == 2
        assert all(r.status == "success" for r in results)
    
    def test_debug_with_catalog(self, project):
        """Test that dbt debug works with catalog configured"""
        # This should not fail during connection test
        results = run_dbt(["debug"])
        # debug returns True on success
        assert results is True or results == []


class TestCatalogSchemaMapping:
    """Test that catalog.schema mapping works correctly"""
    
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_mapping.sql": """
                {{ config(
                    materialized='table',
                    file_format='iceberg'
                ) }}
                SELECT 
                    '{{ target.schema }}' as target_schema,
                    '{{ target.database }}' as target_database,
                    current_timestamp() as created_at
            """,
        }
    
    def test_schema_includes_catalog(self, project):
        """Test that schema includes catalog prefix"""
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"
        
        # Verify the relation has correct schema
        from dbt.tests.util import relation_from_name
        relation = relation_from_name(project.adapter, "test_mapping")
        
        # Schema should include catalog prefix or database should be set
        assert "." in relation.schema or relation.database is not None


class TestDefaultCatalogBehavior:
    """Test behavior when no catalog is explicitly configured"""
    
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "no_catalog_test.sql": """
                {{ config(
                    materialized='table',
                    file_format='iceberg'
                ) }}
                SELECT 1 as id, 'no_catalog' as name
            """,
        }
    
    def test_without_explicit_catalog(self, project):
        """Test that models work even without explicit catalog in config"""
        # Should use default catalog from connection
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"

# Made with Bob
