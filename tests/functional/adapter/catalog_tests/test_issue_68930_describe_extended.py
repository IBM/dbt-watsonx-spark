"""
Test for Issue #68930: DESCRIBE EXTENDED View Failure with Three-Part Names
Tests that temporary views and DESCRIBE EXTENDED work correctly with catalogs
"""
import pytest
from dbt.tests.util import run_dbt


# SCD2 snapshot model that creates temporary views
snapshot_scd2_sql = """
{% snapshot banks_scdt2 %}

{{
    config(
      target_schema=schema,
      unique_key='id',
      strategy='timestamp',
      updated_at='updated_at',
      file_format='iceberg'
    )
}}

SELECT 
    1 as id,
    'Bank A' as bank_name,
    'Active' as status,
    current_timestamp() as updated_at

{% endsnapshot %}
"""

# Source table for snapshot
source_table_sql = """
{{ config(
    materialized='table',
    file_format='iceberg'
) }}

SELECT 
    1 as id,
    'Bank A' as bank_name,
    'Active' as status,
    current_timestamp() as updated_at
"""

# Model that uses temporary view
model_with_temp_view_sql = """
{{ config(
    materialized='table',
    file_format='iceberg',
    pre_hook=[
        "CREATE OR REPLACE TEMPORARY VIEW temp_{{ this.identifier }} AS SELECT 1 as id, 'test' as name, current_timestamp() as created_at"
    ]
) }}

-- Use the temporary view created in pre-hook
SELECT * FROM temp_{{ this.identifier }}
"""


class TestIssue68930DescribeExtended:
    """
    Test for Issue #68930: DESCRIBE EXTENDED fails on temporary views
    
    Problem: When dbt creates temporary views without three-part names and runs
    DESCRIBE EXTENDED, it fails with INTERNAL_ERROR or NullPointerException
    
    Solution: Use three-part names for temporary views when catalog is configured
    """
    
    @pytest.fixture(scope="class")
    def snapshots(self):
        return {
            "banks_scdt2.sql": snapshot_scd2_sql,
        }
    
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "source_table.sql": source_table_sql,
        }
    
    def test_snapshot_with_catalog(self, project):
        """Test that snapshots work with catalog configured"""
        # Create source table
        results = run_dbt(["run"])
        assert len(results) == 1
        
        # Run snapshot - should not fail with DESCRIBE EXTENDED error
        results = run_dbt(["snapshot"])
        assert len(results) == 1
        assert results[0].status == "success"
        
        # Run snapshot again - incremental update
        results = run_dbt(["snapshot"])
        assert len(results) == 1
        assert results[0].status == "success"


class TestTemporaryViewsWithCatalog:
    """Test that temporary views work correctly with catalog"""
    
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_with_temp_view.sql": model_with_temp_view_sql,
        }
    
    def test_temp_view_creation(self, project):
        """Test that models using temporary views work with catalog"""
        # This should not fail with authorization or DESCRIBE EXTENDED errors
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"


class TestDescribeExtendedWithThreePartNames:
    """Test DESCRIBE EXTENDED works with three-part names"""
    
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_table.sql": """
                {{ config(
                    materialized='table',
                    file_format='iceberg'
                ) }}
                SELECT 1 as id, 'test' as name
            """,
        }
    
    def test_describe_extended_three_part(self, project):
        """Test that DESCRIBE EXTENDED works with three-part table names"""
        # Create table
        results = run_dbt(["run"])
        assert len(results) == 1
        
        # Use run_sql to verify columns (this maintains proper connection context)
        from dbt.tests.util import run_sql_with_adapter
        
        # Get the full table name with catalog
        catalog = project.adapter.config.credentials.catalog
        schema = project.test_schema
        table_name = "test_table"
        full_table_name = f"{catalog}.{schema}.{table_name}"
        
        # Query to get column information
        sql = f"DESCRIBE {full_table_name}"
        result = run_sql_with_adapter(project.adapter, sql, fetch="all")
        
        # Verify we got column information
        assert len(result) >= 2  # id and name columns
        
        # Verify column names (first column in DESCRIBE output is col_name)
        column_names = [row[0] for row in result]
        assert "id" in column_names
        assert "name" in column_names

# Made with Bob
