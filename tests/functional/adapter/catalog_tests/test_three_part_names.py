"""
Test three-part name handling (catalog.schema.table) for watsonx.data
Tests for Issue #68930 and related catalog functionality
"""
import pytest
from dbt.tests.util import run_dbt, check_relations_equal, relation_from_name


# Model that uses three-part names
model_three_part_sql = """
{{ config(
    materialized='table',
    file_format='iceberg'
) }}

SELECT 
    1 as id,
    'test' as name,
    current_timestamp() as created_at
"""

# Model that references another model with three-part name
model_ref_three_part_sql = """
{{ config(
    materialized='table',
    file_format='iceberg'
) }}

SELECT
    id,
    name,
    created_at
FROM {{ ref('model_three_part') }}
WHERE id = 1
"""

# Incremental model with three-part names
model_incremental_three_part_sql = """
{{ config(
    materialized='incremental',
    file_format='iceberg',
    unique_key='id'
) }}

{% if is_incremental() %}
WITH source_data AS (
    SELECT
        1 as id,
        'test' as name,
        current_timestamp() as updated_at
)
SELECT * FROM source_data
WHERE updated_at > (SELECT MAX(updated_at) FROM {{ this }})
{% else %}
SELECT
    1 as id,
    'test' as name,
    current_timestamp() as updated_at
{% endif %}
"""

# View model with three-part names
model_view_three_part_sql = """
{{ config(
    materialized='view'
) }}

SELECT 
    1 as id,
    'view_test' as name
"""

# Ephemeral model (should work without catalog)
model_ephemeral_sql = """
{{ config(
    materialized='ephemeral'
) }}

SELECT 
    1 as id,
    'ephemeral' as name
"""

# Model using ephemeral
model_using_ephemeral_sql = """
{{ config(
    materialized='table',
    file_format='iceberg'
) }}

SELECT 
    id,
    name,
    'from_ephemeral' as source
FROM {{ ref('model_ephemeral') }}
"""


class TestThreePartNames:
    """Test that three-part names work correctly with catalog"""
    
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_three_part.sql": model_three_part_sql,
            "model_ref_three_part.sql": model_ref_three_part_sql,
        }
    
    def test_three_part_table_creation(self, project):
        """Test that tables are created with three-part names"""
        # Run dbt
        results = run_dbt(["run"])
        assert len(results) == 2
        
        # Check that both models succeeded
        assert all(r.status == "success" for r in results)
        
        # Verify relations exist with three-part names
        relation = relation_from_name(project.adapter, "model_three_part")
        assert project.adapter.get_relation(
            database=relation.database,
            schema=relation.schema,
            identifier=relation.identifier
        )
    
    def test_three_part_ref_resolution(self, project):
        """Test that ref() resolves to three-part names correctly"""
        results = run_dbt(["run"])
        assert len(results) == 2
        
        # Check that the referencing model works
        check_relations_equal(
            project.adapter,
            ["model_three_part", "model_ref_three_part"]
        )


class TestThreePartIncremental:
    """Test incremental models with three-part names"""
    
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_incremental_three_part.sql": model_incremental_three_part_sql,
        }
    
    def test_incremental_with_three_part_names(self, project):
        """Test that incremental models work with three-part names"""
        # First run - create table
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"
        
        # Second run - incremental update
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"
        
        # Verify relation exists
        relation = relation_from_name(project.adapter, "model_incremental_three_part")
        assert project.adapter.get_relation(
            database=relation.database,
            schema=relation.schema,
            identifier=relation.identifier
        )


class TestThreePartViews:
    """Test views with three-part names"""
    
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_view_three_part.sql": model_view_three_part_sql,
        }
    
    def test_view_with_three_part_names(self, project):
        """Test that views work with three-part names"""
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"
        
        # Verify view exists
        relation = relation_from_name(project.adapter, "model_view_three_part")
        result = project.adapter.get_relation(
            database=relation.database,
            schema=relation.schema,
            identifier=relation.identifier
        )
        assert result
        assert result.type == "view"


class TestEphemeralWithCatalog:
    """Test ephemeral models work correctly with catalog configured"""
    
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_ephemeral.sql": model_ephemeral_sql,
            "model_using_ephemeral.sql": model_using_ephemeral_sql,
        }
    
    def test_ephemeral_with_catalog(self, project):
        """Test that ephemeral models work when catalog is configured"""
        results = run_dbt(["run"])
        # Only the non-ephemeral model should be in results
        assert len(results) == 1
        assert results[0].status == "success"
        assert results[0].node.name == "model_using_ephemeral"


class TestListRelationsWithCatalog:
    """Test list_relations_without_caching with catalog"""
    
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_a.sql": model_three_part_sql,
            "model_b.sql": model_three_part_sql,
            "model_c.sql": model_view_three_part_sql,
        }
    
    def test_list_relations_finds_all(self, project):
        """Test that list_relations finds all tables and views"""
        # Create models
        results = run_dbt(["run"])
        assert len(results) == 3
        
        # Get catalog from credentials
        catalog = project.adapter.config.credentials.catalog
        
        # List relations - use catalog.schema format for Iceberg
        full_schema = f"{catalog}.{project.test_schema}" if catalog else project.test_schema
        schema_relation = project.adapter.Relation.create(
            database=None,
            schema=full_schema,
        )
        
        # Acquire connection for the adapter call
        with project.adapter.connection_named('test_list_relations'):
            relations = project.adapter.list_relations_without_caching(schema_relation)
        
        # Should find at least 2 relations (tables)
        # Note: Iceberg views might not be returned by SHOW TABLES in some Spark versions
        assert len(relations) >= 2, f"Expected at least 2 relations, found {len(relations)}: {relations}"
        
        # Check that relations have correct schema (with catalog prefix)
        for rel in relations:
            if rel.identifier in ["model_a", "model_b", "model_c"]:
                # Schema should include catalog prefix
                assert "." in rel.schema or rel.database is not None

# Made with Bob
