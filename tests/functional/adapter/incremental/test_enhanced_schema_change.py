import pytest

from dbt.tests.util import run_dbt

from dbt.tests.adapter.incremental.test_incremental_on_schema_change import (
    BaseIncrementalOnSchemaChangeSetup,
)


class TestEnhancedSchemaChangeStrategies(BaseIncrementalOnSchemaChangeSetup):
    """Test class for enhanced schema change strategies."""

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+file_format": "delta",
                "+incremental_strategy": "merge",
                "+unique_key": "id",
            }
        }

    def test_run_incremental_migrate(self, project):
        """Test the migrate strategy for schema changes."""
        # Setup models with migrate strategy
        select = "model_a incremental_migrate incremental_migrate_target"
        
        # Create the models with initial schema
        run_dbt(["run", "--models", select, "--full-refresh"])
        
        # Run again with schema changes
        results = run_dbt(["run", "--models", select])
        
        # Verify that the migration was successful
        # The run_dbt function will raise an exception if the run fails
        
        # Verify that the data was migrated correctly
        compare_source = "incremental_migrate"
        compare_target = "incremental_migrate_target"
        self.run_twice_and_assert(select, compare_source, compare_target, project)

    def test_run_incremental_version(self, project):
        """Test the version strategy for schema changes."""
        # Setup models with version strategy
        select = "model_a incremental_version incremental_version_target"
        
        # Create the models with initial schema
        run_dbt(["run", "--models", select, "--full-refresh"])
        
        # Run again with schema changes
        results = run_dbt(["run", "--models", select])
        
        # Verify that the versioning was successful
        # The run_dbt function will raise an exception if the run fails
        
        # Verify that the new version has the expected schema
        compare_source = "incremental_version"
        compare_target = "incremental_version_target"
        self.run_twice_and_assert(select, compare_source, compare_target, project)
        
        # Verify that the old version exists
        # This would require checking the database for the versioned table
        # We'll need to implement this based on the adapter's capabilities

    def test_run_incremental_transform(self, project):
        """Test the transform strategy for schema changes."""
        # Setup models with transform strategy
        select = "model_a incremental_transform incremental_transform_target"
        
        # Create the models with initial schema
        run_dbt(["run", "--models", select, "--full-refresh"])
        
        # Run again with schema changes
        results = run_dbt(["run", "--models", select])
        
        # Verify that the transformation was successful
        # The run_dbt function will raise an exception if the run fails
        
        # Verify that the data was transformed correctly
        compare_source = "incremental_transform"
        compare_target = "incremental_transform_target"
        self.run_twice_and_assert(select, compare_source, compare_target, project)

    def test_run_incremental_column_rename(self, project):
        """Test column rename detection and handling."""
        # Setup models with migrate strategy (which handles renames)
        select = "model_a incremental_rename incremental_rename_target"
        
        # Create the models with initial schema
        run_dbt(["run", "--models", select, "--full-refresh"])
        
        # Run again with renamed columns
        results = run_dbt(["run", "--models", select])
        
        # Verify that the rename was detected and handled
        # The run_dbt function will raise an exception if the run fails
        
        # Verify that the data was migrated correctly with renamed columns
        compare_source = "incremental_rename"
        compare_target = "incremental_rename_target"
        self.run_twice_and_assert(select, compare_source, compare_target, project)

    def test_run_incremental_type_change(self, project):
        """Test column type change handling."""
        # Setup models with migrate strategy (which handles type changes)
        select = "model_a incremental_type_change incremental_type_change_target"
        
        # Create the models with initial schema
        run_dbt(["run", "--models", select, "--full-refresh"])
        
        # Run again with type changes
        results = run_dbt(["run", "--models", select])
        
        # Verify that the type change was handled
        # The run_dbt function will raise an exception if the run fails
        
        # Verify that the data was migrated correctly with type changes
        compare_source = "incremental_type_change"
        compare_target = "incremental_type_change_target"
        self.run_twice_and_assert(select, compare_source, compare_target, project)


# Add test fixtures for the models used in the tests
models__incremental_migrate_sql = """
{{
  config(
    materialized = "incremental",
    on_schema_change = "migrate"
  )
}}

{% if is_incremental() %}
  -- Add a new column in the incremental run
  select id, value, current_timestamp() as updated_at
  from {{ ref('model_a') }}
{% else %}
  -- Initial schema without the updated_at column
  select id, value
  from {{ ref('model_a') }}
{% endif %}
"""

models__incremental_migrate_target_sql = """
-- Target model with the expected schema after migration
select id, value, current_timestamp() as updated_at
from {{ ref('model_a') }}
"""

models__incremental_version_sql = """
{{
  config(
    materialized = "incremental",
    on_schema_change = "version"
  )
}}

{% if is_incremental() %}
  -- Add a new column in the incremental run
  select id, value, current_timestamp() as updated_at
  from {{ ref('model_a') }}
{% else %}
  -- Initial schema without the updated_at column
  select id, value
  from {{ ref('model_a') }}
{% endif %}
"""

models__incremental_version_target_sql = """
-- Target model with the expected schema after versioning
select id, value, current_timestamp() as updated_at
from {{ ref('model_a') }}
"""

models__incremental_transform_sql = """
{{
  config(
    materialized = "incremental",
    on_schema_change = "transform",
    column_mapping = {
      "calculated_value": "value * 2"
    }
  )
}}

{% if is_incremental() %}
  -- Add a new calculated column in the incremental run
  select id, value, value * 2 as calculated_value
  from {{ ref('model_a') }}
{% else %}
  -- Initial schema without the calculated_value column
  select id, value
  from {{ ref('model_a') }}
{% endif %}
"""

models__incremental_transform_target_sql = """
-- Target model with the expected schema after transformation
select id, value, value * 2 as calculated_value
from {{ ref('model_a') }}
"""

models__incremental_rename_sql = """
{{
  config(
    materialized = "incremental",
    on_schema_change = "migrate"
  )
}}

{% if is_incremental() %}
  -- Rename the 'value' column to 'amount'
  select id, value as amount
  from {{ ref('model_a') }}
{% else %}
  -- Initial schema with 'value' column
  select id, value
  from {{ ref('model_a') }}
{% endif %}
"""

models__incremental_rename_target_sql = """
-- Target model with the expected schema after column rename
select id, value as amount
from {{ ref('model_a') }}
"""

models__incremental_type_change_sql = """
{{
  config(
    materialized = "incremental",
    on_schema_change = "migrate"
  )
}}

{% if is_incremental() %}
  -- Change the type of 'value' from integer to string
  select id, cast(value as string) as value
  from {{ ref('model_a') }}
{% else %}
  -- Initial schema with 'value' as integer
  select id, value
  from {{ ref('model_a') }}
{% endif %}
"""

models__incremental_type_change_target_sql = """
-- Target model with the expected schema after type change
select id, cast(value as string) as value
from {{ ref('model_a') }}
"""

# Made with Bob
