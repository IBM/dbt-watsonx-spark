{#
  This file contains macros for handling schema changes in incremental models.
  It provides enhanced schema change detection and handling mechanisms.
#}

{% macro incremental_validate_on_schema_change(on_schema_change, default='ignore') %}
  {#-- Validate the on_schema_change parameter --#}
  {% set valid_options = ['ignore', 'fail', 'append_new_columns', 'sync_all_columns', 'migrate', 'version', 'transform'] %}
  
  {% if on_schema_change is none %}
    {% set on_schema_change = default %}
  {% endif %}
  
  {% if on_schema_change not in valid_options %}
    {% set error_msg %}
      Invalid value for 'on_schema_change': '{{ on_schema_change }}'
      Expected one of: {{ valid_options | join(', ') }}
    {% endset %}
    {% do exceptions.raise_compiler_error(error_msg) %}
  {% endif %}
  
  {{ return(on_schema_change) }}
{% endmacro %}

{% macro get_schema_changes(tmp_relation, existing_relation) %}
  {#-- Detect schema changes between the temporary and existing relations --#}
  {% set existing_columns = adapter.get_columns_in_relation(existing_relation) %}
  {% set new_columns = adapter.get_columns_in_relation(tmp_relation) %}
  
  {# Convert to dictionaries for easier comparison #}
  {% set existing_dict = {} %}
  {% for column in existing_columns %}
    {% do existing_dict.update({column.name: column}) %}
  {% endfor %}
  
  {% set new_dict = {} %}
  {% for column in new_columns %}
    {% do new_dict.update({column.name: column}) %}
  {% endfor %}
  
  {# Identify new, removed, and changed columns #}
  {% set new_column_names = [] %}
  {% set removed_column_names = [] %}
  {% set changed_column_types = [] %}
  {% set renamed_columns = [] %}
  
  {# Find new columns and changed types #}
  {% for col_name, col_data in new_dict.items() %}
    {% if col_name not in existing_dict %}
      {% do new_column_names.append(col_name) %}
    {% elif existing_dict[col_name].dtype != col_data.dtype %}
      {% do changed_column_types.append({
        'name': col_name,
        'old_type': existing_dict[col_name].dtype,
        'new_type': col_data.dtype
      }) %}
    {% endif %}
  {% endfor %}
  
  {# Find removed columns #}
  {% for col_name in existing_dict %}
    {% if col_name not in new_dict %}
      {% do removed_column_names.append(col_name) %}
    {% endif %}
  {% endfor %}
  
  {# Attempt to detect renamed columns using heuristics #}
  {% if removed_column_names and new_column_names %}
    {# Simple heuristic: if column types match, it might be a rename #}
    {% for removed_col in removed_column_names %}
      {% for new_col in new_column_names %}
        {% if existing_dict[removed_col].dtype == new_dict[new_col].dtype %}
          {% do renamed_columns.append({
            'old_name': removed_col,
            'new_name': new_col
          }) %}
          {# Remove from the lists to avoid double counting #}
          {% do removed_column_names.remove(removed_col) %}
          {% do new_column_names.remove(new_col) %}
          {% break %}
        {% endif %}
      {% endfor %}
    {% endfor %}
  {% endif %}
  
  {# Return a dictionary with all the changes #}
  {% set schema_changes = {
    'has_new_columns': new_column_names | length > 0,
    'has_removed_columns': removed_column_names | length > 0,
    'has_changed_types': changed_column_types | length > 0,
    'has_renamed_columns': renamed_columns | length > 0,
    'new_columns': new_column_names,
    'removed_columns': removed_column_names,
    'changed_types': changed_column_types,
    'renamed_columns': renamed_columns,
    'existing_columns': existing_columns,
    'new_columns_all': new_columns
  } %}
  
  {{ return(schema_changes) }}
{% endmacro %}

{% macro process_schema_changes(on_schema_change, tmp_relation, existing_relation) %}
  {#-- Process schema changes based on the detected changes and the chosen strategy --#}
  
  {# Get schema changes #}
  {% set schema_changes = get_schema_changes(tmp_relation, existing_relation) %}
  
  {# Log detected changes for debugging #}
  {% if schema_changes.has_new_columns or schema_changes.has_removed_columns or schema_changes.has_changed_types or schema_changes.has_renamed_columns %}
    {% do log("Schema changes detected:", info=True) %}
    {% if schema_changes.has_new_columns %}
      {% do log("New columns: " ~ schema_changes.new_columns | join(", "), info=True) %}
    {% endif %}
    {% if schema_changes.has_removed_columns %}
      {% do log("Removed columns: " ~ schema_changes.removed_columns | join(", "), info=True) %}
    {% endif %}
    {% if schema_changes.has_changed_types %}
      {% do log("Changed column types: " ~ schema_changes.changed_types | map(attribute='name') | join(", "), info=True) %}
    {% endif %}
    {% if schema_changes.has_renamed_columns %}
      {% do log("Potentially renamed columns: " ~ schema_changes.renamed_columns | map(attribute='old_name') | zip(schema_changes.renamed_columns | map(attribute='new_name')) | map('join', ' -> ') | join(", "), info=True) %}
    {% endif %}
  {% endif %}
  
  {# Handle schema changes based on the strategy #}
  {% if on_schema_change == 'ignore' %}
    {# Do nothing, just log the changes #}
    {% if schema_changes.has_new_columns or schema_changes.has_removed_columns or schema_changes.has_changed_types %}
      {% do log("Ignoring schema changes", info=True) %}
    {% endif %}
  
  {% elif on_schema_change == 'fail' %}
    {# Fail if there are any schema changes #}
    {% if schema_changes.has_new_columns or schema_changes.has_removed_columns or schema_changes.has_changed_types %}
      {% set error_msg %}
        Schema changes detected but not handled:
        {% if schema_changes.has_new_columns %}
          - New columns: {{ schema_changes.new_columns | join(", ") }}
        {% endif %}
        {% if schema_changes.has_removed_columns %}
          - Removed columns: {{ schema_changes.removed_columns | join(", ") }}
        {% endif %}
        {% if schema_changes.has_changed_types %}
          - Changed column types: {{ schema_changes.changed_types | map(attribute='name') | join(", ") }}
        {% endif %}
        
        To ignore these schema changes, set the 'on_schema_change' config to 'ignore'.
        To append new columns, set the 'on_schema_change' config to 'append_new_columns'.
        To sync all columns, set the 'on_schema_change' config to 'sync_all_columns'.
        To migrate data to the new schema, set the 'on_schema_change' config to 'migrate'.
        To create a new version with the updated schema, set the 'on_schema_change' config to 'version'.
        To apply transformations to adapt old data, set the 'on_schema_change' config to 'transform'.
      {% endset %}
      {% do exceptions.raise_compiler_error(error_msg) %}
    {% endif %}
  
  {% elif on_schema_change == 'append_new_columns' %}
    {# Add new columns to the existing table #}
    {% if schema_changes.has_new_columns %}
      {% set add_columns = [] %}
      {% for column_name in schema_changes.new_columns %}
        {% for col in schema_changes.new_columns_all %}
          {% if col.name == column_name %}
            {% do add_columns.append(col) %}
          {% endif %}
        {% endfor %}
      {% endfor %}
      
      {% do adapter.alter_relation_add_remove_columns(existing_relation, add_columns, remove_columns=none) %}
      {% do log("Added new columns: " ~ schema_changes.new_columns | join(", "), info=True) %}
    {% endif %}
    
    {# Fail if there are removed columns or changed types #}
    {% if schema_changes.has_removed_columns or schema_changes.has_changed_types %}
      {% set error_msg %}
        Schema changes detected that cannot be handled with 'append_new_columns':
        {% if schema_changes.has_removed_columns %}
          - Removed columns: {{ schema_changes.removed_columns | join(", ") }}
        {% endif %}
        {% if schema_changes.has_changed_types %}
          - Changed column types: {{ schema_changes.changed_types | map(attribute='name') | join(", ") }}
        {% endif %}
        
        To ignore these schema changes, set the 'on_schema_change' config to 'ignore'.
        To sync all columns, set the 'on_schema_change' config to 'sync_all_columns'.
        To migrate data to the new schema, set the 'on_schema_change' config to 'migrate'.
        To create a new version with the updated schema, set the 'on_schema_change' config to 'version'.
        To apply transformations to adapt old data, set the 'on_schema_change' config to 'transform'.
      {% endset %}
      {% do exceptions.raise_compiler_error(error_msg) %}
    {% endif %}
  
  {% elif on_schema_change == 'sync_all_columns' %}
    {# Add new columns and remove old columns to match the new schema #}
    {% if schema_changes.has_new_columns %}
      {% set add_columns = [] %}
      {% for column_name in schema_changes.new_columns %}
        {% for col in schema_changes.new_columns_all %}
          {% if col.name == column_name %}
            {% do add_columns.append(col) %}
          {% endif %}
        {% endfor %}
      {% endfor %}
      
      {% do adapter.alter_relation_add_remove_columns(existing_relation, add_columns, remove_columns=none) %}
      {% do log("Added new columns: " ~ schema_changes.new_columns | join(", "), info=True) %}
    {% endif %}
    
    {% if schema_changes.has_removed_columns %}
      {# Note: Spark doesn't support dropping columns, so this will fail #}
      {% set remove_columns = schema_changes.removed_columns %}
      {% do adapter.alter_relation_add_remove_columns(existing_relation, add_columns=none, remove_columns=remove_columns) %}
      {% do log("Removed columns: " ~ schema_changes.removed_columns | join(", "), info=True) %}
    {% endif %}
    
    {% if schema_changes.has_changed_types %}
      {% for change in schema_changes.changed_types %}
        {% do adapter.alter_column_type(existing_relation, change.name, change.new_type) %}
        {% do log("Changed column type for " ~ change.name ~ " from " ~ change.old_type ~ " to " ~ change.new_type, info=True) %}
      {% endfor %}
    {% endif %}
  
  {% elif on_schema_change == 'migrate' %}
    {# Migrate data from old schema to new schema #}
    {% if schema_changes.has_new_columns or schema_changes.has_removed_columns or schema_changes.has_changed_types or schema_changes.has_renamed_columns %}
      {# Create a backup of the existing table #}
      {% set backup_relation = existing_relation.incorporate(path={"identifier": existing_relation.identifier ~ "_backup_" ~ modules.datetime.datetime.now().strftime("%Y%m%d%H%M%S")}) %}
      {% do adapter.rename_relation(existing_relation, backup_relation) %}
      {% do log("Created backup of existing table: " ~ backup_relation, info=True) %}
      
      {# Create a new table with the new schema #}
      {% do adapter.rename_relation(tmp_relation, existing_relation) %}
      {% do log("Created new table with updated schema", info=True) %}
      
      {# Migrate data from the backup to the new table #}
      {% set migrate_sql %}
        INSERT INTO {{ existing_relation }}
        SELECT 
          {% for col in schema_changes.new_columns_all %}
            {% if col.name in schema_changes.new_columns %}
              {# New column - use default or NULL #}
              NULL as {{ adapter.quote(col.name) }}
            {% elif col.name in [item.new_name for item in schema_changes.renamed_columns] %}
              {# Renamed column - map from old name #}
              {% for rename in schema_changes.renamed_columns %}
                {% if rename.new_name == col.name %}
                  {{ adapter.quote(rename.old_name) }} as {{ adapter.quote(col.name) }}
                  {% break %}
                {% endif %}
              {% endfor %}
            {% else %}
              {# Existing column - direct mapping #}
              {{ adapter.quote(col.name) }}
            {% endif %}
            {{ ", " if not loop.last else "" }}
          {% endfor %}
        FROM {{ backup_relation }}
      {% endset %}
      
      {% do run_query(migrate_sql) %}
      {% do log("Migrated data from backup to new table", info=True) %}
    {% endif %}
  
  {% elif on_schema_change == 'version' %}
    {# Create a new version of the table with the updated schema #}
    {% if schema_changes.has_new_columns or schema_changes.has_removed_columns or schema_changes.has_changed_types %}
      {# Rename the existing table to include a version suffix #}
      {% set version_suffix = modules.datetime.datetime.now().strftime("%Y%m%d%H%M%S") %}
      {% set versioned_relation = existing_relation.incorporate(path={"identifier": existing_relation.identifier ~ "_v" ~ version_suffix}) %}
      {% do adapter.rename_relation(existing_relation, versioned_relation) %}
      {% do log("Renamed existing table to: " ~ versioned_relation, info=True) %}
      
      {# Rename the new table to the original name #}
      {% do adapter.rename_relation(tmp_relation, existing_relation) %}
      {% do log("Created new version of table with updated schema", info=True) %}
    {% endif %}
  
  {% elif on_schema_change == 'transform' %}
    {# Apply transformations to adapt old data to new schema #}
    {% if schema_changes.has_new_columns or schema_changes.has_removed_columns or schema_changes.has_changed_types or schema_changes.has_renamed_columns %}
      {# Create a backup of the existing table #}
      {% set backup_relation = existing_relation.incorporate(path={"identifier": existing_relation.identifier ~ "_backup_" ~ modules.datetime.datetime.now().strftime("%Y%m%d%H%M%S")}) %}
      {% do adapter.rename_relation(existing_relation, backup_relation) %}
      {% do log("Created backup of existing table: " ~ backup_relation, info=True) %}
      
      {# Create a new table with the new schema #}
      {% do adapter.rename_relation(tmp_relation, existing_relation) %}
      {% do log("Created new table with updated schema", info=True) %}
      
      {# Get column mapping configuration if provided #}
      {% set column_mapping = config.get('column_mapping', none) %}
      
      {# Transform and migrate data from the backup to the new table #}
      {% set transform_sql %}
        INSERT INTO {{ existing_relation }}
        SELECT 
          {% for col in schema_changes.new_columns_all %}
            {% if col.name in schema_changes.new_columns %}
              {# New column - check if there's a transformation defined #}
              {% if column_mapping and col.name in column_mapping %}
                {{ column_mapping[col.name] }} as {{ adapter.quote(col.name) }}
              {% else %}
                NULL as {{ adapter.quote(col.name) }}
              {% endif %}
            {% elif col.name in [item.new_name for item in schema_changes.renamed_columns] %}
              {# Renamed column - map from old name #}
              {% for rename in schema_changes.renamed_columns %}
                {% if rename.new_name == col.name %}
                  {{ adapter.quote(rename.old_name) }} as {{ adapter.quote(col.name) }}
                  {% break %}
                {% endif %}
              {% endfor %}
            {% elif col.name in [item.name for item in schema_changes.changed_types] %}
              {# Changed type - apply safe casting #}
              CAST({{ adapter.quote(col.name) }} AS {{ col.dtype }}) as {{ adapter.quote(col.name) }}
            {% else %}
              {# Existing column - direct mapping #}
              {{ adapter.quote(col.name) }}
            {% endif %}
            {{ ", " if not loop.last else "" }}
          {% endfor %}
        FROM {{ backup_relation }}
      {% endset %}
      
      {% do run_query(transform_sql) %}
      {% do log("Transformed and migrated data from backup to new table", info=True) %}
    {% endif %}
  
  {% else %}
    {% do exceptions.raise_compiler_error("Unhandled on_schema_change strategy: " ~ on_schema_change) %}
  {% endif %}
  
  {{ return(none) }}
{% endmacro %}

-- Made with Bob
