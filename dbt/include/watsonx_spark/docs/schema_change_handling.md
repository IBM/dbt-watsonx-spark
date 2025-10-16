# Schema Change Handling in Incremental Models

This document describes the enhanced schema change handling capabilities in the dbt-watsonx-spark adapter for incremental models.

## Overview

When running incremental models, schema changes can occur between runs. These changes can include:

- New columns added to the model
- Columns removed from the model
- Column data types changed
- Columns renamed

The `on_schema_change` configuration option determines how dbt handles these schema changes.

## Configuration

To configure schema change handling, use the `on_schema_change` option in your model configuration:

```yaml
models:
  - name: my_incremental_model
    materialized: incremental
    config:
      on_schema_change: 'strategy_name'
```

## Available Strategies

### Standard Strategies

These strategies are available in all dbt adapters:

- **ignore** (default): Ignore schema changes and continue with the incremental update. This may cause errors if the schema changes are incompatible with the incremental update.
- **fail**: Fail the run if any schema changes are detected. This is useful when you want to be notified of schema changes and handle them manually.
- **append_new_columns**: Add new columns to the existing table, but fail if columns have been removed or changed. This is useful when you want to automatically add new columns but be notified of other schema changes.
- **sync_all_columns**: Add new columns and remove old columns to match the new schema. Note that in Spark, removing columns is not supported, so this will fail if columns have been removed.

### Enhanced Strategies

The dbt-watsonx-spark adapter adds these additional strategies:

- **migrate**: Attempt to migrate data from the old schema to the new schema. This creates a backup of the existing table, creates a new table with the updated schema, and migrates data from the backup to the new table.
- **version**: Create a new version of the table with the updated schema. This renames the existing table to include a version suffix and creates a new table with the original name and the updated schema.
- **transform**: Apply transformations to adapt old data to the new schema. This is similar to migrate, but allows you to specify transformations for new columns.

## Column Mapping

When using the `transform` strategy, you can specify transformations for new columns using the `column_mapping` configuration:

```yaml
models:
  - name: my_incremental_model
    materialized: incremental
    config:
      on_schema_change: 'transform'
      column_mapping:
        new_column_1: 'expression_to_calculate_value'
        new_column_2: 'another_expression'
```

## Examples

### Example 1: Ignoring Schema Changes

```yaml
models:
  - name: my_incremental_model
    materialized: incremental
    config:
      on_schema_change: 'ignore'
```

### Example 2: Adding New Columns Automatically

```yaml
models:
  - name: my_incremental_model
    materialized: incremental
    config:
      on_schema_change: 'append_new_columns'
```

### Example 3: Migrating Data to a New Schema

```yaml
models:
  - name: my_incremental_model
    materialized: incremental
    config:
      on_schema_change: 'migrate'
```

### Example 4: Creating a New Version with Updated Schema

```yaml
models:
  - name: my_incremental_model
    materialized: incremental
    config:
      on_schema_change: 'version'
```

### Example 5: Transforming Data for New Columns

```yaml
models:
  - name: my_incremental_model
    materialized: incremental
    config:
      on_schema_change: 'transform'
      column_mapping:
        new_column: 'COALESCE(old_column * 2, 0)'
```

## Limitations

- **Column Removal**: Spark does not support dropping columns from tables. The `sync_all_columns` strategy will fail if columns have been removed.
- **Column Type Changes**: Some column type changes may not be compatible and could result in data loss or conversion errors.
- **Renamed Columns**: Column renaming detection uses heuristics and may not always correctly identify renamed columns.

## Best Practices

1. **Test Schema Changes**: Always test schema changes in a development environment before applying them to production.
2. **Use Version Control**: Keep your models under version control to track schema changes over time.
3. **Consider Full Refreshes**: For major schema changes, consider doing a full refresh instead of an incremental update.
4. **Monitor Schema Changes**: Regularly review your models for schema changes and update your configurations accordingly.
5. **Document Schema Changes**: Document schema changes in your model documentation to help other team members understand the changes.