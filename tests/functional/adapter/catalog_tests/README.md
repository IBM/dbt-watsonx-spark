# Catalog Tests for dbt-watsonx-spark

This directory contains comprehensive tests for catalog functionality in dbt-watsonx-spark, specifically addressing issues encountered in customer deployments.

## Test Files

### 1. `test_three_part_names.py`
Tests three-part name handling (catalog.schema.table) for watsonx.data.

**Test Classes:**
- `TestThreePartNames`: Basic three-part name functionality
  - Table creation with three-part names
  - `ref()` resolution with three-part names
  
- `TestThreePartIncremental`: Incremental models with three-part names
  - Initial table creation
  - Incremental updates
  
- `TestThreePartViews`: Views with three-part names
  - View creation and verification
  
- `TestEphemeralWithCatalog`: Ephemeral models with catalog configured
  - Ensures ephemeral models work when catalog is set
  
- `TestListRelationsWithCatalog`: Relation listing with catalog
  - Tests `list_relations_without_caching()` finds all tables/views
  - Verifies relations have correct schema with catalog prefix

**Key Scenarios Tested:**
- ✅ Tables created with three-part names
- ✅ Views created with three-part names
- ✅ Incremental models with three-part names
- ✅ `ref()` macro resolves to three-part names
- ✅ Ephemeral models work with catalog configured
- ✅ `list_relations` finds all relations with correct names

### 2. `test_issue_68930_describe_extended.py`
Tests for Issue #68930: DESCRIBE EXTENDED View Failure with Three-Part Names

**Problem:** When dbt creates temporary views without three-part names and runs DESCRIBE EXTENDED, it fails with `INTERNAL_ERROR` or `NullPointerException` in authorization checks.

**Test Classes:**
- `TestIssue68930DescribeExtended`: SCD2 snapshots with catalog
  - Tests snapshot creation (uses temporary views internally)
  - Tests incremental snapshot updates
  
- `TestTemporaryViewsWithCatalog`: Temporary view handling
  - Tests models that explicitly create temporary views
  
- `TestDescribeExtendedWithThreePartNames`: DESCRIBE EXTENDED functionality
  - Tests `get_columns_in_relation()` with three-part names
  - Verifies column metadata retrieval works

**Key Scenarios Tested:**
- ✅ SCD2 snapshots work with catalog configured
- ✅ Temporary views can be created and used
- ✅ DESCRIBE EXTENDED works on tables with three-part names
- ✅ Column metadata retrieval works correctly

### 3. `test_issue_71459_authz.py`
Tests for Issue #71459: AuthZ Connection Failure

**Problem:** When AuthZ is enabled (`spark.sql.extensions=authz.IBMSparkACExtension`), dbt fails with: "No catalog schema mapping received from MDS for default"

**Root Cause:** dbt doesn't pass `database` parameter to `hive.connect()`, causing Spark to default to non-existent `spark_catalog`.

**Test Classes:**
- `TestIssue71459AuthZ`: Connection with AuthZ enabled
  - Tests basic connection with catalog configured
  - Tests `dbt debug` command
  
- `TestCatalogSchemaMapping`: Catalog.schema mapping
  - Verifies schema includes catalog prefix
  - Tests target variables include correct catalog info
  
- `TestDefaultCatalogBehavior`: Default catalog handling
  - Tests behavior when no explicit catalog in model config
  - Verifies default catalog from connection is used

**Key Scenarios Tested:**
- ✅ Connection succeeds with catalog configured
- ✅ `dbt debug` works with catalog
- ✅ Schema includes catalog prefix
- ✅ Default catalog from connection is used correctly

## Running the Tests

### Run All Catalog Tests
```bash
pytest tests/functional/adapter/catalog_tests/ -v
```

### Run Specific Test File
```bash
pytest tests/functional/adapter/catalog_tests/test_three_part_names.py -v
```

### Run Specific Test Class
```bash
pytest tests/functional/adapter/catalog_tests/test_three_part_names.py::TestThreePartNames -v
```

### Run Specific Test Method
```bash
pytest tests/functional/adapter/catalog_tests/test_three_part_names.py::TestThreePartNames::test_three_part_table_creation -v
```

## Test Configuration

These tests require a profile configured with:
- `catalog`: Set to your Iceberg catalog name (e.g., `iceberg_catalog`)
- `schema`: Your test schema name
- `host`: watsonx.data query server URL
- `auth_type`: Authentication method (e.g., `wxd_api_key`)

Example `profiles.yml`:
```yaml
watsonx:
  target: dev
  outputs:
    dev:
      type: watsonx_spark
      method: http
      catalog: iceberg_catalog
      schema: test_schema
      host: cpd-instance.com
      port: 443
      auth_type: wxd_api_key
      wxd_api_key: "{{ env_var('WXD_API_KEY') }}"
      wxd_instance_id: "{{ env_var('WXD_INSTANCE_ID') }}"
```

## Issues Addressed

### Issue #68930: DESCRIBE EXTENDED View Failure
- **Status**: Fixed in v0.1.5
- **Solution**: Use three-part names for all operations when catalog is configured
- **Tests**: `test_issue_68930_describe_extended.py`

### Issue #71459: AuthZ Connection Failure
- **Status**: Fixed in v0.1.5
- **Solution**: Pass database parameter as `catalog.schema` to `hive.connect()`
- **Tests**: `test_issue_71459_authz.py`

### Adapter Class Name Bug
- **Status**: Fixed in v0.1.5
- **Problem**: `No module named 'dbt.adapters.spark'` after successful model execution
- **Solution**: Renamed `SparkAdapter` to `WatsonxSparkAdapter` and added `get_adapter_run_info()` override
- **Tests**: Covered by all tests (ensures no import errors)

## Test Coverage

These tests cover:
- ✅ Three-part name handling (catalog.schema.table)
- ✅ Two-part name fallback (schema.table)
- ✅ Catalog prefix detection and restoration
- ✅ SHOW TABLE EXTENDED with 3-part and 2-part names
- ✅ SHOW TABLES fallback with 3-part and 2-part names
- ✅ DESCRIBE EXTENDED on tables and views
- ✅ Temporary view creation and usage
- ✅ SCD2 snapshots with catalog
- ✅ Incremental models with catalog
- ✅ View materialization with catalog
- ✅ Ephemeral models with catalog
- ✅ Connection with AuthZ enabled
- ✅ Default catalog behavior

## Expected Behavior

### With Catalog Configured
1. All DDL uses three-part names: `CREATE TABLE catalog.schema.table`
2. All DML uses three-part names: `SELECT * FROM catalog.schema.table`
3. `ref()` macro resolves to three-part names
4. Temporary views use three-part names
5. `list_relations` returns relations with catalog prefix in schema

### Fallback Strategy (v0.1.5)
1. Try 3-part name first (preferred for v2 tables)
2. If fails, try 2-part name (compatibility for older Spark)
3. Relations are restored to full 3-part names after query

### Without Catalog Configured
1. Uses default Spark behavior (typically `spark_catalog.default`)
2. Two-part names used: `schema.table`
3. Works with traditional Hive tables

## Debugging Failed Tests

If tests fail, check:

1. **Catalog Configuration**
   - Verify catalog exists in watsonx.data
   - Check catalog type (Iceberg, Hudi, Delta)
   - Ensure user has permissions on catalog

2. **Schema Permissions**
   - User must have CREATE, SELECT, DROP permissions
   - Check AuthZ policies if enabled

3. **Connection Settings**
   - Verify query server URL is correct
   - Check authentication credentials
   - Ensure network connectivity

4. **Spark Version**
   - Some features require Spark 3.x
   - Check if Spark distribution supports catalogs

5. **Log Analysis**
   - Look for "No catalog schema mapping" errors (Issue #71459)
   - Look for "INTERNAL_ERROR" or "NullPointerException" (Issue #68930)
   - Check for "No module named 'dbt.adapters.spark'" (Adapter class name bug)

## Contributing

When adding new catalog-related tests:
1. Add test to appropriate file or create new file
2. Document the scenario being tested
3. Include both positive and negative test cases
4. Update this README with new test information