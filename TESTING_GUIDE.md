# Testing Guide for dbt-watsonx-spark

This guide provides step-by-step instructions to test new releases of dbt-watsonx-spark.

## Quick Start - Testing a New Release

### Step 0: Install Test Dependencies (First Time Only)

```bash
# Navigate to the project directory
cd /Users/rons/projects/wxd/spark/DBT/dbt-watsonx-spark

# Install development dependencies
pip3 install -r dev-requirements.txt

# This installs:
# - dbt-tests-adapter (required for functional tests)
# - pytest and related testing tools
# - All dbt dependencies
```

### Step 1: Build the Package

```bash
# Navigate to the project directory
cd /Users/rons/projects/wxd/spark/DBT/dbt-watsonx-spark

# Install build tools (if not already installed)
pip3 install build twine

# Build the package
python3 -m build

# This creates:
# - dist/dbt_watsonx_spark-X.Y.Z-py3-none-any.whl
# - dist/dbt_watsonx_spark-X.Y.Z.tar.gz
```

### Step 2: Install the New Package

**Option A: Install from Local Wheel (Recommended for Testing)**
```bash
# Uninstall old version first
pip3 uninstall dbt-watsonx-spark -y

# Install the new wheel
pip3 install dist/dbt_watsonx_spark-*.whl

# Verify installation
pip3 show dbt-watsonx-spark
dbt --version
```

**Option B: Install in Development Mode (for active development)**
```bash
# Navigate to the project directory
cd /Users/rons/projects/wxd/spark/DBT/dbt-watsonx-spark

# Install in editable mode
pip3 install -e .

# Verify installation
pip3 show dbt-watsonx-spark
```

**Option C: Install from Git Branch (for remote testing)**
```bash
# Install from specific branch
pip3 install git+https://github.com/IBM/dbt-watsonx-spark.git@<branch-name>

# Or install from specific commit
pip3 install git+https://github.com/IBM/dbt-watsonx-spark.git@<commit-hash>
```

### Step 3: Run Automated Tests

```bash
# Navigate to the project directory
cd /Users/rons/projects/wxd/spark/DBT/dbt-watsonx-spark

# Run all tests
pytest tests/ -v

# Run specific test categories
pytest tests/unit/ -v                                    # Unit tests
pytest tests/functional/adapter/catalog_tests/ -v        # Catalog tests
pytest tests/functional/adapter/test_basic.py -v         # Basic functionality

# Run with debug output
pytest tests/ -v -s

# Run specific test
pytest tests/functional/adapter/catalog_tests/test_issue_68930_describe_extended.py -v
```

### Step 4: Test with Real dbt Project

```bash
# Create a test dbt project
mkdir -p ~/test_dbt_project
cd ~/test_dbt_project

# Initialize dbt project
dbt init my_test_project

# Configure profiles.yml (see Configuration section below)
# Edit ~/.dbt/profiles.yml with your watsonx.data credentials

# Test connection
dbt debug

# Run models
dbt run --debug

# Check logs for clean output (no verbose [EXCEPTION_HANDLER] logs)
cat logs/dbt.log | grep -i "exception\|error"
```

## Configuration for Testing

### Sample profiles.yml

Create or update `~/.dbt/profiles.yml`:

```yaml
my_test_project:
  target: dev
  outputs:
    dev:
      type: watsonx_spark
      method: "http"
      threads: 1
      
      # Your watsonx.data connection details
      host: https://your-host.com
      uri: "/lakehouse/api/v3/spark_engines/<engine_id>/query_servers/<server_id>/connect/cliservice"
      catalog: "iceberg_data"
      schema: "test_schema"
      
      # Optional: Control schema creation (default: true)
      create_schemas: true
      
      # Optional: Control LOCATION clause (default: false)
      auto_location: false
      
      # Authentication
      auth:
        instance: "<CRN or InstanceId>"
        user: "<your-email or username>"
        apikey: "<your-apikey>"
```

## Testing Checklist

### 1. Version Verification
```bash
# Check installed version
pip3 show dbt-watsonx-spark | grep Version

# Check dbt recognizes the adapter
dbt --version

# Should show: watsonx_spark=X.Y.Z
```

### 2. Connection Test
```bash
cd ~/test_dbt_project
dbt debug

# Expected output:
# ✓ Connection test: OK
# ✓ All checks passed!
```

### 3. Catalog Operations Test
```bash
# Run a simple model
dbt run --select my_model --debug

# Check logs for:
# ✓ "Spark adapter: Catalog configured: <catalog_name>"
# ✓ "Spark adapter: Will use 3-part names"
# ✓ "Listed X relations using SHOW TABLE EXTENDED (3-part)"
# ✗ NO verbose [EXCEPTION_HANDLER] logs
```

### 4. Error Handling Test
```bash
# Test with invalid credentials (should show clear error message)
# Edit profiles.yml with wrong apikey
dbt debug

# Expected: Clear error message about connection failure
# NOT: Misleading "authentication error" for EOFError
```

### 5. Fallback Behavior Test
```bash
# Run against schema with v2 Iceberg tables
dbt run --debug

# Check logs for:
# ✓ Fallback from SHOW TABLE EXTENDED to SHOW TABLES (if needed)
# ✓ Clean debug logs (no verbose exception handling)
# ✓ Successful completion
```

## Automated Test Suite

### Run Full Test Suite
```bash
cd /Users/rons/projects/wxd/spark/DBT/dbt-watsonx-spark

# Run all tests with coverage
pytest tests/ -v --cov=dbt.adapters.watsonx_spark

# Run catalog-specific tests
pytest tests/functional/adapter/catalog_tests/ -v

# Run tests for specific issues
pytest tests/functional/adapter/catalog_tests/test_issue_68930_describe_extended.py -v
pytest tests/functional/adapter/catalog_tests/test_issue_71459_authz.py -v
pytest tests/functional/adapter/catalog_tests/test_three_part_names.py -v
```

### Test Categories

**Unit Tests** (`tests/unit/`)
- Adapter logic
- Connection handling
- Credential validation
- Catalog utilities

**Functional Tests** (`tests/functional/adapter/`)
- Basic operations (create, read, update, delete)
- Incremental models
- Seeds
- Snapshots
- Python models
- Grants and permissions

**Catalog Tests** (`tests/functional/adapter/catalog_tests/`)
- Three-part name handling
- SHOW TABLE EXTENDED fallback (Issue #68930)
- AuthZ catalog context (Issue #71459)

## Verifying Fixes

### 1. Clean Logging (No Verbose Debug)
```bash
# Run dbt and check logs
dbt run --debug > test_output.log 2>&1

# Should NOT see these:
grep "\[EXCEPTION_HANDLER\]" test_output.log
# Expected: No matches

# Should see clean logs:
grep "Listed.*relations using SHOW TABLE EXTENDED" test_output.log
# Expected: Clean info messages
```

### 2. Correct EOFError Message
```bash
# Simulate connection failure (stop query server or use wrong URL)
dbt debug

# Expected error message should include:
# "connection closed unexpectedly (EOFError)"
# "Possible causes:"
# "- Query server may not be running"
# "- Network connectivity issues"

# Should NOT say: "authentication error" (unless it's actually auth)
```

### 3. Three-Part Names Working
```bash
# Check logs for catalog usage
dbt run --debug 2>&1 | grep -i catalog

# Expected:
# "Spark adapter: Catalog configured: <catalog_name>"
# "Spark adapter: Will use 3-part names"
# SQL queries using: catalog.schema.table format
```

## Troubleshooting Test Failures

### Package Not Found
```bash
# Reinstall package
pip3 uninstall dbt-watsonx-spark -y
pip3 install dist/dbt_watsonx_spark-*.whl --force-reinstall
```

### Import Errors
```bash
# Clear Python cache
find . -type d -name __pycache__ -exec rm -rf {} +
find . -type f -name "*.pyc" -delete

# Reinstall
pip3 install -e . --force-reinstall --no-deps
```

### Test Database Connection Issues
```bash
# Verify credentials
dbt debug --profiles-dir ~/.dbt

# Check network connectivity
curl -v https://your-host.com

# Verify query server is running in watsonx.data console
```

## Release Testing Workflow

1. **Build Package**: `python3 -m build`
2. **Install Locally**: `pip3 install dist/*.whl`
3. **Run Unit Tests**: `pytest tests/unit/ -v`
4. **Run Functional Tests**: `pytest tests/functional/ -v`
5. **Test with Real Project**: `dbt run --debug`
6. **Verify Logs**: Check for clean output, no verbose debug
7. **Test Error Scenarios**: Invalid credentials, network issues
8. **Document Results**: Note any issues or improvements

## Additional Resources

- **Test README**: `tests/functional/adapter/catalog_tests/README.md`
- **Release Process**: `/Users/rons/projects/wxd/spark/docs/handbook/docs/integrations/dbt/release-process.md`
- **Troubleshooting**: `/Users/rons/projects/wxd/spark/docs/handbook/docs/integrations/dbt/troubleshooting.md`

---

## Test Case 1: Testing `create_schemas` Flag

### Setup Test Project

**Option 1: Use the provided example project (RECOMMENDED)**
```bash
# The example project is already included in the repository
cd /Users/rons/projects/wxd/spark/DBT/dbt-watsonx-spark/example_dbt_project

# Copy and edit the profiles.yml
cp profiles.yml ~/.dbt/profiles.yml
# Edit ~/.dbt/profiles.yml with your credentials

# Test the connection
dbt debug
```

**Option 2: Create your own test project**
```bash
mkdir -p ~/dbt_test_project
cd ~/dbt_test_project
dbt init test_schemas
cd test_schemas
```

### Test 1.1: Default Behavior (create_schemas=true)

**profiles.yml:**
```yaml
test_schemas:
  target: dev
  outputs:
    dev:
      type: watsonx_spark
      method: http
      host: "https://your-host.com"
      uri: "/lakehouse/api/v2/spark_engines/<engine-id>/sql_servers/<server-id>/connect/cliservice"
      schema: "test_schema_default"
      catalog: "test_catalog"
      create_schemas: false  # Default behavior
      auth:
        instance: "<instance-id>"
        user: "<username>"
        apikey: "<apikey>"
```

**Expected SQL Generated:**
```sql
-- When running: dbt run
CREATE SCHEMA IF NOT EXISTS test_catalog.test_schema_default LOCATION 's3a://bucket/catalog/schema'
```

**Test Command:**
```bash
# Using the example project
cd /Users/rons/projects/wxd/spark/DBT/dbt-watsonx-spark/example_dbt_project

# Test connection
dbt debug

# Run a specific model
dbt run --models stg_customers

# Or run all staging models
dbt run --models staging.*
```

**Expected Result:** Schema is created automatically if it doesn't exist.

---

### Test 1.2: Disabled Schema Creation (create_schemas=false)

**profiles.yml:**
```yaml
test_schemas:
  target: dev
  outputs:
    dev:
      type: watsonx_spark
      method: http
      host: "https://your-host.com"
      uri: "/lakehouse/api/v2/spark_engines/<engine-id>/sql_servers/<server-id>/connect/cliservice"
      schema: "test_schema_disabled"
      catalog: "test_catalog"
      create_schemas: false  # Disable automatic schema creation
      auth:
        instance: "<instance-id>"
        user: "<username>"
        apikey: "<apikey>"
```

**Expected SQL Generated:**
```sql
-- When running: dbt run
SELECT 1 AS noop  -- No schema creation, just a no-op query
```

**Test Command:**
```bash
# First, manually create the schema in watsonx.data
# CREATE SCHEMA IF NOT EXISTS test_catalog.test_schema LOCATION 's3a://bucket/path';

# Then run dbt with the example project
cd /Users/rons/projects/wxd/spark/DBT/dbt-watsonx-spark/example_dbt_project
dbt run --models stg_customers
```

**Expected Result:** No schema creation attempt; assumes schema already exists.

---

### Test 1.3: Model-Level Override

**dbt_project.yml:**
```yaml
name: 'test_schemas'
version: '1.0.0'
config-version: 2

models:
  test_schemas:
    staging:
      +create_schemas: false  # Disable for staging models only
    marts:
      +create_schemas: true   # Enable for marts models
```

**Test Command:**
```bash
dbt run --models staging.*  # Should not create schema
dbt run --models marts.*    # Should create schema
```

---

## Test Case 2: Testing `auto_location` Flag

### Test 2.1: Default Behavior (auto_location=true)

**profiles.yml:**
```yaml
test_location:
  target: dev
  outputs:
    dev:
      type: watsonx_spark
      method: http
      host: "https://your-host.com"
      uri: "/lakehouse/api/v2/spark_engines/<engine-id>/sql_servers/<server-id>/connect/cliservice"
      schema: "test_schema"
      catalog: "test_catalog"
      auto_location: true  # Default behavior
      location_root: "'s3a://my-bucket/iceberg-dev/my_schema'"
      auth:
        instance: "<instance-id>"
        user: "<username>"
        apikey: "<apikey>"
```

**Model File (models/my_table.sql):**
```sql
{{ config(
    materialized='table',
    file_format='iceberg'
) }}

SELECT 1 as id, 'test' as name
```

**Expected SQL Generated:**
```sql
CREATE OR REPLACE TABLE test_catalog.test_schema.my_table
USING iceberg
LOCATION 's3a://my-bucket/iceberg-dev/my_schema/my_table'
AS
SELECT 1 as id, 'test' as name
```

**Test Command:**
```bash
dbt run --models my_table
```

**Expected Result:** Table created with LOCATION clause pointing to specified path.

---

### Test 2.2: Disabled Location (auto_location=false)

**profiles.yml:**
```yaml
test_location:
  target: dev
  outputs:
    dev:
      type: watsonx_spark
      method: http
      host: "https://your-host.com"
      uri: "/lakehouse/api/v2/spark_engines/<engine-id>/sql_servers/<server-id>/connect/cliservice"
      schema: "test_schema"
      catalog: "test_catalog"
      auto_location: false  # Disable automatic location
      auth:
        instance: "<instance-id>"
        user: "<username>"
        apikey: "<apikey>"
```

**Model File (models/my_table.sql):**
```sql
{{ config(
    materialized='table',
    file_format='iceberg'
) }}

SELECT 1 as id, 'test' as name
```

**Expected SQL Generated:**
```sql
CREATE OR REPLACE TABLE test_catalog.test_schema.my_table
USING iceberg
-- NO LOCATION CLAUSE
AS
SELECT 1 as id, 'test' as name
```

**Test Command:**
```bash
dbt run --models my_table
```

**Expected Result:** Table created without LOCATION clause; uses schema's default location.

---

### Test 2.3: Model-Level Override

**Model File (models/custom_location.sql):**
```sql
{{ config(
    materialized='table',
    file_format='iceberg',
    auto_location=false  -- Override at model level
) }}

SELECT 1 as id, 'test' as name
```

**Expected SQL Generated:**
```sql
CREATE OR REPLACE TABLE test_catalog.test_schema.custom_location
USING iceberg
-- NO LOCATION CLAUSE even if profile has auto_location=true
AS
SELECT 1 as id, 'test' as name
```

---

## Test Case 3: Combined Testing (Both Flags)

**profiles.yml:**
```yaml
test_combined:
  target: dev
  outputs:
    dev:
      type: watsonx_spark
      method: http
      host: "https://your-host.com"
      uri: "/lakehouse/api/v2/spark_engines/<engine-id>/sql_servers/<server-id>/connect/cliservice"
      schema: "test_schema"
      catalog: "test_catalog"
      create_schemas: false  # Ops team manages schemas
      auto_location: false   # Ops team manages locations
      auth:
        instance: "<instance-id>"
        user: "<username>"
        apikey: "<apikey>"
```

**Expected Behavior:**
1. No schema creation (assumes schema exists)
2. No LOCATION clause in CREATE TABLE statements
3. Tables use schema's default location

**Test Command:**
```bash
# Manually create schema first
dbt run --models my_model
```

---

## Verification Steps

### 1. Check Generated SQL
Enable debug mode to see generated SQL:
```bash
dbt --debug run --models my_model 2>&1 | grep -A 20 "CREATE"
```

### 2. Check Logs
Look for schema creation attempts:
```bash
dbt run --models my_model 2>&1 | grep -i "schema"
```

### 3. Verify in watsonx.data
```sql
-- Check if schema was created
SHOW SCHEMAS IN test_catalog;

-- Check table location
DESCRIBE EXTENDED test_catalog.test_schema.my_table;
```

### 4. Check S3 Path
Verify the actual S3 path where data is written:
```bash
aws s3 ls s3://your-bucket/path/ --recursive
```

---

## Troubleshooting

### Issue: Schema not found error
**Solution:** When `create_schemas=false`, ensure schema exists before running dbt:
```sql
CREATE SCHEMA IF NOT EXISTS test_catalog.test_schema 
LOCATION 's3a://bucket/catalog/schema';
```

### Issue: Permission denied on S3 path
**Solution:** Set `auto_location=false` to avoid writing to restricted paths.

### Issue: Changes not taking effect
**Solution:** Reinstall the package:
```bash
pip install -e . --force-reinstall --no-deps
```

---

## Expected Test Results Summary

| Test Case | create_schemas | auto_location | Schema Created? | Location Clause? |
|-----------|---------------|---------------|-----------------|------------------|
| Default   | true          | true          | ✅ Yes          | ✅ Yes           |
| No Schema | false         | true          | ❌ No           | ✅ Yes           |
| No Location | true        | false         | ✅ Yes          | ❌ No            |
| Both Off  | false         | false         | ❌ No           | ❌ No            |

---

## Cleanup

After testing:
```bash
# Drop test schemas
DROP SCHEMA IF EXISTS test_catalog.test_schema_default CASCADE;
DROP SCHEMA IF EXISTS test_catalog.test_schema_disabled CASCADE;

# Uninstall test package
pip uninstall dbt-watsonx-spark