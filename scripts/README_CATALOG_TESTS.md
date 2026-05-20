# Catalog Test Automation Script

## Overview

The `run_catalog_tests.sh` script provides comprehensive automation for testing dbt-watsonx-spark catalog functionality. It handles the complete test lifecycle:

1. **Infrastructure Setup**: Creates Iceberg, Delta, and Hudi catalogs in watsonx.data
2. **Engine Configuration**: Creates Spark query engines (with and without authorization)
3. **Catalog Attachment**: Attaches all catalogs to both engines
4. **Test Execution**: Runs all catalog tests including authorization validation

**Supports both deployment types:**
- ☁️ **IBM Cloud SaaS** - watsonx.data on IBM Cloud
- 🏢 **Cloud Pak for Data (CPD)** - On-premises or private cloud deployment

## Prerequisites

- **watsonx.data instance** (SaaS or CPD) with API access
- **Authentication credentials**:
  - SaaS: IBM Cloud API Key
  - CPD: Username and password
- **Python environment** with dbt-watsonx-spark installed
- **jq** command-line JSON processor (`brew install jq` on macOS)
- **curl** for API calls

## Quick Start

### 1. Create Environment File

Run the script once to generate a template `.env` file:

```bash
./scripts/run_catalog_tests.sh
```

This creates a `.env` file with the following template:

```bash
# watsonx.data Configuration
WATSONX_HOSTNAME=your-wxd-hostname.cloud.ibm.com
WATSONX_APIKEY=your-api-key-here
WATSONX_INSTANCE_ID=your-instance-id

# Catalog Configuration
ICEBERG_CATALOG_NAME=iceberg_data
DELTA_CATALOG_NAME=delta_data
HUDI_CATALOG_NAME=hudi_data

# Engine Configuration
SPARK_ENGINE_NAME=spark-dbt-test
SPARK_ENGINE_AUTHZ_NAME=spark-dbt-authz-test

# Test Configuration
TEST_SCHEMA_PREFIX=dbt_test
PYTHON_VENV_PATH=~/projects/python-env/venv

# Optional: Specific test patterns
TEST_PATTERN=tests/functional/adapter/catalog_tests/
```

### 2. Configure Environment

Edit the `.env` file with your actual values:

### For IBM Cloud SaaS:

```bash
# Deployment type
DEPLOYMENT_TYPE=saas

# Get your watsonx.data hostname from the IBM Cloud console
WATSONX_HOSTNAME=wxd-instance-12345.lakehouse.cloud.ibm.com

# Use your IBM Cloud API key
WATSONX_APIKEY=your-actual-api-key

# Get instance ID from watsonx.data console
WATSONX_INSTANCE_ID=crn:v1:bluemix:public:lakehouse:us-south:a/...

# Adjust paths as needed
PYTHON_VENV_PATH=/path/to/your/venv
```

### For Cloud Pak for Data (CPD):

```bash
# Deployment type
DEPLOYMENT_TYPE=cpd

# CPD instance URL
CPD_URL=https://cpd-instance.example.com

# CPD credentials
CPD_USERNAME=your-username
CPD_PASSWORD=your-password

# Adjust paths as needed
PYTHON_VENV_PATH=/path/to/your/venv
```

### 3. Run the Script

Execute the full test automation:

```bash
./scripts/run_catalog_tests.sh
```

Or specify a custom environment file:

```bash
./scripts/run_catalog_tests.sh /path/to/custom.env
```

## What the Script Does

### Step 1: Create Catalogs

Creates three types of catalogs in watsonx.data:

- **Iceberg Catalog** (`iceberg_data`): For Apache Iceberg table format
- **Delta Catalog** (`delta_data`): For Delta Lake table format  
- **Hudi Catalog** (`hudi_data`): For Apache Hudi table format

Each catalog is created via the watsonx.data REST API with appropriate configuration.

### Step 2: Create Spark Engines

Creates two Spark query engines:

1. **Standard Engine** (`spark-dbt-test`): 
   - No authorization enforcement
   - Used for general catalog testing

2. **Authorization Engine** (`spark-dbt-authz-test`):
   - Authorization enabled and enforced
   - Used for testing access control scenarios
   - Properties set:
     - `spark.hadoop.wxd.cas.authorization.enabled=true`
     - `spark.hadoop.wxd.cas.authorization.mode=enforce`

### Step 3: Attach Catalogs to Engines

Attaches all three catalogs to both engines, enabling:
- Cross-catalog queries
- Multi-format testing
- Authorization validation across catalogs

### Step 4: Run Standard Tests

Executes all catalog tests against the standard engine:

```bash
pytest tests/functional/adapter/catalog_tests/ -v --profile watsonx
```

Tests include:
- Three-part name resolution (`catalog.schema.table`)
- Incremental models with catalogs
- View creation with catalogs
- Ephemeral model handling
- List relations functionality
- DESCRIBE EXTENDED compatibility

### Step 5: Run Authorization Tests

Executes authorization-specific tests against the authz engine:

```bash
pytest tests/functional/adapter/catalog_tests/test_issue_71459_authz.py -v --profile watsonx
```

Tests validate:
- Access control enforcement
- Permission denied scenarios
- Proper error handling for unauthorized operations

## Configuration Options

### Environment Variables

#### Common Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `DEPLOYMENT_TYPE` | No | `saas` | Deployment type: `saas` or `cpd` |
| `ICEBERG_CATALOG_NAME` | No | `iceberg_data` | Name for Iceberg catalog |
| `DELTA_CATALOG_NAME` | No | `delta_data` | Name for Delta catalog |
| `HUDI_CATALOG_NAME` | No | `hudi_data` | Name for Hudi catalog |
| `SPARK_ENGINE_NAME` | No | `spark-dbt-test` | Standard engine name |
| `SPARK_ENGINE_AUTHZ_NAME` | No | `spark-dbt-authz-test` | Authz engine name |
| `TEST_PATTERN` | No | `tests/functional/adapter/catalog_tests/` | Pytest pattern |
| `PYTHON_VENV_PATH` | No | - | Path to Python virtual environment |

#### SaaS-Specific Variables (when DEPLOYMENT_TYPE=saas)

| Variable | Required | Description |
|----------|----------|-------------|
| `WATSONX_HOSTNAME` | Yes | watsonx.data instance hostname (e.g., `wxd-instance.lakehouse.cloud.ibm.com`) |
| `WATSONX_APIKEY` | Yes | IBM Cloud API key |
| `WATSONX_INSTANCE_ID` | Yes | watsonx.data instance CRN |

#### CPD-Specific Variables (when DEPLOYMENT_TYPE=cpd)

| Variable | Required | Description |
|----------|----------|-------------|
| `CPD_URL` | Yes | Cloud Pak for Data URL (e.g., `https://cpd-instance.example.com`) |
| `CPD_USERNAME` | Yes | CPD username |
| `CPD_PASSWORD` | Yes | CPD password |

### Custom Test Patterns

Run specific test files or patterns:

```bash
# Run only three-part name tests
TEST_PATTERN=tests/functional/adapter/catalog_tests/test_three_part_names.py ./scripts/run_catalog_tests.sh

# Run only authz tests
TEST_PATTERN=tests/functional/adapter/catalog_tests/test_issue_71459_authz.py ./scripts/run_catalog_tests.sh
```

## Output and Logging

The script provides color-coded output:

- 🔵 **[INFO]**: General information messages
- 🟢 **[SUCCESS]**: Successful operations
- 🟡 **[WARNING]**: Non-critical issues (e.g., resource already exists)
- 🔴 **[ERROR]**: Critical failures

Example output:

```
[INFO] === dbt-watsonx-spark Catalog Test Automation ===
[INFO] Loading environment from: .env
[SUCCESS] Environment loaded successfully
[INFO] === Step 1: Creating Catalogs ===
[INFO] Creating iceberg catalog: iceberg_data
[SUCCESS] Created iceberg catalog: iceberg_data
[INFO] Creating delta catalog: delta_data
[SUCCESS] Created delta catalog: delta_data
...
[SUCCESS] === All test automation completed successfully! ===
```

## Troubleshooting

### Authentication Errors

**Problem**: `Failed to get authentication token`

**Solution**: 
- Verify your `WATSONX_APIKEY` is correct
- Ensure the API key has appropriate permissions
- Check that the API key is not expired

### Catalog Creation Fails

**Problem**: `Failed to create catalog`

**Solution**:
- Check that catalog name doesn't already exist
- Verify you have permissions to create catalogs
- Ensure `WATSONX_INSTANCE_ID` is correct

### Engine Creation Fails

**Problem**: `Failed to create engine`

**Solution**:
- Verify engine name is unique
- Check resource quotas in your watsonx.data instance
- Ensure you have permissions to create engines

### Tests Fail

**Problem**: Tests fail with connection errors

**Solution**:
- Verify engines are running (check watsonx.data console)
- Ensure catalogs are properly attached to engines
- Check that test schemas can be created in the catalogs

### jq Command Not Found

**Problem**: `jq: command not found`

**Solution**:
```bash
# macOS
brew install jq

# Linux (Ubuntu/Debian)
sudo apt-get install jq

# Linux (RHEL/CentOS)
sudo yum install jq
```

## Advanced Usage

### Running Specific Test Classes

```bash
# Run only TestThreePartNames class
pytest tests/functional/adapter/catalog_tests/test_three_part_names.py::TestThreePartNames -v --profile watsonx
```

### Debugging Failed Tests

Enable verbose pytest output:

```bash
pytest tests/functional/adapter/catalog_tests/ -vv --tb=long --profile watsonx
```

### Cleanup Resources

The script doesn't automatically clean up resources. To manually clean up:

1. **Delete Engines** via watsonx.data console or API
2. **Delete Catalogs** via watsonx.data console or API
3. **Drop Test Schemas** in each catalog

### CI/CD Integration

Use the script in CI/CD pipelines:

```yaml
# Example GitHub Actions workflow
- name: Run Catalog Tests
  env:
    WATSONX_HOSTNAME: ${{ secrets.WATSONX_HOSTNAME }}
    WATSONX_APIKEY: ${{ secrets.WATSONX_APIKEY }}
    WATSONX_INSTANCE_ID: ${{ secrets.WATSONX_INSTANCE_ID }}
  run: |
    ./scripts/run_catalog_tests.sh
```

## API Reference

The script uses the following watsonx.data REST API endpoints:

- `POST /lakehouse/api/v2/catalogs` - Create catalog
- `GET /lakehouse/api/v2/catalogs/{name}` - Get catalog details
- `POST /lakehouse/api/v2/spark_engines` - Create Spark engine
- `GET /lakehouse/api/v2/spark_engines/{name}` - Get engine details
- `POST /lakehouse/api/v2/spark_engines/{name}/catalogs` - Attach catalog to engine

## Support

For issues or questions:

1. Check the [TESTING_GUIDE.md](../TESTING_GUIDE.md) for general testing information
2. Review [CATALOG_ARCHITECTURE.md](../docs/handbook/CATALOG_ARCHITECTURE.md) for catalog concepts
3. Open an issue in the GitHub repository

## License

This script is part of the dbt-watsonx-spark adapter and follows the same license.