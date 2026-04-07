**[dbt](https://www.getdbt.com/)** enables data analysts and engineers to transform their data using the same practices that software engineers use to build applications.

dbt is the T in ELT. Organize, cleanse, denormalize, filter, rename, and pre-aggregate the raw data in your warehouse so that it's ready for analysis.

## dbt-watsonx-spark

The `dbt-watsonx-spark` package contains all of the code enabling dbt to work with IBM Spark on watsonx.data. Read the official documentation for using watsonx.data with dbt-watsonx-spark 
 - [Documentation for IBM Cloud and SaaS offerings](https://cloud.ibm.com/docs/watsonxdata?topic=watsonxdata-dbt_watsonx_spark_inst)
 - [Documentation for IBM watsonx.data software](https://www.ibm.com/docs/en/watsonx/watsonxdata/2.0.x?topic=integration-data-build-tool-adapter-spark)

## Getting started

- [Install dbt](https://docs.getdbt.com/docs/installation)
- Read the [introduction](https://docs.getdbt.com/docs/introduction/) and [viewpoint](https://docs.getdbt.com/docs/about/viewpoint/)

### Installation

To install the `dbt-watsonx-spark` plugin, use pip:
```
$ pip install dbt-watsonx-spark
```

### Configuration

Ensure you have started a query server from watsonx.data. Create an entry in your ~/.dbt/profiles.yml file using the following options:
- You can view connection details by clicking on the three-dot menu for query server.
- You can construct and configure the profile using the below template
- You can copy your connection information details also from going to **Configuration** tab -> **Connection Information** -> **Data Build Tool (DBT)**

```
dbt_wxd:

  target: dev
  outputs:
    dev:
      type: watsonx_spark
      method: "http"
      
      # number of threads for DBT operations, refer: https://docs.getdbt.com/docs/running-a-dbt-project/using-threads
      threads: 1

      # value of 'schema' for an existing schema in Data Manager in watsonx.data or to create a new one in watsonx.data
      schema: '<wxd_schema>'
      
      # Hostname of your watsonx.data console (ex: us-south.lakehouse.cloud.ibm.com)
      host: https://<your-host>.com

      # URI of your query server running on watsonx.data
      uri: "/lakehouse/api/v2/spark_engines/<spark_engine_id>/sql_servers/<server_id>/connect/cliservice"
      
      # Catalog linked to your Spark engine within the query server
      catalog: "<wxd_catalog>"
      
      # Optional: Disable SSL verification
      use_ssl: false

      # Optional: Control automatic schema creation (default: true)
      # Set to false if schemas are managed externally (e.g., by Ops team)
      create_schemas: true

      # Optional: Control automatic LOCATION clause in CREATE TABLE (default: false)
      # Set to true if you want dbt to automatically add LOCATION clauses
      auto_location: false

      auth:
        # In case of SaaS, set it as CRN of watsonx.data service
        # In case of Software, set it as instance id of watsonx.data
        instance: "<CRN/InstanceId>"
        
        # In case of SaaS, set it as your email id
        # In case of Software, set it as your username
        user: "<user@example.com/username>"

        # This must be your API Key
        apikey: "<apikey>"
        
```

#### Schema Creation Control

By default, dbt-watsonx-spark automatically creates schemas if they don't exist. However, in some environments where schema creation is managed by an operations team or through automation, you may want to disable this behavior.

You can control schema creation at three levels:

1. **Profile level** (applies to all models in the profile):
```yaml
dbt_wxd:
  target: dev
  outputs:
    dev:
      type: watsonx_spark
      # ... other settings ...
      create_schemas: false  # Disable automatic schema creation
```

2. **Project level** (in `dbt_project.yml`):
```yaml
models:
  my_project:
    +create_schemas: false  # Disable for all models in project
```

3. **Model level** (in model config or `dbt_project.yml`):
```yaml
# In model file
{{ config(create_schemas=false) }}

# Or in dbt_project.yml
models:
  my_project:
    my_folder:
      +create_schemas: false  # Disable for specific folder
```

When `create_schemas` is set to `false`, dbt will skip schema creation and assume the schema already exists. This is useful when:
- Schemas are created by an external automation or Ops team
- You want to enforce strict schema management policies
- You need to prevent accidental schema creation in production environments
#### Table Location Control

By default, dbt-watsonx-spark automatically adds a LOCATION clause when creating tables based on the `location_root` configuration. However, in some environments where table locations are managed externally or to avoid S3 permission issues, you may want to disable this behavior.

You can control automatic location setting at three levels:

1. **Profile level** (applies to all models in the profile):
```yaml
dbt_wxd:
  target: dev
  outputs:
    dev:
      type: watsonx_spark
      # ... other settings ...
      auto_location: false  # Disable automatic LOCATION clause
```

2. **Project level** (in `dbt_project.yml`):
```yaml
models:
  my_project:
    +auto_location: false  # Disable for all models in project
```

3. **Model level** (in model config or `dbt_project.yml`):
```yaml
# In model file
{{ config(auto_location=false) }}

# Or in dbt_project.yml
models:
  my_project:
    my_folder:
      +auto_location: false  # Disable for specific folder
```

When `auto_location` is set to `false`, dbt will not add a LOCATION clause to CREATE TABLE statements, allowing the database to use its default location or a location specified by external schema management. This is useful when:
- Table locations are managed by an external automation or Ops team
- You want to avoid S3 permission issues related to specific paths
- The schema already has a default location configured
- You need to comply with strict data governance policies


