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
