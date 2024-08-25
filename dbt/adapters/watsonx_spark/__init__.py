from dbt.adapters.watsonx_spark.connections import SparkConnectionManager  # noqa
from dbt.adapters.watsonx_spark.connections import SparkCredentials
from dbt.adapters.watsonx_spark.relation import SparkRelation  # noqa
from dbt.adapters.watsonx_spark.column import SparkColumn  # noqa
from dbt.adapters.watsonx_spark.impl import SparkAdapter

from dbt.adapters.base import AdapterPlugin
from dbt.include import watsonx_spark

Plugin = AdapterPlugin(
    adapter=SparkAdapter, credentials=SparkCredentials, include_path=watsonx_spark.PACKAGE_PATH
)
