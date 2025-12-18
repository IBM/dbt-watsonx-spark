from unittest import mock

from dbt.adapters.watsonx_spark.connections import SparkConnectionMethod, SparkCredentials


class _FakeAuthenticator:
    def get_token(self):
        return "dummy-token"

    def get_catlog_details(self, catalog_name):
        return ("", "parquet")


def test_credentials_server_side_parameters_keys_and_values_are_strings() -> None:
    with mock.patch(
        "dbt.adapters.watsonx_spark.connections.get_authenticator",
        return_value=_FakeAuthenticator(),
    ):
        credentials = SparkCredentials(
            host="localhost",
            method=SparkConnectionMethod.THRIFT,
            database="tests",
            schema="tests",
            catalog="spark_catalog",
            token="abc123",
            server_side_parameters={"spark.configuration": 10},
        )
        assert credentials.server_side_parameters["spark.configuration"] == "10"
