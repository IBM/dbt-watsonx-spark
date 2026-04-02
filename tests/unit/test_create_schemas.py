import unittest
from unittest import mock
from dbt.adapters.watsonx_spark.connections import SparkCredentials


class TestCreateSchemasFlag(unittest.TestCase):
    """Test the create_schemas configuration flag"""

    def setUp(self):
        """Set up test fixtures"""
        self.auth_patcher = mock.patch(
            "dbt.adapters.watsonx_spark.connections.get_authenticator"
        )
        self.mock_auth = self.auth_patcher.start()
        self.mock_auth.return_value.get_token.return_value = "test-token"
        self.mock_auth.return_value.get_catlog_details.return_value = ("bucket", "parquet")

    def tearDown(self):
        """Clean up after tests"""
        self.auth_patcher.stop()

    def test_create_schemas_default_true(self):
        """Test that create_schemas defaults to True"""
        creds = SparkCredentials(
            host="test.host.com",
            method="http",
            schema="test_schema",
            catalog="test_catalog",
        )
        self.assertTrue(creds.create_schemas)

    def test_create_schemas_can_be_set_false(self):
        """Test that create_schemas can be set to False"""
        creds = SparkCredentials(
            host="test.host.com",
            method="http",
            schema="test_schema",
            catalog="test_catalog",
            create_schemas=False,
        )
        self.assertFalse(creds.create_schemas)

    def test_create_schemas_can_be_set_true(self):
        """Test that create_schemas can be explicitly set to True"""
        creds = SparkCredentials(
            host="test.host.com",
            method="http",
            schema="test_schema",
            catalog="test_catalog",
            create_schemas=True,
        )
        self.assertTrue(creds.create_schemas)


if __name__ == "__main__":
    unittest.main()

# Made with Bob
