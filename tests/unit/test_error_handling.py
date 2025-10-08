import unittest
from unittest.mock import patch, MagicMock
import requests
import json

from dbt.adapters.watsonx_spark.http_auth.exceptions import (
    TokenRetrievalError,
    InvalidCredentialsError,
    CatalogDetailsError,
    ConnectionError,
    AuthenticationError
)
from dbt.adapters.watsonx_spark.http_auth.wxd_authenticator import WatsonxData
from dbt.adapters.watsonx_spark.http_auth.status_codes import StatusCodeHandler
from dbt.adapters.watsonx_spark.connections import _is_retryable_error


class TestErrorHandling(unittest.TestCase):
    
    def setUp(self):
        self.mock_profile = {
            "type": "test",
            "instance": "test-instance",
            "user": "test-user",
            "apikey": "test-apikey"
        }
        self.mock_host = "https://test-host.com"
        self.mock_uri = "/test-uri"
        
    @patch('requests.post')
    def test_token_retrieval_error(self, mock_post):
        # Setup mock response
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.text = "Internal Server Error"
        mock_post.return_value = mock_response
        
        # Create authenticator
        authenticator = WatsonxData(self.mock_profile, self.mock_host, self.mock_uri)
        
        # Test token retrieval error
        with self.assertRaises(TokenRetrievalError) as context:
            authenticator.get_token()
            
        # Verify error message
        self.assertIn("500", str(context.exception))
        self.assertIn("Internal Server Error", str(context.exception))
        
    @patch('requests.post')
    def test_invalid_credentials_error(self, mock_post):
        # Setup mock response
        mock_response = MagicMock()
        mock_response.status_code = 401
        mock_response.text = "Unauthorized"
        mock_post.return_value = mock_response
        
        # Create authenticator
        authenticator = WatsonxData(self.mock_profile, self.mock_host, self.mock_uri)
        
        # Test invalid credentials error
        with self.assertRaises(InvalidCredentialsError) as context:
            authenticator.get_token()
            
        # Verify error message
        self.assertIn("Authentication failed", str(context.exception))
        
    @patch('requests.post')
    @patch('requests.get')
    def test_catalog_details_error(self, mock_get, mock_post):
        # Setup mock post response for token
        mock_post_response = MagicMock()
        mock_post_response.status_code = 200
        mock_post_response.json.return_value = {"token": "test-token"}
        mock_post.return_value = mock_post_response
        
        # Setup mock get response for catalog details
        mock_get_response = MagicMock()
        mock_get_response.status_code = 404
        mock_get_response.text = "Catalog not found"
        mock_get.return_value = mock_get_response
        
        # Create authenticator
        authenticator = WatsonxData(self.mock_profile, self.mock_host, self.mock_uri)
        
        # Test catalog details error
        with self.assertRaises(CatalogDetailsError) as context:
            authenticator.get_catlog_details("test-catalog")
            
        # Verify error message
        self.assertIn("Catalog 'test-catalog' not found", str(context.exception))
        
    def test_retryable_error_detection(self):
        # Test various error messages
        
        # Test pending error
        exc = Exception("Operation is pending")
        self.assertTrue(_is_retryable_error(exc))
        
        # Test temporarily unavailable error
        exc = Exception("Service temporarily_unavailable")
        self.assertTrue(_is_retryable_error(exc))
        
        # Test timeout error
        exc = Exception("Connection timeout")
        self.assertTrue(_is_retryable_error(exc))
        
        # Test HTTP status code in message
        exc = Exception("HTTP Error 429: Too Many Requests")
        self.assertTrue(_is_retryable_error(exc))
        
        # Test non-retryable error
        exc = Exception("Invalid parameter")
        self.assertEqual(_is_retryable_error(exc), "")
        
        
class TestStatusCodeHandler(unittest.TestCase):
    
    def test_is_retryable(self):
        # Test retryable status codes
        self.assertTrue(StatusCodeHandler.is_retryable(429))
        self.assertTrue(StatusCodeHandler.is_retryable(500))
        self.assertTrue(StatusCodeHandler.is_retryable(503))
        
        # Test non-retryable status codes
        self.assertFalse(StatusCodeHandler.is_retryable(400))
        self.assertFalse(StatusCodeHandler.is_retryable(401))
        self.assertFalse(StatusCodeHandler.is_retryable(404))
        
    def test_get_error_message(self):
        # Test default error message
        self.assertIn("Bad request", StatusCodeHandler.get_error_message(400))
        
        # Test with context
        self.assertIn("Token retrieval", StatusCodeHandler.get_error_message(401, context="Token retrieval"))
        
        # Test with response text
        self.assertIn("Invalid credentials", StatusCodeHandler.get_error_message(401, response_text="Invalid credentials"))
        
        # Test unknown status code
        self.assertIn("Unexpected status code", StatusCodeHandler.get_error_message(499))
        
    def test_handle_response(self):
        # Mock response for success
        mock_success_response = MagicMock()
        mock_success_response.status_code = 200
        
        # Test successful response
        success, error_msg = StatusCodeHandler.handle_response(mock_success_response)
        self.assertTrue(success)
        self.assertEqual(error_msg, "")
        
        # Mock response for error
        mock_error_response = MagicMock()
        mock_error_response.status_code = 401
        mock_error_response.text = "Unauthorized"
        
        # Test error response with custom handler
        def custom_handler(response, msg):
            return False, "Custom error message"
            
        success, error_msg = StatusCodeHandler.handle_response(
            mock_error_response,
            error_handlers={401: custom_handler}
        )
        self.assertFalse(success)
        self.assertEqual(error_msg, "Custom error message")
        
        # Test error response with exception handler
        def exception_handler(response, msg):
            return False, InvalidCredentialsError("Authentication failed")
            
        success, error_msg = StatusCodeHandler.handle_response(
            mock_error_response,
            error_handlers={401: exception_handler}
        )
        self.assertFalse(success)
        self.assertIsInstance(error_msg, InvalidCredentialsError)


if __name__ == '__main__':
    unittest.main()
