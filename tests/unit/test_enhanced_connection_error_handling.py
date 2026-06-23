"""
Unit tests for enhanced connection error handling during query polling.

This test suite validates the improvements made to handle RemoteDisconnected
and other connection errors that occur during query execution and polling,
particularly addressing the issue where queries complete successfully but
fail during the polling phase.
"""
import unittest
from unittest.mock import Mock, patch, MagicMock, call
import http.client
from dbt.adapters.watsonx_spark.connections import (
    PyhiveConnectionWrapper,
    CONNECTION_LOST_EXCEPTIONS,
)
from dbt_common.exceptions import DbtRuntimeError


class TestEnhancedConnectionErrorDetection(unittest.TestCase):
    """Test the enhanced _is_connection_error method."""

    def setUp(self):
        """Set up test fixtures."""
        self.mock_handle = Mock()
        self.wrapper = PyhiveConnectionWrapper(
            self.mock_handle,
            poll_interval=5,
            query_timeout=None,
            query_retries=2,
        )

    def test_detects_remote_disconnected_by_type(self):
        """Test that RemoteDisconnected is detected by exception type."""
        exc = http.client.RemoteDisconnected("Remote end closed connection without response")
        self.assertTrue(self.wrapper._is_connection_error(exc))

    def test_detects_connection_reset_error(self):
        """Test that ConnectionResetError is detected."""
        exc = ConnectionResetError("Connection reset by peer")
        self.assertTrue(self.wrapper._is_connection_error(exc))

    def test_detects_broken_pipe_error(self):
        """Test that BrokenPipeError is detected."""
        exc = BrokenPipeError("Broken pipe")
        self.assertTrue(self.wrapper._is_connection_error(exc))

    def test_detects_eof_error(self):
        """Test that EOFError is detected."""
        exc = EOFError("EOF occurred in violation of protocol")
        self.assertTrue(self.wrapper._is_connection_error(exc))

    def test_detects_connection_error_by_message_pattern(self):
        """Test that connection errors are detected by message patterns."""
        test_cases = [
            Exception("Remote end closed connection without response"),
            Exception("Connection closed by peer"),
            Exception("Broken pipe error occurred"),
            Exception("Connection reset by server"),
            Exception("Connection aborted"),
            Exception("Connection timeout"),
            Exception("Connection lost during operation"),
            Exception("EOF occurred in violation of protocol"),
        ]
        
        for exc in test_cases:
            with self.subTest(msg=str(exc)):
                self.assertTrue(
                    self.wrapper._is_connection_error(exc),
                    f"Failed to detect connection error: {exc}"
                )

    def test_does_not_detect_non_connection_errors(self):
        """Test that non-connection errors are not falsely detected."""
        test_cases = [
            ValueError("Invalid value"),
            TypeError("Type mismatch"),
            KeyError("Key not found"),
            Exception("Some other error"),
            Exception("Query syntax error"),
        ]
        
        for exc in test_cases:
            with self.subTest(msg=str(exc)):
                self.assertFalse(
                    self.wrapper._is_connection_error(exc),
                    f"Falsely detected as connection error: {exc}"
                )

    def test_detects_exception_by_class_name(self):
        """Test that exceptions are detected by their class name."""
        # Create a mock exception with RemoteDisconnected in the name
        class CustomRemoteDisconnected(Exception):
            pass
        
        exc = CustomRemoteDisconnected("Connection issue")
        self.assertTrue(self.wrapper._is_connection_error(exc))


class TestPollingWithConnectionErrorHandling(unittest.TestCase):
    """Test query polling with enhanced connection error handling."""

    def setUp(self):
        """Set up test fixtures."""
        self.mock_handle = Mock()
        self.mock_cursor = Mock()
        self.mock_handle.cursor.return_value = self.mock_cursor
        
        self.wrapper = PyhiveConnectionWrapper(
            self.mock_handle,
            poll_interval=1,  # Short interval for tests
            query_timeout=None,
            query_retries=2,
        )
        self.wrapper._cursor = self.mock_cursor

    @patch('dbt.adapters.watsonx_spark.connections.time.sleep')
    @patch('dbt.adapters.watsonx_spark.connections.ThriftState')
    def test_initial_poll_connection_error_triggers_retry(self, mock_thrift_state, mock_sleep):
        """Test that connection error during initial poll triggers retry."""
        # Setup mock states
        mock_thrift_state.FINISHED_STATE = 3
        
        # First attempt: connection error on initial poll
        # Second attempt: success
        attempt_count = [0]
        
        def execute_side_effect(*args, **kwargs):
            attempt_count[0] += 1
            if attempt_count[0] == 1:
                raise http.client.RemoteDisconnected("Remote end closed connection without response")
        
        self.mock_cursor.execute.side_effect = execute_side_effect
        
        # Setup successful poll for second attempt
        poll_state = Mock()
        poll_state.operationState = 3  # FINISHED
        poll_state.errorMessage = None
        self.mock_cursor.poll.return_value = poll_state
        
        # Execute query
        self.wrapper.execute("SELECT 1", None)
        
        # Verify retry happened
        self.assertEqual(self.mock_cursor.execute.call_count, 2)
        self.assertEqual(mock_sleep.call_count, 1)  # Sleep between retries

    @patch('dbt.adapters.watsonx_spark.connections.time.sleep')
    @patch('dbt.adapters.watsonx_spark.connections.ThriftState')
    @patch('dbt.adapters.watsonx_spark.connections.logger')
    def test_polling_loop_connection_error_triggers_retry(self, mock_logger, mock_thrift_state, mock_sleep):
        """Test that connection error during polling loop triggers retry."""
        # Setup mock states
        mock_thrift_state.INITIALIZED_STATE = 0
        mock_thrift_state.RUNNING_STATE = 1
        mock_thrift_state.PENDING_STATE = 2
        mock_thrift_state.FINISHED_STATE = 3
        
        # First attempt: connection error during polling
        # Second attempt: success
        attempt_count = [0]
        poll_count = [0]
        
        def poll_side_effect():
            poll_count[0] += 1
            
            if attempt_count[0] == 0:
                # First attempt: fail on second poll
                if poll_count[0] == 1:
                    poll_state = Mock()
                    poll_state.operationState = 1  # RUNNING
                    poll_state.errorMessage = None
                    return poll_state
                else:
                    raise http.client.RemoteDisconnected("Remote end closed connection without response")
            else:
                # Second attempt: succeed
                poll_state = Mock()
                poll_state.operationState = 3  # FINISHED
                poll_state.errorMessage = None
                return poll_state
        
        def execute_side_effect(*args, **kwargs):
            attempt_count[0] += 1
            poll_count[0] = 0  # Reset poll count for each attempt
        
        self.mock_cursor.execute.side_effect = execute_side_effect
        self.mock_cursor.poll.side_effect = poll_side_effect
        
        # Execute query
        self.wrapper.execute("SELECT 1", None)
        
        # Verify retry happened
        self.assertEqual(self.mock_cursor.execute.call_count, 2)
        # Sleep once between retries + sleep in polling loop
        self.assertGreaterEqual(mock_sleep.call_count, 1)

    @patch('dbt.adapters.watsonx_spark.connections.time.sleep')
    @patch('dbt.adapters.watsonx_spark.connections.logger')
    def test_all_retries_exhausted_raises_error(self, mock_logger, mock_sleep):
        """Test that error is raised after all retries are exhausted."""
        # All attempts fail with connection error
        self.mock_cursor.execute.side_effect = http.client.RemoteDisconnected(
            "Remote end closed connection without response"
        )
        
        # Execute query and expect failure
        with self.assertRaises(DbtRuntimeError) as context:
            self.wrapper.execute("SELECT 1", None)
        
        # Verify error message
        self.assertIn("failed after 3 attempts", str(context.exception).lower())
        self.assertIn("connection loss", str(context.exception).lower())
        
        # Verify all retries were attempted (initial + 2 retries)
        self.assertEqual(self.mock_cursor.execute.call_count, 3)

    @patch('dbt.adapters.watsonx_spark.connections.logger')
    def test_cursor_refresh_on_retry(self, mock_logger):
        """Test that cursor is refreshed on retry."""
        # Track cursor refreshes
        cursor_calls = []
        
        def cursor_side_effect():
            new_cursor = Mock()
            cursor_calls.append(new_cursor)
            return new_cursor
        
        self.mock_handle.cursor.side_effect = cursor_side_effect
        
        # First attempt fails, second succeeds
        attempt_count = [0]
        
        def execute_side_effect(*args, **kwargs):
            attempt_count[0] += 1
            if attempt_count[0] == 1:
                raise http.client.RemoteDisconnected("Remote end closed connection without response")
        
        self.mock_cursor.execute.side_effect = execute_side_effect
        
        # Setup successful poll for second attempt
        poll_state = Mock()
        poll_state.operationState = 3  # FINISHED (assuming ThriftState.FINISHED_STATE = 3)
        poll_state.errorMessage = None
        self.mock_cursor.poll.return_value = poll_state
        
        # Execute query
        with patch('dbt.adapters.watsonx_spark.connections.ThriftState') as mock_thrift_state:
            mock_thrift_state.FINISHED_STATE = 3
            with patch('dbt.adapters.watsonx_spark.connections.time.sleep'):
                self.wrapper.execute("SELECT 1", None)
        
        # Verify cursor was refreshed once (on retry)
        self.assertEqual(len(cursor_calls), 1)

    @patch('dbt.adapters.watsonx_spark.connections.logger')
    def test_cursor_refresh_failure_raises_error(self, mock_logger):
        """Test that failure to refresh cursor raises appropriate error."""
        # First attempt fails with connection error
        self.mock_cursor.execute.side_effect = http.client.RemoteDisconnected(
            "Remote end closed connection without response"
        )
        
        # Cursor refresh fails
        self.mock_handle.cursor.side_effect = Exception("Failed to create cursor")
        
        # Execute query and expect failure
        with self.assertRaises(DbtRuntimeError) as context:
            with patch('dbt.adapters.watsonx_spark.connections.time.sleep'):
                self.wrapper.execute("SELECT 1", None)
        
        # Verify error message mentions cursor refresh failure
        self.assertIn("failed to refresh cursor", str(context.exception).lower())


class TestNonConnectionErrorHandling(unittest.TestCase):
    """Test that non-connection errors are not retried."""

    def setUp(self):
        """Set up test fixtures."""
        self.mock_handle = Mock()
        self.mock_cursor = Mock()
        self.mock_handle.cursor.return_value = self.mock_cursor
        
        self.wrapper = PyhiveConnectionWrapper(
            self.mock_handle,
            poll_interval=1,
            query_timeout=None,
            query_retries=2,
        )
        self.wrapper._cursor = self.mock_cursor

    @patch('dbt.adapters.watsonx_spark.connections.time.sleep')
    def test_non_connection_error_not_retried(self, mock_sleep):
        """Test that non-connection errors are raised immediately without retry."""
        # Raise a non-connection error
        self.mock_cursor.execute.side_effect = ValueError("Invalid SQL syntax")
        
        # Execute query and expect immediate failure
        with self.assertRaises(ValueError):
            self.wrapper.execute("SELECT 1", None)
        
        # Verify no retries happened
        self.assertEqual(self.mock_cursor.execute.call_count, 1)
        mock_sleep.assert_not_called()


class TestLoggingImprovements(unittest.TestCase):
    """Test enhanced logging for better debugging."""

    def setUp(self):
        """Set up test fixtures."""
        self.mock_handle = Mock()
        self.mock_cursor = Mock()
        self.mock_handle.cursor.return_value = self.mock_cursor
        
        self.wrapper = PyhiveConnectionWrapper(
            self.mock_handle,
            poll_interval=1,
            query_timeout=None,
            query_retries=1,
        )
        self.wrapper._cursor = self.mock_cursor

    @patch('dbt.adapters.watsonx_spark.connections.logger')
    @patch('dbt.adapters.watsonx_spark.connections.time.sleep')
    @patch('dbt.adapters.watsonx_spark.connections.ThriftState')
    def test_retry_attempt_logged(self, mock_thrift_state, mock_sleep, mock_logger):
        """Test that retry attempts are logged."""
        mock_thrift_state.FINISHED_STATE = 3
        
        # First attempt fails, second succeeds
        attempt_count = [0]
        
        def execute_side_effect(*args, **kwargs):
            attempt_count[0] += 1
            if attempt_count[0] == 1:
                raise http.client.RemoteDisconnected("Remote end closed connection without response")
        
        self.mock_cursor.execute.side_effect = execute_side_effect
        
        poll_state = Mock()
        poll_state.operationState = 3
        poll_state.errorMessage = None
        self.mock_cursor.poll.return_value = poll_state
        
        # Execute query
        self.wrapper.execute("SELECT 1", None)
        
        # Verify logging calls
        log_calls = [str(call) for call in mock_logger.warning.call_args_list]
        self.assertTrue(
            any("connection lost" in str(call).lower() for call in log_calls),
            "Expected connection loss warning to be logged"
        )

    @patch('dbt.adapters.watsonx_spark.connections.logger')
    @patch('dbt.adapters.watsonx_spark.connections.time.sleep')
    @patch('dbt.adapters.watsonx_spark.connections.ThriftState')
    def test_successful_retry_logged(self, mock_thrift_state, mock_sleep, mock_logger):
        """Test that successful retry is logged."""
        mock_thrift_state.FINISHED_STATE = 3
        
        # First attempt fails, second succeeds
        attempt_count = [0]
        
        def execute_side_effect(*args, **kwargs):
            attempt_count[0] += 1
            if attempt_count[0] == 1:
                raise http.client.RemoteDisconnected("Remote end closed connection without response")
        
        self.mock_cursor.execute.side_effect = execute_side_effect
        
        poll_state = Mock()
        poll_state.operationState = 3
        poll_state.errorMessage = None
        self.mock_cursor.poll.return_value = poll_state
        
        # Execute query
        self.wrapper.execute("SELECT 1", None)
        
        # Verify success logging
        log_calls = [str(call) for call in mock_logger.info.call_args_list]
        self.assertTrue(
            any("succeeded on retry" in str(call).lower() for call in log_calls),
            "Expected success message to be logged"
        )


if __name__ == '__main__':
    unittest.main()

# Made with Bob
