"""Unit tests for query retry mechanism and polling improvements."""
import unittest
from unittest.mock import Mock, patch, MagicMock
import time
from dbt.adapters.watsonx_spark.connections import (
    PyhiveConnectionWrapper,
    CONNECTION_LOST_EXCEPTIONS,
)
from dbt_common.exceptions import DbtRuntimeError


class TestQueryRetryMechanism(unittest.TestCase):
    """Test the query retry mechanism for connection loss during polling."""

    def setUp(self):
        """Set up test fixtures."""
        self.mock_handle = Mock()
        self.mock_cursor = Mock()
        self.mock_handle.cursor.return_value = self.mock_cursor

    def test_poll_interval_configuration(self):
        """Test that poll_interval is properly configured."""
        wrapper = PyhiveConnectionWrapper(
            self.mock_handle,
            poll_interval=10,
            query_timeout=None,
            query_retries=2,
        )
        
        self.assertEqual(wrapper.poll_interval, 10)
        self.assertEqual(wrapper.query_retries, 2)
        self.assertIsNone(wrapper.query_timeout)

    def test_query_timeout_configuration(self):
        """Test that query_timeout is properly configured."""
        wrapper = PyhiveConnectionWrapper(
            self.mock_handle,
            poll_interval=5,
            query_timeout=300,
            query_retries=1,
        )
        
        self.assertEqual(wrapper.query_timeout, 300)

    @patch('dbt.adapters.watsonx_spark.connections.time.sleep')
    @patch('dbt.adapters.watsonx_spark.connections.ThriftState')
    def test_polling_with_sleep(self, mock_thrift_state, mock_sleep):
        """Test that polling includes sleep between polls."""
        # Setup mock states
        mock_thrift_state.INITIALIZED_STATE = 0
        mock_thrift_state.RUNNING_STATE = 1
        mock_thrift_state.PENDING_STATE = 2
        mock_thrift_state.FINISHED_STATE = 3
        
        # Create wrapper
        wrapper = PyhiveConnectionWrapper(
            self.mock_handle,
            poll_interval=5,
            query_timeout=None,
            query_retries=1,
        )
        wrapper._cursor = self.mock_cursor
        
        # Setup poll states: pending -> pending -> finished
        poll_state_1 = Mock()
        poll_state_1.operationState = 1  # RUNNING
        poll_state_1.errorMessage = None  # No error
        
        poll_state_2 = Mock()
        poll_state_2.operationState = 1  # RUNNING
        poll_state_2.errorMessage = None  # No error
        
        poll_state_3 = Mock()
        poll_state_3.operationState = 3  # FINISHED
        poll_state_3.errorMessage = None  # No error
        
        self.mock_cursor.poll.side_effect = [
            poll_state_1,
            poll_state_2,
            poll_state_3,
        ]
        
        # Execute query
        wrapper._execute_with_polling("SELECT 1", None)
        
        # Verify sleep was called with correct interval
        self.assertEqual(mock_sleep.call_count, 2)  # Two pending states
        mock_sleep.assert_called_with(5)

    @patch('dbt.adapters.watsonx_spark.connections.time.sleep')
    @patch('dbt.adapters.watsonx_spark.connections.time.time')
    @patch('dbt.adapters.watsonx_spark.connections.ThriftState')
    def test_query_timeout_enforcement(self, mock_thrift_state, mock_time, mock_sleep):
        """Test that query timeout is enforced during polling."""
        # Setup mock states
        mock_thrift_state.INITIALIZED_STATE = 0
        mock_thrift_state.RUNNING_STATE = 1
        mock_thrift_state.PENDING_STATE = 2
        mock_thrift_state.FINISHED_STATE = 3
        
        # Create wrapper with timeout
        wrapper = PyhiveConnectionWrapper(
            self.mock_handle,
            poll_interval=5,
            query_timeout=10,  # 10 second timeout
            query_retries=1,
        )
        wrapper._cursor = self.mock_cursor
        
        # Setup time progression: 0, 5, 11 (exceeds timeout)
        mock_time.side_effect = [0, 5, 11]
        
        # Setup poll states: all pending
        poll_state = Mock()
        poll_state.operationState = 1  # RUNNING
        self.mock_cursor.poll.return_value = poll_state
        
        # Execute query and expect timeout
        with self.assertRaises(DbtRuntimeError) as context:
            wrapper._execute_with_polling("SELECT 1", None)
        
        self.assertIn("exceeded timeout", str(context.exception).lower())

    @patch('dbt.adapters.watsonx_spark.connections.logger')
    @patch('dbt.adapters.watsonx_spark.connections.time.sleep')
    @patch('dbt.adapters.watsonx_spark.connections.ThriftState')
    def test_retry_on_connection_lost(self, mock_thrift_state, mock_sleep, mock_logger):
        """Test that queries are retried on connection loss."""
        # Setup mock states
        mock_thrift_state.INITIALIZED_STATE = 0
        mock_thrift_state.RUNNING_STATE = 1
        mock_thrift_state.PENDING_STATE = 2
        mock_thrift_state.FINISHED_STATE = 3
        
        # Create wrapper with retries
        wrapper = PyhiveConnectionWrapper(
            self.mock_handle,
            poll_interval=5,
            query_timeout=None,
            query_retries=2,
        )
        wrapper._cursor = self.mock_cursor
        
        # First attempt: connection lost during polling
        # Second attempt: success
        attempt_count = [0]
        
        def execute_side_effect(*args, **kwargs):
            attempt_count[0] += 1
            if attempt_count[0] == 1:
                # First attempt fails
                raise ConnectionResetError("Connection lost")
            # Second attempt succeeds - do nothing
        
        # Mock _execute_with_polling
        wrapper._execute_with_polling = Mock(side_effect=execute_side_effect)
        
        # Execute query
        wrapper.execute("SELECT 1", None)
        
        # Verify retry happened
        self.assertEqual(wrapper._execute_with_polling.call_count, 2)
        self.assertEqual(mock_sleep.call_count, 1)  # Sleep between retries
        mock_sleep.assert_called_with(5)

    @patch('dbt.adapters.watsonx_spark.connections.logger')
    @patch('dbt.adapters.watsonx_spark.connections.time.sleep')
    def test_retry_exhaustion(self, mock_sleep, mock_logger):
        """Test that error is raised after all retries are exhausted."""
        # Create wrapper with limited retries
        wrapper = PyhiveConnectionWrapper(
            self.mock_handle,
            poll_interval=5,
            query_timeout=None,
            query_retries=1,  # Only 1 retry
        )
        wrapper._cursor = self.mock_cursor
        
        # All attempts fail
        wrapper._execute_with_polling = Mock(
            side_effect=ConnectionResetError("Connection lost")
        )
        
        # Execute query and expect failure
        with self.assertRaises(DbtRuntimeError) as context:
            wrapper.execute("SELECT 1", None)
        
        # Verify all retries were attempted
        self.assertEqual(wrapper._execute_with_polling.call_count, 2)  # Initial + 1 retry
        self.assertIn("failed after 2 attempts", str(context.exception).lower())

    def test_connection_lost_exceptions_tuple(self):
        """Test that CONNECTION_LOST_EXCEPTIONS includes expected types."""
        self.assertIn(ConnectionResetError, CONNECTION_LOST_EXCEPTIONS)
        self.assertIn(BrokenPipeError, CONNECTION_LOST_EXCEPTIONS)
        self.assertIn(EOFError, CONNECTION_LOST_EXCEPTIONS)
        
        # Check for http.client.RemoteDisconnected
        import http.client
        self.assertIn(http.client.RemoteDisconnected, CONNECTION_LOST_EXCEPTIONS)

    @patch('dbt.adapters.watsonx_spark.connections.time.sleep')
    def test_non_connection_error_not_retried(self, mock_sleep):
        """Test that non-connection errors are not retried."""
        # Create wrapper
        wrapper = PyhiveConnectionWrapper(
            self.mock_handle,
            poll_interval=5,
            query_timeout=None,
            query_retries=2,
        )
        wrapper._cursor = self.mock_cursor
        
        # Raise a non-connection error
        wrapper._execute_with_polling = Mock(
            side_effect=ValueError("Invalid SQL")
        )
        
        # Execute query and expect immediate failure
        with self.assertRaises(ValueError):
            wrapper.execute("SELECT 1", None)
        
        # Verify no retries happened
        self.assertEqual(wrapper._execute_with_polling.call_count, 1)
        mock_sleep.assert_not_called()

    @patch('dbt.adapters.watsonx_spark.connections.logger')
    @patch('dbt.adapters.watsonx_spark.connections.time.sleep')
    def test_cursor_refresh_on_retry(self, mock_sleep, mock_logger):
        """Test that cursor is refreshed on retry."""
        # Create wrapper
        wrapper = PyhiveConnectionWrapper(
            self.mock_handle,
            poll_interval=5,
            query_timeout=None,
            query_retries=1,
        )
        
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
                raise ConnectionResetError("Connection lost")
        
        wrapper._execute_with_polling = Mock(side_effect=execute_side_effect)
        
        # Execute query
        wrapper.execute("SELECT 1", None)
        
        # Verify cursor was refreshed
        self.assertEqual(len(cursor_calls), 1)  # One refresh on retry


class TestPollingImprovements(unittest.TestCase):
    """Test polling loop improvements."""

    @patch('dbt.adapters.watsonx_spark.connections.time.sleep')
    @patch('dbt.adapters.watsonx_spark.connections.ThriftState')
    def test_default_poll_interval(self, mock_thrift_state, mock_sleep):
        """Test that default poll interval is 5 seconds."""
        mock_handle = Mock()
        mock_cursor = Mock()
        mock_handle.cursor.return_value = mock_cursor
        
        # Create wrapper with defaults
        wrapper = PyhiveConnectionWrapper(mock_handle)
        
        self.assertEqual(wrapper.poll_interval, 5)
        self.assertEqual(wrapper.query_retries, 1)
        self.assertIsNone(wrapper.query_timeout)


if __name__ == '__main__':
    unittest.main()

# Made with Bob
