"""
Unit tests for connection close handling, specifically testing the fix for
RemoteDisconnected exceptions during session cleanup.

This addresses the issue where long-running queries complete successfully but
fail during connection cleanup when the HTTP server has already closed the
connection due to timeout.
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
from http.client import RemoteDisconnected


class TestPyhiveConnectionWrapperClose(unittest.TestCase):
    """Test the PyhiveConnectionWrapper.close() method handles RemoteDisconnected gracefully."""

    def setUp(self):
        """Set up test fixtures."""
        # Import here to avoid import errors in CI
        try:
            from dbt.adapters.watsonx_spark.connections import PyhiveConnectionWrapper
            self.PyhiveConnectionWrapper = PyhiveConnectionWrapper
        except ImportError:
            self.skipTest("Required dependencies not available")

    def test_close_with_successful_cleanup(self):
        """Test normal close operation when connection is still active."""
        mock_handle = Mock()
        mock_cursor = Mock()
        
        wrapper = self.PyhiveConnectionWrapper(mock_handle)
        wrapper._cursor = mock_cursor
        
        # Should close cursor and handle without exceptions
        wrapper.close()
        
        mock_cursor.close.assert_called_once()
        mock_handle.close.assert_called_once()

    def test_close_with_remote_disconnected_exception(self):
        """Test close operation when server has already closed the connection."""
        mock_handle = Mock()
        mock_cursor = Mock()
        
        # Simulate RemoteDisconnected during handle.close()
        mock_handle.close.side_effect = RemoteDisconnected("Remote end closed connection without response")
        
        wrapper = self.PyhiveConnectionWrapper(mock_handle)
        wrapper._cursor = mock_cursor
        
        # Should handle the exception gracefully and not raise
        try:
            wrapper.close()
        except RemoteDisconnected:
            self.fail("RemoteDisconnected should be caught and handled gracefully")
        
        mock_cursor.close.assert_called_once()
        mock_handle.close.assert_called_once()

    def test_close_with_cursor_exception_and_remote_disconnected(self):
        """Test close when both cursor and handle cleanup fail."""
        mock_handle = Mock()
        mock_cursor = Mock()
        
        # Both cursor and handle cleanup fail
        mock_cursor.close.side_effect = EnvironmentError("Cursor cleanup failed")
        mock_handle.close.side_effect = RemoteDisconnected("Remote end closed connection without response")
        
        wrapper = self.PyhiveConnectionWrapper(mock_handle)
        wrapper._cursor = mock_cursor
        
        # Should handle both exceptions gracefully
        try:
            wrapper.close()
        except (EnvironmentError, RemoteDisconnected):
            self.fail("Exceptions should be caught and handled gracefully")
        
        mock_cursor.close.assert_called_once()
        mock_handle.close.assert_called_once()

    def test_close_with_generic_connection_error(self):
        """Test close with generic connection error containing 'closed connection'."""
        mock_handle = Mock()
        mock_cursor = Mock()
        
        # Simulate a generic connection error
        mock_handle.close.side_effect = Exception("Connection closed by peer")
        
        wrapper = self.PyhiveConnectionWrapper(mock_handle)
        wrapper._cursor = mock_cursor
        
        # Should handle the exception gracefully
        try:
            wrapper.close()
        except Exception:
            self.fail("Connection-related exceptions should be caught and handled gracefully")
        
        mock_cursor.close.assert_called_once()
        mock_handle.close.assert_called_once()

    def test_close_with_unexpected_exception(self):
        """Test close with unexpected exception that should be re-raised."""
        mock_handle = Mock()
        mock_cursor = Mock()
        
        # Simulate an unexpected exception
        mock_handle.close.side_effect = ValueError("Unexpected error")
        
        wrapper = self.PyhiveConnectionWrapper(mock_handle)
        wrapper._cursor = mock_cursor
        
        # Should re-raise unexpected exceptions
        with self.assertRaises(ValueError):
            wrapper.close()
        
        mock_cursor.close.assert_called_once()
        mock_handle.close.assert_called_once()

    def test_close_without_cursor(self):
        """Test close when no cursor has been created."""
        mock_handle = Mock()
        
        wrapper = self.PyhiveConnectionWrapper(mock_handle)
        # _cursor is None
        
        # Should only close handle
        wrapper.close()
        
        mock_handle.close.assert_called_once()

    def test_close_without_cursor_with_remote_disconnected(self):
        """Test close without cursor when server has closed connection."""
        mock_handle = Mock()
        mock_handle.close.side_effect = RemoteDisconnected("Remote end closed connection without response")
        
        wrapper = self.PyhiveConnectionWrapper(mock_handle)
        # _cursor is None
        
        # Should handle the exception gracefully
        try:
            wrapper.close()
        except RemoteDisconnected:
            self.fail("RemoteDisconnected should be caught and handled gracefully")
        
        mock_handle.close.assert_called_once()


class TestConnectionCloseIntegration(unittest.TestCase):
    """Integration tests simulating the actual failure scenario from the logs."""

    def setUp(self):
        """Set up test fixtures."""
        try:
            from dbt.adapters.watsonx_spark.connections import PyhiveConnectionWrapper
            self.PyhiveConnectionWrapper = PyhiveConnectionWrapper
        except ImportError:
            self.skipTest("Required dependencies not available")

    def test_long_running_query_scenario(self):
        """
        Simulate the scenario from the logs:
        1. Query executes successfully (225+ seconds)
        2. Query completes with OK status
        3. Connection cleanup attempts to close session
        4. Server has already closed HTTP connection due to timeout
        5. RemoteDisconnected is raised during CloseSession
        
        Expected: The exception should be caught and logged, not propagated.
        """
        mock_handle = Mock()
        mock_cursor = Mock()
        
        # Simulate successful query execution
        mock_cursor.execute = Mock()
        mock_cursor.fetchall = Mock(return_value=[])
        
        # Simulate RemoteDisconnected during cleanup (as seen in logs)
        mock_handle.close.side_effect = RemoteDisconnected(
            "Remote end closed connection without response"
        )
        
        wrapper = self.PyhiveConnectionWrapper(mock_handle)
        wrapper._cursor = mock_cursor
        
        # Execute query successfully
        wrapper._cursor.execute("SELECT * FROM table")
        
        # Close should not raise exception even though server disconnected
        try:
            wrapper.close()
        except RemoteDisconnected:
            self.fail(
                "RemoteDisconnected during close should be handled gracefully. "
                "This simulates the failure seen in logs where queries complete "
                "successfully but fail during cleanup."
            )

    @patch('dbt.adapters.watsonx_spark.connections.logger')
    def test_remote_disconnected_is_logged(self, mock_logger):
        """Verify that RemoteDisconnected exceptions are logged for debugging."""
        mock_handle = Mock()
        mock_handle.close.side_effect = RemoteDisconnected("Remote end closed connection without response")
        
        wrapper = self.PyhiveConnectionWrapper(mock_handle)
        
        wrapper.close()
        
        # Verify that the exception was logged
        mock_logger.debug.assert_called()
        call_args = str(mock_logger.debug.call_args)
        self.assertIn("Connection already closed by remote server", call_args)


class TestMultipleConnectionsScenario(unittest.TestCase):
    """Test scenarios with multiple concurrent connections as seen in the logs."""

    def setUp(self):
        """Set up test fixtures."""
        try:
            from dbt.adapters.watsonx_spark.connections import PyhiveConnectionWrapper
            self.PyhiveConnectionWrapper = PyhiveConnectionWrapper
        except ImportError:
            self.skipTest("Required dependencies not available")

    def test_two_connections_both_fail_cleanup(self):
        """
        Simulate the log scenario where two models (stg_fraud__cha and stg_fraud__chae)
        both complete successfully but fail during cleanup.
        """
        # Create two connection wrappers
        mock_handle1 = Mock()
        mock_handle2 = Mock()
        
        mock_handle1.close.side_effect = RemoteDisconnected("Remote end closed connection without response")
        mock_handle2.close.side_effect = RemoteDisconnected("Remote end closed connection without response")
        
        wrapper1 = self.PyhiveConnectionWrapper(mock_handle1)
        wrapper2 = self.PyhiveConnectionWrapper(mock_handle2)
        
        # Both should close without raising exceptions
        try:
            wrapper1.close()
            wrapper2.close()
        except RemoteDisconnected:
            self.fail("Both connections should handle RemoteDisconnected gracefully")

    def test_mixed_connection_cleanup_results(self):
        """
        Test scenario where some connections close successfully and others fail.
        This simulates the log where stg_fraud__srv_case succeeded but the other two failed.
        """
        # Connection 1: fails with RemoteDisconnected
        mock_handle1 = Mock()
        mock_handle1.close.side_effect = RemoteDisconnected("Remote end closed connection without response")
        
        # Connection 2: fails with RemoteDisconnected
        mock_handle2 = Mock()
        mock_handle2.close.side_effect = RemoteDisconnected("Remote end closed connection without response")
        
        # Connection 3: succeeds (long-running query, connection still alive)
        mock_handle3 = Mock()
        
        wrapper1 = self.PyhiveConnectionWrapper(mock_handle1)
        wrapper2 = self.PyhiveConnectionWrapper(mock_handle2)
        wrapper3 = self.PyhiveConnectionWrapper(mock_handle3)
        
        # All should complete without raising exceptions
        try:
            wrapper1.close()  # Should handle RemoteDisconnected
            wrapper2.close()  # Should handle RemoteDisconnected
            wrapper3.close()  # Should close normally
        except Exception as e:
            self.fail(f"All connections should close gracefully, but got: {e}")
        
        # Verify all close methods were called
        mock_handle1.close.assert_called_once()
        mock_handle2.close.assert_called_once()
        mock_handle3.close.assert_called_once()


if __name__ == '__main__':
    unittest.main()

# Made with Bob
