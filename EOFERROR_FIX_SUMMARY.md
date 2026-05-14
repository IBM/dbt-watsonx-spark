# EOFError Fix Summary

## Issue Description

The dbt-watsonx-spark adapter was experiencing `EOFError` exceptions during query execution and connection cleanup. The errors occurred in two main scenarios:

1. **During query polling**: When checking query status via `GetOperationStatus`
2. **During connection cleanup**: When closing cursors and connections via `CloseOperation`

### Error Stack Trace
```
File "/usr/local/lib/python3.12/site-packages/pyhive/hive.py", line 530, in poll
    response = self._connection.client.GetOperationStatus(req)
...
File "/usr/local/lib/python3.12/site-packages/thrift/transport/TTransport.py", line 69, in readAll
    raise EOFError()
```

## Root Cause

The `EOFError` indicates that the Thrift connection was unexpectedly closed by the server or network. This can happen due to:

1. **Authentication token expiration** during long-running queries
2. **Network connectivity issues** causing connection drops
3. **Server-side timeouts** or resource constraints
4. **Load balancer timeouts** in cloud environments

The original code had insufficient error handling:
- The `close()` method only caught `EnvironmentError`, missing `EOFError`
- The `execute()` method's polling loop had no error handling for connection drops
- The connection opening logic didn't retry on `EOFError`

## Changes Made

### 1. Enhanced Error Handling in `cancel()` Method (Line 270-277)
**Before:**
```python
except EnvironmentError as exc:
    logger.debug("Exception while cancelling query: {}".format(exc))
```

**After:**
```python
except (EnvironmentError, EOFError) as exc:
    logger.debug("Exception while cancelling query: {}".format(exc))
```

### 2. Enhanced Error Handling in `close()` Method (Line 279-295)
**Before:**
```python
def close(self) -> None:
    if self._cursor:
        try:
            self._cursor.close()
        except EnvironmentError as exc:
            logger.debug("Exception while closing cursor: {}".format(exc))
    self.handle.close()
```

**After:**
```python
def close(self) -> None:
    if self._cursor:
        try:
            self._cursor.close()
        except (EnvironmentError, EOFError) as exc:
            logger.debug("Exception while closing cursor: {}".format(exc))
    
    # Handle connection close errors gracefully
    try:
        self.handle.close()
    except (EnvironmentError, EOFError) as exc:
        logger.debug("Exception while closing connection handle: {}".format(exc))
```

### 3. Added Error Handling in `execute()` Polling Loop (Line 320-350)
**Before:**
```python
self._cursor.execute(sql, bindings, async_=True)
poll_state = self._cursor.poll()
state = poll_state.operationState

while state in STATE_PENDING:
    logger.debug("Poll status: {}, sleeping".format(state))
    poll_state = self._cursor.poll()
    state = poll_state.operationState
```

**After:**
```python
self._cursor.execute(sql, bindings, async_=True)

try:
    poll_state = self._cursor.poll()
    state = poll_state.operationState
except EOFError as e:
    error_msg = (
        "Connection lost while polling query status. "
        "This may indicate the server closed the connection unexpectedly. "
        "Please check server logs and network connectivity."
    )
    logger.error(error_msg)
    raise DbtDatabaseError(error_msg) from e

while state in STATE_PENDING:
    logger.debug("Poll status: {}, sleeping".format(state))

    try:
        poll_state = self._cursor.poll()
        state = poll_state.operationState
    except EOFError as e:
        error_msg = (
            "Connection lost while polling query status. "
            "The query may still be running on the server. "
            "Please check server logs for query status."
        )
        logger.error(error_msg)
        raise DbtDatabaseError(error_msg) from e
```

### 4. Enhanced EOFError Handling During Connection Opening (Line 664-683)
**Before:**
```python
except EOFError as e:
    exc = e
    msg = "Failed to connect - authentication error"
    if creds.token is not None:
        msg += ". Please check if your token is valid and not expired."
    logger.error(msg)
    raise FailedToConnectError(msg) from e
```

**After:**
```python
except EOFError as e:
    exc = e
    msg = "Connection closed unexpectedly (EOFError)"
    if creds.token is not None:
        msg += ". This may indicate an authentication error - please check if your token is valid and not expired."
    else:
        msg += ". This may indicate a network issue or server-side connection problem."
    
    # Check if we should retry
    if creds.connect_retries > 0 and i < creds.connect_retries:
        logger.warning(
            f"{msg}\n\tRetrying in {creds.connect_timeout} seconds "
            f"({i + 1} of {creds.connect_retries})"
        )
        time.sleep(creds.connect_timeout)
    else:
        logger.error(msg)
        raise FailedToConnectError(msg) from e
```

## Benefits

1. **Graceful Degradation**: Connections can now close cleanly even when the server has already dropped the connection
2. **Better Error Messages**: Users get clear, actionable error messages explaining what went wrong
3. **Retry Logic**: EOFError during connection opening now respects the `connect_retries` configuration
4. **Improved Debugging**: Detailed logging helps diagnose connection issues
5. **Prevents Cascading Failures**: Proper cleanup prevents resource leaks

## Testing Recommendations

1. **Token Expiration**: Test with expired authentication tokens
2. **Network Interruption**: Simulate network drops during query execution
3. **Long-Running Queries**: Test queries that exceed typical timeout thresholds
4. **Connection Cleanup**: Verify connections close properly even after errors
5. **Retry Logic**: Confirm retries work as expected with `connect_retries` configuration

## Configuration

Users can configure retry behavior in their `profiles.yml`:

```yaml
your_profile:
  target: dev
  outputs:
    dev:
      type: watsonx_spark
      method: http
      host: your-host
      # ... other settings ...
      connect_retries: 3        # Number of connection retry attempts
      connect_timeout: 10       # Seconds to wait between retries
```

## Related Issues

This fix addresses connection stability issues that manifest as:
- "EOFError" during query execution
- "Connection lost while polling query status"
- Failed connection cleanup causing resource leaks
- Intermittent authentication failures

## Files Modified

- `dbt/adapters/watsonx_spark/connections.py`
  - `PyhiveConnectionWrapper.cancel()` method
  - `PyhiveConnectionWrapper.close()` method
  - `PyhiveConnectionWrapper.execute()` method
  - `SparkConnectionManager.open()` method (EOFError handling)