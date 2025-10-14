# Zerobus SDK Examples

This directory contains example applications demonstrating different usage patterns of the Zerobus Ingest SDK for Python.

## Examples

### 1. Synchronous Ingestion (`sync_example.py`)

Demonstrates synchronous record ingestion where each record is waited for before proceeding to the next.

**Best for:**
- Simple scripts and applications
- Use cases requiring immediate confirmation per record
- Applications not using asyncio
- Straightforward error handling

**Key features:**
- Waits for each record to be durably written
- Simple, blocking API
- Immediate error handling
- Works in any Python environment

**Demonstrates:**
- SDK initialization and configuration
- Stream creation with configuration options
- Synchronous record ingestion with acknowledgment
- Progress tracking
- Stream flushing and closing
- Error handling
- Performance metrics

**Run:**
```bash
# Set environment variables
export DATABRICKS_CLIENT_ID="your-service-principal-id"
export DATABRICKS_CLIENT_SECRET="your-service-principal-secret"
export ZEROBUS_SERVER_ENDPOINT="workspace-id.zerobus.region.cloud.databricks.com"
export DATABRICKS_WORKSPACE_URL="https://your-workspace.cloud.databricks.com"
export ZEROBUS_TABLE_NAME="catalog.schema.table"

# Run the example
python examples/sync_example.py
```

### 2. Asynchronous Ingestion (`async_example.py`)

Demonstrates asynchronous record ingestion using Python's asyncio framework.

**Best for:**
- Applications already using asyncio
- Concurrent operations with other async tasks
- Async web frameworks (FastAPI, aiohttp, etc.)
- Event-driven architectures
- Integration with other async operations in your application

**Key features:**
- Asynchronous ingestion with asyncio
- Non-blocking API for concurrent execution
- Ack callback for progress tracking
- Integrates seamlessly with async event loops
- Allows concurrent execution with other async operations

**Demonstrates:**
- Async SDK initialization
- Stream configuration with acknowledgment callbacks
- Asynchronous record ingestion
- Batch submission tracking
- Stream flushing and durability waiting
- Error handling in async context
- Performance metrics including submit time vs total time

**Run:**
```bash
# Set environment variables (same as sync example)
export DATABRICKS_CLIENT_ID="your-service-principal-id"
export DATABRICKS_CLIENT_SECRET="your-service-principal-secret"
export ZEROBUS_SERVER_ENDPOINT="workspace-id.zerobus.region.cloud.databricks.com"
export DATABRICKS_WORKSPACE_URL="https://your-workspace.cloud.databricks.com"
export ZEROBUS_TABLE_NAME="catalog.schema.table"

# Run the example
python examples/async_example.py
```

## Setup

### 1. Install the SDK

```bash
pip install databricks-zerobus-ingest-sdk
```

Or install from source:
```bash
cd /path/to/zerobus-sdk-py
pip install .
```

### 2. Generate Protobuf Classes

The examples include a sample `record.proto` file that defines an `AirQuality` message:

```proto
syntax = "proto2";

message AirQuality {
    optional string device_name = 1;
    optional int32 temp = 2;
    optional int64 humidity = 3;
}
```

The examples are already set up to use this schema. To regenerate the Python classes if needed:

```bash
cd examples
pip install "grpcio-tools>=1.60.0,<2.0"
python -m grpc_tools.protoc --python_out=. --proto_path=. record.proto
```

This generates a `record_pb2.py` file compatible with protobuf 6.x that is imported by the example scripts.

To use your own schema, modify `record.proto` and regenerate the Python classes.

### 3. Configure Credentials

The examples are fully runnable and use environment variables for configuration:

```bash
export DATABRICKS_CLIENT_ID="your-service-principal-application-id"
export DATABRICKS_CLIENT_SECRET="your-service-principal-secret"
export ZEROBUS_SERVER_ENDPOINT="workspace-id.zerobus.region.cloud.databricks.com"
export DATABRICKS_WORKSPACE_URL="https://your-workspace.cloud.databricks.com"
export ZEROBUS_TABLE_NAME="catalog.schema.table"
```

Alternatively, you can edit the default values directly in the example files.

### 4. Run the Examples

The examples are now ready to run! They will automatically:
- Check for proper configuration
- Initialize logging
- Create a stream with the configured credentials
- Ingest sample records
- Display progress and performance metrics
- Handle errors gracefully
- Clean up resources

Simply run:
```bash
python examples/sync_example.py
# or
python examples/async_example.py
```

The examples demonstrate all SDK functionalities including:
- SDK and stream initialization
- Table properties configuration
- Stream configuration options
- Record ingestion (sync/async)
- Progress tracking with callbacks
- Stream flushing and closing
- Error handling
- Performance measurement

## Configuration Options

### StreamConfigurationOptions

| Option | Default | Description |
|--------|---------|-------------|
| `max_inflight_records` | 50000 | Maximum number of unacknowledged records |
| `recovery` | True | Enable automatic stream recovery |
| `recovery_timeout_ms` | 15000 | Timeout for recovery operations (ms) |
| `recovery_backoff_ms` | 2000 | Delay between recovery attempts (ms) |
| `recovery_retries` | 3 | Maximum number of recovery attempts |
| `flush_timeout_ms` | 300000 | Timeout for flush operations (ms) |
| `server_lack_of_ack_timeout_ms` | 60000 | Server acknowledgment timeout (ms) |
| `ack_callback` | None | Callback invoked on record acknowledgment |

## API Comparison

| Metric | Synchronous | Asynchronous |
|--------|-------------|--------------|
| Concurrency | Sequential execution | Concurrent execution with asyncio |
| Memory usage | Low | Medium (buffering) |
| Complexity | Simple | Moderate |
| Error handling | Immediate | Deferred to flush |
| Use case | Simple applications, scripts | Async applications, event loops |
| Integration | Any Python code | Requires asyncio event loop |

## Best Practices

1. **Choose the right API**: Use synchronous for simple scripts, asynchronous for async applications
2. **Monitor progress**: Use `ack_callback` to track acknowledgment progress
3. **Handle errors**: Always wrap ingestion in try-except blocks
4. **Close streams**: Always close streams to ensure all records are flushed
5. **Tune buffer size**: Adjust `max_inflight_records` based on your memory and latency needs

## Common Issues

### Module Not Found

Make sure the SDK is installed:
```bash
pip install databricks-zerobus-ingest-sdk
# or for development
pip install .
```

### Authentication Failures

- Verify your `CLIENT_ID` and `CLIENT_SECRET` are correct
- Check that your OAuth client has permissions for the target table
- Ensure the `UNITY_CATALOG_ENDPOINT` and `SERVER_ENDPOINT` are correct

### Performance Issues

- Increase `max_inflight_records` in stream configuration for more concurrent operations
- Check network connectivity to the Zerobus endpoint
- Monitor memory usage and adjust buffer settings accordingly

### Protobuf Import Errors

Make sure you've generated the Python protobuf classes:
```bash
pip install "grpcio-tools>=1.60.0,<2.0"
python -m grpc_tools.protoc --python_out=. --proto_path=. record.proto
```

And that the generated file is in the same directory or in your Python path.

## Additional Resources

- [SDK Documentation](../README.md)
- [Protocol Buffers Guide](https://developers.google.com/protocol-buffers)
- [Databricks Documentation](https://docs.databricks.com)
- [Python asyncio Documentation](https://docs.python.org/3/library/asyncio.html)
