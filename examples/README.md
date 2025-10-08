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

**Run:**
```bash
python examples/sync_example.py
```

### 2. Asynchronous Ingestion (`async_example.py`)

Demonstrates asynchronous record ingestion using Python's asyncio.

**Best for:**
- Applications already using asyncio
- Concurrent operations with other async tasks
- Async web frameworks (FastAPI, aiohttp, etc.)
- Event-driven architectures

**Key features:**
- Asynchronous ingestion with asyncio
- Non-blocking API
- Ack callback for progress tracking
- Integrates with async event loops
- Concurrent execution with other async operations

**Run:**
```bash
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

Define your schema in a `.proto` file (see `record.proto` for an example):

```proto
syntax = "proto2";

message AirQuality {
    optional string device_name = 1;
    optional int32 temp = 2;
    optional int64 humidity = 3;
}
```

Generate Python classes:
```bash
pip install protobuf
protoc --python_out=. record.proto
```

This will create a `record_pb2.py` file that you can import in your code.

### 3. Configure Credentials

Set environment variables:
```bash
export DATABRICKS_CLIENT_ID="your-oauth-client-id"
export DATABRICKS_CLIENT_SECRET="your-oauth-client-secret"
```

Or update the values directly in the example files:
```python
CLIENT_ID = "your-oauth-client-id"
CLIENT_SECRET = "your-oauth-client-secret"
SERVER_ENDPOINT = "your-shard-id.zerobus.region.cloud.databricks.com"
UNITY_CATALOG_ENDPOINT = "https://your-workspace.cloud.databricks.com"
TABLE_NAME = "catalog.schema.table"
```

### 4. Update the Examples

Uncomment the relevant code sections in the examples and update them to use your protobuf schema:

```python
# Import your generated protobuf module
import record_pb2

# Create table properties with your descriptor
table_properties = TableProperties(
    TABLE_NAME,
    record_pb2.AirQuality.DESCRIPTOR
)

# Create and ingest records
record = record_pb2.AirQuality(
    device_name="sensor-1",
    temp=25,
    humidity=60
)
```

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
protoc --python_out=. record.proto
```

And that the generated file is in the same directory or in your Python path.

## Additional Resources

- [SDK Documentation](../README.md)
- [Protocol Buffers Guide](https://developers.google.com/protocol-buffers)
- [Databricks Documentation](https://docs.databricks.com)
- [Python asyncio Documentation](https://docs.python.org/3/library/asyncio.html)
