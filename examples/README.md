# Zerobus SDK Examples

This directory contains runnable example applications demonstrating both synchronous and asynchronous usage of the Zerobus Ingest SDK for Python.

For complete SDK documentation including installation, API reference, and configuration details, see the [main README](../README.md).

## Running the Examples

### 1. Clone or Check Out the Repository

```bash
git clone https://github.com/databricks/zerobus-sdk-py.git
cd zerobus-sdk-py
```

### 2. Install Dependencies

```bash
pip install -e .
```

The examples use a pre-generated protobuf file (`record_pb2.py`) based on the included `record.proto` schema.

### 3. Configure Credentials

Set the following environment variables:

```bash
export DATABRICKS_CLIENT_ID="your-service-principal-application-id"
export DATABRICKS_CLIENT_SECRET="your-service-principal-secret"
# For AWS:
export ZEROBUS_SERVER_ENDPOINT="workspace-id.zerobus.region.cloud.databricks.com"
export DATABRICKS_WORKSPACE_URL="https://your-workspace.cloud.databricks.com"
# For Azure:
# export ZEROBUS_SERVER_ENDPOINT="workspace-id.zerobus.region.azuredatabricks.net"
# export DATABRICKS_WORKSPACE_URL="https://your-workspace.azuredatabricks.net"
export ZEROBUS_TABLE_NAME="catalog.schema.table"
```

### 4. Run an Example

```bash
# Synchronous example
python examples/sync_example.py

# Asynchronous example
python examples/async_example.py
```

## Examples Overview

### Synchronous Example (`sync_example.py`)

Uses the synchronous API (`zerobus.sdk.sync`). Suitable for:
- Simple scripts and applications
- Code that doesn't use asyncio
- Straightforward blocking I/O patterns

**Key characteristics:**
- Uses standard Python synchronous functions
- Blocking API calls
- Works in any Python environment

### Asynchronous Example (`async_example.py`)

Uses the asynchronous API (`zerobus.sdk.aio`). Suitable for:
- Applications already using asyncio
- Async web frameworks (FastAPI, aiohttp, etc.)
- Event-driven architectures
- Integration with other async operations

**Key characteristics:**
- Uses Python's `async`/`await` syntax
- Non-blocking API calls
- Requires an asyncio event loop

## API Differences

Both APIs provide the same functionality and performance. The key differences are:

| Aspect | Synchronous (`sync`) | Asynchronous (`aio`) |
|--------|---------------------|----------------------|
| Import | `from zerobus.sdk.sync import ZerobusSdk` | `from zerobus.sdk.aio import ZerobusSdk` |
| Stream creation | `stream = sdk.create_stream(...)` | `stream = await sdk.create_stream(...)` |
| Record ingestion | `ack = stream.ingest_record(record)` | `ack = await stream.ingest_record(record)` |
| Flush | `stream.flush()` | `await stream.flush()` |
| Close | `stream.close()` | `await stream.close()` |
| Execution context | Standard Python | Requires asyncio event loop |
| Use case | General Python applications | Asyncio-based applications |

**Performance:** Both APIs offer equivalent throughput and durability. Choose based on your application's architecture, not performance needs.

## Authentication

Both examples demonstrate:
- OAuth 2.0 authentication (default, using `create_stream()`)
- Custom headers provider (advanced, using `create_stream_with_headers_provider()`)

See the inline comments in each example file for details.

## Using Your Own Schema

To use your own protobuf schema:

1. Modify `record.proto` or create a new proto file
2. Generate Python code:
   ```bash
   python -m grpc_tools.protoc --python_out=. --proto_path=. your_schema.proto
   ```
3. Update the example code to import and use your generated protobuf classes

## Additional Resources

- [Main README](../README.md) - Complete SDK documentation
- [API Reference](../README.md#api-reference) - Detailed API documentation
- [Best Practices](../README.md#best-practices) - Recommendations for production use
