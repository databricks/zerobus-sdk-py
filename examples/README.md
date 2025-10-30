# Zerobus SDK Examples

This directory contains runnable example applications demonstrating both synchronous and asynchronous usage of the Zerobus Ingest SDK for Python, with examples for all three record type modes: **implicit protobuf** (default), **explicit protobuf**, and **explicit JSON**.

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
# Synchronous examples (blocking I/O)
python examples/sync_example_proto_implicit.py     # Default: implicit protobuf
python examples/sync_example_proto_explicit.py     # Explicit protobuf serialization
python examples/sync_example_json.py               # JSON mode

# Asynchronous examples (non-blocking I/O)
python examples/async_example_proto_implicit.py    # Default: implicit protobuf
python examples/async_example_proto_explicit.py    # Explicit protobuf serialization
python examples/async_example_json.py              # JSON mode
```

## Examples Overview

### Record Type Modes

The SDK supports three ways to serialize records:

#### 1. Implicit Protobuf (Default, Recommended)
**Files:** `sync_example_proto_implicit.py`, `async_example_proto_implicit.py`

- Pass protobuf objects directly to the SDK
- SDK handles serialization automatically
- Simplest approach for protobuf schemas
- **Best for:** Most use cases with protobuf schemas

```python
# Create protobuf object
record = record_pb2.AirQuality(device_name="sensor-1", temp=25, humidity=60)

# SDK serializes automatically
ack = stream.ingest_record(record)
```

#### 2. Explicit Protobuf
**Files:** `sync_example_proto_explicit.py`, `async_example_proto_explicit.py`

- Manually serialize protobuf objects to bytes before passing to SDK
- Gives you control over the serialization process
- **Best for:** Custom serialization logic or performance optimization

```python
# Create and serialize protobuf object
record = record_pb2.AirQuality(device_name="sensor-1", temp=25, humidity=60)
serialized = record.SerializeToString()

# Pass serialized bytes to SDK
table_properties = TableProperties(TABLE_NAME, record_pb2.AirQuality.DESCRIPTOR, record_type=RecordType.PROTOBUF)
ack = stream.ingest_record(serialized)
```

#### 3. Explicit JSON
**Files:** `sync_example_json.py`, `async_example_json.py`

- Send records as JSON-encoded strings
- No protobuf schema required
- **Best for:** Dynamic schemas, JSON data pipelines, or integration with JSON-based systems

```python
# Create JSON string
json_record = json.dumps({"device_name": "sensor-1", "temp": 25, "humidity": 60})

# Configure for JSON mode
table_properties = TableProperties(TABLE_NAME, record_type=RecordType.JSON)
ack = stream.ingest_record(json_record)
```

### Synchronous vs Asynchronous APIs

All record type modes are available in both synchronous and asynchronous variants:

#### Synchronous API (`zerobus.sdk.sync`)
Suitable for:
- Simple scripts and applications
- Code that doesn't use asyncio
- Straightforward blocking I/O patterns

**Key characteristics:**
- Uses standard Python synchronous functions
- Blocking API calls
- Works in any Python environment

#### Asynchronous API (`zerobus.sdk.aio`)
Suitable for:
- Applications already using asyncio
- Async web frameworks (FastAPI, aiohttp, etc.)
- Event-driven architectures
- Integration with other async operations

**Key characteristics:**
- Uses Python's `async`/`await` syntax
- Non-blocking API calls
- Requires an asyncio event loop

## Quick Reference

### API Comparison: Sync vs Async

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

### Record Type Mode Comparison

| Mode | Record Input | Table Properties Configuration | When to Use |
|------|-------------|-------------------------------|-------------|
| **Implicit Protobuf** (Default) | Protobuf object | `TableProperties(table_name, descriptor)` | Most protobuf use cases (recommended) |
| **Explicit Protobuf** | Serialized bytes | `TableProperties(table_name, descriptor, record_type=RecordType.PROTOBUF)` | Custom serialization control |
| **Explicit JSON** | JSON string | `TableProperties(table_name, record_type=RecordType.JSON)` | Dynamic schemas, JSON pipelines |

## Authentication

Both examples demonstrate:
- OAuth 2.0 authentication (default, using `create_stream()`)
- Custom headers provider (advanced, using `create_stream_with_headers_provider()`)

See the inline comments in each example file for details.

## Using Your Own Schema

### For Protobuf Schemas

To use your own protobuf schema:

1. Modify `record.proto` or create a new proto file
2. Generate Python code:
   ```bash
   python -m grpc_tools.protoc --python_out=. --proto_path=. your_schema.proto
   ```
3. Update the example code to import and use your generated protobuf classes

### For JSON Mode

To use your own JSON structure:

1. Define your JSON structure in code:
   ```python
   json_record = json.dumps({"field1": "value1", "field2": 123})
   ```
2. Configure TableProperties with `record_type=RecordType.JSON`
3. Ensure your JSON structure matches the schema of your Databricks table

Note: The SDK sends JSON strings directly without client-side schema validation.

## Additional Resources

- [Main README](../README.md) - Complete SDK documentation
- [API Reference](../README.md#api-reference) - Detailed API documentation
- [Best Practices](../README.md#best-practices) - Recommendations for production use
