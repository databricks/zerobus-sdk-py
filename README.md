# Databricks Zerobus Ingest SDK for Python

[![PyPI version](https://img.shields.io/pypi/v/databricks-zerobus-ingest-sdk)](https://pypi.org/project/databricks-zerobus-ingest-sdk/)
[![Python](https://img.shields.io/badge/python-3.7+-blue)](https://www.python.org/)

The Databricks Zerobus Ingest SDK for Python provides a high-performance client for ingesting data directly into Databricks Delta tables using the Zerobus streaming protocol.

## Table of Contents

- [Features](#features)
- [Requirements](#requirements)
- [Quick Start User Guide](#quick-start-user-guide)
  - [Prerequisites](#prerequisites)
  - [Installation](#installation)
  - [Define Your Protocol Buffer Schema](#define-your-protocol-buffer-schema)
  - [Write Your Client Code](#write-your-client-code)
- [Usage Examples](#usage-examples)
  - [Blocking Ingestion](#blocking-ingestion)
  - [Non-Blocking Ingestion](#non-blocking-ingestion)
- [Configuration](#configuration)
- [Error Handling](#error-handling)
- [API Reference](#api-reference)
- [Best Practices](#best-practices)
- [Disclaimer](#disclaimer)

## Features

- **High-throughput ingestion**: Optimized for high-volume data ingestion
- **Automatic recovery**: Built-in retry and recovery mechanisms
- **Flexible configuration**: Customizable stream behavior and timeouts
- **Protocol Buffers**: Strongly-typed schema using protobuf
- **OAuth 2.0 authentication**: Secure authentication with client credentials
- **Sync and Async support**: Both synchronous and asynchronous APIs

## Requirements

### Runtime Requirements

- **Python**: 3.7 or higher
- **Databricks workspace** with Zerobus access enabled

### Dependencies

- `protobuf` >= 6.31.0, < 7.0
- `grpcio` >= 1.68.0, < 2.0
- `requests` >= 2.28.1, < 3

## Quick Start User Guide

### Prerequisites

Before using the SDK, you'll need the following:

#### 1. Workspace URL and Workspace ID

After logging into your Databricks workspace, look at the browser URL:

```
https://<databricks-instance>.cloud.databricks.com/o=<workspace-id>
```

- **Workspace URL**: The part before `/o=` → `https://<databricks-instance>.cloud.databricks.com`
- **Workspace ID**: The part after `/o=` → `<workspace-id>`

Example:
- Full URL: `https://dbc-a1b2c3d4-e5f6.cloud.databricks.com/o=1234567890123456`
- Workspace URL: `https://dbc-a1b2c3d4-e5f6.cloud.databricks.com`
- Workspace ID: `1234567890123456`

#### 2. Create a Delta Table

Create a table using Databricks SQL:

```sql
CREATE TABLE <catalog_name>.default.air_quality (
    device_name STRING,
    temp INT,
    humidity BIGINT
)
USING DELTA;
```

Replace `<catalog_name>` with your catalog name (e.g., `main`).

#### 3. Create a Service Principal

1. Navigate to **Settings > Identity and Access** in your Databricks workspace
2. Click **Service principals** and create a new service principal
3. Generate a new secret for the service principal and save it securely
4. Grant the following permissions:
   - `USE_CATALOG` on the catalog (e.g., `main`)
   - `USE_SCHEMA` on the schema (e.g., `default`)
   - `MODIFY` and `SELECT` on the table (e.g., `air_quality`)

Grant permissions using SQL:

```sql
-- Grant catalog permission
GRANT USE CATALOG ON CATALOG <catalog_name> TO `<service-principal-application-id>`;

-- Grant schema permission
GRANT USE SCHEMA ON SCHEMA <catalog_name>.default TO `<service-principal-application-id>`;

-- Grant table permissions
GRANT SELECT, MODIFY ON TABLE <catalog_name>.default.air_quality TO `<service-principal-application-id>`;
```

### Installation

#### From Source

```bash
git clone https://github.com/databricks/zerobus-sdk-py.git
cd zerobus-sdk-py
pip install -e .
```

### Define Your Protocol Buffer Schema

Create a file named `record.proto`:

```protobuf
syntax = "proto2";

message AirQuality {
    optional string device_name = 1;
    optional int32 temp = 2;
    optional int64 humidity = 3;
}
```

Compile the protobuf:

```bash
pip install "grpcio-tools>=1.60.0,<2.0"
python -m grpc_tools.protoc --python_out=. --proto_path=. record.proto
```

This generates a `record_pb2.py` file compatible with protobuf 6.x.

### Write Your Client Code

#### Synchronous Example

```python
from zerobus.sdk.sync import ZerobusSdk
from zerobus.sdk.shared import TableProperties
import record_pb2

# Configuration
server_endpoint = "1234567890123456.zerobus.us-west-2.cloud.databricks.com"
workspace_url = "https://dbc-a1b2c3d4-e5f6.cloud.databricks.com"
table_name = "main.default.air_quality"
client_id = "your-service-principal-application-id"
client_secret = "your-service-principal-secret"

# Initialize SDK
sdk = ZerobusSdk(server_endpoint, workspace_url)

# Configure table properties
table_properties = TableProperties(
    table_name,
    record_pb2.AirQuality.DESCRIPTOR
)

# Create stream
stream = sdk.create_stream(
    client_id,
    client_secret,
    table_properties
)

try:
    # Ingest records
    for i in range(100):
        record = record_pb2.AirQuality(
            device_name=f"sensor-{i % 10}",
            temp=20 + (i % 15),
            humidity=50 + (i % 40)
        )

        ack = stream.ingest_record(record)
        ack.wait_for_ack()  # Wait for durability

        print(f"Ingested record {i + 1}")

    print("Successfully ingested 100 records!")
finally:
    stream.close()
```

#### Asynchronous Example

```python
import asyncio
from zerobus.sdk.aio import ZerobusSdk
from zerobus.sdk.shared import TableProperties
import record_pb2

async def main():
    # Configuration
    server_endpoint = "1234567890123456.zerobus.us-west-2.cloud.databricks.com"
    workspace_url = "https://dbc-a1b2c3d4-e5f6.cloud.databricks.com"
    table_name = "main.default.air_quality"
    client_id = "your-service-principal-application-id"
    client_secret = "your-service-principal-secret"

    # Initialize SDK
    sdk = ZerobusSdk(server_endpoint, workspace_url)

    # Configure table properties
    table_properties = TableProperties(
        table_name,
        record_pb2.AirQuality.DESCRIPTOR
    )

    # Create stream
    stream = await sdk.create_stream(
        client_id,
        client_secret,
        table_properties
    )

    try:
        # Ingest records
        for i in range(100):
            record = record_pb2.AirQuality(
                device_name=f"sensor-{i % 10}",
                temp=20 + (i % 15),
                humidity=50 + (i % 40)
            )

            future = await stream.ingest_record(record)
            await future  # Wait for durability

            print(f"Ingested record {i + 1}")

        print("Successfully ingested 100 records!")
    finally:
        await stream.close()

asyncio.run(main())
```

## Usage Examples

See the `examples/` directory for complete working examples:

- **sync_example.py** - Synchronous ingestion with progress tracking
- **async_example.py** - High-throughput asynchronous ingestion

### Blocking Ingestion

Ingest records synchronously, waiting for each record to be acknowledged:

```python
from zerobus.sdk.sync import ZerobusSdk
from zerobus.sdk.shared import TableProperties
import record_pb2

sdk = ZerobusSdk(server_endpoint, workspace_url)
table_properties = TableProperties(table_name, record_pb2.AirQuality.DESCRIPTOR)

stream = sdk.create_stream(client_id, client_secret, table_properties)

try:
    for i in range(1000):
        record = record_pb2.AirQuality(
            device_name=f"sensor-{i}",
            temp=20 + i % 15,
            humidity=50 + i % 40
        )

        ack = stream.ingest_record(record)
        ack.wait_for_ack()  # Wait for durability
finally:
    stream.close()
```

### Non-Blocking Ingestion

Ingest records asynchronously for maximum throughput:

```python
import asyncio
from zerobus.sdk.aio import ZerobusSdk
from zerobus.sdk.shared import TableProperties, StreamConfigurationOptions
import record_pb2

async def main():
    options = StreamConfigurationOptions(
        max_inflight_records=50000,
        ack_callback=lambda response: print(
            f"Acknowledged offset: {response.durability_ack_up_to_offset}"
        )
    )

    sdk = ZerobusSdk(server_endpoint, workspace_url)
    table_properties = TableProperties(table_name, record_pb2.AirQuality.DESCRIPTOR)

    stream = await sdk.create_stream(
        client_id,
        client_secret,
        table_properties,
        options
    )

    futures = []

    try:
        for i in range(100000):
            record = record_pb2.AirQuality(
                device_name=f"sensor-{i % 10}",
                temp=20 + i % 15,
                humidity=50 + i % 40
            )

            future = await stream.ingest_record(record)
            futures.append(future)

        # Flush and wait for all records
        await stream.flush()
        await asyncio.gather(*futures)
    finally:
        await stream.close()

asyncio.run(main())
```

## Configuration

### Stream Configuration Options

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

### Example Configuration

```python
from zerobus.sdk.shared import StreamConfigurationOptions

options = StreamConfigurationOptions(
    max_inflight_records=10000,
    recovery=True,
    recovery_timeout_ms=20000,
    ack_callback=lambda response: print(
        f"Ack: {response.durability_ack_up_to_offset}"
    )
)

stream = sdk.create_stream(
    client_id,
    client_secret,
    table_properties,
    options
)
```

## Error Handling

The SDK raises two types of exceptions:

- `ZerobusException`: Retriable errors (e.g., network issues, temporary server errors)
- `NonRetriableException`: Non-retriable errors (e.g., invalid credentials, missing table)

```python
from zerobus.sdk.shared import ZerobusException, NonRetriableException

try:
    stream.ingest_record(record)
except NonRetriableException as e:
    # Fatal error - do not retry
    print(f"Non-retriable error: {e}")
    raise
except ZerobusException as e:
    # Retriable error - can retry with backoff
    print(f"Retriable error: {e}")
    # Implement retry logic
```

## API Reference

### ZerobusSdk

Main entry point for the SDK.

**Synchronous API:**
```python
from zerobus.sdk.sync import ZerobusSdk

sdk = ZerobusSdk(server_endpoint, unity_catalog_endpoint)
```

**Constructor Parameters:**
- `server_endpoint` (str) - The Zerobus gRPC endpoint (e.g., `<workspace-id>.zerobus.region.cloud.databricks.com`)
- `unity_catalog_endpoint` (str) - The Unity Catalog endpoint (your workspace URL)

**Methods:**

```python
def create_stream(
    client_id: str,
    client_secret: str,
    table_properties: TableProperties,
    options: StreamConfigurationOptions = None
) -> ZerobusStream
```
Creates a new ingestion stream. Returns a `ZerobusStream` instance.

---

**Asynchronous API:**
```python
from zerobus.sdk.aio import ZerobusSdk

sdk = ZerobusSdk(server_endpoint, unity_catalog_endpoint)
```

**Methods:**

```python
async def create_stream(
    client_id: str,
    client_secret: str,
    table_properties: TableProperties,
    options: StreamConfigurationOptions = None
) -> ZerobusStream
```
Creates a new ingestion stream. Returns a `ZerobusStream` instance.

---

### ZerobusStream

Represents an active ingestion stream.

**Synchronous Methods:**

```python
def ingest_record(record: Message) -> RecordAcknowledgment
```
Ingests a single record into the stream. Returns a `RecordAcknowledgment` for tracking.

```python
def flush() -> None
```
Flushes all pending records and waits for server acknowledgment. Does not close the stream.

```python
def close() -> None
```
Flushes and closes the stream gracefully. Always call in a `finally` block.

```python
def get_state() -> StreamState
```
Returns the current stream state.

```python
@property
def stream_id() -> str
```
Returns the unique stream ID assigned by the server.

---

**Asynchronous Methods:**

```python
async def ingest_record(record: Message) -> Awaitable
```
Ingests a single record into the stream. Returns an awaitable that completes when the record is durably written.

```python
async def flush() -> None
```
Flushes all pending records and waits for server acknowledgment. Does not close the stream.

```python
async def close() -> None
```
Flushes and closes the stream gracefully. Always call in a `finally` block.

```python
def get_state() -> StreamState
```
Returns the current stream state.

```python
@property
def stream_id() -> str
```
Returns the unique stream ID assigned by the server.

---

### TableProperties

Configuration for the target table.

**Constructor:**
```python
TableProperties(table_name: str, descriptor: Descriptor)
```

**Parameters:**
- `table_name` (str) - Fully qualified table name (e.g., `catalog.schema.table`)
- `descriptor` (Descriptor) - Protobuf message descriptor (e.g., `MyMessage.DESCRIPTOR`)

**Properties:**

```python
@property
def table_name() -> str
```
Returns the table name.

```python
@property
def descriptor() -> Descriptor
```
Returns the protobuf message descriptor.

---

### StreamConfigurationOptions

Configuration options for stream behavior.

**Constructor:**
```python
StreamConfigurationOptions(
    max_inflight_records: int = 50000,
    recovery: bool = True,
    recovery_timeout_ms: int = 15000,
    recovery_backoff_ms: int = 2000,
    recovery_retries: int = 3,
    flush_timeout_ms: int = 300000,
    server_lack_of_ack_timeout_ms: int = 60000,
    ack_callback: Callable = None
)
```

**Parameters:**
- `max_inflight_records` (int) - Maximum number of unacknowledged records (default: 50000)
- `recovery` (bool) - Enable or disable automatic stream recovery (default: True)
- `recovery_timeout_ms` (int) - Recovery operation timeout in milliseconds (default: 15000)
- `recovery_backoff_ms` (int) - Delay between recovery attempts in milliseconds (default: 2000)
- `recovery_retries` (int) - Maximum number of recovery attempts (default: 3)
- `flush_timeout_ms` (int) - Flush operation timeout in milliseconds (default: 300000)
- `server_lack_of_ack_timeout_ms` (int) - Server acknowledgment timeout in milliseconds (default: 60000)
- `ack_callback` (Callable) - Callback to be invoked when records are acknowledged by the server (default: None)

---

### RecordAcknowledgment (Sync API only)

Future-like object for waiting on acknowledgments.

**Methods:**

```python
def wait_for_ack(timeout_sec: float = None) -> None
```
Blocks until the record is acknowledged or timeout is reached.

```python
def add_done_callback(callback: Callable) -> None
```
Adds a callback to be invoked when the record is acknowledged.

```python
def is_done() -> bool
```
Returns True if the record has been acknowledged.

---

### StreamState (Enum)

Represents the lifecycle state of a stream.

**Values:**
- `UNINITIALIZED` - Stream created but not yet initialized
- `OPENED` - Stream is open and accepting records
- `FLUSHING` - Stream is flushing pending records
- `RECOVERING` - Stream is recovering from a failure
- `CLOSED` - Stream has been gracefully closed
- `FAILED` - Stream has failed and cannot be recovered

---

### ZerobusException

Base exception for retriable errors.

**Constructor:**
```python
ZerobusException(message: str, cause: Exception = None)
```

---

### NonRetriableException

Exception for non-retriable errors (extends `ZerobusException`).

**Constructor:**
```python
NonRetriableException(message: str, cause: Exception = None)
```

## Best Practices

1. **Reuse SDK instances**: Create one `ZerobusSdk` instance per application
2. **Stream lifecycle**: Always close streams in a `finally` block to ensure all records are flushed
3. **Batch size**: Adjust `max_inflight_records` based on your throughput requirements
4. **Error handling**: Implement proper retry logic for retriable errors
5. **Monitoring**: Use `ack_callback` to track ingestion progress
6. **Choose the right API**: Use sync API for low-volume, async API for high-volume ingestion
7. **Token refresh**: Tokens are automatically refreshed on stream creation and recovery

## Disclaimer

[Public Preview](https://docs.databricks.com/release-notes/release-types.html): This SDK is supported for production use cases and is available to all customers. Databricks is actively working on stabilizing the Zerobus Ingest SDK for Python. Minor version updates may include backwards-incompatible changes.

We are keen to hear feedback from you on this SDK. Please [file issues](https://github.com/databricks/zerobus-sdk-py/issues), and we will address them.
