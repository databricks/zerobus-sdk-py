# Databricks Zerobus Ingest SDK for Python

[![PyPI - Downloads](https://img.shields.io/pypi/dw/databricks-zerobus-ingest-sdk)](https://pypistats.org/packages/databricks-zerobus-ingest-sdk)
[![PyPI - License](https://img.shields.io/pypi/l/databricks-zerobus-ingest-sdk)](https://github.com/databricks/zerobus-sdk-py/blob/main/LICENSE)
![PyPI](https://img.shields.io/pypi/v/databricks-zerobus-ingest-sdk)

[Public Preview](https://docs.databricks.com/release-notes/release-types.html): This SDK is supported for production use cases and is available to all customers. Databricks is actively working on stabilizing the Zerobus Ingest SDK for Python. Minor version updates may include backwards-incompatible changes.

We are keen to hear feedback from you on this SDK. Please [file issues](https://github.com/databricks/zerobus-sdk-py/issues), and we will address them.

The Databricks Zerobus Ingest SDK for Python provides a high-performance, Rust-backed client for ingesting data directly into Databricks Delta tables using the Zerobus streaming protocol. Built on top of the battle-tested [Rust SDK](https://github.com/databricks/zerobus-sdk-rs) using PyO3 bindings, it delivers native performance with a Python-friendly API. | See also the [SDK for Java](https://github.com/databricks/zerobus-sdk-java)

## Table of Contents

- [Disclaimer](#disclaimer)
- [Features](#features)
- [Requirements](#requirements)
- [Quick Start User Guide](#quick-start-user-guide)
  - [Prerequisites](#prerequisites)
  - [Installation](#installation)
  - [Choose Your Serialization Format](#choose-your-serialization-format)
  - [Option 1: Using JSON (Simplest)](#option-1-using-json-simplest)
  - [Option 2: Using Protocol Buffers](#option-2-using-protocol-buffers)
- [Usage Examples](#usage-examples)
  - [JSON Examples](#json-examples)
  - [Protocol Buffer Examples](#protocol-buffer-examples)
- [Authentication](#authentication)
- [Configuration](#configuration)
- [Error Handling](#error-handling)
- [API Reference](#api-reference)
- [Best Practices](#best-practices)

## Features

- **Rust-backed performance**: Native Rust implementation with Python bindings for maximum throughput and minimal latency
- **High-throughput ingestion**: Optimized for high-volume data ingestion with native async/await support
- **Automatic recovery**: Built-in retry and recovery mechanisms from the Rust SDK
- **Flexible configuration**: Customizable stream behavior and timeouts
- **Multiple serialization formats**: Support for JSON and Protocol Buffers
- **OAuth 2.0 authentication**: Secure authentication with client credentials
- **Type safety**: Rust's type system ensures reliability and correctness
- **Sync and Async support**: Both synchronous and asynchronous Python APIs
- **Zero-copy operations**: Efficient data handling with minimal overhead

## Architecture

The Python SDK is a thin wrapper around the [Databricks Zerobus Rust SDK](https://github.com/databricks/zerobus-sdk-rs), built using PyO3 bindings:

```
┌─────────────────────────────────────────┐
│         Python Application Code         │
│  (Your code using the Python SDK API)  │
└─────────────────────────────────────────┘
                    │
                    ▼
┌─────────────────────────────────────────┐
│       Python SDK (Thin Wrapper)         │
│    • API compatibility layer            │
│    • Python types & error handling      │
└─────────────────────────────────────────┘
                    │
                    ▼ (PyO3 bindings)
┌─────────────────────────────────────────┐
│         Rust Core Implementation        │
│    • gRPC communication                 │
│    • OAuth 2.0 authentication           │
│    • Stream management & recovery       │
│    • Protocol encoding/decoding         │
└─────────────────────────────────────────┘
```

This architecture provides:
- **Native performance** through Rust's zero-cost abstractions
- **Memory safety** without garbage collection overhead
- **Single source of truth** for all SDK implementations
- **Python-friendly API** with full type hints and IDE support

## Requirements

### Runtime Requirements

- **Python**: 3.9 or higher
- **Databricks workspace** with Zerobus access enabled

### Dependencies

- `protobuf` >= 4.25.0, < 7.0 (for Protocol Buffer schema handling)
- `requests` >= 2.28.1, < 3 (only for the `generate_proto` utility tool)

**Note**: All core ingestion functionality (gRPC, OAuth authentication, stream management) is handled by the native Rust implementation. The `requests` dependency is only used by the optional `generate_proto.py` tool for fetching table schemas from Unity Catalog.

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

> **Note:** The examples above show AWS endpoints (`.cloud.databricks.com`). For Azure deployments, the workspace URL will be `https://<databricks-instance>.azuredatabricks.net`.

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

#### From PyPI (Recommended)

Install the latest stable version using pip:

```bash
pip install databricks-zerobus-ingest-sdk
```

Pre-built wheels are available for:
- **Linux**: x86_64, aarch64 (manylinux)
- **macOS**: x86_64, arm64 (universal2)
- **Windows**: x86_64

#### From Source

Building from source requires the **Rust toolchain** (install from [rustup.rs](https://rustup.rs/)).

```bash
# Install Rust (if not already installed)
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# Clone and install
git clone https://github.com/databricks/zerobus-sdk-py.git
cd zerobus-sdk-py
pip install -e .
```

The SDK uses [maturin](https://github.com/PyO3/maturin) to build Python bindings for the Rust implementation. Installation via `pip install -e .` automatically:
1. Installs maturin if needed
2. Compiles the Rust extension
3. Installs the package in editable mode

**For active development**, see [CONTRIBUTING.md](CONTRIBUTING.md) for detailed build instructions and development workflows.

### Choose Your Serialization Format

The SDK supports two serialization formats:

1. **JSON** - Simple, no schema compilation needed. Good for getting started.
2. **Protocol Buffers (Default to maintain backwards compatibility)** - Strongly-typed schemas. More efficient over the wire.

### Option 1: Using JSON

#### Write Your Client Code (JSON)

**Synchronous Example:**

```python
import json
import logging
from zerobus.sdk.sync import ZerobusSdk
from zerobus.sdk.shared import RecordType, StreamConfigurationOptions, TableProperties

# Configure logging (optional but recommended)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

# Configuration
# For AWS:
server_endpoint = "1234567890123456.zerobus.us-west-2.cloud.databricks.com"
workspace_url = "https://dbc-a1b2c3d4-e5f6.cloud.databricks.com"
# For Azure:
# server_endpoint = "1234567890123456.zerobus.us-west-2.azuredatabricks.net"
# workspace_url = "https://dbc-a1b2c3d4-e5f6.azuredatabricks.net"

table_name = "main.default.air_quality"
client_id = "your-service-principal-application-id"
client_secret = "your-service-principal-secret"

# Initialize SDK
sdk = ZerobusSdk(server_endpoint, workspace_url)

# Configure table properties
table_properties = TableProperties(table_name)

# Configure stream with JSON record type
options = StreamConfigurationOptions(record_type=RecordType.JSON)

# Create stream
stream = sdk.create_stream(client_id, client_secret, table_properties, options)

try:
    # Ingest records
    for i in range(100):
        # Option 1: Pass a dict (SDK serializes to JSON)
        record_dict = {
            "device_name": f"sensor-{i % 10}",
            "temp": 20 + (i % 15),
            "humidity": 50 + (i % 40)
        }
        ack = stream.ingest_record(record_dict)

        # Option 2: Pass a pre-serialized JSON string (client controls serialization)
        # json_string = json.dumps(record_dict)
        # ack = stream.ingest_record(json_string)

        # Optional: Wait for durability confirmation
        ack.wait_for_ack()

        print(f"Ingested record {i + 1}")

    print("Successfully ingested 100 records!")
finally:
    stream.close()
```

**Asynchronous Example:**

```python
import asyncio
import json
import logging
from zerobus.sdk.aio import ZerobusSdk
from zerobus.sdk.shared import RecordType, StreamConfigurationOptions, TableProperties

# Configure logging (optional but recommended)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

async def main():
    # Configuration
    # For AWS:
    server_endpoint = "1234567890123456.zerobus.us-west-2.cloud.databricks.com"
    workspace_url = "https://dbc-a1b2c3d4-e5f6.cloud.databricks.com"
    # For Azure:
    # server_endpoint = "1234567890123456.zerobus.us-west-2.azuredatabricks.net"
    # workspace_url = "https://dbc-a1b2c3d4-e5f6.azuredatabricks.net"

    table_name = "main.default.air_quality"
    client_id = "your-service-principal-application-id"
    client_secret = "your-service-principal-secret"

    # Initialize SDK
    sdk = ZerobusSdk(server_endpoint, workspace_url)

    # Configure table properties
    table_properties = TableProperties(table_name)

    # Configure stream with JSON record type
    options = StreamConfigurationOptions(record_type=RecordType.JSON)

    # Create stream
    stream = await sdk.create_stream(client_id, client_secret, table_properties, options)

    try:
        # Ingest records
        for i in range(100):
            # Option 1: Pass a dict (SDK serializes to JSON)
            record_dict = {
                "device_name": f"sensor-{i % 10}",
                "temp": 20 + (i % 15),
                "humidity": 50 + (i % 40)
            }
            future = await stream.ingest_record(record_dict)

            # Option 2: Pass a pre-serialized JSON string (client controls serialization)
            # json_string = json.dumps(record_dict)
            # future = await stream.ingest_record(json_string)

            # Optional: Wait for durability confirmation
            await future

            print(f"Ingested record {i + 1}")

        print("Successfully ingested 100 records!")
    finally:
        await stream.close()

asyncio.run(main())
```

### Option 2: Using Protocol Buffers

You'll need to define and compile a protobuf schema.

#### Define Your Protocol Buffer Schema

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

#### Generate Protocol Buffer Schema from Unity Catalog (Alternative)

Instead of manually writing your protobuf schema, you can automatically generate it from an existing Unity Catalog table using the included `generate_proto.py` tool.

**Basic Usage:**

```bash
python -m zerobus.tools.generate_proto \
    --uc-endpoint "https://dbc-a1b2c3d4-e5f6.cloud.databricks.com" \
    --client-id "your-service-principal-application-id" \
    --client-secret "your-service-principal-secret" \
    --table "main.default.air_quality" \
    --output "record.proto" \
    --proto-msg "AirQuality"
```

**Parameters:**
- `--uc-endpoint`: Your workspace URL (required)
- `--client-id`: Service principal application ID (required)
- `--client-secret`: Service principal secret (required)
- `--table`: Fully qualified table name in format catalog.schema.table (required)
- `--output`: Output path for the generated proto file (required)
- `--proto-msg`: Name of the protobuf message (optional, defaults to table name)

After generating, compile it as shown above.

**Type Mappings:**

| Delta Type | Proto2 Type |
|-----------|-------------|
| TINYINT, BYTE, INT, SMALLINT, SHORT | int32 |
| BIGINT, LONG | int64 |
| FLOAT | float |
| DOUBLE | double |
| STRING, VARCHAR | string |
| BOOLEAN | bool |
| BINARY | bytes |
| DATE | int32 |
| TIMESTAMP | int64 |
| TIMESTAMP_NTZ | int64 |
| ARRAY\<type\> | repeated type |
| MAP\<key, value\> | map\<key, value\> |
| STRUCT\<fields\> | nested message |
| VARIANT | string (unshredded, JSON string) |

#### Write Your Client Code (Protocol Buffers)

**Synchronous Example:**

```python
import logging
from zerobus.sdk.sync import ZerobusSdk
from zerobus.sdk.shared import TableProperties
import record_pb2

# Configure logging (optional but recommended)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

# Configuration
# For AWS:
server_endpoint = "1234567890123456.zerobus.us-west-2.cloud.databricks.com"
workspace_url = "https://dbc-a1b2c3d4-e5f6.cloud.databricks.com"
# For Azure:
# server_endpoint = "1234567890123456.zerobus.us-west-2.azuredatabricks.net"
# workspace_url = "https://dbc-a1b2c3d4-e5f6.azuredatabricks.net"

table_name = "main.default.air_quality"
client_id = "your-service-principal-application-id"
client_secret = "your-service-principal-secret"

# Initialize SDK
sdk = ZerobusSdk(server_endpoint, workspace_url)

# Configure table properties with protobuf descriptor
table_properties = TableProperties(table_name, record_pb2.AirQuality.DESCRIPTOR)

# Create stream
stream = sdk.create_stream(client_id, client_secret, table_properties)

try:
    # Ingest records
    for i in range(100):
        # Option 1: Pass a Message object (SDK serializes to bytes)
        record = record_pb2.AirQuality(
            device_name=f"sensor-{i % 10}",
            temp=20 + (i % 15),
            humidity=50 + (i % 40)
        )
        ack = stream.ingest_record(record)

        # Option 2: Pass pre-serialized bytes (client controls serialization)
        # serialized_bytes = record.SerializeToString()
        # ack = stream.ingest_record(serialized_bytes)

        # Optional: Wait for durability confirmation
        ack.wait_for_ack()

        print(f"Ingested record {i + 1}")

    print("Successfully ingested 100 records!")
finally:
    stream.close()
```

**Asynchronous Example:**

```python
import asyncio
import logging
from zerobus.sdk.aio import ZerobusSdk
from zerobus.sdk.shared import TableProperties
import record_pb2

# Configure logging (optional but recommended)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

async def main():
    # Configuration
    # For AWS:
    server_endpoint = "1234567890123456.zerobus.us-west-2.cloud.databricks.com"
    workspace_url = "https://dbc-a1b2c3d4-e5f6.cloud.databricks.com"
    # For Azure:
    # server_endpoint = "1234567890123456.zerobus.us-west-2.azuredatabricks.net"
    # workspace_url = "https://dbc-a1b2c3d4-e5f6.azuredatabricks.net"

    table_name = "main.default.air_quality"
    client_id = "your-service-principal-application-id"
    client_secret = "your-service-principal-secret"

    # Initialize SDK
    sdk = ZerobusSdk(server_endpoint, workspace_url)

    # Configure table properties with protobuf descriptor
    table_properties = TableProperties(table_name, record_pb2.AirQuality.DESCRIPTOR)

    # Create stream
    stream = await sdk.create_stream(client_id, client_secret, table_properties)

    try:
        # Ingest records
        for i in range(100):
            # Option 1: Pass a Message object (SDK serializes to bytes)
            record = record_pb2.AirQuality(
                device_name=f"sensor-{i % 10}",
                temp=20 + (i % 15),
                humidity=50 + (i % 40)
            )
            future = await stream.ingest_record(record)

            # Option 2: Pass pre-serialized bytes (client controls serialization)
            # serialized_bytes = record.SerializeToString()
            # future = await stream.ingest_record(serialized_bytes)

            # Optional: Wait for durability confirmation
            await future

            print(f"Ingested record {i + 1}")

        print("Successfully ingested 100 records!")
    finally:
        await stream.close()

asyncio.run(main())
```

## Usage Examples

See the `examples/` directory for complete, runnable examples in both JSON and protobuf formats (sync and async variants). See [examples/README.md](examples/README.md) for detailed instructions.

### JSON Examples

#### Blocking Ingestion (JSON)

```python
import json
import logging
from zerobus.sdk.sync import ZerobusSdk
from zerobus.sdk.shared import RecordType, StreamConfigurationOptions, TableProperties

logging.basicConfig(level=logging.INFO)

sdk = ZerobusSdk(server_endpoint, workspace_url)
table_properties = TableProperties(table_name)
options = StreamConfigurationOptions(record_type=RecordType.JSON)
stream = sdk.create_stream(client_id, client_secret, table_properties, options)

try:
    for i in range(1000):
        # Pass a dict (SDK serializes) or a pre-serialized JSON string
        record_dict = {
            "device_name": f"sensor-{i}",
            "temp": 20 + i % 15,
            "humidity": 50 + i % 40
        }
        ack = stream.ingest_record(record_dict)

        # Optional: Wait for durability confirmation
        ack.wait_for_ack()
finally:
    stream.close()
```

#### Non-Blocking Ingestion (JSON)

```python
import asyncio
import json
import logging
from zerobus.sdk.aio import ZerobusSdk
from zerobus.sdk.shared import RecordType, StreamConfigurationOptions, TableProperties

logging.basicConfig(level=logging.INFO)

async def main():
    options = StreamConfigurationOptions(
        record_type=RecordType.JSON,
        max_inflight_records=50000,
        ack_callback=lambda response: print(
            f"Acknowledged offset: {response.durability_ack_up_to_offset}"
        )
    )

    sdk = ZerobusSdk(server_endpoint, workspace_url)
    table_properties = TableProperties(table_name)
    stream = await sdk.create_stream(client_id, client_secret, table_properties, options)

    futures = []
    try:
        for i in range(100000):
            # Pass a dict (SDK serializes) or a pre-serialized JSON string
            record_dict = {
                "device_name": f"sensor-{i % 10}",
                "temp": 20 + i % 15,
                "humidity": 50 + i % 40
            }
            future = await stream.ingest_record(record_dict)
            futures.append(future)

        await stream.flush()
        await asyncio.gather(*futures)
    finally:
        await stream.close()

asyncio.run(main())
```

### Protocol Buffer Examples

#### Blocking Ingestion (Protobuf)

```python
import logging
from zerobus.sdk.sync import ZerobusSdk
from zerobus.sdk.shared import TableProperties
import record_pb2

logging.basicConfig(level=logging.INFO)

sdk = ZerobusSdk(server_endpoint, workspace_url)
table_properties = TableProperties(table_name, record_pb2.AirQuality.DESCRIPTOR)
stream = sdk.create_stream(client_id, client_secret, table_properties)

try:
    for i in range(1000):
        # Pass a Message object (SDK serializes) or pre-serialized bytes
        record = record_pb2.AirQuality(
            device_name=f"sensor-{i}",
            temp=20 + i % 15,
            humidity=50 + i % 40
        )
        ack = stream.ingest_record(record)

        # Optional: Wait for durability confirmation
        ack.wait_for_ack()
finally:
    stream.close()
```

#### Non-Blocking Ingestion (Protobuf)

```python
import asyncio
import logging
from zerobus.sdk.aio import ZerobusSdk
from zerobus.sdk.shared import TableProperties, StreamConfigurationOptions
import record_pb2

logging.basicConfig(level=logging.INFO)

async def main():
    options = StreamConfigurationOptions(
        max_inflight_records=50000,
        ack_callback=lambda response: print(
            f"Acknowledged offset: {response.durability_ack_up_to_offset}"
        )
    )

    sdk = ZerobusSdk(server_endpoint, workspace_url)
    table_properties = TableProperties(table_name, record_pb2.AirQuality.DESCRIPTOR)
    stream = await sdk.create_stream(client_id, client_secret, table_properties, options)

    futures = []
    try:
        for i in range(100000):
            # Pass a Message object (SDK serializes) or pre-serialized bytes
            record = record_pb2.AirQuality(
                device_name=f"sensor-{i % 10}",
                temp=20 + i % 15,
                humidity=50 + i % 40
            )
            future = await stream.ingest_record(record)
            futures.append(future)

        await stream.flush()
        await asyncio.gather(*futures)
    finally:
        await stream.close()

asyncio.run(main())
```

## Authentication

The SDK uses OAuth 2.0 Client Credentials for authentication:

```python
from zerobus.sdk.sync import ZerobusSdk
from zerobus.sdk.shared import TableProperties
import record_pb2

sdk = ZerobusSdk(server_endpoint, workspace_url)
table_properties = TableProperties(table_name, record_pb2.AirQuality.DESCRIPTOR)

# Create stream with OAuth authentication
stream = sdk.create_stream(client_id, client_secret, table_properties)
```

The SDK automatically handles OAuth 2.0 authentication and uses secure TLS connections by default.

For advanced use cases requiring custom authentication headers, see the `HeadersProvider` section in the API Reference below.

## Configuration

### Stream Configuration Options

| Option | Default | Description |
|--------|---------|-------------|
| `record_type` | `RecordType.PROTO` | Serialization format: `RecordType.PROTO` or `RecordType.JSON` |
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
- `server_endpoint` (str) - The Zerobus gRPC endpoint (e.g., `<workspace-id>.zerobus.<region>.cloud.databricks.com` for AWS, or `<workspace-id>.zerobus.<region>.azuredatabricks.net` for Azure)
- `unity_catalog_endpoint` (str) - The Unity Catalog endpoint (your workspace URL)

**Methods:**

```python
def create_stream(
    client_id: str,
    client_secret: str,
    table_properties: TableProperties,
    options: StreamConfigurationOptions = None,
    headers_provider: HeadersProvider = None
) -> ZerobusStream
```
Creates a new ingestion stream using OAuth 2.0 Client Credentials authentication.

**Parameters:**
- `client_id` (str) - OAuth client ID (ignored if `headers_provider` is provided)
- `client_secret` (str) - OAuth client secret (ignored if `headers_provider` is provided)
- `table_properties` (TableProperties) - Target table configuration
- `options` (StreamConfigurationOptions) - Stream behavior configuration (optional)
- `headers_provider` (HeadersProvider) - Custom headers provider (optional, defaults to OAuth)

Automatically includes these headers (when using default OAuth):
- `"authorization": "Bearer <oauth_token>"` (fetched via OAuth 2.0 Client Credentials flow)
- `"x-databricks-zerobus-table-name": "<table_name>"`

Returns a `ZerobusStream` instance.

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
    options: StreamConfigurationOptions = None,
    headers_provider: HeadersProvider = None
) -> ZerobusStream
```
Creates a new ingestion stream using OAuth 2.0 Client Credentials authentication.

**Parameters:**
- `client_id` (str) - OAuth client ID (ignored if `headers_provider` is provided)
- `client_secret` (str) - OAuth client secret (ignored if `headers_provider` is provided)
- `table_properties` (TableProperties) - Target table configuration
- `options` (StreamConfigurationOptions) - Stream behavior configuration (optional)
- `headers_provider` (HeadersProvider) - Custom headers provider (optional, defaults to OAuth)

Automatically includes these headers (when using default OAuth):
- `"authorization": "Bearer <oauth_token>"` (fetched via OAuth 2.0 Client Credentials flow)
- `"x-databricks-zerobus-table-name": "<table_name>"`

Returns a `ZerobusStream` instance.

---

### ZerobusStream

Represents an active ingestion stream.

**Synchronous Methods:**

**Single Record Ingestion:**

```python
def ingest_record_offset(record: Union[Message, dict, bytes, str]) -> int
```
**RECOMMENDED** - Ingests a single record and returns the offset after queueing.

```python
def ingest_record_nowait(record: Union[Message, dict, bytes, str]) -> None
```
**RECOMMENDED** - Fire-and-forget ingestion. Submits the record without waiting or returning an offset. Best for maximum throughput.

```python
def ingest_record(record: Union[Message, dict, bytes, str]) -> RecordAcknowledgment
```
**DEPRECATED since v0.3.0** - Use `ingest_record_offset()` or `ingest_record_nowait()` instead for better performance.

**Batch Ingestion:**

```python
def ingest_records_offset(records: List[Union[Message, dict, bytes, str]]) -> int
```
Ingests a batch of records and returns the final offset immediately. More efficient than individual calls for bulk ingestion.

```python
def ingest_records_nowait(records: List[Union[Message, dict, bytes, str]]) -> None
```
Fire-and-forget batch ingestion. Submits all records without waiting. Most efficient for bulk ingestion.

**Stream Management:**

```python
def flush() -> None
```
Flushes all pending records and waits for server acknowledgment. Does not close the stream.

```python
def close() -> None
```
Flushes and closes the stream gracefully. Always call in a `finally` block.


**Accepted Record Types (all methods):**
- **JSON mode**: `dict` (SDK serializes) or `str` (pre-serialized JSON string)
- **Protobuf mode**: `Message` object (SDK serializes) or `bytes` (pre-serialized)

---

**Asynchronous Methods:**

**Single Record Ingestion:**

```python
async def ingest_record_offset(record: Union[Message, dict, bytes, str]) -> int
```
**RECOMMENDED** - Ingests a single record and returns the offset after queueing.

```python
def ingest_record_nowait(record: Union[Message, dict, bytes, str]) -> None
```
**RECOMMENDED** - Fire-and-forget ingestion. Submits the record without waiting. Not async (don't use `await`). Best for maximum throughput.

```python
async def ingest_record(record: Union[Message, dict, bytes, str]) -> Awaitable
```
**DEPRECATED since v0.3.0** - Use `ingest_record_offset()` or `ingest_record_nowait()` instead for better performance.

**Batch Ingestion:**

```python
async def ingest_records_offset(records: List[Union[Message, dict, bytes, str]]) -> int
```
Ingests a batch of records and returns the final offset immediately. More efficient than individual calls for bulk ingestion.

```python
def ingest_records_nowait(records: List[Union[Message, dict, bytes, str]]) -> None
```
Fire-and-forget batch ingestion. Submits all records without waiting. Not async (don't use `await`). Most efficient for bulk ingestion.

**Offset Tracking:**

```python
async def wait_for_offset(offset: int) -> None
```
Waits for a specific offset to be acknowledged by the server. Useful when you have an offset from `ingest_record_offset()` and want to ensure it's durably written:
```python
offset = await stream.ingest_record_offset(record)
# Do other work...
await stream.wait_for_offset(offset)  # Ensure this offset is acknowledged
```

**Stream Management:**

```python
async def flush() -> None
```
Flushes all pending records and waits for server acknowledgment. Does not close the stream.

```python
async def close() -> None
```
Flushes and closes the stream gracefully. Always call in a `finally` block.

Returns the unique stream ID assigned by the server.

**Accepted Record Types (all methods):**
- **JSON mode**: `dict` (SDK serializes) or `str` (pre-serialized JSON string)
- **Protobuf mode**: `Message` object (SDK serializes) or `bytes` (pre-serialized)

---

### TableProperties

Configuration for the target table.

**Constructor:**

```python
TableProperties(table_name: str, descriptor: Descriptor = None)
```

**Parameters:**
- `table_name` (str) - Fully qualified table name (e.g., `catalog.schema.table`)
- `descriptor` (Descriptor) - Protobuf message descriptor (e.g., `MyMessage.DESCRIPTOR`). Required for protobuf mode, not needed for JSON mode.

**Examples:**

```python
# JSON mode
table_properties = TableProperties("catalog.schema.table")

# Protobuf mode (default)
table_properties = TableProperties("catalog.schema.table", record_pb2.MyMessage.DESCRIPTOR)
```

---

### HeadersProvider

Abstract base class for providing authentication headers to gRPC streams.

**Default:** The SDK uses `OAuthHeadersProvider` internally, which handles OAuth 2.0 Client Credentials authentication automatically when you call `create_stream()`.

**Custom Implementation:** For advanced use cases, you can implement a custom `HeadersProvider` by extending the base class and implementing the `get_headers()` method. Custom providers must include both the `authorization` and `x-databricks-zerobus-table-name` headers. See example files for implementation details.

---

### StreamConfigurationOptions

Configuration options for stream behavior.

**Constructor:**
```python
StreamConfigurationOptions(
    record_type: RecordType = RecordType.PROTO,
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
- `record_type` (RecordType) - Serialization format: `RecordType.PROTO` (default) or `RecordType.JSON`
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

## Performance Tips

The SDK provides multiple ingestion methods optimized for different use cases:

### Method Comparison

| Method | Throughput | Acknowledgment | Use Case |
|--------|-----------|----------------|----------|
| `ingest_record()` | Low | Yes, tracked | When you need individual record tracking |
| `ingest_record_offset()` | Medium | Returns offset | When you need offsets but not full tracking |
| `ingest_record_nowait()` | **Highest** | No | Maximum throughput, fire-and-forget |

### Performance Comparison

Benchmarked with 100k records on a local connection:

| Record Size | `ingest_record` (sequential) | `ingest_record_nowait` |
|-------------|------------------------------|------------------------|
| 20 bytes    | 0.35 MB/s                    | 7.55 MB/s (20x faster) |
| 220 bytes   | 2.00 MB/s                    | 77 MB/s (38x faster)   |
| 750 bytes   | 16 MB/s                      | 257 MB/s (16x faster)  |
| 10 KB       | 188 MB/s                     | 382 MB/s (2x faster)   |

**Key Insight**: The performance gap is largest for small records due to context switching overhead in sequential awaits. Use batched submission or `nowait` methods for optimal throughput.
