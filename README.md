# Databricks Zerobus Ingest SDK for Python

[![PyPI - Downloads](https://img.shields.io/pypi/dw/databricks-zerobus-ingest-sdk)](https://pypistats.org/packages/databricks-zerobus-ingest-sdk)
[![PyPI - License](https://img.shields.io/pypi/l/databricks-zerobus-ingest-sdk)](https://github.com/databricks/zerobus-sdk-py/blob/main/LICENSE)
![PyPI](https://img.shields.io/pypi/v/databricks-zerobus-ingest-sdk)

[Public Preview](https://docs.databricks.com/release-notes/release-types.html): This SDK is supported for production use cases and is available to all customers. Databricks is actively working on stabilizing the Zerobus Ingest SDK for Python. Minor version updates may include backwards-incompatible changes.

We are keen to hear feedback from you on this SDK. Please [file issues](https://github.com/databricks/zerobus-sdk-py/issues), and we will address them.

The Databricks Zerobus Ingest SDK for Python provides a high-performance client for ingesting data directly into Databricks Delta tables using the Zerobus streaming protocol. | See also the [SDK for Rust](https://github.com/databricks/zerobus-sdk-rs) | See also the [SDK for Java](https://github.com/databricks/zerobus-sdk-java)

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

- **High-throughput ingestion**: Optimized for high-volume data ingestion
- **Automatic recovery**: Built-in retry and recovery mechanisms
- **Flexible configuration**: Customizable stream behavior and timeouts
- **Multiple serialization formats**: Support for JSON and Protocol Buffers
- **OAuth 2.0 authentication**: Secure authentication with client credentials
- **Sync and Async support**: Both synchronous and asynchronous APIs
- **Comprehensive logging**: Detailed logging using Python's standard logging framework

## Requirements

### Runtime Requirements

- **Python**: 3.9 or higher
- **Databricks workspace** with Zerobus access enabled

### Dependencies

- `protobuf` >= 4.25.0, < 7.0
- `grpcio` >= 1.60.0, < 2.0
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

#### From PyPI

Install the latest stable version using pip:

```bash
pip install databricks-zerobus-ingest-sdk
```

#### From Source

Clone the repository and install from source:

```bash
git clone https://github.com/databricks/zerobus-sdk-py.git
cd zerobus-sdk-py
pip install -e .
```

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
from zerobus.sdk.shared import RecordType, TableProperties

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
        # Create JSON record
        record_dict = {
            "device_name": f"sensor-{i % 10}",
            "temp": 20 + (i % 15),
            "humidity": 50 + (i % 40)
        }
        json_record = json.dumps(record_dict)

        ack = stream.ingest_record(json_record)
        ack.wait_for_ack()  # Optional: Wait for durability confirmation

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
from zerobus.sdk.shared import RecordType, TableProperties

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
            # Create JSON record
            record_dict = {
                "device_name": f"sensor-{i % 10}",
                "temp": 20 + (i % 15),
                "humidity": 50 + (i % 40)
            }
            json_record = json.dumps(record_dict)

            future = await stream.ingest_record(json_record)
            await future  # Optional: Wait for durability confirmation

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
    --output "record.proto"
```

**Parameters:**
- `--uc-endpoint`: Your workspace URL
- `--client-id`: Service principal application ID
- `--client-secret`: Service principal secret
- `--table`: Fully qualified table name (catalog.schema.table)
- `--output`: Output path for the generated proto file

After generating, compile it as shown above.

**Type Mappings:**

| Delta Type | Proto2 Type |
|-----------|-------------|
| INT, SMALLINT, SHORT | int32 |
| BIGINT, LONG | int64 |
| FLOAT | float |
| DOUBLE | double |
| STRING, VARCHAR | string |
| BOOLEAN | bool |
| BINARY | bytes |
| DATE | int32 |
| TIMESTAMP | int64 |
| ARRAY\<type\> | repeated type |
| MAP\<key, value\> | map\<key, value\> |
| STRUCT\<fields\> | nested message |

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
        record = record_pb2.AirQuality(
            device_name=f"sensor-{i % 10}",
            temp=20 + (i % 15),
            humidity=50 + (i % 40)
        )

        ack = stream.ingest_record(record)
        ack.wait_for_ack()  # Optional: Wait for durability confirmation

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
            record = record_pb2.AirQuality(
                device_name=f"sensor-{i % 10}",
                temp=20 + (i % 15),
                humidity=50 + (i % 40)
            )

            future = await stream.ingest_record(record)
            await future  # Optional: Wait for durability confirmation

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
        record_dict = {
            "device_name": f"sensor-{i}",
            "temp": 20 + i % 15,
            "humidity": 50 + i % 40
        }
        json_record = json.dumps(record_dict)

        ack = stream.ingest_record(json_record)
        ack.wait_for_ack()  # Optional: Wait for durability confirmation
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
            record_dict = {
                "device_name": f"sensor-{i % 10}",
                "temp": 20 + i % 15,
                "humidity": 50 + i % 40
            }
            json_record = json.dumps(record_dict)

            future = await stream.ingest_record(json_record)
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
        record = record_pb2.AirQuality(
            device_name=f"sensor-{i}",
            temp=20 + i % 15,
            humidity=50 + i % 40
        )

        ack = stream.ingest_record(record)
        ack.wait_for_ack()  # Optional: Wait for durability confirmation
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

The SDK automatically fetches access tokens and includes these headers:
- `"authorization": "Bearer <oauth_token>"` - Obtained via OAuth 2.0 Client Credentials flow
- `"x-databricks-zerobus-table-name": "<table_name>"` - The fully qualified table name

### Advanced: Custom Headers

For advanced use cases where you need to provide custom headers (e.g., for future authentication methods or additional metadata), you can implement a custom `HeadersProvider`:

```python
from zerobus.sdk.shared.headers_provider import HeadersProvider

class CustomHeadersProvider(HeadersProvider):
    """
    Custom headers provider for advanced use cases.

    Note: Currently, OAuth 2.0 Client Credentials (via create_stream())
    is the standard authentication method. Use this only if you have
    specific requirements for custom headers.
    
    IMPORTANT: Custom headers providers MUST include both:
    - "authorization": "Bearer <token>" header for authentication
    - "x-databricks-zerobus-table-name": "<table_name>" header for routing
    """

    def __init__(self, token: str, table_name: str):
        self.token = token
        self.table_name = table_name

    def get_headers(self):
        """
        Return headers for gRPC metadata.

        Returns:
            List of (header_name, header_value) tuples
        """
        return [
            ("authorization", f"Bearer {self.token}"),
            ("x-databricks-zerobus-table-name", self.table_name),
            ("x-custom-header", "custom-value"),  # Optional: additional custom headers
        ]

# Use the custom provider
custom_provider = CustomHeadersProvider(
    token="your-token",
    table_name=table_properties.table_name
)
stream = sdk.create_stream_with_headers_provider(
    custom_provider,
    table_properties
)
```

**Potential use cases for custom headers:**
- Integration with existing token management systems
- Additional metadata headers for request tracking
- Future authentication methods
- Special routing or service mesh requirements

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
- `server_endpoint` (str) - The Zerobus gRPC endpoint (e.g., `<workspace-id>.zerobus.<region>.cloud.databricks.com` for AWS, or `<workspace-id>.zerobus.<region>.azuredatabricks.net` for Azure)
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
Creates a new ingestion stream using OAuth 2.0 Client Credentials authentication.

Automatically includes these headers:
- `"authorization": "Bearer <oauth_token>"` (fetched via OAuth 2.0 Client Credentials flow)
- `"x-databricks-zerobus-table-name": "<table_name>"`

Returns a `ZerobusStream` instance.

```python
def create_stream_with_headers_provider(
    headers_provider: HeadersProvider,
    table_properties: TableProperties,
    options: StreamConfigurationOptions = None
) -> ZerobusStream
```
Creates a new ingestion stream using a custom headers provider. For advanced use cases only where custom headers are required. Returns a `ZerobusStream` instance.

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
Creates a new ingestion stream using OAuth 2.0 Client Credentials authentication.

Automatically includes these headers:
- `"authorization": "Bearer <oauth_token>"` (fetched via OAuth 2.0 Client Credentials flow)
- `"x-databricks-zerobus-table-name": "<table_name>"`

Returns a `ZerobusStream` instance.

```python
async def create_stream_with_headers_provider(
    headers_provider: HeadersProvider,
    table_properties: TableProperties,
    options: StreamConfigurationOptions = None
) -> ZerobusStream
```
Creates a new ingestion stream using a custom headers provider. For advanced use cases only where custom headers are required. Returns a `ZerobusStream` instance.

---

### ZerobusStream

Represents an active ingestion stream.

**Synchronous Methods:**

```python
def ingest_record(record: Union[str, bytes, Message]) -> RecordAcknowledgment
```
Ingests a single record. Pass a JSON string (JSON mode) or protobuf message/bytes (protobuf mode). Returns a `RecordAcknowledgment` for tracking.

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
async def ingest_record(record: Union[str, bytes, Message]) -> Awaitable
```
Ingests a single record. Pass a JSON string (JSON mode) or protobuf message/bytes (protobuf mode). Returns an awaitable that completes when the record is durably written.

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
Returns the protobuf message descriptor (None for JSON mode).

```python
@property
def record_type() -> RecordType
```
Returns the record serialization type.

---

### HeadersProvider

Abstract base class for providing headers to gRPC streams. For advanced use cases only.

**Abstract Method:**

```python
@abstractmethod
def get_headers(self) -> List[Tuple[str, str]]
```
Returns headers for gRPC metadata as a list of (header_name, header_value) tuples.

**Built-in Implementation:**

#### OAuthHeadersProvider

OAuth 2.0 Client Credentials flow headers provider (used internally by `create_stream()`).

```python
OAuthHeadersProvider(
    workspace_id: str,
    workspace_url: str,
    table_name: str,
    client_id: str,
    client_secret: str
)
```

Returns these headers:
- `"authorization": "Bearer <oauth_token>"` (fetched via OAuth 2.0)
- `"x-databricks-zerobus-table-name": "<table_name>"`

**Custom Implementation (Advanced):**

For advanced use cases requiring custom headers, extend the `HeadersProvider` class:

```python
from zerobus.sdk.shared.headers_provider import HeadersProvider

class MyCustomProvider(HeadersProvider):
    def get_headers(self):
        return [
            ("authorization", "Bearer my-token"),  # Required
            ("x-databricks-zerobus-table-name", "catalog.schema.table"),  # Required
            ("x-custom-header", "value"),  # Optional: additional custom headers
        ]
```

**Important:** Custom headers providers MUST include both required headers:
- `"authorization"`: Bearer token for authentication
- `"x-databricks-zerobus-table-name"`: Fully qualified table name for routing

Note: Most users should use `create_stream()` with OAuth credentials rather than implementing a custom provider.

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
