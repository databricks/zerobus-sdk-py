# Version changelog

## Release v0.2.0

### New Features and Improvements

- Loosened protobuf dependency constraint to support versions >= 4.25.0 and < 7.0
- **JSON Serialization Support**: Added support for JSON record serialization alongside Protocol Buffers (default)
  - New `RecordType.JSON` mode for ingesting JSON-encoded strings
  - No protobuf schema compilation required
- Added `HeadersProvider` abstraction for flexible authentication strategies
- Implemented `OAuthHeadersProvider` for OAuth 2.0 Client Credentials flow (default authentication method used by `create_stream()`)

### Bug Fixes

- **generate_proto tool**: Fixed uppercase field names bug for nested fields
- **generate_proto tool**: Added validation for unsupported nested type combinations
  - Now properly rejects: `array<array<...>>`, `array<map<...>>`, `map<map<...>, ...>`, `map<array<...>, ...>`, `map<..., map<...>>`, `map<..., array<...>>`
- **Logging**: Fixed false alarm "Retriable gRPC error" logs when calling `stream.close()`
  - CANCELLED errors during intentional stream closure are no longer logged as errors
- **Logging**: Unified log messages between sync and async SDK implementations
  - Both SDKs now produce consistent logging output with same verbosity and format
- **Error handling**: Improved error messages to distinguish between recoverable and non-recoverable errors
  - "Stream closed due to a non-recoverable error" vs "Stream failed permanently after failed recovery attempt"

### Documentation

- Added JSON and protobuf serialization examples for both sync and async APIs
- Restructured Quick Start guide to present JSON first as the simpler option
- Enhanced API Reference with JSON mode documentation
- Added Azure workspace and endpoint URL examples

### Internal Changes

- **Build system**: Loosened setuptools requirement from `>=77` to `>=61`xw
- **License format**: Changed license specification to PEP 621 table format for setuptools <77 compatibility
  - Changed from `license = "LicenseRef-Proprietary"` to `license = {text = "LicenseRef-Proprietary"}`
- **generate_proto tool**: Added support for TINYINT and BYTE data types (both map to int32)
- **Logging**: Added detailed initialization logging to async SDK to match sync SDK
  - "Starting initializing stream", "Attempting retry X out of Y", "Sending CreateIngestStreamRequest", etc.

### API Changes

- **StreamConfigurationOptions**: Added `record_type` parameter to specify serialization format
  - `RecordType.PROTO` (default): For protobuf serialization
  - `RecordType.JSON`: For JSON serialization
  - Example: `StreamConfigurationOptions(record_type=RecordType.JSON)`
- **ZerobusStream.ingest_record**: Now accepts JSON strings (when using `RecordType.JSON`) in addition to protobuf messages and bytes
- Added `RecordType` enum with `PROTO` and `JSON` values
- Added `HeadersProvider` abstract base class for custom header strategies
- Added `OAuthHeadersProvider` class for OAuth 2.0 authentication with Databricks OIDC endpoint
- Added `create_stream_with_headers_provider` method to `ZerobusSdk` and `aio.ZerobusSdk` for custom authentication header providers
  - **Note**: Custom headers providers must include both `authorization` and `x-databricks-zerobus-table-name` headers

## Release v0.1.0

Initial release of the Databricks Zerobus Ingest SDK for Python.

### API Changes

- Added `ZerobusSdk` class for creating ingestion streams
- Added `ZerobusStream` class for managing stateful gRPC streams
- Added `RecordAcknowledgment` for blocking until record acknowledgment
- Added asynchronous versions: `zerobus.sdk.aio.ZerobusSdk` and `zerobus.sdk.aio.ZerobusStream`
- Added `TableProperties` for configuring table schema and name
- Added `StreamConfigurationOptions` for stream behavior configuration
- Added `ZerobusException` and `NonRetriableException` for error handling
- Added `StreamState` enum for tracking stream lifecycle
- Support for Python 3.9, 3.10, 3.11, 3.12, and 3.13
