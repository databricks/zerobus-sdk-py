# NEXT CHANGELOG

## Release v0.2.0

### New Features and Improvements

- Loosened protobuf dependency constraint to support versions >= 4.25.0 and < 7.0
- **JSON Serialization Support**: Added support for JSON record serialization alongside Protocol Buffers (default)
  - New `RecordType.JSON` mode for ingesting JSON-encoded strings
  - No protobuf schema compilation required
- Added `HeadersProvider` abstraction for flexible authentication strategies
- Implemented `OAuthHeadersProvider` for OAuth 2.0 Client Credentials flow (default authentication method used by `create_stream()`)

### Bug Fixes

### Documentation

- Added JSON and protobuf serialization examples for both sync and async APIs
- Restructured Quick Start guide to present JSON first as the simpler option
- Enhanced API Reference with JSON mode documentation
- Added Azure workspace and endpoint URL examples

### Internal Changes

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
