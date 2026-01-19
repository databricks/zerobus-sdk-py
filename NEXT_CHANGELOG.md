# NEXT CHANGELOG

## Release v0.3.0

### New Features and Improvements

- **Custom TLS Configuration**: Added support for custom TLS/SSL configuration via the `TlsConfig` interface. The SDK uses `SecureTlsConfig` (system CA certificates) by default, with optional custom implementations for advanced use cases such as custom CA certificates, mutual TLS (mTLS), or custom cipher suites.

- **Flexible Record Serialization**: `ingest_record()` now accepts multiple input types, giving clients control over serialization:
  - **JSON mode**: Accepts both `dict` (SDK serializes) and `str` (pre-serialized JSON string)
  - **Protobuf mode**: Accepts both `Message` objects (SDK serializes) and `bytes` (pre-serialized)
  - This allows clients to optimize serialization separately or use custom serialization logic while maintaining backward compatibility

### Bug Fixes

### Documentation

- Updated README with TLS configuration documentation
- Added `TlsConfig` section to API Reference
- Updated example files to include custom TLS configuration examples
- Added brief mentions of advanced configuration options in appropriate sections
- Updated `ingest_record()` API documentation to show all accepted record types
- Added inline examples demonstrating both serialization approaches (SDK-controlled vs. client-controlled)
- Updated examples README with clear explanations of serialization options

### Internal Changes

- Implemented `TlsConfig` Strategy pattern for flexible TLS configuration
- Added `SecureTlsConfig` as default TLS implementation
- Streams now preserve TLS configuration during recreation for consistency
- Added comprehensive test coverage for all combinations of TLS and headers provider configurations
- Enhanced `ingest_record()` type validation to accept wider range of input types
- Added test coverage for both high-level objects (dict/Message) and pre-serialized data (str/bytes)

### Breaking Changes

- **BREAKING**: Removed `create_stream_with_headers_provider()` method
  - **Migration**: Use `create_stream()` with the `headers_provider` parameter instead
  - Old: `sdk.create_stream_with_headers_provider(custom_provider, table_properties, options)`
  - New: `sdk.create_stream(client_id, client_secret, table_properties, options, headers_provider=custom_provider)`

### API Changes

- Added optional `tls_config` parameter to `create_stream()` methods (both sync and async)
  - Defaults to `SecureTlsConfig()` (system CA certificates) when not provided
- Added optional `headers_provider` parameter to `create_stream()` methods
  - Defaults to `OAuthHeadersProvider()` (OAuth 2.0 Client Credentials) when not provided
- Widened `ingest_record()` type signature to accept:
  - JSON mode: `Union[dict, str]` (previously `str` only)
  - Protobuf mode: `Union[Message, bytes]` (previously `Message` only)
- All changes except removal of `create_stream_with_headers_provider()` are backward compatible
