# NEXT CHANGELOG

## Release v0.3.0

### New Features and Improvements

- **Custom TLS Configuration**: Added support for custom TLS/SSL configuration via the `TlsConfig` interface. The SDK uses `SecureTlsConfig` (system CA certificates) by default, with optional custom implementations for advanced use cases such as custom CA certificates, mutual TLS (mTLS), or custom cipher suites.

- **Configurable Message Size Limit**: Added `max_message_size_bytes` option to `StreamConfigurationOptions` to limit the size of individual records sent to the server. Defaults to 10MB. Records exceeding this limit will fail fast with an actionable error message before being sent to the server. Set to `-1` for unlimited (not recommended for production).

### Bug Fixes

### Documentation

- Updated README with TLS configuration documentation
- Added `TlsConfig` section to API Reference
- Updated example files to include custom TLS configuration examples
- Added brief mentions of advanced configuration options in appropriate sections
- Added `max_message_size_bytes` to Configuration table and API Reference
- Updated all example files to show message size limit configuration

### Internal Changes

- Implemented `TlsConfig` Strategy pattern for flexible TLS configuration
- Added `SecureTlsConfig` as default TLS implementation
- Streams now preserve TLS configuration during recreation for consistency
- Added comprehensive test coverage for all combinations of TLS and headers provider configurations

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
- All changes except removal of `create_stream_with_headers_provider()` are backward compatible
