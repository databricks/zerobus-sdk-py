# NEXT CHANGELOG

## Release v0.3.0

### New Features and Improvements

- **Custom TLS Configuration**: Added support for custom TLS/SSL configuration via the `TlsConfig` interface. The SDK uses `SecureTlsConfig` (system CA certificates) by default, with optional custom implementations for advanced use cases such as custom CA certificates, mutual TLS (mTLS), or custom cipher suites.

### Bug Fixes

### Documentation

- Updated README with TLS configuration documentation
- Added `TlsConfig` section to API Reference
- Updated example files to include custom TLS configuration examples
- Added brief mentions of advanced configuration options in appropriate sections

### Internal Changes

- Implemented `TlsConfig` Strategy pattern for flexible TLS configuration
- Added `SecureTlsConfig` as default TLS implementation
- Streams now preserve TLS configuration during recreation for consistency
- Added comprehensive test coverage for all combinations of TLS and headers provider configurations

### API Changes

- **Non-breaking**: Added optional `tls_config` parameter to `create_stream()` methods (both sync and async)
  - Signature: `create_stream(client_id, client_secret, table_properties, options=None, tls_config=None, headers_provider=None)`
  - Defaults to `SecureTlsConfig()` when not provided
  - Fully backward compatible with existing code
