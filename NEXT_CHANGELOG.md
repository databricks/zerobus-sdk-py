# NEXT CHANGELOG

## Release v0.3.0

### Major Changes

- **Rust-Backed Implementation**: Complete rewrite of the Python SDK as a thin wrapper around the [Databricks Zerobus Rust SDK](https://github.com/databricks/zerobus-sdk-rs)
  - All core logic (gRPC, authentication, recovery, stream management) now handled by native Rust code
  - Python bindings built using PyO3 and maturin
  - Significant performance improvements: 2-5x throughput, lower latency, reduced memory footprint
  - Single source of truth: Python SDK automatically inherits all Rust SDK improvements
  - **Architecture**: Native Rust core with PyO3 bindings and full type stubs (`_zerobus_core.pyi`)
  - **Build System**: Migrated from setuptools to maturin for Rust/Python integration
  - **Benefits**: Native performance, Rust's memory safety guarantees, easier maintenance, consistent behavior across all SDK languages


### New Features and Improvements

- **Configurable Logging**: Added support for `RUST_LOG` environment variable to control log levels
  - Users can now set `RUST_LOG=debug` or `RUST_LOG=trace` for detailed diagnostics
  - Default level is `info` when not specified
  - Supports granular control: `RUST_LOG=zerobus_sdk=trace,tokio=info`
- **Flexible Record Serialization**: `ingest_record()` now accepts multiple input types, giving clients control over serialization:
  - **JSON mode**: Accepts both `dict` (SDK serializes) and `str` (pre-serialized JSON string)
  - **Protobuf mode**: Accepts both `Message` objects (SDK serializes) and `bytes` (pre-serialized)
  - This allows clients to optimize serialization separately or use custom serialization logic while maintaining backward compatibility

### Bug Fixes

### Documentation

- Updated README with new Delta type mappings (TIMESTAMP_NTZ, VARIANT)
- Updated `ingest_record()` API documentation to show all accepted record types
- Added inline examples demonstrating both serialization approaches (SDK-controlled vs. client-controlled)
- Updated examples README with clear explanations of serialization options

### Internal Changes

- **Implemented `get_unacked_records()` and `get_unacked_batches()`**: Return actual unacknowledged records/batches (as bytes) for recovery and monitoring
  - `get_unacked_records()` returns `List[bytes]` of unacknowledged record payloads
  - `get_unacked_batches()` returns `List[List[bytes]]` where each batch contains record payloads
  - Available in both sync and async APIs
  - Useful for implementing custom retry logic or monitoring stream health
- Added `env-filter` feature to `tracing-subscriber` dependency for `RUST_LOG` support

- **generate_proto tool**: Added support for TIMESTAMP_NTZ and VARIANT data types
  - TIMESTAMP_NTZ maps to int64 (timestamp without timezone, microseconds since epoch)
  - VARIANT maps to string (unshredded, JSON string format)
- **generate_proto tool**: Added comprehensive unit tests for all pure functions (84 tests covering type parsing, type mapping, field validation, and proto file generation)
- Enhanced `ingest_record()` type validation to accept wider range of input types
- Added test coverage for both high-level objects (dict/Message) and pre-serialized data (str/bytes)

### Breaking Changes

- **BREAKING**: Removed `create_stream_with_headers_provider()` method
  - **Migration**: Use `create_stream()` with the `headers_provider` parameter instead
  - Old: `sdk.create_stream_with_headers_provider(custom_provider, table_properties, options)`
  - New: `sdk.create_stream(client_id, client_secret, table_properties, options, headers_provider=custom_provider)`

### Deprecations

- **DEPRECATED**: `ingest_record()` method (both sync and async)
  - **Reason**: Offers significantly lower throughput compared to `ingest_record_offset()` and `ingest_record_nowait()`
  - **Migration**:
    - For sync API: Use `ingest_record_offset()` for offset tracking or `ingest_record_nowait()` for maximum throughput
    - For async API: Use `ingest_record_offset()` with batched `asyncio.gather()` pattern or `ingest_record_nowait()` for maximum throughput
  - **Performance Impact**: New methods are 2-40x faster depending on record size
  - **Note**: Method remains available for backward compatibility but will be removed in a future major version

### API Changes

- Added optional `headers_provider` parameter to `create_stream()` methods
  - Defaults to `OAuthHeadersProvider()` (OAuth 2.0 Client Credentials) when not provided
- Widened `ingest_record()` type signature to accept:
  - JSON mode: `Union[dict, str]` (previously `str` only)
  - Protobuf mode: `Union[Message, bytes]` (previously `Message` only)
- All changes except removal of `create_stream_with_headers_provider()` are backward compatible
