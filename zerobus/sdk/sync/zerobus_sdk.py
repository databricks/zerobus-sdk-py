"""
Synchronous Zerobus SDK (Rust-backed).

This module provides a high-performance synchronous interface for ingesting records
into Databricks tables via the Zerobus service. The implementation is backed by a
Rust core for optimal performance while maintaining a Pythonic API.

Example:
    >>> from zerobus.sdk.sync import ZerobusSdk, TableProperties
    >>>
    >>> sdk = ZerobusSdk(
    ...     host="shard.zerobus.databricks.com",
    ...     unity_catalog_url="https://workspace.databricks.com"
    ... )
    >>>
    >>> props = TableProperties("catalog.schema.table")
    >>> stream = sdk.create_stream(
    ...     table_properties=props,
    ...     client_id="your-client-id",
    ...     client_secret="your-client-secret"
    ... )
    >>>
    >>> # Optimized API - returns offset directly
    >>> offset = stream.ingest_record_offset(b"record_data")
    >>>
    >>> # Batch API - returns one offset for the batch
    >>> offsets = stream.ingest_records_offset([b"record1", b"record2"])
    >>>
    >>> # Fire-and-forget for maximum throughput
    >>> stream.ingest_record_nowait(b"record_data")
    >>> stream.ingest_records_nowait([b"record1", b"record2"])
    >>>
    >>> stream.flush()  # Ensure all records are sent
    >>> stream.close()
    >>>
    >>> # Legacy API (deprecated) - returns acknowledgment object
    >>> ack = stream.ingest_record(b"record_data")
    >>> offset = ack.wait_for_ack(timeout_sec=30)
"""

# Import Rust-backed implementations
import zerobus._zerobus_core as _core

# Import base Rust SDK class
_RustZerobusSdk = _core.sync.ZerobusSdk


class ZerobusSdk:
    """Python wrapper around Rust ZerobusSdk that provides unified create_stream API."""

    def __init__(self, host: str, unity_catalog_url: str):
        self._inner = _RustZerobusSdk(host, unity_catalog_url)

    def create_stream(self, client_id: str, client_secret: str, table_properties, options=None, headers_provider=None):
        """
        Create a stream with OAuth authentication or custom headers provider.

        Args:
            client_id: OAuth client ID
            client_secret: OAuth client secret
            table_properties: Table configuration
            options: Optional stream configuration
            headers_provider: Optional custom headers provider (if set, overrides OAuth)
        """
        if headers_provider is not None:
            # Use custom headers provider (ignores client_id/client_secret)
            return self._inner.create_stream_with_headers_provider(table_properties, headers_provider, options)
        else:
            # Use OAuth authentication
            return self._inner.create_stream(client_id, client_secret, table_properties, options)

    def recreate_stream(self, old_stream):
        """Recreate a stream from an old stream."""
        return self._inner.recreate_stream(old_stream)


# Direct re-exports
RecordAcknowledgment = _core.sync.RecordAcknowledgment
ZerobusStream = _core.sync.ZerobusStream

# Re-export common types for convenience
HeadersProvider = _core.HeadersProvider
OAuthHeadersProvider = _core.OAuthHeadersProvider
RecordType = _core.RecordType
StreamConfigurationOptions = _core.StreamConfigurationOptions
TableProperties = _core.TableProperties
AckCallback = _core.AckCallback
ZerobusException = _core.ZerobusException
NonRetriableException = _core.NonRetriableException

__all__ = [
    "ZerobusSdk",
    "ZerobusStream",
    "RecordAcknowledgment",
    "TableProperties",
    "StreamConfigurationOptions",
    "RecordType",
    "AckCallback",
    "HeadersProvider",
    "OAuthHeadersProvider",
    "ZerobusException",
    "NonRetriableException",
]
