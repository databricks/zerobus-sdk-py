"""Type stubs for _zerobus_core Rust module."""

from typing import Any, List, Optional, Tuple

from typing_extensions import Self

# =============================================================================
# COMMON TYPES
# =============================================================================

class RecordType:
    """Type of records to ingest into the stream."""

    value: int

    @staticmethod
    def PROTO() -> int: ...
    @staticmethod
    def JSON() -> int: ...
    def __int__(self) -> int: ...
    def __eq__(self, other: Self) -> bool: ...
    def __repr__(self) -> str: ...

class TableProperties:
    """Table properties for the stream."""

    table_name: str
    descriptor_proto: Optional[bytes]

    def __init__(self, table_name: str, descriptor_proto: Optional[bytes] = None) -> None: ...
    def __repr__(self) -> str: ...

class RecordCallback:
    """
    Base class for record acknowledgment callbacks.

    Subclass this in Python to create custom callbacks that are invoked
    when records are acknowledged by the server.

    Example:
        class MyCallback(RecordCallback):
            def on_ack(self, offset):
                print(f"Record acknowledged at offset {offset}")
    """

    def __init__(self) -> None: ...
    def on_ack(self, offset: int) -> None:
        """
        Called when a record is acknowledged by the server.

        Args:
            offset: The offset of the acknowledged record
        """
        ...

class StreamConfigurationOptions:
    """Configuration options for the stream."""

    max_inflight_records: int
    recovery: bool
    recovery_timeout_ms: int
    recovery_backoff_ms: int
    recovery_retries: int
    server_lack_of_ack_timeout_ms: int
    flush_timeout_ms: int
    record_type: RecordType
    ack_callback: Optional[RecordCallback]

    def __init__(self, **kwargs: Any) -> None: ...
    def __repr__(self) -> str: ...

# =============================================================================
# EXCEPTIONS
# =============================================================================

class ZerobusException(Exception):
    """Base class for all exceptions in the Zerobus SDK."""

    ...

class NonRetriableException(ZerobusException):
    """Indicates a non-retriable error has occurred."""

    ...

# =============================================================================
# AUTHENTICATION
# =============================================================================

class HeadersProvider:
    """Base class for headers strategies (subclassable from Python)."""

    def __init__(self) -> None: ...
    def get_headers(self) -> List[Tuple[str, str]]: ...

class OAuthHeadersProvider(HeadersProvider):
    """OAuth 2.0 Client Credentials flow headers provider."""

    def __init__(
        self,
        unity_catalog_url: str,
        client_id: str,
        client_secret: str,
        table_name: str,
    ) -> None: ...
    def get_headers(self) -> List[Tuple[str, str]]: ...

# =============================================================================
# SYNC SDK
# =============================================================================

class sync:
    """Synchronous Zerobus SDK."""

    class RecordAcknowledgment:
        """Future-like object for waiting on record acknowledgment."""

        def wait_for_ack(self, timeout_sec: Optional[float] = None) -> int:
            """
            Wait for the acknowledgment and return the offset ID.

            Args:
                timeout_sec: Optional timeout in seconds

            Returns:
                The offset ID of the acknowledged record

            Raises:
                RuntimeError: If called more than once
            """
            ...

        def is_done(self) -> bool:
            """Check if the acknowledgment is done."""
            ...

    class ZerobusStream:
        """Manages a single, stateful stream for ingesting records."""

        def ingest_record(self, payload: bytes | str) -> RecordAcknowledgment:
            """
            Ingest a single record and return RecordAcknowledgment (legacy API).

            .. deprecated:: 0.3.0
                Use :meth:`ingest_record_offset` instead for better performance.
                This method returns an intermediate acknowledgment object that requires
                an additional wait_for_ack() call. The new API returns offsets directly.

            Args:
                payload: bytes (for Proto) or str (for Json)

            Returns:
                RecordAcknowledgment that can be awaited
            """
            ...

        def ingest_record_offset(self, payload: bytes | str) -> int:
            """
            Ingest a single record and return the offset directly (optimized API).

            Args:
                payload: bytes (for Proto) or str (for Json)

            Returns:
                The offset ID
            """
            ...

        def ingest_record_nowait(self, payload: bytes | str) -> None:
            """
            Ingest a single record without waiting for acknowledgment (fire-and-forget).

            Args:
                payload: bytes (for Proto) or str (for Json)
            """
            ...

        def ingest_records_offset(self, payloads: List[bytes] | List[str]) -> Optional[int]:
            """
            Ingest multiple records and return one offset for the whole batch (batch API).

            Args:
                payloads: List of bytes (for Proto) or list of str (for Json)

            Returns:
                The offset ID for the batch, or None if empty list
            """
            ...

        def ingest_records_nowait(self, payloads: List[bytes] | List[str]) -> None:
            """
            Ingest multiple records without waiting (batch fire-and-forget).

            Args:
                payloads: List of bytes (for Proto) or list of str (for Json)
            """
            ...

        def wait_for_offset(self, offset: int, timeout_sec: Optional[float] = None) -> None:
            """
            Wait for a specific offset to be acknowledged.

            Args:
                offset: The offset to wait for
                timeout_sec: Optional timeout in seconds
            """
            ...

        def flush(self) -> None:
            """Flush the stream, waiting for all pending records to be acknowledged."""
            ...

        def close(self) -> None:
            """Close the stream gracefully."""
            ...

        def get_unacked_records(self) -> int:
            """Get the number of unacknowledged records."""
            ...

    class ZerobusSdk:
        """Main entry point for synchronous Zerobus ingestion."""

        def __init__(self, host: str, unity_catalog_url: str) -> None: ...
        def set_use_tls(self, use_tls: bool) -> None:
            """
            Set whether to use TLS for connections (default: True).

            Set to False for testing with local mock servers.

            Args:
                use_tls: Whether to use TLS
            """
            ...

        def create_stream(
            self,
            table_properties: TableProperties,
            client_id: str,
            client_secret: str,
            options: Optional[StreamConfigurationOptions] = None,
        ) -> ZerobusStream:
            """
            Create a new stream with OAuth authentication.

            Args:
                table_properties: Table properties
                client_id: OAuth client ID
                client_secret: OAuth client secret
                options: Optional configuration options

            Returns:
                A new ZerobusStream
            """
            ...

        def create_stream_with_headers_provider(
            self,
            table_properties: TableProperties,
            headers_provider: HeadersProvider,
            options: Optional[StreamConfigurationOptions] = None,
        ) -> ZerobusStream:
            """
            Create a new stream with custom headers provider.

            Args:
                table_properties: Table properties
                headers_provider: Custom headers provider
                options: Optional configuration options

            Returns:
                A new ZerobusStream
            """
            ...

        def recreate_stream(self, old_stream: ZerobusStream) -> ZerobusStream:
            """
            Recreate a closed stream with the same configuration.

            Args:
                old_stream: The closed stream to recreate

            Returns:
                A new ZerobusStream
            """
            ...

# =============================================================================
# ASYNC SDK
# =============================================================================

class aio:
    """Asynchronous Zerobus SDK."""

    class ZerobusStream:
        """Manages a single, stateful stream for ingesting records (async)."""

        async def ingest_record_offset(self, payload: bytes | str) -> int:
            """
            Ingest a single record and return the offset directly.

            Args:
                payload: bytes (for Proto) or str (for Json)

            Returns:
                The offset ID
            """
            ...

        def ingest_record_nowait(self, payload: bytes | str) -> None:
            """
            Ingest a single record without waiting (fire-and-forget).

            Args:
                payload: bytes (for Proto) or str (for Json)
            """
            ...

        async def ingest_records_offset(self, payloads: List[bytes] | List[str]) -> Optional[int]:
            """
            Ingest multiple records and return one offset for the whole batch.

            Args:
                payloads: List of bytes (for Proto) or list of str (for Json)

            Returns:
                The offset ID for the batch, or None if empty list
            """
            ...

        def ingest_records_nowait(self, payloads: List[bytes] | List[str]) -> None:
            """
            Ingest multiple records without waiting (batch fire-and-forget).

            Args:
                payloads: List of bytes (for Proto) or list of str (for Json)
            """
            ...

        async def wait_for_offset(self, offset: int, timeout_sec: Optional[float] = None) -> None:
            """
            Wait for a specific offset to be acknowledged.

            Args:
                offset: The offset to wait for
                timeout_sec: Optional timeout in seconds
            """
            ...

        async def flush(self) -> None:
            """Flush the stream, waiting for all pending records."""
            ...

        async def close(self) -> None:
            """Close the stream gracefully."""
            ...

        def get_unacked_records(self) -> int:
            """Get the number of unacknowledged records."""
            ...

    class ZerobusSdk:
        """Main entry point for asynchronous Zerobus ingestion."""

        def __init__(self, host: str, unity_catalog_url: str) -> None: ...
        async def set_use_tls(self, use_tls: bool) -> None:
            """
            Set whether to use TLS for connections (default: True).

            Set to False for testing with local mock servers.

            Args:
                use_tls: Whether to use TLS
            """
            ...

        async def create_stream(
            self,
            table_properties: TableProperties,
            client_id: str,
            client_secret: str,
            options: Optional[StreamConfigurationOptions] = None,
        ) -> ZerobusStream:
            """
            Create a new stream with OAuth authentication.

            Args:
                table_properties: Table properties
                client_id: OAuth client ID
                client_secret: OAuth client secret
                options: Optional configuration options

            Returns:
                A new ZerobusStream
            """
            ...

        async def create_stream_with_headers_provider(
            self,
            table_properties: TableProperties,
            headers_provider: HeadersProvider,
            options: Optional[StreamConfigurationOptions] = None,
        ) -> ZerobusStream:
            """
            Create a new stream with custom headers provider.

            Args:
                table_properties: Table properties
                headers_provider: Custom headers provider
                options: Optional configuration options

            Returns:
                A new ZerobusStream
            """
            ...

        async def recreate_stream(self, old_stream: ZerobusStream) -> ZerobusStream:
            """
            Recreate a closed stream with the same configuration.

            Args:
                old_stream: The closed stream to recreate

            Returns:
                A new ZerobusStream
            """
            ...
