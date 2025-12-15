"""TLS configuration for Zerobus connections.

This module provides a strategy pattern for TLS configuration.
"""

from abc import ABC, abstractmethod
import grpc


class TlsConfig(ABC):
    """Abstract base class for TLS configuration strategies.

    Implementations define how to configure the gRPC channel's TLS settings.
    """

    @abstractmethod
    def to_channel_credentials(self) -> grpc.ChannelCredentials:
        """Convert TLS configuration to gRPC ChannelCredentials.

        Returns:
            grpc.ChannelCredentials: Channel credentials for secure connection
        """
        pass


class SecureTlsConfig(TlsConfig):
    """Secure TLS configuration using system CA certificates.

    This is the default and recommended configuration for production use.
    It enables TLS encryption using the operating system's trusted CA certificates.

    Examples:
        >>> from zerobus.sdk.shared.tls_config import SecureTlsConfig
        >>>
        >>> tls = SecureTlsConfig()
        >>> stream = sdk.create_stream(
        ...     table_properties,
        ...     client_id,
        ...     client_secret,
        ...     options,
        ...     tls_config=tls
        ... )
    """

    def to_channel_credentials(self) -> grpc.ChannelCredentials:
        """Return secure TLS credentials using system CA certificates.

        Returns:
            grpc.ChannelCredentials: SSL channel credentials with system CAs
        """
        return grpc.ssl_channel_credentials()
