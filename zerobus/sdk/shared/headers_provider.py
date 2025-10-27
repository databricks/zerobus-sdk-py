"""
Headers provider for Zerobus SDK.

This module implements the Strategy pattern for flexible headers.
"""

from abc import ABC, abstractmethod
from typing import List, Tuple

from .definitions import get_zerobus_token


class HeadersProvider(ABC):
    """
    Base class for headers strategies.

    Implementations of this class define how to obtain headers for the Zerobus gRPC stream.
    """

    @abstractmethod
    def get_headers(self) -> List[Tuple[str, str]]:
        """
        Returns headers for gRPC metadata.

        Returns:
            List of (header_name, header_value) tuples for headers
        """


class OAuthHeadersProvider(HeadersProvider):
    """
    OAuth 2.0 Client Credentials flow headers provider.

    This provider fetches an access token using client_id and client_secret
    via the Databricks OIDC endpoint and returns the headers.

    Example:
        >>> provider = OAuthHeadersProvider(
        ...     workspace_id="1234567890",
        ...     workspace_url="https://my-workspace.cloud.databricks.com",
        ...     table_name="catalog.schema.table",
        ...     client_id="my-client-id",
        ...     client_secret="my-client-secret"
        ... )
        >>> headers = provider.get_headers()
    """

    def __init__(self, workspace_id: str, workspace_url: str, table_name: str, client_id: str, client_secret: str):
        """
        Initialize OAuth headers provider.

        Args:
            workspace_id: The Databricks workspace ID
            workspace_url: The Databricks workspace URL
            table_name: The fully qualified table name (catalog.schema.table)
            client_id: OAuth client ID
            client_secret: OAuth client secret
        """
        self._workspace_id = workspace_id
        self._workspace_url = workspace_url
        self._table_name = table_name
        self._client_id = client_id
        self._client_secret = client_secret

    def get_headers(self) -> List[Tuple[str, str]]:
        """
        Fetch OAuth token and return authorization header.

        Returns:
            List containing the authorization header with Bearer token
        """
        token = get_zerobus_token(
            table_name=self._table_name,
            workspace_id=self._workspace_id,
            workspace_url=self._workspace_url,
            client_id=self._client_id,
            client_secret=self._client_secret,
        )
        return [("authorization", f"Bearer {token}"), ("x-databricks-zerobus-table-name", self._table_name)]
