"""
Headers provider for authentication.

This module re-exports HeadersProvider from the Rust core for backward compatibility.
"""

# Re-export from Rust core
from zerobus._zerobus_core import HeadersProvider, OAuthHeadersProvider

__all__ = [
    "HeadersProvider",
    "OAuthHeadersProvider",
]
