"""
Shared utilities for Zerobus SDK.

This module re-exports common types from the Rust core for backward compatibility.
"""

# Re-export from Rust core for backward compatibility
from zerobus._zerobus_core import (
    AckCallback,
    NonRetriableException,
    RecordType,
    StreamConfigurationOptions,
    TableProperties,
    ZerobusException,
)

__all__ = [
    "RecordType",
    "StreamConfigurationOptions",
    "TableProperties",
    "ZerobusException",
    "NonRetriableException",
    "AckCallback",
]
