"""
Python SDK for the Ingest API.

This is the synchronous version of the SDK. For the asynchronous version,
please use `from zerobus_sdk.aio import ...`.
"""

from . import sync
from .shared import (NonRetriableException, StreamConfigurationOptions,
                     StreamState, TableProperties, ZerobusException)

ZerobusSdk = sync.ZerobusSdk
ZerobusStream = sync.ZerobusStream
RecordAcknowledgment = sync.RecordAcknowledgment

__all__ = [
    "ZerobusSdk",
    "ZerobusStream",
    "RecordAcknowledgment",
    "TableProperties",
    "StreamConfigurationOptions",
    "ZerobusException",
    "NonRetriableException",
    "StreamState",
]
