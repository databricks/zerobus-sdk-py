# Version changelog

## Release v0.1.0

Initial release of the Databricks Zerobus Ingest SDK for Python.

### API Changes

- Added `ZerobusSdk` class for creating ingestion streams
- Added `ZerobusStream` class for managing stateful gRPC streams
- Added `RecordAcknowledgment` for blocking until record acknowledgment
- Added asynchronous versions: `zerobus.sdk.aio.ZerobusSdk` and `zerobus.sdk.aio.ZerobusStream`
- Added `TableProperties` for configuring table schema and name
- Added `StreamConfigurationOptions` for stream behavior configuration
- Added `ZerobusException` and `NonRetriableException` for error handling
- Added `StreamState` enum for tracking stream lifecycle
- Support for Python 3.9, 3.10, 3.11, 3.12, and 3.13
