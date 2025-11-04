"""
Tests for different record types (PROTO and JSON).
"""

import unittest
from unittest.mock import patch

import tests.row_pb2 as test_row_pb2
from tests.mock_grpc import (InjectedRecordResponse, MockGrpcChannel,
                             SdkManager, for_both_sdks)
from zerobus.sdk import (RecordType, StreamConfigurationOptions, StreamState,
                         TableProperties)

SERVER_ENDPOINT = "SERVER_ENDPOINT"
TABLE_NAME = "catalog.schema.test_table"


def token_factory():
    return "TOKEN"


class TestRecordTypes(unittest.IsolatedAsyncioTestCase):
    """Test different record type configurations."""

    @for_both_sdks
    async def test_proto_default(self, sdk: SdkManager):
        """Test default PROTO behavior."""
        calls_count = 0
        mock_grpc_stream = sdk.get_mock_class()(calls_count)

        def create_ephemeral_stream(generator, **kwargs):
            nonlocal calls_count
            nonlocal mock_grpc_stream
            calls_count += 1
            mock_grpc_stream = sdk.get_mock_class()(calls_count, generator)
            mock_grpc_stream.inject_response(False, InjectedRecordResponse(0, timeout_ms=10))
            return mock_grpc_stream

        mock_channel = MockGrpcChannel()
        mock_channel.injected_methods["/databricks.zerobus.Zerobus/EphemeralStream"] = create_ephemeral_stream

        # Create stream without specifying record_type (should default to PROTO)
        options = StreamConfigurationOptions(
            recovery=False,
            max_inflight_records=150,
            token_factory=token_factory,
        )

        with patch(sdk.get_grpc_override(), return_value=mock_channel):
            sdk_handle = sdk.create(SERVER_ENDPOINT)
            stream = await sdk_handle.create_stream(
                TableProperties(TABLE_NAME, test_row_pb2.AirQuality.DESCRIPTOR),
                options,
            )

            # Verify stream is created with default PROTO type
            self.assertEqual(stream.get_state(), StreamState.OPENED)

            # Ingest a protobuf record (should work with default)
            ack = await stream.ingest_record(test_row_pb2.AirQuality(device_name="device1", temp=20, humidity=50))
            offset_ack = await ack
            self.assertEqual(offset_ack, 0)

            await stream.close()

        mock_grpc_stream.cancel()

    @for_both_sdks
    async def test_proto(self, sdk: SdkManager):
        """Test PROTO configuration."""
        calls_count = 0
        mock_grpc_stream = sdk.get_mock_class()(calls_count)

        def create_ephemeral_stream(generator, **kwargs):
            nonlocal calls_count
            nonlocal mock_grpc_stream
            calls_count += 1
            mock_grpc_stream = sdk.get_mock_class()(calls_count, generator)
            mock_grpc_stream.inject_response(False, InjectedRecordResponse(0, timeout_ms=10))
            mock_grpc_stream.inject_response(False, InjectedRecordResponse(1, timeout_ms=10))
            return mock_grpc_stream

        mock_channel = MockGrpcChannel()
        mock_channel.injected_methods["/databricks.zerobus.Zerobus/EphemeralStream"] = create_ephemeral_stream

        # Specify PROTO record type
        options = StreamConfigurationOptions(
            recovery=False,
            max_inflight_records=150,
            token_factory=token_factory,
            record_type=RecordType.PROTO,
        )

        with patch(sdk.get_grpc_override(), return_value=mock_channel):
            sdk_handle = sdk.create(SERVER_ENDPOINT)
            stream = await sdk_handle.create_stream(
                TableProperties(TABLE_NAME, test_row_pb2.AirQuality.DESCRIPTOR),
                options,
            )

            self.assertEqual(stream.get_state(), StreamState.OPENED)

            # Ingest multiple protobuf records
            ack1 = await stream.ingest_record(test_row_pb2.AirQuality(device_name="device1", temp=20, humidity=50))
            offset1 = await ack1
            self.assertEqual(offset1, 0)

            ack2 = await stream.ingest_record(test_row_pb2.AirQuality(device_name="device2", temp=22, humidity=55))
            offset2 = await ack2
            self.assertEqual(offset2, 1)

            await stream.close()

        mock_grpc_stream.cancel()

    @for_both_sdks
    async def test_json(self, sdk: SdkManager):
        """Test JSON record ingestion."""
        calls_count = 0
        mock_grpc_stream = sdk.get_mock_class()(calls_count)

        def create_ephemeral_stream(generator, **kwargs):
            nonlocal calls_count
            nonlocal mock_grpc_stream
            calls_count += 1
            mock_grpc_stream = sdk.get_mock_class()(calls_count, generator)
            mock_grpc_stream.inject_response(False, InjectedRecordResponse(0, timeout_ms=10))
            mock_grpc_stream.inject_response(False, InjectedRecordResponse(1, timeout_ms=10))
            mock_grpc_stream.inject_response(False, InjectedRecordResponse(2, timeout_ms=10))
            return mock_grpc_stream

        mock_channel = MockGrpcChannel()
        mock_channel.injected_methods["/databricks.zerobus.Zerobus/EphemeralStream"] = create_ephemeral_stream

        # Specify JSON record type
        options = StreamConfigurationOptions(
            recovery=False,
            max_inflight_records=150,
            token_factory=token_factory,
            record_type=RecordType.JSON,
        )

        with patch(sdk.get_grpc_override(), return_value=mock_channel):
            sdk_handle = sdk.create(SERVER_ENDPOINT)

            # For JSON, descriptor is not required
            stream = await sdk_handle.create_stream(
                TableProperties(TABLE_NAME),
                options,
            )

            self.assertEqual(stream.get_state(), StreamState.OPENED)

            # Ingest JSON records as dicts
            ack1 = await stream.ingest_record({"device_name": "device1", "temp": 20, "humidity": 50})
            offset1 = await ack1
            self.assertEqual(offset1, 0)

            ack2 = await stream.ingest_record({"device_name": "device2", "temp": 22, "humidity": 55})
            offset2 = await ack2
            self.assertEqual(offset2, 1)

            ack3 = await stream.ingest_record({"device_name": "device3", "temp": 25, "humidity": 60})
            offset3 = await ack3
            self.assertEqual(offset3, 2)

            await stream.close()

        mock_grpc_stream.cancel()

    @for_both_sdks
    async def test_wrong_record_type_for_proto_stream(self, sdk: SdkManager):
        """Test that passing wrong record type raises ValueError for PROTO stream."""
        calls_count = 0
        mock_grpc_stream = sdk.get_mock_class()(calls_count)

        def create_ephemeral_stream(generator, **kwargs):
            nonlocal calls_count
            nonlocal mock_grpc_stream
            calls_count += 1
            mock_grpc_stream = sdk.get_mock_class()(calls_count, generator)
            return mock_grpc_stream

        mock_channel = MockGrpcChannel()
        mock_channel.injected_methods["/databricks.zerobus.Zerobus/EphemeralStream"] = create_ephemeral_stream

        options = StreamConfigurationOptions(
            recovery=False,
            max_inflight_records=150,
            token_factory=token_factory,
            record_type=RecordType.PROTO,
        )

        with patch(sdk.get_grpc_override(), return_value=mock_channel):
            sdk_handle = sdk.create(SERVER_ENDPOINT)
            stream = await sdk_handle.create_stream(
                TableProperties(TABLE_NAME, test_row_pb2.AirQuality.DESCRIPTOR),
                options,
            )

            # Try to ingest a dict when stream expects protobuf
            with self.assertRaises(ValueError) as context:
                await stream.ingest_record({"device_name": "device1", "temp": 20})

            self.assertIn("PROTO records", str(context.exception))
            self.assertIn("dict", str(context.exception))

            await stream.close()

        mock_grpc_stream.cancel()

    @for_both_sdks
    async def test_wrong_record_type_for_json_stream(self, sdk: SdkManager):
        """Test that passing wrong record type raises ValueError for JSON stream."""
        calls_count = 0
        mock_grpc_stream = sdk.get_mock_class()(calls_count)

        def create_ephemeral_stream(generator, **kwargs):
            nonlocal calls_count
            nonlocal mock_grpc_stream
            calls_count += 1
            mock_grpc_stream = sdk.get_mock_class()(calls_count, generator)
            return mock_grpc_stream

        mock_channel = MockGrpcChannel()
        mock_channel.injected_methods["/databricks.zerobus.Zerobus/EphemeralStream"] = create_ephemeral_stream

        options = StreamConfigurationOptions(
            recovery=False,
            max_inflight_records=150,
            token_factory=token_factory,
            record_type=RecordType.JSON,
        )

        with patch(sdk.get_grpc_override(), return_value=mock_channel):
            sdk_handle = sdk.create(SERVER_ENDPOINT)
            stream = await sdk_handle.create_stream(
                TableProperties(TABLE_NAME),
                options,
            )

            # Try to ingest a protobuf when stream expects JSON
            with self.assertRaises(ValueError) as context:
                await stream.ingest_record(test_row_pb2.AirQuality(device_name="device1", temp=20, humidity=50))

            self.assertIn("JSON records", str(context.exception))
            self.assertIn("dict", str(context.exception))

            await stream.close()

        mock_grpc_stream.cancel()

    @for_both_sdks
    async def test_missing_descriptor_for_proto_stream(self, sdk: SdkManager):
        """Test that missing descriptor for PROTO stream raises ValueError."""
        options = StreamConfigurationOptions(
            recovery=False,
            max_inflight_records=150,
            token_factory=token_factory,
            record_type=RecordType.PROTO,
        )

        mock_channel = MockGrpcChannel()

        with patch(sdk.get_grpc_override(), return_value=mock_channel):
            sdk_handle = sdk.create(SERVER_ENDPOINT)

            # Try to create stream without descriptor for PROTO type
            with self.assertRaises(ValueError) as context:
                await sdk_handle.create_stream(
                    TableProperties(TABLE_NAME),  # No descriptor
                    options,
                )

            self.assertIn("descriptor_proto is required", str(context.exception))
            self.assertIn("PROTO", str(context.exception))

    @for_both_sdks
    async def test_descriptor_ignored_for_json_stream(self, sdk: SdkManager):
        """Test that descriptor is ignored (with warning) for JSON stream."""
        calls_count = 0
        mock_grpc_stream = sdk.get_mock_class()(calls_count)

        def create_ephemeral_stream(generator, **kwargs):
            nonlocal calls_count
            nonlocal mock_grpc_stream
            calls_count += 1
            mock_grpc_stream = sdk.get_mock_class()(calls_count, generator)
            mock_grpc_stream.inject_response(False, InjectedRecordResponse(0, timeout_ms=10))
            return mock_grpc_stream

        mock_channel = MockGrpcChannel()
        mock_channel.injected_methods["/databricks.zerobus.Zerobus/EphemeralStream"] = create_ephemeral_stream

        options = StreamConfigurationOptions(
            recovery=False,
            max_inflight_records=150,
            token_factory=token_factory,
            record_type=RecordType.JSON,
        )

        with patch(sdk.get_grpc_override(), return_value=mock_channel):
            sdk_handle = sdk.create(SERVER_ENDPOINT)

            # Create JSON stream but provide descriptor (should log warning but work)
            with self.assertLogs("zerobus_sdk", level="WARNING") as log:
                stream = await sdk_handle.create_stream(
                    TableProperties(TABLE_NAME, test_row_pb2.AirQuality.DESCRIPTOR),
                    options,
                )

            # Verify warning was logged
            self.assertTrue(
                any("descriptor_proto provided for JSON stream will be ignored" in msg for msg in log.output)
            )

            # Should still work with JSON records
            ack = await stream.ingest_record({"device_name": "device1", "temp": 20, "humidity": 50})
            offset_ack = await ack
            self.assertEqual(offset_ack, 0)

            await stream.close()

        mock_grpc_stream.cancel()


if __name__ == "__main__":
    unittest.main()
