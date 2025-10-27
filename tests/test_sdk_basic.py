"""
Basic SDK functionality tests with full mocking.
These tests are adapted from the integration tests but use mocks instead of real connections.
"""

import asyncio
import unittest
from unittest.mock import patch

import tests.row_pb2 as test_row_pb2
from tests.mock_grpc import (InjectedRecordResponse, MockGrpcChannel,
                             SdkManager, for_both_sdks)
from zerobus.sdk import (StreamConfigurationOptions, StreamState,
                         TableProperties, ZerobusException)
from zerobus.sdk.shared.headers_provider import HeadersProvider

SERVER_ENDPOINT = "SERVER_ENDPOINT"
TABLE_NAME = "catalog.schema.test_table"


def token_factory():
    return "TOKEN"


class CustomHeadersProviderForTest(HeadersProvider):
    """
    Custom headers provider for testing that mimics default OAuth behavior.

    This demonstrates that the custom headers provider API works correctly
    when providing the same headers that the default OAuth provider would.
    """

    def __init__(self, token: str, table_name: str):
        self.token = token
        self.table_name = table_name

    def get_headers(self):
        """
        Return headers matching the default OAuth provider format.

        Returns:
            List of (header_name, header_value) tuples
        """
        return [
            ("authorization", f"Bearer {self.token}"),
            ("x-databricks-zerobus-table-name", self.table_name),
        ]


class TestZerobusSdkBasic(unittest.IsolatedAsyncioTestCase):
    """Basic SDK functionality tests with mocking."""

    def setUp(self):
        self.options = StreamConfigurationOptions(
            recovery=False,
            max_inflight_records=150,
            token_factory=token_factory,
        )

    @for_both_sdks
    async def test_create_stream(self, sdk: SdkManager):
        """Test basic stream creation."""
        calls_count = 0

        def create_ephemeral_stream(generator, **kwargs):
            nonlocal calls_count
            calls_count += 1
            mock_stream = sdk.get_mock_class()(calls_count, generator)
            return mock_stream

        mock_channel = MockGrpcChannel()
        mock_channel.injected_methods["/databricks.zerobus.Zerobus/EphemeralStream"] = create_ephemeral_stream

        with patch(sdk.get_grpc_override(), return_value=mock_channel):
            sdk_handle = sdk.create(SERVER_ENDPOINT)
            stream = await sdk_handle.create_stream(
                TableProperties(TABLE_NAME, test_row_pb2.AirQuality.DESCRIPTOR),
                self.options,
            )
            self.assertIsNotNone(stream)
            self.assertEqual(stream.get_state(), StreamState.OPENED)
            await stream.close()

    @for_both_sdks
    async def test_ingest_single_record(self, sdk: SdkManager):
        """Test ingesting a single record and receiving acknowledgment."""
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

        with patch(sdk.get_grpc_override(), return_value=mock_channel):
            sdk_handle = sdk.create(SERVER_ENDPOINT)
            stream = await sdk_handle.create_stream(
                TableProperties(TABLE_NAME, test_row_pb2.AirQuality.DESCRIPTOR),
                self.options,
            )

            ack = await stream.ingest_record(test_row_pb2.AirQuality(device_name="my_device", temp=17, humidity=42))
            offset_ack = await ack
            self.assertEqual(offset_ack, 0)
            await stream.close()

        mock_grpc_stream.cancel()

    @for_both_sdks
    async def test_ingest_two_records_sequentially(self, sdk: SdkManager):
        """Test ingesting two records one after another."""
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

        with patch(sdk.get_grpc_override(), return_value=mock_channel):
            sdk_handle = sdk.create(SERVER_ENDPOINT)
            stream = await sdk_handle.create_stream(
                TableProperties(TABLE_NAME, test_row_pb2.AirQuality.DESCRIPTOR),
                self.options,
            )

            ack1 = await stream.ingest_record(test_row_pb2.AirQuality(device_name="device1", temp=17, humidity=42))
            offset1 = await ack1
            self.assertEqual(offset1, 0)

            ack2 = await stream.ingest_record(test_row_pb2.AirQuality(device_name="device2", temp=18, humidity=43))
            offset2 = await ack2
            self.assertEqual(offset2, 1)

            await stream.close()

        mock_grpc_stream.cancel()

    @for_both_sdks
    async def test_ingest_parallel(self, sdk: SdkManager):
        """Test parallel ingestion of multiple records."""
        calls_count = 0
        mock_grpc_stream = sdk.get_mock_class()(calls_count)
        NUM_RECORDS = 100

        def create_ephemeral_stream(generator, **kwargs):
            nonlocal calls_count
            nonlocal mock_grpc_stream
            calls_count += 1
            mock_grpc_stream = sdk.get_mock_class()(calls_count, generator)
            # Inject responses for all records
            for i in range(NUM_RECORDS):
                mock_grpc_stream.inject_response(False, InjectedRecordResponse(i, timeout_ms=1))
            return mock_grpc_stream

        mock_channel = MockGrpcChannel()
        mock_channel.injected_methods["/databricks.zerobus.Zerobus/EphemeralStream"] = create_ephemeral_stream

        with patch(sdk.get_grpc_override(), return_value=mock_channel):
            sdk_handle = sdk.create(SERVER_ENDPOINT)
            stream = await sdk_handle.create_stream(
                TableProperties(TABLE_NAME, test_row_pb2.AirQuality.DESCRIPTOR),
                self.options,
            )

            acks = [
                await stream.ingest_record(test_row_pb2.AirQuality(device_name=f"device_{i}", temp=i, humidity=i + 1))
                for i in range(NUM_RECORDS)
            ]
            acked_offsets = await asyncio.gather(*acks)
            self.assertEqual(acked_offsets, list(range(NUM_RECORDS)))

            await stream.close()

        mock_grpc_stream.cancel()

    @for_both_sdks
    async def test_ingestion_with_ack_callback(self, sdk: SdkManager):
        """Test ingestion with acknowledgment callback."""
        offset_acks = []

        def record_ack_callback(response):
            offset_acks.append(response.durability_ack_up_to_offset)

        calls_count = 0
        mock_grpc_stream = sdk.get_mock_class()(calls_count)
        NUM_RECORDS = 50

        def create_ephemeral_stream(generator, **kwargs):
            nonlocal calls_count
            nonlocal mock_grpc_stream
            calls_count += 1
            mock_grpc_stream = sdk.get_mock_class()(calls_count, generator)
            # Inject batched responses
            for i in range(0, NUM_RECORDS, 10):
                mock_grpc_stream.inject_response(
                    False, InjectedRecordResponse(min(i + 9, NUM_RECORDS - 1), timeout_ms=5)
                )
            return mock_grpc_stream

        mock_channel = MockGrpcChannel()
        mock_channel.injected_methods["/databricks.zerobus.Zerobus/EphemeralStream"] = create_ephemeral_stream

        options = StreamConfigurationOptions(
            ack_callback=record_ack_callback,
            recovery=False,
            token_factory=token_factory,
        )

        with patch(sdk.get_grpc_override(), return_value=mock_channel):
            sdk_handle = sdk.create(SERVER_ENDPOINT)
            stream = await sdk_handle.create_stream(
                TableProperties(TABLE_NAME, test_row_pb2.AirQuality.DESCRIPTOR),
                options,
            )

            for i in range(NUM_RECORDS):
                await stream.ingest_record(test_row_pb2.AirQuality(device_name=f"device_{i}", temp=i, humidity=i + 1))

            await stream.flush()
            self.assertTrue(len(offset_acks) > 0)

            max_offset = max(offset_acks)
            self.assertEqual(max_offset, NUM_RECORDS - 1)
            await stream.close()

        mock_grpc_stream.cancel()

    @for_both_sdks
    async def test_ingestion_with_record_callbacks(self, sdk: SdkManager):
        """Test adding callbacks to individual record acknowledgments."""
        offset_acks_callback = []

        def record_ack_callback(offset):
            offset_acks_callback.append(offset)

        calls_count = 0
        mock_grpc_stream = sdk.get_mock_class()(calls_count)
        NUM_RECORDS = 50
        CALLBACK_INTERVAL = 10

        def create_ephemeral_stream(generator, **kwargs):
            nonlocal calls_count
            nonlocal mock_grpc_stream
            calls_count += 1
            mock_grpc_stream = sdk.get_mock_class()(calls_count, generator)
            for i in range(NUM_RECORDS):
                mock_grpc_stream.inject_response(False, InjectedRecordResponse(i, timeout_ms=1))
            return mock_grpc_stream

        mock_channel = MockGrpcChannel()
        mock_channel.injected_methods["/databricks.zerobus.Zerobus/EphemeralStream"] = create_ephemeral_stream

        with patch(sdk.get_grpc_override(), return_value=mock_channel):
            sdk_handle = sdk.create(SERVER_ENDPOINT)
            stream = await sdk_handle.create_stream(
                TableProperties(TABLE_NAME, test_row_pb2.AirQuality.DESCRIPTOR),
                self.options,
            )

            for i in range(NUM_RECORDS):
                ack = await stream.ingest_record(
                    test_row_pb2.AirQuality(device_name=f"device_{i}", temp=i, humidity=i + 1)
                )
                if i % CALLBACK_INTERVAL == 0:
                    ack.add_done_callback(record_ack_callback)

            await stream.flush()
            self.assertEqual(len(offset_acks_callback), NUM_RECORDS // CALLBACK_INTERVAL)
            await stream.close()

        mock_grpc_stream.cancel()

    @for_both_sdks
    async def test_callback_error_does_not_fail_stream(self, sdk: SdkManager):
        """Test that errors in user callbacks don't fail the stream."""

        def bad_callback(offset):
            raise Exception("User callback error")

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

        with patch(sdk.get_grpc_override(), return_value=mock_channel):
            sdk_handle = sdk.create(SERVER_ENDPOINT)
            stream = await sdk_handle.create_stream(
                TableProperties(TABLE_NAME, test_row_pb2.AirQuality.DESCRIPTOR),
                self.options,
            )

            ack = await stream.ingest_record(test_row_pb2.AirQuality(device_name="device1", temp=0, humidity=1))
            ack.add_done_callback(bad_callback)
            await ack

            # Stream should still be open
            self.assertEqual(stream.get_state(), StreamState.OPENED)
            await stream.close()

        mock_grpc_stream.cancel()

    @for_both_sdks
    async def test_flush_and_close(self, sdk: SdkManager):
        """Test flushing records and closing stream."""
        calls_count = 0
        mock_grpc_stream = sdk.get_mock_class()(calls_count)
        NUM_RECORDS = 50

        def create_ephemeral_stream(generator, **kwargs):
            nonlocal calls_count
            nonlocal mock_grpc_stream
            calls_count += 1
            mock_grpc_stream = sdk.get_mock_class()(calls_count, generator)
            for i in range(NUM_RECORDS * 2):
                mock_grpc_stream.inject_response(False, InjectedRecordResponse(i, timeout_ms=1))
            return mock_grpc_stream

        mock_channel = MockGrpcChannel()
        mock_channel.injected_methods["/databricks.zerobus.Zerobus/EphemeralStream"] = create_ephemeral_stream

        with patch(sdk.get_grpc_override(), return_value=mock_channel):
            sdk_handle = sdk.create(SERVER_ENDPOINT)
            stream = await sdk_handle.create_stream(
                TableProperties(TABLE_NAME, test_row_pb2.AirQuality.DESCRIPTOR),
                self.options,
            )

            for i in range(NUM_RECORDS):
                await stream.ingest_record(test_row_pb2.AirQuality(device_name=f"device_{i}", temp=i, humidity=i + 1))

            await stream.flush()

            for i in range(NUM_RECORDS):
                await stream.ingest_record(test_row_pb2.AirQuality(device_name=f"device_{i}", temp=i, humidity=i + 1))

            await stream.close()

        mock_grpc_stream.cancel()

    @for_both_sdks
    async def test_ingest_after_close_raises_error(self, sdk: SdkManager):
        """Test that ingesting after close raises an error."""
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

        with patch(sdk.get_grpc_override(), return_value=mock_channel):
            sdk_handle = sdk.create(SERVER_ENDPOINT)
            stream = await sdk_handle.create_stream(
                TableProperties(TABLE_NAME, test_row_pb2.AirQuality.DESCRIPTOR),
                self.options,
            )

            await stream.close()

            with self.assertRaises(ZerobusException) as e:
                await stream.ingest_record(test_row_pb2.AirQuality(device_name="device", temp=17, humidity=42))
            self.assertIn("Cannot ingest records after stream is closed", str(e.exception))

        mock_grpc_stream.cancel()

    @for_both_sdks
    async def test_recreate_active_stream_raises_error(self, sdk: SdkManager):
        """Test that recreating an active stream raises an error."""
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

        with patch(sdk.get_grpc_override(), return_value=mock_channel):
            sdk_handle = sdk.create(SERVER_ENDPOINT)
            stream = await sdk_handle.create_stream(
                TableProperties(TABLE_NAME, test_row_pb2.AirQuality.DESCRIPTOR),
                self.options,
            )

            self.assertEqual(stream.get_state(), StreamState.OPENED)

            with self.assertRaises(ZerobusException) as e:
                await sdk_handle.recreate_stream(stream)

            self.assertIn("Stream is not closed", str(e.exception))
            await stream.close()

        mock_grpc_stream.cancel()

    @for_both_sdks
    async def test_ingest_then_immediate_close(self, sdk: SdkManager):
        """Test ingesting a record and immediately closing."""
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

        with patch(sdk.get_grpc_override(), return_value=mock_channel):
            sdk_handle = sdk.create(SERVER_ENDPOINT)
            stream = await sdk_handle.create_stream(
                TableProperties(TABLE_NAME, test_row_pb2.AirQuality.DESCRIPTOR),
                self.options,
            )

            await stream.ingest_record(test_row_pb2.AirQuality(device_name="device", temp=0, humidity=1))
            await stream.close()

        mock_grpc_stream.cancel()

    async def test_create_stream_with_custom_headers_provider_async(self):
        """Test creating async stream with custom headers provider that mimics default OAuth."""
        from zerobus.sdk.aio import ZerobusSdk as ZerobusSdkAsync

        calls_count = 0
        mock_grpc_stream = None

        def create_ephemeral_stream(generator, **kwargs):
            nonlocal calls_count, mock_grpc_stream
            calls_count += 1
            from tests.mock_grpc import MockAsyncStream

            mock_grpc_stream = MockAsyncStream(calls_count, generator)
            mock_grpc_stream.inject_response(False, InjectedRecordResponse(0, timeout_ms=10))
            return mock_grpc_stream

        mock_channel = MockGrpcChannel()
        mock_channel.injected_methods["/databricks.zerobus.Zerobus/EphemeralStream"] = create_ephemeral_stream

        with patch("grpc.aio.secure_channel", return_value=mock_channel):
            # Create custom headers provider that mimics default behavior
            custom_provider = CustomHeadersProviderForTest(token="test_token", table_name=TABLE_NAME)

            # Verify the headers match expected format
            headers = custom_provider.get_headers()
            self.assertEqual(len(headers), 2)
            self.assertEqual(headers[0], ("authorization", "Bearer test_token"))
            self.assertEqual(headers[1], ("x-databricks-zerobus-table-name", TABLE_NAME))

            # Create SDK and stream using custom provider
            sdk_instance = ZerobusSdkAsync(SERVER_ENDPOINT, unity_catalog_url="https://test.unity.catalog.url")
            table_props = TableProperties(TABLE_NAME, test_row_pb2.AirQuality.DESCRIPTOR)

            # Use create_stream_with_headers_provider instead of create_stream
            stream = await sdk_instance.create_stream_with_headers_provider(custom_provider, table_props, self.options)

            # Verify stream was created successfully
            self.assertIsNotNone(stream)
            self.assertEqual(stream.get_state(), StreamState.OPENED)

            # Test basic ingestion with custom provider
            ack = await stream.ingest_record(test_row_pb2.AirQuality(device_name="device", temp=17, humidity=42))
            offset = await ack

            self.assertEqual(offset, 0)

            # Close stream
            await stream.close()

        if mock_grpc_stream:
            mock_grpc_stream.cancel()

    def test_create_stream_with_custom_headers_provider_sync(self):
        """Test creating sync stream with custom headers provider that mimics default OAuth."""
        from zerobus.sdk.sync import ZerobusSdk as ZerobusSdkSync

        calls_count = 0
        mock_grpc_stream = None

        def create_ephemeral_stream(generator, **kwargs):
            nonlocal calls_count, mock_grpc_stream
            calls_count += 1
            from tests.mock_grpc import MockSyncStream

            mock_grpc_stream = MockSyncStream(calls_count, generator)
            mock_grpc_stream.inject_response(False, InjectedRecordResponse(0, timeout_ms=10))
            return mock_grpc_stream

        mock_channel = MockGrpcChannel()
        mock_channel.injected_methods["/databricks.zerobus.Zerobus/EphemeralStream"] = create_ephemeral_stream

        with patch("grpc.secure_channel", return_value=mock_channel):
            # Create custom headers provider that mimics default behavior
            custom_provider = CustomHeadersProviderForTest(token="test_token", table_name=TABLE_NAME)

            # Verify the headers match expected format
            headers = custom_provider.get_headers()
            self.assertEqual(len(headers), 2)
            self.assertEqual(headers[0], ("authorization", "Bearer test_token"))
            self.assertEqual(headers[1], ("x-databricks-zerobus-table-name", TABLE_NAME))

            # Create SDK and stream using custom provider
            sdk_instance = ZerobusSdkSync(SERVER_ENDPOINT, unity_catalog_url="https://test.unity.catalog.url")
            table_props = TableProperties(TABLE_NAME, test_row_pb2.AirQuality.DESCRIPTOR)

            # Use create_stream_with_headers_provider instead of create_stream
            stream = sdk_instance.create_stream_with_headers_provider(custom_provider, table_props, self.options)

            # Verify stream was created successfully
            self.assertIsNotNone(stream)
            self.assertEqual(stream.get_state(), StreamState.OPENED)

            # Test basic ingestion with custom provider
            ack = stream.ingest_record(test_row_pb2.AirQuality(device_name="device", temp=17, humidity=42))
            offset = ack.wait_for_ack()

            self.assertEqual(offset, 0)

            # Close stream
            stream.close()

        if mock_grpc_stream:
            mock_grpc_stream.cancel()


if __name__ == "__main__":
    unittest.main()
