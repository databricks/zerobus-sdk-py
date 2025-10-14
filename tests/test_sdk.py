import asyncio
import unittest
from unittest.mock import patch

import grpc

import tests.row_pb2 as test_row_pb2
from tests.mock_grpc import (CloseStreamSignalResponse, InjectedError,
                             InjectedRecordResponse, MockGrpcChannel,
                             MockSyncStream, SdkManager,
                             WrongCreateStreamResponse, for_both_sdks)
from zerobus.sdk import (NonRetriableException, StreamConfigurationOptions,
                         StreamState, TableProperties, ZerobusException,
                         ZerobusSdk)

SERVER_ENDPOINT = "SERVER_ENDPOINT"
TABLE_NAME = "catalog.schema.test_table"


def len_of_iterator(gen):
    return sum(1 for _ in gen)


def token_factory():
    return "TOKEN"


class TestZerobusEphemeralStream(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.options = StreamConfigurationOptions(
            recovery=True,
            recovery_timeout_ms=500,
            recovery_backoff_ms=500,
            recovery_retries=4,
            token_factory=token_factory,
        )

    def test_sync_grpc_stream_not_ready_in_timeout(self):
        calls_count = 0
        mock_grpc_stream = MockSyncStream(calls_count)

        def create_ephemeral_stream(generator, **kwargs):
            nonlocal calls_count
            nonlocal mock_grpc_stream
            calls_count += 1
            mock_grpc_stream = MockSyncStream(calls_count, generator)
            return mock_grpc_stream

        mock_channel = MockGrpcChannel(initial_state=grpc.ChannelConnectivity.IDLE)
        mock_channel.injected_methods["/databricks.zerobus.Zerobus/EphemeralStream"] = create_ephemeral_stream

        with (
            patch("grpc.secure_channel", return_value=mock_channel),
            patch("zerobus.sdk.sync.zerobus_sdk.get_zerobus_token", return_value="mock_token"),
        ):
            sdk_handle = ZerobusSdk(SERVER_ENDPOINT, unity_catalog_url="https://test.unity.catalog.url")
            try:
                sdk_handle.create_stream(
                    client_id="test_client_id",
                    client_secret="test_client_secret",
                    table_properties=TableProperties(TABLE_NAME, test_row_pb2.AirQuality.DESCRIPTOR),
                    options=self.options,
                )
                self.fail("Expected an exception to be raised")
            except Exception as e:
                self.assertIn("gRPC channel did not become ready", str(e))
        mock_grpc_stream.cancel()

    @for_both_sdks
    async def test_create_stream_failed_with_recovery(self, sdk: SdkManager):
        calls_count = 0

        def create_ephemeral_stream(generator, **kwargs):
            nonlocal calls_count
            calls_count += 1
            raise Exception("TestZerobusSdkRecovery: Test error")

        mock_channel = MockGrpcChannel()
        mock_channel.injected_methods["/databricks.zerobus.Zerobus/EphemeralStream"] = create_ephemeral_stream

        with patch(sdk.get_grpc_override(), return_value=mock_channel):
            try:
                sdk_handle = sdk.create(SERVER_ENDPOINT)
                stream = await sdk_handle.create_stream(  # noqa: F841
                    TableProperties(TABLE_NAME, test_row_pb2.AirQuality.DESCRIPTOR),
                    self.options,
                )

                # fail test
                self.fail("Expected an exception to be raised")
            except Exception as e:
                self.assertEqual(str(e), "Failed to create a stream: TestZerobusSdkRecovery: Test error")
                self.assertEqual(calls_count, self.options.recovery_retries)

    @for_both_sdks
    async def test_stream_no_stream_creation_response(self, sdk: SdkManager):
        calls_count = 0

        def create_ephemeral_stream(generator, **kwargs):
            nonlocal calls_count
            calls_count += 1
            mock_grpc_stream = sdk.get_mock_class()(calls_count, generator)
            mock_grpc_stream.inject_skip_create_stream()
            return mock_grpc_stream

        mock_channel = MockGrpcChannel()
        mock_channel.injected_methods["/databricks.zerobus.Zerobus/EphemeralStream"] = create_ephemeral_stream

        with patch(sdk.get_grpc_override(), return_value=mock_channel):
            try:
                sdk_handle = sdk.create(SERVER_ENDPOINT)
                stream = await sdk_handle.create_stream(  # noqa: F841
                    TableProperties(TABLE_NAME, test_row_pb2.AirQuality.DESCRIPTOR),
                    self.options,
                )

                # fail test
                self.fail("Expected an exception to be raised")
            except Exception as e:
                self.assertEqual(str(e), "Failed to create a stream: Stream operation timed out...")
                self.assertEqual(calls_count, self.options.recovery_retries)

    @for_both_sdks
    async def test_end_stream_before_creating_stream(self, sdk: SdkManager):
        calls_count = 0

        def create_ephemeral_stream(generator, **kwargs):
            nonlocal calls_count
            calls_count += 1
            mock_grpc_stream = sdk.get_mock_class()(calls_count, generator)
            mock_grpc_stream.inject_create_stream_response(True, sdk.get_stop_iter())
            return mock_grpc_stream

        mock_channel = MockGrpcChannel()
        mock_channel.injected_methods["/databricks.zerobus.Zerobus/EphemeralStream"] = create_ephemeral_stream

        with patch(sdk.get_grpc_override(), return_value=mock_channel):
            try:
                sdk_handle = sdk.create(SERVER_ENDPOINT)
                stream = await sdk_handle.create_stream(  # noqa: F841
                    TableProperties(TABLE_NAME, test_row_pb2.AirQuality.DESCRIPTOR),
                    self.options,
                )

                # fail test
                self.fail("Expected an exception to be raised")
            except Exception as e:
                self.assertEqual(
                    str(e),
                    "Failed to create a stream: No response received from the server on stream creation",
                )
                self.assertEqual(calls_count, self.options.recovery_retries)

    @for_both_sdks
    async def test_wrong_response_from_server_on_create_stream(self, sdk: SdkManager):
        calls_count = 0

        def create_ephemeral_stream(generator, **kwargs):
            nonlocal calls_count
            calls_count += 1
            stream = sdk.get_mock_class()(calls_count, generator)
            stream.inject_create_stream_response(False, WrongCreateStreamResponse(timeout_ms=150))
            return stream

        mock_channel = MockGrpcChannel()
        mock_channel.injected_methods["/databricks.zerobus.Zerobus/EphemeralStream"] = create_ephemeral_stream

        with patch(sdk.get_grpc_override(), return_value=mock_channel):
            try:
                sdk_handle = sdk.create(SERVER_ENDPOINT)
                stream = await sdk_handle.create_stream(  # noqa: F841
                    TableProperties(TABLE_NAME, test_row_pb2.AirQuality.DESCRIPTOR),
                    self.options,
                )

                # fail test
                self.fail("Expected an exception to be raised")
            except Exception as e:
                self.assertEqual(
                    str(e),
                    "Failed to create a stream: No response received from the server on stream creation",
                )
                self.assertEqual(calls_count, self.options.recovery_retries)


class TestZerobusFailedReceiver(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.options = StreamConfigurationOptions(
            recovery=True,
            recovery_timeout_ms=500,
            recovery_backoff_ms=500,
            recovery_retries=2,
            max_inflight_records=150,
            flush_timeout_ms=10000,
            server_lack_of_ack_timeout_ms=2500,
            token_factory=token_factory,
        )

    @for_both_sdks
    async def test_recovery_after_receiver_fails(self, sdk: SdkManager):
        calls_count = 0
        mock_grpc_stream = sdk.get_mock_class()(calls_count)

        def create_ephemeral_stream(generator, **kwargs):
            nonlocal calls_count
            nonlocal mock_grpc_stream

            calls_count += 1
            mock_grpc_stream = sdk.get_mock_class()(calls_count, generator)

            if calls_count == 1:
                # ack first 15 records and then fail
                mock_grpc_stream.inject_response(False, InjectedRecordResponse(15, timeout_ms=5))
                mock_grpc_stream.inject_response(True, Exception("test_receiver_fails_recovery: Receive test error"))
            elif calls_count == 2:
                mock_grpc_stream.inject_response(False, InjectedRecordResponse(10, timeout_ms=35))
                mock_grpc_stream.inject_response(True, Exception("test_receiver_fails_recovery: Receive test error"))
            else:
                # since new stream is created and first 27 (16 + 11) records are acked
                # now 0 offset is 28th record
                mock_grpc_stream.inject_response(False, InjectedRecordResponse(40, timeout_ms=40))
                mock_grpc_stream.inject_response(False, InjectedRecordResponse(72, timeout_ms=0))

            return mock_grpc_stream

        mock_channel = MockGrpcChannel()
        mock_channel.injected_methods["/databricks.zerobus.Zerobus/EphemeralStream"] = create_ephemeral_stream

        with patch(sdk.get_grpc_override(), return_value=mock_channel):
            try:
                sdk_handle = sdk.create(SERVER_ENDPOINT)
                stream = await sdk_handle.create_stream(
                    TableProperties(TABLE_NAME, test_row_pb2.AirQuality.DESCRIPTOR),
                    self.options,
                )

                self.assertEqual(calls_count, 1)

                # ingesting records (with 1 second break on 26th record)
                acks = [
                    await stream.ingest_record(test_row_pb2.AirQuality(device_name="my_device", temp=i, humidity=i))
                    for i in range(25)
                ]

                await asyncio.sleep(1)

                acks += [
                    await stream.ingest_record(test_row_pb2.AirQuality(device_name="my_device", temp=i, humidity=i))
                    for i in range(25, 100)
                ]

                await stream.flush()

                await asyncio.gather(*acks)

                self.assertEqual(mock_grpc_stream.max_offset_sent, 72)
                self.assertEqual(mock_grpc_stream.num_of_writes(), 73)
                self.assertEqual(calls_count, 3)

                mock_grpc_stream.inject_response(False, InjectedRecordResponse(92, timeout_ms=1000))

                for i in range(20):
                    await stream.ingest_record(test_row_pb2.AirQuality(device_name="my_device", temp=17, humidity=42))

                await stream.close()

                self.assertEqual(mock_grpc_stream.max_offset_sent, 92)
                self.assertEqual(mock_grpc_stream.num_of_writes(), 93)
                self.assertEqual(len_of_iterator(stream.get_unacked_records()), 0)
            except Exception as e:
                self.fail(f"Expected no exception to be raised, but got: {e}")

        mock_grpc_stream.cancel()

    @for_both_sdks
    async def test_server_unresponsive(self, sdk: SdkManager):
        calls_count = 0
        mock_grpc_stream = sdk.get_mock_class()(calls_count)

        def create_ephemeral_stream(generator, **kwargs):
            nonlocal calls_count
            nonlocal mock_grpc_stream

            calls_count += 1
            mock_grpc_stream = sdk.get_mock_class()(calls_count, generator)
            # no responses injected so receiver will be blocked
            return mock_grpc_stream

        mock_channel = MockGrpcChannel()
        mock_channel.injected_methods["/databricks.zerobus.Zerobus/EphemeralStream"] = create_ephemeral_stream

        with patch(sdk.get_grpc_override(), return_value=mock_channel):
            try:
                sdk_handle = sdk.create(SERVER_ENDPOINT)

                stream = await sdk_handle.create_stream(
                    TableProperties(TABLE_NAME, test_row_pb2.AirQuality.DESCRIPTOR),
                    self.options,
                )

                self.assertEqual(calls_count, 1)

                # ingesting records
                ingest_failed_count = 0

                # ingst_record should succeed until max_inflight_records is reached
                all_acks = [
                    await stream.ingest_record(test_row_pb2.AirQuality(device_name="my_device", temp=i, humidity=i))
                    for i in range(self.options.max_inflight_records)
                ]

                # ingest_record should fail after max_inflight_records is reached
                for i in range(self.options.max_inflight_records, 200):
                    try:
                        await stream.ingest_record(test_row_pb2.AirQuality(device_name="my_device", temp=i, humidity=i))

                        self.fail("Expected exception to be raised...")
                    except Exception as e:
                        ingest_failed_count += 1
                        self.assertTrue("Cannot ingest records after stream is closed or before it's opened." in str(e))

                self.assertEqual(ingest_failed_count, 200 - self.options.max_inflight_records)

                await stream.close()
                self.fail("Expected exception to be raised...")
            except Exception as e:
                # all acks should fail
                for ack in all_acks:
                    try:
                        await ack
                    except Exception as ex:
                        self.assertTrue("Server is unresponsive. Stream failed." in str(ex))

                self.assertEqual(str(e), "Stream failed. Cannot close.")
                self.assertEqual(len(all_acks), 150)

                unacked_records = stream.get_unacked_records()
                self.assertEqual(len_of_iterator(unacked_records), 150)
                [self.assertEqual(unacked_record.temp, index) for index, unacked_record in enumerate(unacked_records)]

        mock_grpc_stream.cancel()

    @for_both_sdks
    async def test_receiver_fails_during_close(self, sdk: SdkManager):
        calls_count = 0
        mock_grpc_stream = sdk.get_mock_class()(calls_count)

        def create_ephemeral_stream(generator, **kwargs):
            nonlocal calls_count
            nonlocal mock_grpc_stream

            calls_count += 1
            mock_grpc_stream = sdk.get_mock_class()(calls_count, generator)

            if calls_count == 1:
                mock_grpc_stream.inject_response(False, InjectedRecordResponse(15, timeout_ms=1000))
                mock_grpc_stream.inject_response(True, Exception("test_receiver_fails_recovery: Receive test error"))

            return mock_grpc_stream

        mock_channel = MockGrpcChannel()
        mock_channel.injected_methods["/databricks.zerobus.Zerobus/EphemeralStream"] = create_ephemeral_stream

        with patch(sdk.get_grpc_override(), return_value=mock_channel):
            try:
                sdk_handle = sdk.create(SERVER_ENDPOINT)
                stream = await sdk_handle.create_stream(
                    TableProperties(TABLE_NAME, test_row_pb2.AirQuality.DESCRIPTOR),
                    self.options,
                )

                self.assertEqual(calls_count, 1)

                # ingesting records
                success_acks = [
                    await stream.ingest_record(test_row_pb2.AirQuality(device_name="my_device", temp=i, humidity=i))
                    for i in range(16)
                ]
                failed_acks = [
                    await stream.ingest_record(test_row_pb2.AirQuality(device_name="my_device", temp=i, humidity=i))
                    for i in range(16, 100)
                ]

                await stream.close()
                self.fail("Expected exception to be raised ... ")

            except Exception as e:
                # Wait for success acks to be received
                await asyncio.gather(*success_acks)

                # Wait for failed acks to be received
                for ack in failed_acks:
                    try:
                        await ack
                    except Exception as ex:
                        self.assertEqual(
                            str(ex),
                            "Error happened in receiving records: test_receiver_fails_recovery: Receive test error",
                        )

                self.assertEqual(str(e), "Stream failed with unacknowledged records. Cannot flush.")
                self.assertEqual(calls_count, 1)

                self.assertEqual(len(failed_acks), 84)
                self.assertEqual(len(success_acks), 16)

                unacked_records = stream.get_unacked_records()
                self.assertEqual(len_of_iterator(unacked_records), 84)
                [
                    self.assertEqual(unacked_record.temp, 16 + index)
                    for index, unacked_record in enumerate(unacked_records)
                ]

        mock_grpc_stream.cancel()

    @for_both_sdks
    async def test_close_stream_after_failed_stream(self, sdk: SdkManager):
        calls_count = 0
        mock_grpc_stream = sdk.get_mock_class()(calls_count)

        def create_ephemeral_stream(generator, **kwargs):
            nonlocal calls_count
            nonlocal mock_grpc_stream

            calls_count += 1
            mock_grpc_stream = sdk.get_mock_class()(calls_count, generator)

            if calls_count == 1:
                mock_grpc_stream.inject_response(True, Exception("test_receiver_fails_recovery: Receive test error"))
            else:
                mock_grpc_stream.inject_response(False, InjectedRecordResponse(99, timeout_ms=0))

            return mock_grpc_stream

        mock_channel = MockGrpcChannel()
        mock_channel.injected_methods["/databricks.zerobus.Zerobus/EphemeralStream"] = create_ephemeral_stream

        with patch(sdk.get_grpc_override(), return_value=mock_channel):
            try:
                sdk_handle = sdk.create(SERVER_ENDPOINT)
                options_changed = self.options
                options_changed.flush_timeout_ms = 30000
                options_changed.server_lack_of_ack_timeout_ms = 10000
                stream = await sdk_handle.create_stream(
                    TableProperties(TABLE_NAME, test_row_pb2.AirQuality.DESCRIPTOR),
                    options_changed,
                )

                self.assertEqual(calls_count, 1)

                # ingesting records
                acks = [
                    await stream.ingest_record(test_row_pb2.AirQuality(device_name="my_device", temp=i, humidity=i))
                    for i in range(100)
                ]

                await asyncio.sleep(1)
                await stream.close()

                await asyncio.gather(*acks)

                self.assertEqual(mock_grpc_stream.max_offset_sent, 99)
                self.assertEqual(mock_grpc_stream.num_of_writes(), 100)
                self.assertEqual(calls_count, 2)
                self.assertEqual(len_of_iterator(stream.get_unacked_records()), 0)

            except Exception as e:
                self.fail(f"Expected no exception to be raised but got {e}... ")

        mock_grpc_stream.cancel()

    @for_both_sdks
    async def test_recovery_receiver_blocked(self, sdk: SdkManager):
        calls_count = 0
        mock_grpc_stream = sdk.get_mock_class()(calls_count)

        def create_ephemeral_stream(generator, **kwargs):
            nonlocal calls_count
            nonlocal mock_grpc_stream

            calls_count += 1
            mock_grpc_stream = sdk.get_mock_class()(calls_count, generator)
            # no responses injected so receiver will be blocked
            return mock_grpc_stream

        mock_channel = MockGrpcChannel()
        mock_channel.injected_methods["/databricks.zerobus.Zerobus/EphemeralStream"] = create_ephemeral_stream

        with patch(sdk.get_grpc_override(), return_value=mock_channel):
            try:
                sdk_handle = sdk.create(SERVER_ENDPOINT)

                options_changed = self.options
                options_changed.recovery_retries = 2
                options_changed.server_lack_of_ack_timeout_ms = 2000
                stream = await sdk_handle.create_stream(
                    TableProperties(TABLE_NAME, test_row_pb2.AirQuality.DESCRIPTOR),
                    options_changed,
                )

                self.assertEqual(calls_count, 1)

                # ingesting records
                all_acks = [
                    await stream.ingest_record(test_row_pb2.AirQuality(device_name="my_device", temp=i, humidity=i))
                    for i in range(50)
                ]

                await asyncio.sleep(10)
                await stream.close()
                self.fail("Expected exception to be raised...")
            except Exception as e:
                for ack in all_acks:
                    try:
                        await ack
                    except Exception as ex:
                        self.assertTrue("Server is unresponsive. Stream failed." in str(ex))

                self.assertEqual(str(e), "Stream failed. Cannot close.")
                self.assertEqual(len(all_acks), 50)

                unacked_records = stream.get_unacked_records()
                self.assertEqual(len_of_iterator(unacked_records), 50)
                [self.assertEqual(unacked_record.temp, index) for index, unacked_record in enumerate(unacked_records)]

                self.assertEqual(calls_count, 3)

        mock_grpc_stream.cancel()

    @for_both_sdks
    async def test_resending_after_receiver_blocked(self, sdk: SdkManager):
        calls_count = 0
        mock_grpc_stream = sdk.get_mock_class()(calls_count)

        def create_ephemeral_stream(generator, **kwargs):
            nonlocal calls_count
            nonlocal mock_grpc_stream

            calls_count += 1
            mock_grpc_stream = sdk.get_mock_class()(calls_count, generator)

            if calls_count > 1:
                mock_grpc_stream.inject_response(False, InjectedRecordResponse(14, timeout_ms=1000))
                mock_grpc_stream.inject_response(False, InjectedRecordResponse(20, timeout_ms=1700))
                mock_grpc_stream.inject_response(False, InjectedRecordResponse(30, timeout_ms=200))
                mock_grpc_stream.inject_response(False, InjectedRecordResponse(43, timeout_ms=400))
                mock_grpc_stream.inject_response(False, InjectedRecordResponse(49, timeout_ms=0))

            # no responses injected so receiver will be blocked
            return mock_grpc_stream

        mock_channel = MockGrpcChannel()
        mock_channel.injected_methods["/databricks.zerobus.Zerobus/EphemeralStream"] = create_ephemeral_stream

        with patch(sdk.get_grpc_override(), return_value=mock_channel):
            try:
                sdk_handle = sdk.create(SERVER_ENDPOINT)

                options_changed = self.options
                options_changed.max_inflight_records = 15
                stream = await sdk_handle.create_stream(
                    TableProperties(TABLE_NAME, test_row_pb2.AirQuality.DESCRIPTOR),
                    options_changed,
                )

                self.assertEqual(calls_count, 1)

                # ingesting records
                all_acks = [
                    await stream.ingest_record(test_row_pb2.AirQuality(device_name="my_device", temp=i, humidity=i))
                    for i in range(50)
                ]

                await asyncio.sleep(7)
                await stream.flush()

                await asyncio.gather(*all_acks)

                await stream.close()

                self.assertEqual(mock_grpc_stream.max_offset_sent, 49)
                self.assertEqual(mock_grpc_stream.num_of_writes(), 50)
                self.assertEqual(len_of_iterator(stream.get_unacked_records()), 0)
            except Exception as e:
                self.fail(f"Expected no exception to be raised but got {e}... ")

        mock_grpc_stream.cancel()

    @for_both_sdks
    async def test_resending_after_receiver_blocked_in_the_middle_of_stream(self, sdk: SdkManager):
        calls_count = 0
        mock_grpc_stream = sdk.get_mock_class()(calls_count)

        def create_ephemeral_stream(generator, **kwargs):
            nonlocal calls_count
            nonlocal mock_grpc_stream

            calls_count += 1
            mock_grpc_stream = sdk.get_mock_class()(calls_count, generator)

            if calls_count == 1:
                mock_grpc_stream.inject_response(False, InjectedRecordResponse(5, timeout_ms=1000))
            else:
                mock_grpc_stream.inject_response(False, InjectedRecordResponse(14, timeout_ms=1000))
                mock_grpc_stream.inject_response(False, InjectedRecordResponse(20, timeout_ms=1700))
                mock_grpc_stream.inject_response(False, InjectedRecordResponse(30, timeout_ms=200))
                mock_grpc_stream.inject_response(False, InjectedRecordResponse(43, timeout_ms=400))

            # no responses injected so receiver will be blocked
            return mock_grpc_stream

        mock_channel = MockGrpcChannel()
        mock_channel.injected_methods["/databricks.zerobus.Zerobus/EphemeralStream"] = create_ephemeral_stream

        with patch(sdk.get_grpc_override(), return_value=mock_channel):
            try:
                sdk_handle = sdk.create(SERVER_ENDPOINT)

                options_changed = self.options
                options_changed.max_inflight_records = 15
                stream = await sdk_handle.create_stream(
                    TableProperties(TABLE_NAME, test_row_pb2.AirQuality.DESCRIPTOR),
                    options_changed,
                )

                self.assertEqual(calls_count, 1)

                # ingesting records
                all_acks = [
                    await stream.ingest_record(test_row_pb2.AirQuality(device_name="my_device", temp=i, humidity=i))
                    for i in range(50)
                ]

                await asyncio.sleep(7)
                await stream.flush()

                await asyncio.gather(*all_acks)

                await stream.close()

                self.assertEqual(mock_grpc_stream.max_offset_sent, 43)
                self.assertEqual(mock_grpc_stream.num_of_writes(), 44)
                self.assertEqual(len_of_iterator(stream.get_unacked_records()), 0)
            except Exception as e:
                self.fail(f"Expected no exception to be raised but got {e}... ")

        mock_grpc_stream.cancel()

    @for_both_sdks
    async def test_flush_waiting_for_acks_from_slower_receiver(self, sdk: SdkManager):
        calls_count = 0
        mock_grpc_stream = sdk.get_mock_class()(calls_count)

        def create_ephemeral_stream(generator, **kwargs):
            nonlocal calls_count
            nonlocal mock_grpc_stream

            calls_count += 1
            mock_grpc_stream = sdk.get_mock_class()(calls_count, generator)
            mock_grpc_stream.inject_response(False, InjectedRecordResponse(99, timeout_ms=2000))
            return mock_grpc_stream

        mock_channel = MockGrpcChannel()
        mock_channel.injected_methods["/databricks.zerobus.Zerobus/EphemeralStream"] = create_ephemeral_stream

        with patch(sdk.get_grpc_override(), return_value=mock_channel):
            try:
                sdk_handle = sdk.create(SERVER_ENDPOINT)

                options_changed = self.options
                options_changed.flush_timeout_ms = 30000
                options_changed.server_lack_of_ack_timeout_ms = 10000
                stream = await sdk_handle.create_stream(
                    TableProperties(TABLE_NAME, test_row_pb2.AirQuality.DESCRIPTOR),
                    options_changed,
                )

                self.assertEqual(calls_count, 1)

                # ingesting records
                acks = [
                    await stream.ingest_record(test_row_pb2.AirQuality(device_name="my_device", temp=i, humidity=i))
                    for i in range(100)
                ]

                await stream.close()

                await asyncio.gather(*acks)

                self.assertEqual(mock_grpc_stream.max_offset_sent, 99)
                self.assertEqual(mock_grpc_stream.num_of_writes(), 100)
                self.assertEqual(len_of_iterator(stream.get_unacked_records()), 0)
            except Exception as e:
                self.fail(f"Expected no exception to be raised, but got: {e}")

        mock_grpc_stream.cancel()

    @for_both_sdks
    async def test_cancelled_receiver_error_handling(self, sdk: SdkManager):
        calls_count = 0
        mock_grpc_stream = sdk.get_mock_class()(calls_count)

        def create_ephemeral_stream(generator, **kwargs):
            nonlocal calls_count
            nonlocal mock_grpc_stream

            calls_count += 1
            mock_grpc_stream = sdk.get_mock_class()(calls_count, generator)
            mock_grpc_stream.inject_response(False, InjectedRecordResponse(99, timeout_ms=5000))
            return mock_grpc_stream

        mock_channel = MockGrpcChannel()
        mock_channel.injected_methods["/databricks.zerobus.Zerobus/EphemeralStream"] = create_ephemeral_stream

        with patch(sdk.get_grpc_override(), return_value=mock_channel):
            try:
                sdk_handle = sdk.create(SERVER_ENDPOINT)

                options_changed = self.options
                options_changed.server_lack_of_ack_timeout_ms = 20000
                options_changed.flush_timeout_ms = 60000
                stream = await sdk_handle.create_stream(
                    TableProperties(TABLE_NAME, test_row_pb2.AirQuality.DESCRIPTOR),
                    options_changed,
                )

                self.assertEqual(calls_count, 1)

                # ingesting records
                acks = [
                    await stream.ingest_record(test_row_pb2.AirQuality(device_name="my_device", temp=i, humidity=i))
                    for i in range(100)
                ]

                # cancel receiver task to simulate receiver failure
                stream.stop_receiver_task()

                await stream.flush()
                await asyncio.gather(*acks)

                self.assertEqual(len_of_iterator(stream.get_unacked_records()), 0)

                self.assertEqual(stream.get_state(), StreamState.OPENED)
                await stream.close()

            except Exception as e:
                self.fail(f"Expected no exception to be raised, but got: {e}")

        mock_grpc_stream.cancel()

    @for_both_sdks
    async def test_close_stream_signal_with_auto_close_recovery_enabled(self, sdk: SdkManager):
        calls_count = 0
        mock_grpc_stream = sdk.get_mock_class()(calls_count)

        def create_ephemeral_stream(generator, **kwargs):
            nonlocal calls_count
            nonlocal mock_grpc_stream

            calls_count += 1
            mock_grpc_stream = sdk.get_mock_class()(calls_count, generator)

            if calls_count == 1:
                # Inject some acks
                mock_grpc_stream.inject_response(False, InjectedRecordResponse(24, timeout_ms=200))
                # Inject close stream signal with 2 second duration
                mock_grpc_stream.inject_response(False, CloseStreamSignalResponse(2, timeout_ms=100))
            else:
                # Recovery stream - handle remaining records
                mock_grpc_stream.inject_response(False, InjectedRecordResponse(24, timeout_ms=100))

            return mock_grpc_stream

        mock_channel = MockGrpcChannel()
        mock_channel.injected_methods["/databricks.zerobus.Zerobus/EphemeralStream"] = create_ephemeral_stream

        with patch(sdk.get_grpc_override(), return_value=mock_channel):
            try:
                sdk_handle = sdk.create(SERVER_ENDPOINT)
                stream = await sdk_handle.create_stream(
                    TableProperties(TABLE_NAME, test_row_pb2.AirQuality.DESCRIPTOR), self.options
                )

                self.assertEqual(calls_count, 1)

                # Ingest records
                acks = [
                    await stream.ingest_record(test_row_pb2.AirQuality(device_name="my_device", temp=i, humidity=i))
                    for i in range(50)
                ]
                await asyncio.sleep(2)
                await stream.flush()
                await asyncio.gather(*acks)

                await stream.close()

                self.assertEqual(len_of_iterator(stream.get_unacked_records()), 0)
                self.assertEqual(calls_count, 2)  # Recovery should have occurred due to Error code 6002
            except Exception as e:
                self.fail(f"Expected no exception to be raised, but got: {e}")

        mock_grpc_stream.cancel()

    @for_both_sdks
    async def test_close_stream_signal_with_auto_close_recovery_disabled(self, sdk: SdkManager):
        calls_count = 0
        mock_grpc_stream = sdk.get_mock_class()(calls_count)

        def create_ephemeral_stream(generator, **kwargs):
            nonlocal calls_count
            nonlocal mock_grpc_stream

            calls_count += 1
            mock_grpc_stream = sdk.get_mock_class()(calls_count, generator)

            mock_grpc_stream.inject_response(False, CloseStreamSignalResponse(2, timeout_ms=100))
            # Then inject some acks
            mock_grpc_stream.inject_response(False, InjectedRecordResponse(24, timeout_ms=200))
            # Then inject Error code 6002 after the duration (2000ms timeout)
            mock_grpc_stream.inject_response(True, InjectedError("Error code 6002", timeout_ms=2000))

            return mock_grpc_stream

        mock_channel = MockGrpcChannel()
        mock_channel.injected_methods["/databricks.zerobus.Zerobus/EphemeralStream"] = create_ephemeral_stream

        # Disable recovery
        options = StreamConfigurationOptions(recovery=False, token_factory=token_factory)
        with patch(sdk.get_grpc_override(), return_value=mock_channel):
            try:
                sdk_handle = sdk.create(SERVER_ENDPOINT)
                stream = await sdk_handle.create_stream(
                    TableProperties(TABLE_NAME, test_row_pb2.AirQuality.DESCRIPTOR), options
                )

                self.assertEqual(calls_count, 1)

                # Ingest records
                acks = [  # noqa: F841
                    await stream.ingest_record(test_row_pb2.AirQuality(device_name="my_device", temp=i, humidity=i))
                    for i in range(50)
                ]
                await asyncio.sleep(2)

                try:
                    await stream.flush()
                    self.fail("Excepted flush to throw an exception")
                except Exception:
                    # The remaining 25 records would remain unacked
                    self.assertEqual(len_of_iterator(stream.get_unacked_records()), 25)
                    self.assertEqual(calls_count, 1)
            except Exception as e:
                self.fail(f"Expected no exception to be raised, but got: {e}")

        mock_grpc_stream.cancel()


class TestRecreateStreamAfterFailure(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.options = StreamConfigurationOptions(recovery=False, max_inflight_records=150, token_factory=token_factory)

    @for_both_sdks
    async def test_recreate_stream_after_failure(self, sdk: SdkManager):
        calls_count = 0
        mock_grpc_stream = sdk.get_mock_class()(calls_count)

        def create_ephemeral_stream(generator, **kwargs):
            nonlocal calls_count
            nonlocal mock_grpc_stream

            calls_count += 1
            mock_grpc_stream = sdk.get_mock_class()(calls_count, generator)

            if calls_count == 1:
                # ack first 15 records and then fail
                mock_grpc_stream.inject_response(False, InjectedRecordResponse(14, timeout_ms=50))
                mock_grpc_stream.inject_response(True, Exception("test_receiver_fails_recovery: Receive test error"))
            else:
                mock_grpc_stream.inject_response(False, InjectedRecordResponse(84, timeout_ms=10))

            return mock_grpc_stream

        mock_channel = MockGrpcChannel()
        mock_channel.injected_methods["/databricks.zerobus.Zerobus/EphemeralStream"] = create_ephemeral_stream

        with patch(sdk.get_grpc_override(), return_value=mock_channel):
            try:
                sdk_handle = sdk.create(SERVER_ENDPOINT)
                stream = await sdk_handle.create_stream(
                    TableProperties(TABLE_NAME, test_row_pb2.AirQuality.DESCRIPTOR),
                    self.options,
                )

                # ingesting records
                acks = [
                    await stream.ingest_record(test_row_pb2.AirQuality(device_name="my_device", temp=i, humidity=42))
                    for i in range(100)
                ]

                try:
                    await stream.flush()
                except Exception as e:
                    self.assertEqual(str(e), "Stream failed with unacknowledged records. Cannot flush.")
                    for ack in acks:
                        try:
                            await ack
                        except Exception as e:
                            self.assertEqual(
                                str(e),
                                "Error happened in receiving records: test_receiver_fails_recovery: Receive test error",
                            )

                new_stream = await sdk_handle.recreate_stream(stream)
                await new_stream.flush()

                self.assertEqual(mock_grpc_stream.max_offset_sent, 84)
                self.assertEqual(mock_grpc_stream.num_of_writes(), 85)

                await new_stream.close()
                self.assertEqual(len_of_iterator(new_stream.get_unacked_records()), 0)

                await stream.close()
            except Exception as e:
                self.assertEqual(str(e), "Stream failed. Cannot close.")

        mock_grpc_stream.cancel()


class TestSenderFailureRecovery(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.options = StreamConfigurationOptions(
            recovery=True,
            recovery_timeout_ms=500,
            recovery_backoff_ms=500,
            recovery_retries=2,
            max_inflight_records=150,
            token_factory=token_factory,
        )

    @for_both_sdks
    async def test_write_failure_during_flush(self, sdk: SdkManager):
        calls_count = 0
        mock_grpc_stream = sdk.get_mock_class()(calls_count)

        def create_ephemeral_stream(generator, **kwargs):
            nonlocal calls_count
            nonlocal mock_grpc_stream

            calls_count += 1
            mock_grpc_stream = sdk.get_mock_class()(calls_count, generator)

            if calls_count == 1:
                # ack first 50 records and then fail
                mock_grpc_stream.inject_response(False, InjectedRecordResponse(49, timeout_ms=2000))
                mock_grpc_stream.inject_response(True, Exception("test_receiver_fails_recovery: Write test error"))

            return mock_grpc_stream

        mock_channel = MockGrpcChannel()
        mock_channel.injected_methods["/databricks.zerobus.Zerobus/EphemeralStream"] = create_ephemeral_stream

        with patch(sdk.get_grpc_override(), return_value=mock_channel):
            try:
                sdk_handle = sdk.create(SERVER_ENDPOINT)
                options_changed = self.options
                options_changed.max_inflight_records = 300
                stream = await sdk_handle.create_stream(
                    TableProperties(TABLE_NAME, test_row_pb2.AirQuality.DESCRIPTOR),
                    self.options,
                )

                # ingesting records
                success_acks = [
                    await stream.ingest_record(test_row_pb2.AirQuality(device_name="my_device", temp=i, humidity=i))
                    for i in range(50)
                ]
                failed_acks = [
                    await stream.ingest_record(test_row_pb2.AirQuality(device_name="my_device", temp=i, humidity=i))
                    for i in range(50, 300)
                ]

                await stream.close()
                self.fail("Expected exception to be raised")
            except Exception as e:
                # Wait for success acks to be received
                await asyncio.gather(*success_acks)

                # Wait for failed acks to be received
                for ack in failed_acks:
                    try:
                        await ack
                    except Exception as ex:
                        self.assertEqual(
                            str(ex),
                            "Error happened in receiving records: test_receiver_fails_recovery: Write test error",
                        )

                self.assertEqual(str(e), "Stream failed with unacknowledged records. Cannot flush.")
                self.assertEqual(calls_count, 1)

                self.assertEqual(len(failed_acks), 250)

                unacked_records = stream.get_unacked_records()
                self.assertEqual(len_of_iterator(unacked_records), 250)
                [
                    self.assertEqual(unacked_record.temp, 50 + index)
                    for index, unacked_record in enumerate(unacked_records)
                ]

        mock_grpc_stream.cancel()

    @for_both_sdks
    async def test_close_after_write_failure(self, sdk: SdkManager):
        calls_count = 0
        mock_grpc_stream = sdk.get_mock_class()(calls_count)

        def create_ephemeral_stream(generator, **kwargs):
            nonlocal calls_count
            nonlocal mock_grpc_stream

            calls_count += 1
            mock_grpc_stream = sdk.get_mock_class()(calls_count, generator)

            if calls_count == 1:
                mock_grpc_stream.inject_response(True, Exception("test_receiver_fails_recovery: Write test error"))
            else:
                mock_grpc_stream.inject_response(False, InjectedRecordResponse(29, timeout_ms=10))

            return mock_grpc_stream

        mock_channel = MockGrpcChannel()
        mock_channel.injected_methods["/databricks.zerobus.Zerobus/EphemeralStream"] = create_ephemeral_stream

        with patch(sdk.get_grpc_override(), return_value=mock_channel):
            try:
                sdk_handle = sdk.create(SERVER_ENDPOINT)
                stream = await sdk_handle.create_stream(
                    TableProperties(TABLE_NAME, test_row_pb2.AirQuality.DESCRIPTOR),
                    self.options,
                )

                # ingesting records
                acks = [
                    await stream.ingest_record(test_row_pb2.AirQuality(device_name="my_device", temp=i, humidity=i))
                    for i in range(30)
                ]

                await asyncio.sleep(1)
                await stream.close()

                await asyncio.gather(*acks)

                self.assertEqual(mock_grpc_stream.max_offset_sent, 29)
                self.assertEqual(mock_grpc_stream.num_of_writes(), 30)
                self.assertEqual(calls_count, 2)
                self.assertEqual(len_of_iterator(stream.get_unacked_records()), 0)
            except Exception as e:
                self.fail(f"Expected no exception to be raised, but got: {e}")

        mock_grpc_stream.cancel()


class TestConsecutiveFailures(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.options = StreamConfigurationOptions(
            recovery=True,
            recovery_retries=5,
            recovery_timeout_ms=500,
            recovery_backoff_ms=500,
            server_lack_of_ack_timeout_ms=1000,
            flush_timeout_ms=60000,
            token_factory=token_factory,
        )

    @for_both_sdks
    async def test_server_unresponsive_permanently(self, sdk: SdkManager):
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
            try:
                sdk_handle = sdk.create(SERVER_ENDPOINT)
                stream = await sdk_handle.create_stream(
                    TableProperties(TABLE_NAME, test_row_pb2.AirQuality.DESCRIPTOR),
                    self.options,
                )

                # ingesting records
                acks = [
                    await stream.ingest_record(test_row_pb2.AirQuality(device_name="my_device", temp=i, humidity=i))
                    for i in range(50)
                ]

                try:
                    await stream.flush()
                except Exception as e:
                    self.assertEqual(str(e), "Stream failed with unacknowledged records. Cannot flush.")

                for ack in acks:
                    try:
                        await ack
                    except Exception as e:
                        self.assertEqual(str(e), "Server is unresponsive. Stream failed.")

                self.assertEqual(len_of_iterator(stream.get_unacked_records()), 50)
                self.assertEqual(calls_count, self.options.recovery_retries + 1)
                self.assertEqual(stream.get_state(), StreamState.FAILED)

            except Exception as e:
                self.fail(f"Expected no exception to be raised, but got: {e}")

        mock_grpc_stream.cancel()

    @for_both_sdks
    async def test_receiver_fails_permanently(self, sdk: SdkManager):
        calls_count = 0
        mock_grpc_stream = sdk.get_mock_class()(calls_count)

        def create_ephemeral_stream(generator, **kwargs):
            nonlocal calls_count
            nonlocal mock_grpc_stream

            calls_count += 1
            mock_grpc_stream = sdk.get_mock_class()(calls_count, generator)
            mock_grpc_stream.inject_response(True, Exception("Receiver failed"))
            return mock_grpc_stream

        mock_channel = MockGrpcChannel()
        mock_channel.injected_methods["/databricks.zerobus.Zerobus/EphemeralStream"] = create_ephemeral_stream

        with patch(sdk.get_grpc_override(), return_value=mock_channel):
            try:
                sdk_handle = sdk.create(SERVER_ENDPOINT)
                stream = await sdk_handle.create_stream(
                    TableProperties(TABLE_NAME, test_row_pb2.AirQuality.DESCRIPTOR),
                    self.options,
                )

                # ingesting records
                acks = [
                    await stream.ingest_record(test_row_pb2.AirQuality(device_name="my_device", temp=i, humidity=i))
                    for i in range(50)
                ]

                try:
                    await stream.flush()
                except Exception as e:
                    self.assertEqual(str(e), "Stream failed with unacknowledged records. Cannot flush.")

                for ack in acks:
                    try:
                        await ack
                    except Exception as e:
                        self.assertEqual(str(e), "Error happened in receiving records: Receiver failed")

                self.assertEqual(len_of_iterator(stream.get_unacked_records()), 50)
                self.assertEqual(calls_count, self.options.recovery_retries + 1)
                self.assertEqual(stream.get_state(), StreamState.FAILED)

            except Exception as e:
                self.fail(f"Expected no exception to be raised, but got: {e}")

        mock_grpc_stream.cancel()


class TestStreamBehaviours(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.options = StreamConfigurationOptions(
            recovery=True,
            server_lack_of_ack_timeout_ms=100,
            flush_timeout_ms=1000,
            max_inflight_records=10,
            token_factory=token_factory,
        )

    @for_both_sdks
    async def test_stream_is_healthy_with_no_interaction(self, sdk: SdkManager):
        calls_count = 0
        mock_grpc_stream = sdk.get_mock_class()(calls_count)

        def create_ephemeral_stream(generator, **kwargs):
            nonlocal calls_count
            nonlocal mock_grpc_stream

            calls_count += 1
            mock_grpc_stream = sdk.get_mock_class()(calls_count, generator)
            mock_grpc_stream.inject_response(False, InjectedRecordResponse(0, timeout_ms=0))
            return mock_grpc_stream

        mock_channel = MockGrpcChannel()
        mock_channel.injected_methods["/databricks.zerobus.Zerobus/EphemeralStream"] = create_ephemeral_stream

        with patch(sdk.get_grpc_override(), return_value=mock_channel):
            try:
                sdk_handle = sdk.create(SERVER_ENDPOINT)
                stream = await sdk_handle.create_stream(
                    TableProperties(TABLE_NAME, test_row_pb2.AirQuality.DESCRIPTOR),
                    self.options,
                )

                self.assertEqual(stream.get_state(), StreamState.OPENED)

                # sleep to verify that stream remains healthy
                await asyncio.sleep(5)

                self.assertEqual(stream.get_state(), StreamState.OPENED)

                await stream.ingest_record(test_row_pb2.AirQuality(device_name="my_device", temp=1, humidity=1))
                await stream.flush()

                self.assertEqual(stream.get_state(), StreamState.OPENED)

                await stream.close()

                self.assertEqual(stream.get_state(), StreamState.CLOSED)

            except Exception as e:
                self.fail(f"Expected no exception to be raised, but got: {e}")

        mock_grpc_stream.cancel()


class TestStreamStates(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.options = StreamConfigurationOptions(recovery=False, max_inflight_records=150, token_factory=token_factory)

    @for_both_sdks
    async def test_flush_after_close(self, sdk: SdkManager):
        calls_count = 0
        mock_grpc_stream = sdk.get_mock_class()(calls_count)

        def create_ephemeral_stream(generator, **kwargs):
            nonlocal calls_count
            nonlocal mock_grpc_stream

            calls_count += 1
            mock_grpc_stream = sdk.get_mock_class()(calls_count, generator)
            mock_grpc_stream.inject_response(False, InjectedRecordResponse(99, timeout_ms=2000))
            return mock_grpc_stream

        mock_channel = MockGrpcChannel()
        mock_channel.injected_methods["/databricks.zerobus.Zerobus/EphemeralStream"] = create_ephemeral_stream

        with patch(sdk.get_grpc_override(), return_value=mock_channel):
            try:
                sdk_handle = sdk.create(SERVER_ENDPOINT)
                stream = await sdk_handle.create_stream(
                    TableProperties(TABLE_NAME, test_row_pb2.AirQuality.DESCRIPTOR),
                    self.options,
                )

                # ingesting records
                acks = [
                    await stream.ingest_record(test_row_pb2.AirQuality(device_name="my_device", temp=i, humidity=i))
                    for i in range(100)
                ]

                await stream.close()
                await stream.flush()

                await asyncio.gather(*acks)

                self.assertEqual(mock_grpc_stream.max_offset_sent, 99)
                self.assertEqual(mock_grpc_stream.num_of_writes(), 100)
                self.assertEqual(len_of_iterator(stream.get_unacked_records()), 0)
                self.assertEqual(stream.get_state(), StreamState.CLOSED)

                # ingestion after stream is closed
                try:
                    await stream.ingest_record(test_row_pb2.AirQuality(device_name="my_device", temp=17, humidity=42))
                    self.fail("Expected exception to be raised")
                except Exception as e:
                    self.assertEqual(
                        str(e),
                        "Cannot ingest records after stream is closed or before it's opened.",
                    )
            except Exception as e:
                self.fail(f"Expected no exception to be raised, but got: {e}")

        mock_grpc_stream.cancel()

    @for_both_sdks
    async def test_ingest_after_flush(self, sdk: SdkManager):
        calls_count = 0
        mock_grpc_stream = sdk.get_mock_class()(calls_count)

        def create_ephemeral_stream(generator, **kwargs):
            nonlocal calls_count
            nonlocal mock_grpc_stream

            calls_count += 1
            mock_grpc_stream = sdk.get_mock_class()(calls_count, generator)
            mock_grpc_stream.inject_response(False, InjectedRecordResponse(99, timeout_ms=2000))
            mock_grpc_stream.inject_response(False, InjectedRecordResponse(100, timeout_ms=1000))
            return mock_grpc_stream

        mock_channel = MockGrpcChannel()
        mock_channel.injected_methods["/databricks.zerobus.Zerobus/EphemeralStream"] = create_ephemeral_stream

        with patch(sdk.get_grpc_override(), return_value=mock_channel):
            try:
                sdk_handle = sdk.create(SERVER_ENDPOINT)
                stream = await sdk_handle.create_stream(
                    TableProperties(TABLE_NAME, test_row_pb2.AirQuality.DESCRIPTOR),
                    self.options,
                )

                # ingesting records
                acks = [
                    await stream.ingest_record(test_row_pb2.AirQuality(device_name="my_device", temp=i, humidity=i))
                    for i in range(100)
                ]

                await stream.flush()

                await asyncio.gather(*acks)

                await (
                    await stream.ingest_record(test_row_pb2.AirQuality(device_name="my_device", temp=17, humidity=42))
                )

                self.assertEqual(mock_grpc_stream.max_offset_sent, 100)
                self.assertEqual(mock_grpc_stream.num_of_writes(), 101)
                self.assertEqual(len_of_iterator(stream.get_unacked_records()), 0)
                self.assertEqual(stream.get_state(), StreamState.OPENED)
                await stream.close()
                self.assertEqual(stream.get_state(), StreamState.CLOSED)
            except Exception as e:
                self.fail(f"Expected no exception to be raised, but got: {e}")

        mock_grpc_stream.cancel()


class TestNonRetriableErrors(unittest.IsolatedAsyncioTestCase):
    class _FakeNonRetriableRpcError(grpc.RpcError):
        def __init__(
            self,
            status_code: grpc.StatusCode = grpc.StatusCode.INVALID_ARGUMENT,
            msg: str = "Non-retriable test error",
        ):
            super().__init__()
            self._status_code = status_code
            self._msg = msg

        def code(self):
            return self._status_code

        def __str__(self):
            return self._msg

    def setUp(self):
        self.options = StreamConfigurationOptions(recovery=True, recovery_retries=5, token_factory=token_factory)

    @for_both_sdks
    async def test_create_stream_fails_with_non_retriable_error(self, sdk: SdkManager):
        calls_count = 0

        def create_ephemeral_stream(generator, **kwargs):
            nonlocal calls_count
            calls_count += 1
            raise self._FakeNonRetriableRpcError()

        mock_channel = MockGrpcChannel()
        mock_channel.injected_methods["/databricks.zerobus.Zerobus/EphemeralStream"] = create_ephemeral_stream

        with patch(sdk.get_grpc_override(), return_value=mock_channel):
            try:
                sdk_handle = sdk.create(SERVER_ENDPOINT)
                await sdk_handle.create_stream(
                    TableProperties(TABLE_NAME, test_row_pb2.AirQuality.DESCRIPTOR),
                    self.options,
                )

                self.fail("Expected NonRetriableException to be raised")
            except Exception as e:
                self.assertIsInstance(e, NonRetriableException)
                self.assertEqual(calls_count, 1)

    @for_both_sdks
    async def test_stream_fails_without_recovery_on_non_retriable_error(self, sdk: SdkManager):
        calls_count = 0
        mock_grpc_stream = sdk.get_mock_class()(calls_count)

        def create_ephemeral_stream(generator, **kwargs):
            nonlocal calls_count
            nonlocal mock_grpc_stream
            calls_count += 1
            mock_grpc_stream = sdk.get_mock_class()(calls_count, generator)
            mock_grpc_stream.inject_response(True, self._FakeNonRetriableRpcError(grpc.StatusCode.NOT_FOUND))

            return mock_grpc_stream

        mock_channel = MockGrpcChannel()
        mock_channel.injected_methods["/databricks.zerobus.Zerobus/EphemeralStream"] = create_ephemeral_stream

        with patch(sdk.get_grpc_override(), return_value=mock_channel):
            sdk_handle = sdk.create(SERVER_ENDPOINT)
            stream = await sdk_handle.create_stream(
                TableProperties(TABLE_NAME, test_row_pb2.AirQuality.DESCRIPTOR),
                self.options,
            )
            ack_future = await stream.ingest_record(
                test_row_pb2.AirQuality(device_name="my_device", temp=1, humidity=1)
            )
            try:
                await stream.flush()
                self.fail("Expected ZerobusException to be raised")
            except ZerobusException as e:
                self.assertEqual(str(e), "Stream failed with unacknowledged records. Cannot flush.")
            self.assertEqual(stream.get_state(), StreamState.FAILED)
            self.assertEqual(calls_count, 1)
            with self.assertRaises(ZerobusException):
                await ack_future

        mock_grpc_stream.cancel()


if __name__ == "__main__":
    unittest.main()
