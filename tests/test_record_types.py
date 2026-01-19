"""
Tests for different record types (PROTO and JSON).
"""

import unittest
from unittest.mock import patch

import tests.row_pb2 as test_row_pb2
from tests.mock_grpc import InjectedRecordResponse, MockGrpcChannel, SdkManager, for_both_sdks
from zerobus.sdk import RecordType, StreamConfigurationOptions, StreamState, TableProperties

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
        """Test PROTO configuration with Message objects."""
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
    async def test_proto_with_pre_serialized_bytes(self, sdk: SdkManager):
        """Test PROTO configuration with pre-serialized bytes."""
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

            # Ingest protobuf records as pre-serialized bytes (client controls serialization)
            record1 = test_row_pb2.AirQuality(device_name="device1", temp=20, humidity=50)
            ack1 = await stream.ingest_record(record1.SerializeToString())
            offset1 = await ack1
            self.assertEqual(offset1, 0)

            record2 = test_row_pb2.AirQuality(device_name="device2", temp=22, humidity=55)
            ack2 = await stream.ingest_record(record2.SerializeToString())
            offset2 = await ack2
            self.assertEqual(offset2, 1)

            await stream.close()

        mock_grpc_stream.cancel()

    @for_both_sdks
    async def test_proto_all_value_types(self, sdk: SdkManager):
        """Test PROTO record ingestion with all supported value types."""
        calls_count = 0
        mock_grpc_stream = sdk.get_mock_class()(calls_count)

        def create_ephemeral_stream(generator, **kwargs):
            nonlocal calls_count
            nonlocal mock_grpc_stream
            calls_count += 1
            mock_grpc_stream = sdk.get_mock_class()(calls_count, generator)
            # Inject responses for each record we'll ingest
            for i in range(5):
                mock_grpc_stream.inject_response(False, InjectedRecordResponse(i, timeout_ms=10))
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
                TableProperties(TABLE_NAME, test_row_pb2.AllTypesRecord.DESCRIPTOR),
                options,
            )

            self.assertEqual(stream.get_state(), StreamState.OPENED)

            # Test record with all scalar types
            record1 = test_row_pb2.AllTypesRecord(
                string_field="hello",
                int32_field=42,
                int64_field=9223372036854775807,  # max int64
                float_field=3.14,
                double_field=2.718281828459045,
                bool_field=True,
                bytes_field=b"binary data",
            )
            ack = await stream.ingest_record(record1)
            self.assertEqual(await ack, 0)

            # Test record with repeated (array) fields
            record2 = test_row_pb2.AllTypesRecord(
                string_field="with arrays",
                repeated_string=["one", "two", "three"],
                repeated_int32=[1, 2, 3, 4, 5],
                repeated_double=[1.1, 2.2, 3.3],
            )
            ack = await stream.ingest_record(record2)
            self.assertEqual(await ack, 1)

            # Test record with nested message
            nested = test_row_pb2.NestedData(
                nested_string="nested value",
                nested_int=123,
            )
            record3 = test_row_pb2.AllTypesRecord(
                string_field="with nested",
                nested_message=nested,
            )
            ack = await stream.ingest_record(record3)
            self.assertEqual(await ack, 2)

            # Test record with repeated nested messages
            record4 = test_row_pb2.AllTypesRecord(
                string_field="with repeated nested",
                repeated_nested=[
                    test_row_pb2.NestedData(nested_string="first", nested_int=1),
                    test_row_pb2.NestedData(nested_string="second", nested_int=2),
                ],
            )
            ack = await stream.ingest_record(record4)
            self.assertEqual(await ack, 3)

            # Test record with all fields populated
            record5 = test_row_pb2.AllTypesRecord(
                string_field="complete record",
                int32_field=-2147483648,  # min int32
                int64_field=-9223372036854775808,  # min int64
                float_field=-0.0,
                double_field=float("inf"),
                bool_field=False,
                bytes_field=b"\x00\x01\x02\xff",
                repeated_string=["a", "b"],
                repeated_int32=[0, -1, 1],
                repeated_double=[0.0, -1.5],
                nested_message=test_row_pb2.NestedData(nested_string="deep", nested_int=999),
                repeated_nested=[
                    test_row_pb2.NestedData(nested_string="n1", nested_int=10),
                ],
            )
            ack = await stream.ingest_record(record5)
            self.assertEqual(await ack, 4)

            await stream.close()

        mock_grpc_stream.cancel()

    @for_both_sdks
    async def test_json(self, sdk: SdkManager):
        """Test JSON record ingestion with dict."""
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
    async def test_json_with_pre_serialized_string(self, sdk: SdkManager):
        """Test JSON record ingestion with pre-serialized JSON string."""
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

            self.assertEqual(stream.get_state(), StreamState.OPENED)

            # Ingest JSON records as pre-serialized strings (client controls serialization)
            import json

            ack1 = await stream.ingest_record(json.dumps({"device_name": "device1", "temp": 20, "humidity": 50}))
            offset1 = await ack1
            self.assertEqual(offset1, 0)

            ack2 = await stream.ingest_record(json.dumps({"device_name": "device2", "temp": 22, "humidity": 55}))
            offset2 = await ack2
            self.assertEqual(offset2, 1)

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
            # Error message should mention that Message or bytes are acceptable
            self.assertIn("bytes", str(context.exception))

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
            # Error message should mention that dict or str are acceptable
            self.assertIn("dict", str(context.exception))
            self.assertIn("str", str(context.exception))

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

    @for_both_sdks
    async def test_json_all_value_types(self, sdk: SdkManager):
        """Test JSON record ingestion with all supported value types."""
        calls_count = 0
        mock_grpc_stream = sdk.get_mock_class()(calls_count)

        def create_ephemeral_stream(generator, **kwargs):
            nonlocal calls_count
            nonlocal mock_grpc_stream
            calls_count += 1
            mock_grpc_stream = sdk.get_mock_class()(calls_count, generator)
            # Inject responses for each record we'll ingest
            for i in range(10):
                mock_grpc_stream.inject_response(False, InjectedRecordResponse(i, timeout_ms=10))
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

            self.assertEqual(stream.get_state(), StreamState.OPENED)

            # Test string value
            ack = await stream.ingest_record({"field": "hello world"})
            self.assertEqual(await ack, 0)

            # Test integer value
            ack = await stream.ingest_record({"field": 42})
            self.assertEqual(await ack, 1)

            # Test float value
            ack = await stream.ingest_record({"field": 3.14159})
            self.assertEqual(await ack, 2)

            # Test boolean true
            ack = await stream.ingest_record({"field": True})
            self.assertEqual(await ack, 3)

            # Test boolean false
            ack = await stream.ingest_record({"field": False})
            self.assertEqual(await ack, 4)

            # Test null value
            ack = await stream.ingest_record({"field": None})
            self.assertEqual(await ack, 5)

            # Test array/list value
            ack = await stream.ingest_record({"field": [1, 2, 3, "four", 5.0]})
            self.assertEqual(await ack, 6)

            # Test nested dict
            ack = await stream.ingest_record({"field": {"nested": {"deep": "value"}}})
            self.assertEqual(await ack, 7)

            # Test empty dict
            ack = await stream.ingest_record({})
            self.assertEqual(await ack, 8)

            # Test complex record with all types
            ack = await stream.ingest_record(
                {
                    "string_field": "text",
                    "int_field": 123,
                    "float_field": 45.67,
                    "bool_true": True,
                    "bool_false": False,
                    "null_field": None,
                    "array_field": [1, "two", 3.0, True, None],
                    "nested_field": {"level1": {"level2": {"value": "deep"}}},
                    "empty_array": [],
                    "empty_object": {},
                }
            )
            self.assertEqual(await ack, 9)

            await stream.close()

        mock_grpc_stream.cancel()

    @for_both_sdks
    async def test_json_non_serializable_raises_error(self, sdk: SdkManager):
        """Test that non-JSON-serializable objects raise NonRetriableException."""
        import datetime

        from zerobus.sdk.shared import NonRetriableException

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

            # Test with a custom class (not JSON-serializable) - TypeError
            class CustomClass:
                pass

            with self.assertRaises(NonRetriableException) as context:
                await stream.ingest_record({"field": CustomClass()})

            self.assertIn("Failed to serialize record to JSON", str(context.exception))
            self.assertIn("JSON-serializable", str(context.exception))

            # Test with a set (not JSON-serializable) - TypeError
            with self.assertRaises(NonRetriableException) as context:
                await stream.ingest_record({"field": {1, 2, 3}})

            self.assertIn("Failed to serialize record to JSON", str(context.exception))

            # Test with a function (not JSON-serializable) - TypeError
            with self.assertRaises(NonRetriableException) as context:
                await stream.ingest_record({"field": lambda x: x})

            self.assertIn("Failed to serialize record to JSON", str(context.exception))

            # Test with datetime (not JSON-serializable) - TypeError
            with self.assertRaises(NonRetriableException) as context:
                await stream.ingest_record({"field": datetime.datetime.now()})

            self.assertIn("Failed to serialize record to JSON", str(context.exception))

            # Test with bytes (not JSON-serializable) - TypeError
            with self.assertRaises(NonRetriableException) as context:
                await stream.ingest_record({"field": b"binary data"})

            self.assertIn("Failed to serialize record to JSON", str(context.exception))

            # Test with circular reference - ValueError
            circular_dict = {"a": 1}
            circular_dict["self"] = circular_dict

            with self.assertRaises(NonRetriableException) as context:
                await stream.ingest_record(circular_dict)

            self.assertIn("Failed to serialize record to JSON", str(context.exception))

            await stream.close()

        mock_grpc_stream.cancel()


if __name__ == "__main__":
    unittest.main()
