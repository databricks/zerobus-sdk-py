import asyncio
import functools
import logging
import queue
import threading
import time
from typing import Union

import grpc

from zerobus.sdk import RecordAcknowledgment
from zerobus.sdk import ZerobusSdk as ZerobusSdkSync
from zerobus.sdk import ZerobusStream as ZerobusStreamSync
from zerobus.sdk.aio import ZerobusSdk as ZerobusSdkAsync
from zerobus.sdk.aio import ZerobusStream as ZerobusStreamAsync

logging.basicConfig(level=logging.INFO)


def empty_grpc_method(**kwargs):
    """A placeholder for a gRPC method that does nothing."""
    return None


class MockGrpcChannel:
    def __init__(self, initial_state=grpc.ChannelConnectivity.READY):
        self.injected_methods = {}
        self._state = initial_state
        self._callbacks = []

    def unary_unary(self, method, request_serializer=None, response_deserializer=None, _registered_method=None):
        return self.injected_methods[method] if method in self.injected_methods else empty_grpc_method

    def stream_stream(self, method, request_serializer=None, response_deserializer=None, _registered_method=None):
        return self.injected_methods[method] if method in self.injected_methods else empty_grpc_method

    def subscribe(self, callback, try_to_connect=False):
        self._callbacks.append(callback)
        callback(self._state)

    def unsubscribe(self, callback):
        if callback in self._callbacks:
            self._callbacks.remove(callback)

    def get_state(self, try_to_connect=False):
        return self._state


class InjectedResponse:
    def __init__(self, timeout_ms=0):
        self._timeout_ms = timeout_ms


class CreateStreamResponseInner:
    def __init__(self, id):
        self.stream_id = f"test_id_{id}"


class CreateStreamResponse(InjectedResponse):
    def __init__(self, id, timeout_ms=0):
        super().__init__(timeout_ms)
        self.create_stream_response = CreateStreamResponseInner(id)

    def HasField(self, field):
        return hasattr(self, field)


class WrongCreateStreamResponse(InjectedResponse):
    def __init__(self, timeout_ms=0):
        super().__init__(timeout_ms)

    def HasField(self, field):
        return hasattr(self, field)


class InjectedRecordResponseInner:
    def __init__(self, durability_ack_up_to_offset):
        self.durability_ack_up_to_offset = durability_ack_up_to_offset


class InjectedRecordResponse(InjectedResponse):
    def __init__(self, offset, timeout_ms=0):
        super().__init__(timeout_ms)
        self.ingest_record_response = InjectedRecordResponseInner(offset)

    def HasField(self, field):
        return hasattr(self, field)


class CloseStreamSignalInner:
    def __init__(self, duration_seconds):
        self.duration = DurationInner(duration_seconds)


class DurationInner:
    def __init__(self, seconds):
        self.seconds = seconds


class CloseStreamSignalResponse(InjectedResponse):
    def __init__(self, duration_seconds, timeout_ms=0):
        super().__init__(timeout_ms)
        self.close_stream_signal = CloseStreamSignalInner(duration_seconds)

    def HasField(self, field):
        return hasattr(self, field)


class InjectedError(grpc.RpcError):
    def __init__(self, error_message, timeout_ms=0, status_code=grpc.StatusCode.INTERNAL):
        super().__init__()
        self._timeout_ms = timeout_ms
        self.error_message = error_message
        self._status_code = status_code

    def code(self):
        return self._status_code

    def __str__(self):
        return self.error_message


class MockAsyncStream:
    def __init__(self, id=0, generator_fn=None):
        self.max_offset_sent = -1
        self.__id = id
        self.__stream_created = False
        self.__injected_create_stream_response = (False, CreateStreamResponse(id))
        self.__injected_read_responses = asyncio.Queue()
        self.__writes = {}
        self.__write_event = asyncio.Condition()
        self.__closed = False
        self.__generator_fn = generator_fn
        self.__sender_task = None
        self.__skip_create_stream = False
        self.__create_stream_arrived = False

        if self.__generator_fn is not None:
            self.__sender_task = asyncio.create_task(self.__send_loop())

    def inject_response(self, is_error, response):
        self.__injected_read_responses.put_nowait((is_error, response))

    def inject_create_stream_response(self, is_error, response):
        self.__injected_create_stream_response = (is_error, response)

    def inject_skip_create_stream(self):
        self.__skip_create_stream = True

    def num_of_writes(self):
        return len(self.__writes)

    async def __send_loop(self):
        try:
            async for record in self.__generator_fn:
                await asyncio.sleep(0.05)

                if record.HasField("ingest_record") == False:  # auto-grandfathered # noqa: E712
                    self.__create_stream_arrived = True
                    continue

                async with self.__write_event:
                    if record.ingest_record.offset_id != self.max_offset_sent + 1:
                        raise Exception(
                            f"Offsets have to be sent in order. Expected {self.max_offset_sent + 1}, but got {record.ingest_record.offset_id}"
                        )

                    self.max_offset_sent += 1
                    self.__writes[record.ingest_record.offset_id] = (
                        self.__writes.get(record.ingest_record.offset_id, 0) + 1
                    )
                    self.__write_event.notify_all()
        except:  # auto-grandfathered # noqa: E722
            pass

    def __aiter__(self):
        return self

    async def __anext__(self):
        if not self.__stream_created and not self.__skip_create_stream:
            if not self.__create_stream_arrived:
                await asyncio.sleep(0.1)
            self.__stream_created = True
            is_error, response = self.__injected_create_stream_response
            if is_error:
                raise response
            return response

        while not self.__closed or not self.__injected_read_responses.empty():
            try:
                is_error, response = await asyncio.wait_for(self.__injected_read_responses.get(), timeout=1)
                if is_error:
                    if hasattr(response, "_timeout_ms") and response._timeout_ms > 0:
                        await asyncio.sleep(response._timeout_ms / 1000)
                    raise response

                # Handle close stream signal responses
                if hasattr(response, "close_stream_signal"):
                    if response._timeout_ms > 0:
                        await asyncio.sleep(response._timeout_ms / 1000)
                    return response

                offset = response.ingest_record_response.durability_ack_up_to_offset

                async with self.__write_event:
                    if offset not in self.__writes:
                        await self.__write_event.wait_for(lambda: offset in self.__writes)

                if response._timeout_ms > 0:
                    await asyncio.sleep(response._timeout_ms / 1000)

                return response
            except asyncio.TimeoutError:
                continue

        raise StopAsyncIteration

    def cancel(self):
        self.__closed = True


class MockSyncStream:
    def __init__(self, id=0, generator_fn=None):
        self.max_offset_sent = -1
        self.__id = id
        self.__stream_created = False
        self.__injected_create_stream_response = (False, CreateStreamResponse(id))
        self.__injected_read_responses = queue.Queue()
        self.__writes = {}
        self.__write_event = threading.Condition()
        self.__closed = False
        self.__generator_fn = generator_fn
        self.__sender_thread = None
        self.__skip_create_stream = False
        self.__create_stream_arrived = threading.Event()

        if self.__generator_fn is not None:
            self.__sender_thread = threading.Thread(target=self.__send_loop, daemon=True)
            self.__sender_thread.start()

    def inject_response(self, is_error, response):
        self.__injected_read_responses.put((is_error, response))

    def inject_create_stream_response(self, is_error, response):
        self.__injected_create_stream_response = (is_error, response)

    def inject_skip_create_stream(self):
        self.__skip_create_stream = True

    def num_of_writes(self):
        with self.__write_event:
            return len(self.__writes)

    def __send_loop(self):
        try:
            for record in self.__generator_fn:
                if self.__closed:
                    break

                # The first message from the client must be to create the stream
                if not hasattr(record, "HasField") or not record.HasField("ingest_record"):
                    self.__create_stream_arrived.set()
                    continue

                # All subsequent messages are records to ingest
                with self.__write_event:
                    if record.ingest_record.offset_id != self.max_offset_sent + 1:
                        raise Exception(
                            f"Offsets must be sequential. Expected {self.max_offset_sent + 1}, got {record.ingest_record.offset_id}"
                        )
                    self.max_offset_sent += 1
                    self.__writes[record.ingest_record.offset_id] = (
                        self.__writes.get(record.ingest_record.offset_id, 0) + 1
                    )
                    self.__write_event.notify_all()
        except Exception as e:
            self.inject_response(True, e)
        finally:
            self.cancel()

    def __iter__(self):
        return self

    def __next__(self):
        if not self.__stream_created and not self.__skip_create_stream:
            if not self.__create_stream_arrived:
                time.sleep(0.1)
            self.__stream_created = True
            is_error, response = self.__injected_create_stream_response
            if is_error:
                raise response
            return response

        while not self.__closed or not self.__injected_read_responses.empty():
            try:
                is_error, response = self.__injected_read_responses.get(timeout=1)

                if is_error:
                    if hasattr(response, "_timeout_ms") and response._timeout_ms > 0:
                        time.sleep(response._timeout_ms / 1000)
                    raise response

                # Handle close stream signal responses
                if hasattr(response, "close_stream_signal"):
                    if response._timeout_ms > 0:
                        time.sleep(response._timeout_ms / 1000)
                    return response

                offset = response.ingest_record_response.durability_ack_up_to_offset

                with self.__write_event:
                    if offset not in self.__writes:
                        self.__write_event.wait_for(lambda: offset in self.__writes)

                if response._timeout_ms > 0:
                    time.sleep(response._timeout_ms / 1000)

                return response
            except queue.Empty:
                continue

        raise StopIteration

    def cancel(self):
        if not self.__closed:
            self.__closed = True
            with self.__write_event:
                self.__write_event.notify_all()


# The bellow classes are used as wrappers for Sync/Async SDK
# This way we don't need to duplicate tests for each SDK type


class AckWrapper:
    """
    Wrapper for the "ack" in the implementations
    In the sync version it calls .wait_for_ack()
    In the async version it awaits the ack
    """

    def __init__(self, ack: Union[RecordAcknowledgment, asyncio.Future]):
        self.ack = ack

    def __await__(self):
        if asyncio.iscoroutine(self.ack) or isinstance(self.ack, asyncio.Future):
            return self.ack.__await__()
        else:
            # make the blocking function run in the aio executor to not block the aio runtime
            loop = asyncio.get_running_loop()
            return loop.run_in_executor(None, self.ack.wait_for_ack).__await__()

    def add_done_callback(self, callback):
        self.ack.add_done_callback(callback)


class StreamWrapper:
    """
    Wrapper for ZerobusStream
    In the sync version it simply calls the functions
    In the async version it awaits the functions
    For ingest_record, it returns the ack wrapped in AckWrapper
    """

    def __init__(self, stream: Union[ZerobusStreamSync, ZerobusStreamAsync]):
        self.stream = stream

    async def ingest_record(self, row):
        response = self.stream.ingest_record(row)
        if asyncio.iscoroutine(response):
            return AckWrapper(await response)
        return AckWrapper(response)

    async def close(self):
        if isinstance(self.stream, ZerobusStreamAsync):
            await self.stream.close()
        else:
            self.stream.close()

    def stop_receiver_task(self):
        if isinstance(self.stream, ZerobusStreamAsync):
            self.stream._ZerobusStream__receiver_task.cancel()
        else:
            self.stream._ZerobusStream__stop_event_recv_thread.set()

    def get_unacked_records(self):
        return self.stream.get_unacked_records()

    def get_state(self):
        return self.stream.get_state()

    async def flush(self):
        if isinstance(self.stream, ZerobusStreamAsync):
            await self.stream.flush()
        else:
            self.stream.flush()


class SdkWrapper:
    """
    Wrapper for ZerobusSdk
    In the sync version it simply calls the functions
    In the async version it awaits the functions
    For create_stream, it returns ZerobusStream (sync/async) wrapped in StreamWrapper
    """

    def __init__(self, sdk: Union[ZerobusSdkSync, ZerobusSdkAsync]):
        self.sdk = sdk

    async def create_stream(self, table_props, options):
        # The standalone SDK requires client_id and client_secret as parameters
        response = self.sdk.create_stream(
            client_id="test_client_id",
            client_secret="test_client_secret",
            table_properties=table_props,
            options=options,
        )
        if asyncio.iscoroutine(response):
            return StreamWrapper(await response)
        return StreamWrapper(response)

    async def recreate_stream(self, old_stream: StreamWrapper):
        if isinstance(self.sdk, ZerobusSdkAsync):
            return StreamWrapper(await self.sdk.recreate_stream(old_stream.stream))
        else:
            return StreamWrapper(self.sdk.recreate_stream(old_stream.stream))


class SdkManager:
    """
    Wrapper for the tests, providing the correct classes/mocks depending on the type (sync/async)
    For create(), it returns ZerobusSdk (sync/async) wrapped in SdkWrapper
    """

    def __init__(self, sdk: Union[type[ZerobusSdkSync], type[ZerobusSdkSync]]):
        self.sdk = sdk

    def create(self, server):
        # The standalone SDK requires unity_catalog_url in constructor
        return SdkWrapper(self.sdk(server, unity_catalog_url="https://test.unity.catalog.url"))

    def get_grpc_override(self):
        if self.sdk is ZerobusSdkAsync:
            return "grpc.aio.secure_channel"
        else:
            return "grpc.secure_channel"

    def get_mock_class(self):
        if self.sdk is ZerobusSdkAsync:
            return MockAsyncStream
        else:
            return MockSyncStream

    def get_stop_iter(self):
        if self.sdk is ZerobusSdkAsync:
            return StopAsyncIteration()
        else:
            return StopIteration()


def for_both_sdks(test_func):
    """
    A decorator that runs an async test function twice, with the correct
    execution context for each SDK type.
    Put it above test cases to run the specified async case against both SDKs
    """
    from unittest.mock import patch

    @functools.wraps(test_func)
    async def wrapper(test_instance, *args, **kwargs):
        # Run for async SDK
        logging.info(f"\nRunning test '{test_func.__name__}' for Async SDK")
        async_sdk_manager = SdkManager(ZerobusSdkAsync)
        with patch("zerobus.sdk.shared.headers_provider.get_zerobus_token", return_value="mock_token"):
            await test_func(test_instance, async_sdk_manager, *args, **kwargs)
        logging.info(f"Test '{test_func.__name__}' PASSED for Async SDK")

        # Run for sync SDK
        logging.info(f"\nRunning test '{test_func.__name__}' for Sync SDK")
        sync_sdk_manager = SdkManager(ZerobusSdkSync)
        coro = test_func(test_instance, sync_sdk_manager, *args, **kwargs)
        loop = asyncio.get_running_loop()

        def run_sync_test_in_isolation():
            with patch("zerobus.sdk.shared.headers_provider.get_zerobus_token", return_value="mock_token"):
                asyncio.run(coro)

        await loop.run_in_executor(None, run_sync_test_in_isolation)

        logging.info(f"Test '{test_func.__name__}' PASSED for Sync SDK")

    return wrapper
