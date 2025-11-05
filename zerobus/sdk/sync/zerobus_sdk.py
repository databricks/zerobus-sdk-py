import json
import logging
import queue
import threading
import time
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, Iterator, Optional, Union

import grpc
from google.protobuf.descriptor_pb2 import DescriptorProto
from google.protobuf.message import Message

from ..shared import (NOT_RETRIABLE_GRPC_CODES, NonRetriableException,
                      RecordType, StreamConfigurationOptions, StreamState,
                      TableProperties, ZerobusException, _StreamFailureInfo,
                      _StreamFailureType, log_and_get_exception,
                      zerobus_service_pb2, zerobus_service_pb2_grpc)
from ..shared.headers_provider import HeadersProvider, OAuthHeadersProvider

logger = logging.getLogger("zerobus_sdk")

# Default timeout in milliseconds for stream operations, used when recovery is not enabled.
CREATE_STREAM_TIMEOUT_MS = 15000


class RecordAcknowledgment:
    """
    A thread-safe, future-like object for blocking until an operation is acknowledged.

    This class provides a simple synchronization primitive for a thread to wait for a
    result or an exception to be set by another thread.
    """

    def __init__(self):
        self._event = threading.Event()
        self._exception = None
        self._result: Optional[int] = None
        self.callback = None

    def _set_result(self, result):
        self._result = result
        self._event.set()

        self._call_callback_with_exception_handling()

    def _set_exception(self, exception):
        self._exception = exception
        self._event.set()

    def add_done_callback(self, callback: Callable[[int], None]):
        """
        Adds a callback function to be executed upon acknowledgment.

        If the acknowledgment has already been received before this method is called,
        the callback will be executed immediately in the current thread to ensure
        it is not missed.

        Args:
            callback (Callable[[int], None]): The function to call upon completion.
                It must accept a single integer argument, which will be the
                acknowledged offset ID of the record.
        """

        # If the user adds a callback after it is already acked, immediately call the callback,
        # since it would never be executed if we don't call it here.
        if self._event.is_set():
            self._call_callback_with_exception_handling()

        self.callback = callback

    def wait_for_ack(self, timeout_sec: Optional[float] = None) -> int:
        """
        Blocks until the acknowledgment is received or an optional timeout occurs.

        Args:
            timeout_sec (Optional[float]): The maximum number of seconds to wait.

        Returns:
            int: The offset id of the ack.

        Raises:
            ZerobusException: If the wait times out.
            Exception: The exception set by `set_exception` if the operation failed.
        """
        acked = self._event.wait(timeout=timeout_sec)
        if not acked:
            raise ZerobusException("Timed out waiting for acknowledgment.")
        if self._exception:
            raise self._exception
        return self._result

    def is_done(self) -> bool:
        """
        Checks if the operation has completed.

        Returns:
            bool: True if a result or exception has been set, False otherwise.
        """
        return self._event.is_set()

    def _call_callback_with_exception_handling(self):
        try:
            if self.callback:
                self.callback(self._result)
        except Exception as e:
            logger.error(f"Error in user-provided callback: {e}")


class ZerobusStream:
    """
    Manages a single, stateful gRPC stream for ingesting records into a table.

    This class handles the complexities of a bi-directional streaming RPC, including
    sending records, receiving acknowledgments, managing in-flight record limits,
    and automatic recovery from transient network errors. This class is thread-safe.

    Args:
        stub (zerobus_service_pb2_grpc.ZerobusStub): The gRPC stub for the service.
        headers_provider (HeadersProvider): Provider for headers.
        table_properties (TableProperties): The properties of the target table.
        options (StreamConfigurationOptions): Configuration options for the stream.
        grpc_channel (grpc.Channel): The underlying gRPC channel.
    """

    def __init__(
        self,
        stub: zerobus_service_pb2_grpc.ZerobusStub,
        headers_provider: HeadersProvider,
        table_properties: TableProperties,
        options: StreamConfigurationOptions,
        grpc_channel: grpc.Channel,
    ):
        self.__stub = stub
        self._headers_provider = headers_provider
        self._table_properties = table_properties
        self._options = options

        self.__record_queue = queue.Queue(maxsize=0)
        self.__inflight_records_tokens = queue.Queue(maxsize=self._options.max_inflight_records)
        self.__inflight_records_done = threading.Condition()
        self.__pending_futures = OrderedDict()
        self.__unacked_records = list()

        self.__state = StreamState.UNINITIALIZED
        self.__lock = threading.RLock()
        self.__state_changed = threading.Condition(self.__lock)

        self.__executor: Optional[ThreadPoolExecutor] = None

        self.__receiver_thread: Optional[threading.Thread] = None
        self.__server_unresponsiveness_thread: Optional[threading.Thread] = None

        self.__stop_event = threading.Event()  # signals all threads to stop
        self.__sender_generator_exit = threading.Event()

        self.__last_received_offset = None
        self.stream_id = None
        self._stream = None
        self.__stream_created_event = threading.Event()
        self.__stream_created_exception = None
        self.__stream_failure_info = _StreamFailureInfo()

        self.grpc_channel = grpc_channel
        # use ONLY for testing
        self.__stop_event_recv_thread = threading.Event()

    def __set_state(self, new_state: StreamState):
        with self.__state_changed:
            self.__state = new_state
            self.__state_changed.notify_all()

    def __with_retries(self, func: Callable, max_attempts: int):
        # Here the logic is a little bit different from the async implementation
        # Since waiting for a response is a blocking operation we cannot directly measure the timeout
        # For that the self.__stream_created_event is created with this logic:
        # - main thread creates grpc stream and starts receiver task
        # - receiver task waits for create stream response if that happens sets the __stream_created_event
        # - main thread waits for __stream_created_event or until timeout. If timeout happens,
        #   an exception is raised - Stream timed out...
        backoff_seconds = (self._options.recovery_backoff_ms / 1000) if self._options.recovery else 0
        for attempt in range(max_attempts):
            logger.info(f"Attempting retry {attempt} out of {max_attempts}")
            try:
                func()
                return
            except NonRetriableException as e:
                raise e
            except Exception as e:
                if attempt == max_attempts - 1:
                    raise e
                time.sleep(backoff_seconds)

    def __wait_for_channel_status(self, timeout: float):
        # Waits for a gRPC channel to not be idle.
        # Will raise an exception if it is still idle in the given timeout

        not_idle_event = threading.Event()

        def on_state_change(state: grpc.ChannelConnectivity):
            # This callback is called by grpc when the channel state changes.
            if state != grpc.ChannelConnectivity.IDLE:
                not_idle_event.set()

        try:
            self.grpc_channel.subscribe(on_state_change, try_to_connect=True)

            if not not_idle_event.wait(timeout=timeout):
                raise TimeoutError(f"gRPC channel did not become ready within {timeout}s.")
        finally:
            self.grpc_channel.unsubscribe(on_state_change)

    def __create_stream(self):
        headers = self._headers_provider.get_headers()

        with self.__state_changed:
            if self.__state == StreamState.OPENED:
                return
            if self.__state == StreamState.CLOSED or self.__state == StreamState.FAILED:
                raise ZerobusException("Stream already closed and cannot be reused.")

        # We create 3 main tasks:
        # - Sender task
        # - Receiver task
        # - Unresponsiveness detection task
        # - An additional task is created if an error occurs - which shuts down the other tasks and attempts recovery

        self.__last_received_offset = None
        self.stream_id = None
        self._stream = None
        self.__stop_event.clear()
        self.__sender_generator_exit.clear()
        self.__stop_event_recv_thread.clear()
        self.__stream_created_event.clear()
        self.__stream_created_exception = None

        try:
            timeout_seconds = (
                (self._options.recovery_timeout_ms / 1000)
                if self._options.recovery
                else (CREATE_STREAM_TIMEOUT_MS / 1000)
            )

            # Wait for underlying grpc channel to be ready with a timeout
            start_time = time.time()
            logger.info("Waiting for gRPC channel to be ready")
            self.__wait_for_channel_status(timeout_seconds)
            time_needed_grpc_channel = time.time() - start_time
            logger.info(f"gRPC channel not in IDLE state after {time_needed_grpc_channel} seconds")
            self._stream = self.__stub.EphemeralStream(self.__sender(), metadata=headers)
            logger.info("gRPC channel established successfully")
            self.__receiver_thread = threading.Thread(target=self.__receiver, daemon=True)
            self.__receiver_thread.start()

            # Calculate the remaining time left from the timeout for the CreateStreamResponse from the server
            # We subtract the time needed for establishing the grpc connection
            left_of_timeout = timeout_seconds - time_needed_grpc_channel
            logger.info("Waiting for CreateIngestStreamResponse")
            success_stream = self.__stream_created_event.wait(left_of_timeout)
            if self.__stream_created_exception:
                raise self.__stream_created_exception
            if not success_stream:
                raise Exception("Stream operation timed out...")

            self.__server_unresponsiveness_thread = threading.Thread(
                target=self.__server_unresponsiveness_detection, daemon=True
            )
            self.__server_unresponsiveness_thread.start()
            logger.info(f"Stream created. Stream ID: {self.stream_id}")

        except grpc.RpcError as e:
            if e.code() in NOT_RETRIABLE_GRPC_CODES:
                # Non-retriable gRPC errors
                logger.error(f"Non-retriable gRPC error during stream creation: {str(e)}")
                raise NonRetriableException(f"Failed to create a stream: {str(e)}") from e
            else:
                # Retriable gRPC errors
                logger.error(f"Retriable gRPC error during stream creation: {str(e)}")
                raise ZerobusException(f"Failed to create a stream: {str(e)}") from e

        except Exception as e:
            logger.error(f"Create stream error: {str(e)}")
            raise ZerobusException(f"Failed to create a stream: {str(e)}") from e

    def _initialize(self):
        # Initializes the stream. This is a blocking call.
        try:
            logger.info("Starting initializing stream")
            if self.__executor is None:
                self.__executor = ThreadPoolExecutor(max_workers=2)
            max_attempts = self._options.recovery_retries if self._options.recovery else 1
            self.__with_retries(self.__create_stream, max_attempts)
            self.__set_state(StreamState.OPENED)
        except ZerobusException:
            self.__set_state(StreamState.FAILED)
            raise
        except Exception as e:
            self.__set_state(StreamState.FAILED)
            raise ZerobusException(f"Failed to create a stream: {str(e)}") from e

    def __close(self, hard_failure=False, err_msg="Stream closed."):
        self.__stop_event.set()
        with self.__inflight_records_done:
            self.__inflight_records_done.notify_all()
        with self.__state_changed:
            self.__state_changed.notify_all()

        if self._stream:
            self._stream.cancel()

        try:
            if self.__receiver_thread and self.__receiver_thread.is_alive():
                self.__receiver_thread.join(timeout=2.0)
        except:  # noqa: E722
            pass
        finally:
            self.__receiver_thread = None

        self.__sender_generator_exit.wait(timeout=2.0)

        try:
            if self.__server_unresponsiveness_thread and self.__server_unresponsiveness_thread.is_alive():
                self.__server_unresponsiveness_thread.join(timeout=2.0)
        except:  # noqa: E722
            pass
        finally:
            self.__server_unresponsiveness_thread = None

        if hard_failure:
            with self.__lock:
                for future, record, _ in list(self.__pending_futures.values()):
                    self.__unacked_records.append((record, future))
                    if not future.is_done():
                        future._set_exception(ZerobusException(err_msg))
                self.__pending_futures.clear()

                while not self.__record_queue.empty():
                    try:
                        future, record, _ = self.__record_queue.get_nowait()
                        self.__unacked_records.append((record, future))
                        if not future.is_done():
                            future._set_exception(ZerobusException(err_msg))
                    except queue.Empty:
                        break

                with self.__inflight_records_done:
                    while not self.__inflight_records_tokens.empty():
                        try:
                            self.__inflight_records_tokens.get_nowait()
                        except queue.Empty:
                            break
                    self.__inflight_records_done.notify_all()

    def __recover_stream(self) -> bool:
        if not self._options.recovery:
            return False

        left_retries = max(0, self._options.recovery_retries - self.__stream_failure_info.failure_counts + 1)
        if left_retries == 0:
            return False

        try:
            logger.info("Recovering stream...")
            self.__close(hard_failure=False)
            self.__with_retries(self.__create_stream, left_retries)
            self.__set_state(StreamState.OPENED)
            logger.info("Stream recovered successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to recover stream: {str(e)}")
            return False

    def __handle_stream_failed(self, failed_stream_id, failure_type, exception=None):
        with self.__lock:
            # Step 1: Check if another thread is already handling the failure.
            if (
                self.__state == StreamState.RECOVERING
                or self.__state == StreamState.FAILED
                or self.__state == StreamState.UNINITIALIZED
            ):
                return

            if failed_stream_id != self.stream_id:
                return

            if self.__state == StreamState.CLOSED and (
                exception is None
                or (isinstance(exception, grpc.RpcError) and exception.code() == grpc.StatusCode.CANCELLED)
            ):
                return

            # Step 2: Log the failure and determine if recovery is possible.
            self.__stream_failure_info.log_failure(failure_type)

            should_recover = (
                (self.__state == StreamState.OPENED or self.__state == StreamState.FLUSHING)
                and not isinstance(exception, NonRetriableException)
                and self._options.recovery
            )

            if not should_recover:
                err_msg = str(exception) if exception else "Stream closed unexpectedly!"
                logger.error(f"Stream closed due to a non-recoverable error: {err_msg}")
                self.__set_state(StreamState.FAILED)
                self.__close(hard_failure=True, err_msg=err_msg)
                return

            # Step 3: Set the state to RECOVERING. This now acts as the primary lock
            # against any other concurrent failure handlers.
            self.__set_state(StreamState.RECOVERING)

        # Step 4: Attempt the actual recovery outside the main lock to prevent deadlocks.
        # Because the state is now RECOVERING, we are safe from other threads.
        recovered = self.__recover_stream()

        if not recovered:
            # If recovery ultimately fails, acquire the lock to set the final FAILED state.
            with self.__lock:
                err_msg = str(exception) if exception else "Stream recovery failed."
                logger.error(f"Stream failed permanently after failed recovery attempt: {err_msg}")
                self.__set_state(StreamState.FAILED)
                self.__close(hard_failure=True, err_msg=err_msg)

    def __wait_for_stream_to_finish_initialization(self):
        with self.__state_changed:
            if self.__state == StreamState.UNINITIALIZED or self.__state == StreamState.RECOVERING:
                self.__state_changed.wait_for(
                    lambda: self.__state != StreamState.UNINITIALIZED and self.__state != StreamState.RECOVERING
                )

    def __sender(self) -> Iterator[zerobus_service_pb2.EphemeralStreamRequest]:
        exception = None
        offset_id = -1
        try:
            # Firstly, send the create stream request
            logger.info("Sending CreateIngestStreamRequest to gRPC stream")
            create_stream_request = zerobus_service_pb2.CreateIngestStreamRequest(
                table_name=self._table_properties.table_name.encode("utf-8"),
                record_type=self._options.record_type.value,
            )

            # Only include descriptor for PROTO streams
            if self._options.record_type == RecordType.PROTO:
                create_stream_request.descriptor_proto = self._get_descriptor_bytes(
                    self._table_properties.descriptor_proto
                )

            yield zerobus_service_pb2.EphemeralStreamRequest(create_stream=create_stream_request)
            self.__wait_for_stream_to_finish_initialization()

            # Then, send previous unacked requests
            try:
                with self.__lock:
                    unacked_offset_ids = list(self.__pending_futures.keys())

                for old_offset_id in unacked_offset_ids:
                    if self.__stop_event.is_set():
                        return
                    with self.__lock:
                        if old_offset_id not in self.__pending_futures:
                            continue
                        (future, sent_record, serialized_record) = self.__pending_futures.get(old_offset_id)

                        offset_id += 1

                        if offset_id != old_offset_id:
                            self.__pending_futures[offset_id] = (
                                future,
                                sent_record,
                                serialized_record,
                            )
                            self.__pending_futures.pop(old_offset_id)

                    ingest_request = zerobus_service_pb2.IngestRecordRequest(offset_id=offset_id)
                    if self._options.record_type == RecordType.PROTO:
                        ingest_request.proto_encoded_record = serialized_record
                    elif self._options.record_type == RecordType.JSON:
                        ingest_request.json_record = serialized_record.decode("utf-8")

                    yield zerobus_service_pb2.EphemeralStreamRequest(ingest_record=ingest_request)
            except Exception as e:
                raise ZerobusException("Failed to resend unacked records after stream recovery: " + str(e))

            offset_id += 1

            # Finally, in a loop wait and send new incoming requests
            while not self.__stop_event.is_set():
                if offset_id > 0:
                    self.__stream_failure_info.reset_failure(_StreamFailureType.SENDER_ERROR)

                if self.__state == StreamState.FAILED or self.__state == StreamState.RECOVERING:
                    return

                try:
                    future, record, serialized_record = self.__record_queue.get(timeout=0.5)
                except queue.Empty:
                    if self.__state == StreamState.CLOSED and self.__record_queue.empty():
                        break
                    continue

                with self.__lock:
                    self.__pending_futures[offset_id] = (future, record, serialized_record)

                if self.__state == StreamState.FAILED or self.__state == StreamState.RECOVERING:
                    return

                ingest_request = zerobus_service_pb2.IngestRecordRequest(offset_id=offset_id)
                if self._options.record_type == RecordType.PROTO:
                    ingest_request.proto_encoded_record = serialized_record
                elif self._options.record_type == RecordType.JSON:
                    ingest_request.json_record = serialized_record.decode("utf-8")

                yield zerobus_service_pb2.EphemeralStreamRequest(ingest_record=ingest_request)
                offset_id += 1

        except grpc.RpcError as e:
            # Only log if the stream is not being intentionally stopped
            if self.__stop_event.is_set() and e.code() == grpc.StatusCode.CANCELLED:
                # Stream was cancelled during close() - don't log as error
                exception = ZerobusException(f"Error happened in sending records: {e}")
            else:
                exception = log_and_get_exception(e)
        except Exception as e:
            if not self.__stop_event.is_set():
                logger.error(f"Error in sender: {str(e)}")
                exception = ZerobusException(f"Error happened in sending records: {str(e)}")
        finally:
            self.__sender_generator_exit.set()
            if exception and not self.__stop_event.is_set():
                self.__executor.submit(
                    self.__handle_stream_failed,
                    self.stream_id,
                    _StreamFailureType.SENDER_ERROR,
                    exception,
                )

    def __receiver(self):
        exception = None

        created_stream_success = False
        try:
            for response in self._stream:
                if response.HasField("create_stream_response"):
                    self.stream_id = response.create_stream_response.stream_id
                    created_stream_success = True

                break

            if not created_stream_success:
                logger.error("No response received from the server on stream creation")
                exception = ZerobusException("No response received from the server on stream creation")
                return

            self.__stream_created_event.set()

            self.__wait_for_stream_to_finish_initialization()

            for response in self._stream:
                if self.__stop_event.is_set():
                    break

                # used only for testing - simulating failure
                if self.__stop_event_recv_thread.is_set():
                    break

                if response.HasField("close_stream_signal"):
                    close_stream_duration = response.close_stream_signal.duration.seconds
                    logger.info(f"Stream will be gracefully closed by server in {close_stream_duration} seconds.")
                    if self._options.recovery:
                        # If recover is enabled, immediately start recovery process
                        return
                    else:
                        # If not, keep stream opened until server sends an error
                        continue

                if not response.HasField("ingest_record_response"):
                    raise ZerobusException("Unexpected response type received. Expected IngestRecordResponse.")
                ack_response = response.ingest_record_response
                with self.__lock:
                    first_offset_in_ack = 0 if self.__last_received_offset is None else self.__last_received_offset + 1
                    for offset_to_ack in range(first_offset_in_ack, ack_response.durability_ack_up_to_offset + 1):
                        future, _, _ = self.__pending_futures.pop(offset_to_ack, (None, None, None))
                        if future:
                            future._set_result(offset_to_ack)

                        with self.__inflight_records_done:
                            try:
                                self.__inflight_records_tokens.get_nowait()
                                self.__inflight_records_done.notify_all()
                            except queue.Empty:
                                pass
                    self.__last_received_offset = ack_response.durability_ack_up_to_offset

                if self._options.ack_callback:
                    try:
                        self._options.ack_callback(ack_response)
                    except Exception as e:
                        logger.error(f"Error in user-provided ack_callback: {str(e)}")

                if self.__last_received_offset > 0:
                    self.__stream_failure_info.reset_failure(_StreamFailureType.RECEIVER_ERROR)
                    self.__stream_failure_info.reset_failure(_StreamFailureType.SERVER_UNRESPONSIVE)

                if (
                    self.__state != StreamState.OPENED
                    and self.__state != StreamState.FLUSHING
                    and len(self.__pending_futures) == 0
                    and self.__record_queue.empty()
                ):
                    break
        except grpc.RpcError as e:
            # If we encountered an error and the stream is not yet created, do not handle stream as failed.
            # Instead, directly propagate the error to the user.
            if not created_stream_success:
                exception = e
                return
            # Only log if the stream is not being intentionally stopped
            if self.__stop_event.is_set() and e.code() == grpc.StatusCode.CANCELLED:
                # Stream was cancelled during close() - don't log as error
                exception = ZerobusException(f"Error happened in receiving records: {e}")
            else:
                exception = log_and_get_exception(e)
        except Exception as e:
            logger.error(f"Error happened in receiving records: {str(e)}")
            exception = ZerobusException(f"Error happened in receiving records: {str(e)}")
        finally:
            if created_stream_success and not self.__stop_event.is_set():
                self.__executor.submit(
                    self.__handle_stream_failed,
                    self.stream_id,
                    _StreamFailureType.RECEIVER_ERROR,
                    exception,
                )
            else:
                self.__stream_created_exception = exception

    def __server_unresponsiveness_detection(self):
        self.__wait_for_stream_to_finish_initialization()
        while self.__state == StreamState.OPENED or self.__state == StreamState.FLUSHING:
            try:
                with self.__inflight_records_done:
                    if self.__inflight_records_tokens.empty():
                        self.__inflight_records_done.wait_for(
                            lambda: not self.__inflight_records_tokens.empty() or self.__stop_event.is_set()
                        )

                if self.__stop_event.is_set():
                    break

                last_offset = self.__last_received_offset

                stopped = self.__stop_event.wait(self._options.server_lack_of_ack_timeout_ms / 1000)
                if stopped:
                    break

                if last_offset == self.__last_received_offset and len(self.__pending_futures) > 0:
                    raise ZerobusException("Server is unresponsive. Stream failed.")

            except Exception as e:
                logger.error(e)
                if not self.__stop_event.is_set():
                    self.__executor.submit(
                        self.__handle_stream_failed,
                        self.stream_id,
                        _StreamFailureType.SERVER_UNRESPONSIVE,
                        e,
                    )
                break

    def get_state(self) -> StreamState:
        """
        Returns the current operational state of the stream.

        Returns:
            StreamState: The current state as a StreamState enum member.
        """
        return self.__state

    def get_unacked_records(self) -> Iterator:
        """
        Retrieves records that were not acknowledged before the stream failed or was closed.

        Returns:
            Iterator: An iterator yielding the unacknowledged records.
        """
        with self.__lock:
            return (record[0] for record in self.__unacked_records)

    def ingest_record(self, record: Union[Message, dict]) -> RecordAcknowledgment:
        """
        Submits a single record for ingestion into the stream.

        This method may block if the maximum number of in-flight records has been reached,
        waiting until there is capacity.

        Args:
            record: Either a Protobuf Message object or a dict (for JSON records).
                   Type must match the stream's configured record_type.

        Returns:
            RecordAcknowledgment: An object to wait on for the server's acknowledgment.

        Raises:
            ValueError: If record type doesn't match stream configuration.
            ZerobusException: If the stream is not in a valid state for ingestion.
        """
        # Validate record type and serialize appropriately
        if self._options.record_type == RecordType.PROTO:
            if not isinstance(record, Message):
                raise ValueError(
                    f"Stream is configured for PROTO records, but received {type(record).__name__}. "
                    "Pass a Protobuf Message object."
                )
            serialized_record = record.SerializeToString()

        elif self._options.record_type == RecordType.JSON:
            if not isinstance(record, dict):
                raise ValueError(
                    f"Stream is configured for JSON records, but received {type(record).__name__}. "
                    "Pass a dict object."
                )
            # Serialize dict to JSON string and encode to bytes
            serialized_record = json.dumps(record).encode("utf-8")

        else:
            raise ValueError(f"Unsupported record type: {self._options.record_type}")

        with self.__state_changed:
            if self.__state == StreamState.FLUSHING:
                self.__state_changed.wait_for(lambda: self.__state != StreamState.FLUSHING)
            if (
                self.__state == StreamState.CLOSED
                or self.__state == StreamState.FAILED
                or self.__state == StreamState.UNINITIALIZED
            ):
                raise ZerobusException("Cannot ingest records after stream is closed or before it's opened.")

        future = RecordAcknowledgment()
        with self.__inflight_records_done:
            if self.__inflight_records_tokens.full():
                self.__inflight_records_done.wait_for(lambda: not self.__inflight_records_tokens.full())

        with self.__lock:
            with self.__inflight_records_done:
                if self.__state == StreamState.FLUSHING:
                    self.__state_changed.wait_for(lambda: self.__state != StreamState.FLUSHING)
                if self.__state != StreamState.OPENED and self.__state != StreamState.RECOVERING:
                    raise ZerobusException("Cannot ingest records after stream is closed or before it's opened.")

                self.__inflight_records_tokens.put(None)
                self.__record_queue.put((future, record, serialized_record))
                self.__inflight_records_done.notify_all()

        return future

    def __wait_all_records_to_be_flushed(self):
        with self.__state_changed:
            if self.__state == StreamState.OPENED:
                self.__state = StreamState.FLUSHING

        with self.__inflight_records_done:
            if not self.__inflight_records_done.wait_for(
                lambda: self.__inflight_records_tokens.empty() or self.__stop_event.is_set(),
                timeout=self._options.flush_timeout_ms / 1000,
            ):
                logger.error("Flush timed out ...")
                raise ZerobusException("Flush timed out ...")

        with self.__state_changed:
            if self.__state == StreamState.FLUSHING:
                self.__state = StreamState.OPENED
                self.__state_changed.notify_all()

    def flush(self):
        """
        Blocks until all currently submitted records have been acknowledged.

        Raises:
            ZerobusException: If the stream is not initialized or fails during the flush.
        """

        if self.__state == StreamState.UNINITIALIZED:
            raise ZerobusException("Stream is not initialized. Cannot flush.")

        with self.__state_changed:
            if self.__state == StreamState.RECOVERING:
                self.__state_changed.wait_for(lambda: self.__state != StreamState.RECOVERING)

        self.__wait_all_records_to_be_flushed()

        if self.__state == StreamState.FAILED:
            raise ZerobusException("Stream failed with unacknowledged records. Cannot flush.")

        logger.info("Flush completed successfully")

    def close(self):
        """
        Flushes all pending records and gracefully closes the stream.

        This is a blocking call that waits for all submitted records to be
        acknowledged by the server before shutting down the connection.

        Raises:
            ZerobusException: If the stream is not in a valid state to be closed.
        """
        with self.__state_changed:
            if self.__state == StreamState.UNINITIALIZED:
                raise ZerobusException("Stream is not initialized. Cannot close.")

            if self.__state == StreamState.CLOSED:
                return

            if self.__state == StreamState.FLUSHING or self.__state == StreamState.RECOVERING:
                self.__state_changed.wait_for(
                    lambda: self.__state not in (StreamState.FLUSHING, StreamState.RECOVERING)
                )

            if self.__state == StreamState.FAILED:
                raise ZerobusException("Stream failed. Cannot close.")

            logger.info("Closing the stream gracefully...")
            self.__set_state(StreamState.CLOSED)

        try:
            self.flush()
        finally:
            self.__close(hard_failure=True, err_msg="Stream closed.")
            self.__executor.shutdown()

        logger.info("Stream closed successfully.")

    @staticmethod
    def _get_descriptor_bytes(descriptor):
        descriptor_proto = DescriptorProto()
        descriptor.CopyToProto(descriptor_proto)
        return descriptor_proto.SerializeToString()


class ZerobusSdk:
    """
    This class serves as the main entry point for interacting with the Zerobus service,
    handling gRPC channel setup and stream creation.

    Args:
        host (str): The hostname or IP address of the Zerobus service.
        unity_catalog_url (str): The URL of the Unity Catalog endpoint.
    """

    def __init__(self, host: str, unity_catalog_url: str):
        """
        Initialize ZerobusSdk with standard OAuth authentication.

        Args:
            host: The hostname or IP address of the Zerobus service.
            unity_catalog_url: The URL of the Unity Catalog endpoint.
        """
        self.__host = host
        self.__unity_catalog_url = unity_catalog_url
        self.__workspace_id = host.split(".")[0]

    def create_stream(
        self,
        client_id: str,
        client_secret: str,
        table_properties: TableProperties,
        options: StreamConfigurationOptions = StreamConfigurationOptions(),
    ) -> ZerobusStream:
        """
        Creates, initializes, and returns a new ingestion stream.

        Args:
            client_id (str): The client ID.
            client_secret (str): The client secret.
            table_properties (TableProperties): The properties of the target table,
                including its name and schema.
            options (StreamConfigurationOptions): Optional configuration for the stream's
                behavior, such as recovery settings.

        Returns:
            ZerobusStream: An initialized and active stream instance.
        """
        headers_provider = OAuthHeadersProvider(
            self.__workspace_id, self.__unity_catalog_url, table_properties.table_name, client_id, client_secret
        )
        return self.create_stream_with_headers_provider(headers_provider, table_properties, options)

    def create_stream_with_headers_provider(
        self,
        headers_provider: HeadersProvider,
        table_properties: TableProperties,
        options: StreamConfigurationOptions = StreamConfigurationOptions(),
    ) -> ZerobusStream:
        """
        Creates, initializes, and returns a new ingestion stream using a custom headers provider.

        Args:
            headers_provider: The headers provider.
            table_properties: The properties of the target table.
            options: The options for the stream.

        Returns:
            ZerobusStream: An initialized and active stream instance.

        Raises:
            ValueError: If record_type is PROTO but descriptor_proto is not provided.
        """
        # Validate record_type and descriptor compatibility
        if options.record_type == RecordType.PROTO:
            if table_properties.descriptor_proto is None:
                raise ValueError("descriptor_proto is required in TableProperties when record_type is PROTO")
        elif options.record_type == RecordType.JSON:
            if table_properties.descriptor_proto is not None:
                logger.warning("descriptor_proto provided for JSON stream will be ignored")

        channel = grpc.secure_channel(
            self.__host,
            grpc.ssl_channel_credentials(),
            options=[("grpc.max_send_message_length", -1), ("grpc.max_receive_message_length", -1)],
        )

        stub = zerobus_service_pb2_grpc.ZerobusStub(channel)
        stream = ZerobusStream(
            stub,
            headers_provider,
            table_properties,
            options,
            channel,
        )
        stream._initialize()
        return stream

    def recreate_stream(self, old_stream: ZerobusStream) -> ZerobusStream:
        """
        Creates a new stream to replace a failed or closed stream.

        This method automatically re-ingests any records that were not acknowledged
        by the previous stream.

        Args:
            old_stream (ZerobusStream): The failed or closed stream instance.

        Returns:
            ZerobusStream: A new, active stream with unacknowledged records re-queued.

        Raises:
            ZerobusException: If the provided old stream is still active.
        """
        old_stream_state = old_stream.get_state()
        if old_stream_state != StreamState.CLOSED and old_stream_state != StreamState.FAILED:
            raise ZerobusException("Stream is not closed. Cannot create new stream.")

        new_stream = self.create_stream_with_headers_provider(
            old_stream._headers_provider, old_stream._table_properties, old_stream._options
        )

        unacked_records = old_stream.get_unacked_records()
        for record in unacked_records:
            new_stream.ingest_record(record)

        return new_stream
