from typing import ClassVar as _ClassVar
from typing import Mapping as _Mapping
from typing import Optional as _Optional
from typing import Union as _Union

from google.protobuf import descriptor as _descriptor
from google.protobuf import duration_pb2 as _duration_pb2
from google.protobuf import message as _message
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper

DESCRIPTOR: _descriptor.FileDescriptor

class RecordType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    RECORD_TYPE_UNSPECIFIED: _ClassVar[RecordType]
    PROTO: _ClassVar[RecordType]
    JSON: _ClassVar[RecordType]

RECORD_TYPE_UNSPECIFIED: RecordType
PROTO: RecordType
JSON: RecordType

class CreateIngestStreamRequest(_message.Message):
    __slots__ = ("table_name", "descriptor_proto", "record_type")
    TABLE_NAME_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTOR_PROTO_FIELD_NUMBER: _ClassVar[int]
    RECORD_TYPE_FIELD_NUMBER: _ClassVar[int]
    table_name: str
    descriptor_proto: bytes
    record_type: RecordType
    def __init__(
        self,
        table_name: _Optional[str] = ...,
        descriptor_proto: _Optional[bytes] = ...,
        record_type: _Optional[_Union[RecordType, str]] = ...,
    ) -> None: ...

class CreateIngestStreamResponse(_message.Message):
    __slots__ = ("stream_id",)
    STREAM_ID_FIELD_NUMBER: _ClassVar[int]
    stream_id: str
    def __init__(self, stream_id: _Optional[str] = ...) -> None: ...

class IngestRecordRequest(_message.Message):
    __slots__ = ("offset_id", "proto_encoded_record", "json_record")
    OFFSET_ID_FIELD_NUMBER: _ClassVar[int]
    PROTO_ENCODED_RECORD_FIELD_NUMBER: _ClassVar[int]
    JSON_RECORD_FIELD_NUMBER: _ClassVar[int]
    offset_id: int
    proto_encoded_record: bytes
    json_record: str
    def __init__(
        self,
        offset_id: _Optional[int] = ...,
        proto_encoded_record: _Optional[bytes] = ...,
        json_record: _Optional[str] = ...,
    ) -> None: ...

class EphemeralStreamRequest(_message.Message):
    __slots__ = ("create_stream", "ingest_record")
    CREATE_STREAM_FIELD_NUMBER: _ClassVar[int]
    INGEST_RECORD_FIELD_NUMBER: _ClassVar[int]
    create_stream: CreateIngestStreamRequest
    ingest_record: IngestRecordRequest
    def __init__(
        self,
        create_stream: _Optional[_Union[CreateIngestStreamRequest, _Mapping]] = ...,
        ingest_record: _Optional[_Union[IngestRecordRequest, _Mapping]] = ...,
    ) -> None: ...

class IngestRecordResponse(_message.Message):
    __slots__ = ("durability_ack_up_to_offset",)
    DURABILITY_ACK_UP_TO_OFFSET_FIELD_NUMBER: _ClassVar[int]
    durability_ack_up_to_offset: int
    def __init__(self, durability_ack_up_to_offset: _Optional[int] = ...) -> None: ...

class CloseStreamSignal(_message.Message):
    __slots__ = ("duration",)
    DURATION_FIELD_NUMBER: _ClassVar[int]
    duration: _duration_pb2.Duration
    def __init__(self, duration: _Optional[_Union[_duration_pb2.Duration, _Mapping]] = ...) -> None: ...

class EphemeralStreamResponse(_message.Message):
    __slots__ = ("create_stream_response", "ingest_record_response", "close_stream_signal")
    CREATE_STREAM_RESPONSE_FIELD_NUMBER: _ClassVar[int]
    INGEST_RECORD_RESPONSE_FIELD_NUMBER: _ClassVar[int]
    CLOSE_STREAM_SIGNAL_FIELD_NUMBER: _ClassVar[int]
    create_stream_response: CreateIngestStreamResponse
    ingest_record_response: IngestRecordResponse
    close_stream_signal: CloseStreamSignal
    def __init__(
        self,
        create_stream_response: _Optional[_Union[CreateIngestStreamResponse, _Mapping]] = ...,
        ingest_record_response: _Optional[_Union[IngestRecordResponse, _Mapping]] = ...,
        close_stream_signal: _Optional[_Union[CloseStreamSignal, _Mapping]] = ...,
    ) -> None: ...
