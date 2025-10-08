import json
import logging
from enum import Enum
from typing import Callable, Optional

import grpc
import requests
from google.protobuf.descriptor import Descriptor

from .zerobus_service_pb2 import IngestRecordResponse

logger = logging.getLogger("zerobus_sdk")
logger.addHandler(logging.NullHandler())

NOT_RETRIABLE_GRPC_CODES = {
    grpc.StatusCode.INVALID_ARGUMENT,
    grpc.StatusCode.NOT_FOUND,
    grpc.StatusCode.UNAUTHENTICATED,
    grpc.StatusCode.OUT_OF_RANGE,
}

STREAM_LIFETIME_CODE = "Error code 6002"


def log_and_get_exception(e):
    if e.code() in NOT_RETRIABLE_GRPC_CODES:
        logger.error(f"Non-retriable gRPC error happened in receiving records: {str(e)}")
        exception_class = NonRetriableException
    else:
        # Stream lifecycle ended - recover stream silently
        exception_class = ZerobusException
        if STREAM_LIFETIME_CODE in str(e):
            logger.info("Stream lifetime expired. Initializing a new stream.")
        else:
            logger.error(f"Retriable gRPC error happened in receiving records: {str(e)}")

    return exception_class(f"Error happened in receiving records: {e}")


class TableProperties:
    """
    Table properties for the stream.
    """

    def __init__(self, table_name: str, descriptor_proto: Descriptor):
        self.table_name = table_name
        self.descriptor_proto = descriptor_proto


class StreamConfigurationOptions:
    """
    Configuration options for the stream.
    """

    def __init__(self, **kwargs):
        # Set default values for attributes

        # Maximum number of records that can be sent to the server before waiting for acknowledgment
        self.max_inflight_records: int = 50_000

        # Whether to enable automatic recovery of the stream in case of failure
        self.recovery: bool = True

        # Timeout for stream recovery in milliseconds (for one attempt of recovery).
        # Total timeout = recovery_timeout_ms * recovery_retries
        self.recovery_timeout_ms: int = 15000

        # Backoff time in milliseconds between recovery attempts
        self.recovery_backoff_ms: int = 2000

        # Number of retries for stream recovery
        self.recovery_retries: int = 3

        # The number of ms in which, if we do not receive an acknowledgement from the server, the server will be considered unresponsive
        self.server_lack_of_ack_timeout_ms: int = 60000

        # Timeout for flushing the stream in milliseconds (5 minutes default)
        self.flush_timeout_ms: int = 300_000

        # Callback to be called when a record acknowledgement is received
        self.ack_callback: Optional[Callable[[IngestRecordResponse], None]] = None

        # Dynamically update attributes based on kwargs
        for key, value in kwargs.items():
            if hasattr(self, key):
                setattr(self, key, value)


class StreamState(Enum):
    UNINITIALIZED = 0  # Before stream is created/opened
    OPENED = 1  # Stream is opened and ready to send records
    FLUSHING = 2  # Stream is flushing records
    CLOSED = 3  # Stream is gracefully closed
    RECOVERING = 4  # Stream is recovering from an error
    FAILED = 5  # Stream is closed due to an error


class ZerobusException(Exception):
    """
    Base class for all exceptions in the Zerobus SDK.
    """

    def __init__(self, message: str):
        super().__init__(message)


class NonRetriableException(ZerobusException):
    """
    Inherits from ZerobusException and indicates a non-retriable error has occurred.
    """



class _StreamFailureType(Enum):
    UNKNOWN = 0
    SENDER_ERROR = 1
    RECEIVER_ERROR = 2
    SERVER_UNRESPONSIVE = 3


class _StreamFailureInfo:
    def __init__(self):
        self.failure_counts = 0
        self.failure_type = _StreamFailureType.UNKNOWN

    def log_failure(self, failure_type: _StreamFailureType):
        if self.failure_type == failure_type:
            self.failure_counts += 1
        else:
            self.failure_counts = 1
            self.failure_type = failure_type

    def reset_failure(self, failure_type: _StreamFailureType):
        if self.failure_type == failure_type:
            self.failure_counts = 0
            self.failure_type = _StreamFailureType.UNKNOWN


def get_zerobus_token(
    table_name: str, workspace_id: str, workspace_url: str, client_id: str, client_secret: str
) -> str:
    """
    Makes the token request to Databricks login service and returns the Zerobus access token.

        Args:
            table_name (str): The three part name of the table (catalog.schema.table).
            workspace_id (str): The ID of the workspace.
            workspace_url (str): The URL of the workspace.
            client_id (str): The client ID.
            client_secret (str): The client secret.

        Returns:
            str: The Zerobus access token.

        Raises:
            NonRetriableException: If the table name is not in the format of catalog.schema.table.
            NonRetriableException: If the access token is not received from the token response.
            NonRetriableException: If the error occurs while making the token request.
            NonRetriableException: If the error occurs while parsing the token response.
            NonRetriableException: If the error occurs while getting the token.
    """
    three_part_table_name = table_name.split(".")
    if len(three_part_table_name) != 3:
        raise NonRetriableException(f"Table name '{table_name}' must be in the format of catalog.schema.table")

    catalog_name = three_part_table_name[0]
    schema_name = three_part_table_name[1]
    table_name = three_part_table_name[2]

    authorization_details = [
        {
            "type": "unity_catalog_privileges",
            "privileges": ["USE CATALOG"],
            "object_type": "CATALOG",
            "object_full_path": catalog_name,
        },
        {
            "type": "unity_catalog_privileges",
            "privileges": ["USE SCHEMA"],
            "object_type": "SCHEMA",
            "object_full_path": f"{catalog_name}.{schema_name}",
        },
        {
            "type": "unity_catalog_privileges",
            "privileges": ["SELECT", "MODIFY"],
            "object_type": "TABLE",
            "object_full_path": f"{catalog_name}.{schema_name}.{table_name}",
        },
    ]

    url = f"{workspace_url}/oidc/v1/token"
    data = {
        "grant_type": "client_credentials",
        "scope": "all-apis",
        "resource": f"api://databricks/workspaces/{workspace_id}/zerobusDirectWriteApi",
        "authorization_details": json.dumps(authorization_details),
    }

    url = f"{workspace_url}/oidc/v1/token"

    try:
        response = requests.post(url, auth=(client_id, client_secret), data=data)
        response.raise_for_status()

        token_data = response.json()
        access_token = token_data.get("access_token")

        if not access_token:
            raise NonRetriableException("No access token received from OAuth response")

        logger.info("Successfully obtained OAuth token")  # noqa: F821
        return access_token

    except requests.exceptions.RequestException as e:
        error_message = f"Error making OAuth request: {e}"
        logger.error(error_message)  # noqa: F821
        raise NonRetriableException(error_message)
    except json.JSONDecodeError as e:
        error_message = f"Error parsing OAuth response: {e}"
        logger.error(error_message)  # noqa: F821
        raise NonRetriableException(error_message)
    except Exception as e:
        error_message = f"Unexpected error getting OAuth token: {e}"
        logger.error(error_message)  # noqa: F821
        raise NonRetriableException(error_message)
