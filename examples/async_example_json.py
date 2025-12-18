"""
Asynchronous Ingestion Example - JSON Mode

This example demonstrates record ingestion using the asynchronous API with JSON serialization.

Record Type Mode: JSON
  - Records are sent as JSON-encoded strings
  - Uses RecordType.JSON to specify JSON serialization
  - Best for dynamic schemas or when working with JSON data

Use Case: Best for applications already using asyncio, async web frameworks (FastAPI, aiohttp),
or when integrating ingestion with other asynchronous operations in an event loop.

Authentication:
  - Uses OAuth 2.0 Client Credentials (standard method)
  - Includes example of custom headers provider for advanced use cases

Note: Both sync and async APIs provide the same throughput and durability guarantees.
Choose based on your application's architecture, not performance requirements.
"""

import asyncio
import json
import logging
import os
import time

import grpc

from zerobus.sdk.aio import ZerobusSdk
from zerobus.sdk.shared import (RecordType, StreamConfigurationOptions,
                                TableProperties)
from zerobus.sdk.shared.headers_provider import HeadersProvider
from zerobus.sdk.shared.tls_config import TlsConfig

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


# Configuration - update these with your values
# For AWS:
SERVER_ENDPOINT = os.getenv("ZEROBUS_SERVER_ENDPOINT", "your-shard-id.zerobus.region.cloud.databricks.com")
UNITY_CATALOG_ENDPOINT = os.getenv("DATABRICKS_WORKSPACE_URL", "https://your-workspace.cloud.databricks.com")
# For Azure:
# SERVER_ENDPOINT = os.getenv(
#     "ZEROBUS_SERVER_ENDPOINT", "your-shard-id.zerobus.region.azuredatabricks.net"
# )
# UNITY_CATALOG_ENDPOINT = os.getenv(
#     "DATABRICKS_WORKSPACE_URL", "https://your-workspace.azuredatabricks.net"
# )
TABLE_NAME = os.getenv("ZEROBUS_TABLE_NAME", "catalog.schema.table")

# For OAuth authentication
CLIENT_ID = os.getenv("DATABRICKS_CLIENT_ID", "your-oauth-client-id")
CLIENT_SECRET = os.getenv("DATABRICKS_CLIENT_SECRET", "your-oauth-client-secret")

# Number of records to ingest
NUM_RECORDS = 1000


def create_sample_json_record(index):
    """
    Creates a sample AirQuality record as a dict.

    With JSON mode, you can pass either a dict or a pre-serialized JSON string.
    """
    return {"device_name": f"sensor-{index % 10}", "temp": 20 + (index % 15), "humidity": 50 + (index % 40)}


class CustomHeadersProvider(HeadersProvider):
    """
    Example custom headers provider for advanced use cases.

    Note: OAuth 2.0 Client Credentials (via create_stream()) is the standard
    authentication method. Use this only if you have specific requirements
    for custom headers (e.g., custom metadata, existing token management, etc.).
    """

    def __init__(self, custom_token: str):
        self.custom_token = custom_token

    def get_headers(self):
        """
        Return custom headers for gRPC metadata.

        Returns:
            List of (header_name, header_value) tuples
        """
        return [
            ("authorization", f"Bearer {self.custom_token}"),
            ("x-custom-header", "custom-value"),
        ]


class CustomTlsConfig(TlsConfig):
    """
    Example custom TLS configuration for advanced use cases.

    Note: SecureTlsConfig (using system CA certificates) is the default.
    Use this only if you have specific requirements such as:
    - Custom CA certificates
    - Client certificates (mutual TLS)
    - Custom cipher suites
    """

    def __init__(self, root_certificates=None, private_key=None, certificate_chain=None):
        self.root_certificates = root_certificates
        self.private_key = private_key
        self.certificate_chain = certificate_chain

    def to_channel_credentials(self) -> grpc.ChannelCredentials:
        return grpc.ssl_channel_credentials(
            root_certificates=self.root_certificates,
            private_key=self.private_key,
            certificate_chain=self.certificate_chain,
        )


def create_ack_callback():
    """
    Creates an acknowledgment callback that logs progress.

    The callback is invoked by the SDK whenever records are acknowledged by the server.
    """
    ack_count = [0]  # Use list to maintain state in closure

    def callback(response):
        offset = response.durability_ack_up_to_offset
        ack_count[0] += 1
        # Log every 100 acknowledgments
        if ack_count[0] % 100 == 0:
            logger.info(f"  Acknowledged up to offset: {offset} (batch #{ack_count[0]})")

    return callback


async def main():
    print("Starting asynchronous ingestion example (Explicit JSON Mode)...")
    print("=" * 60)

    # Check if credentials are configured
    if CLIENT_ID == "your-oauth-client-id" or CLIENT_SECRET == "your-oauth-client-secret":
        logger.error("Please set DATABRICKS_CLIENT_ID and DATABRICKS_CLIENT_SECRET environment variables")
        logger.info("Or update the CLIENT_ID and CLIENT_SECRET values in this file")
        return

    if SERVER_ENDPOINT == "your-shard-id.zerobus.region.cloud.databricks.com":
        logger.error("Please set ZEROBUS_SERVER_ENDPOINT environment variable")
        logger.info("Or update the SERVER_ENDPOINT value in this file")
        return

    if TABLE_NAME == "catalog.schema.table":
        logger.error("Please set ZEROBUS_TABLE_NAME environment variable")
        logger.info("Or update the TABLE_NAME value in this file")
        return

    try:
        # Step 1: Initialize the SDK
        sdk = ZerobusSdk(SERVER_ENDPOINT, UNITY_CATALOG_ENDPOINT)
        logger.info("✓ SDK initialized")

        # Step 2: Configure stream options with JSON record type and ack callback
        options = StreamConfigurationOptions(
            record_type=RecordType.JSON,
            max_inflight_records=10_000,  # Allow 10k records in flight
            recovery=True,  # Enable automatic recovery
            ack_callback=create_ack_callback(),  # Track acknowledgments
        )
        logger.info("✓ Stream configuration created")

        # Step 3: Define table properties
        # Note: No protobuf descriptor needed for JSON mode
        table_properties = TableProperties(TABLE_NAME)
        logger.info(f"✓ Table properties configured for: {TABLE_NAME} (JSON mode)")

        # Step 4: Create a stream with OAuth 2.0 authentication
        #
        # Standard method: OAuth 2.0 Client Credentials with default TLS (SecureTlsConfig)
        # The SDK automatically:
        #   - Uses system CA certificates for TLS
        #   - Includes authorization header with OAuth token
        #   - Includes x-databricks-zerobus-table-name header
        stream = await sdk.create_stream(CLIENT_ID, CLIENT_SECRET, table_properties, options)

        # Advanced: Custom TLS configuration (for special use cases only)
        # Uncomment to use custom TLS (e.g., custom CA certificates, mTLS):
        # custom_tls = CustomTlsConfig(root_certificates=your_ca_certs)
        # stream = await sdk.create_stream(CLIENT_ID, CLIENT_SECRET, table_properties, options, custom_tls)

        # Advanced: Custom headers provider (for special use cases only)
        # Uncomment to use custom headers instead of OAuth:
        # custom_provider = CustomHeadersProvider(custom_token="your-custom-token")
        # stream = await sdk.create_stream(CLIENT_ID, CLIENT_SECRET, table_properties, options, headers_provider=custom_provider)

        logger.info(f"✓ Stream created: {stream.stream_id}")

        # Step 5: Ingest JSON records asynchronously
        logger.info(f"\nIngesting {NUM_RECORDS} JSON records (non-blocking mode)...")
        start_time = time.time()

        try:
            # Store futures for later waiting
            futures = []

            for i in range(NUM_RECORDS):
                # Create a record dict
                record_dict = create_sample_json_record(i)

                # Two ways to ingest JSON records:

                # Option 1: Pass a dict (SDK serializes to JSON)
                if i % 2 == 0:
                    future = await stream.ingest_record(record_dict)

                # Option 2: Pass a pre-serialized JSON string (client controls serialization)
                else:
                    json_string = json.dumps(record_dict)
                    future = await stream.ingest_record(json_string)

                futures.append(future)

                # Progress indicator
                if (i + 1) % 100 == 0:
                    logger.info(f"  Submitted {i + 1} records")

            submit_end_time = time.time()
            submit_duration = submit_end_time - start_time
            logger.info(f"\n✓ All records submitted in {submit_duration:.2f} seconds")

            # Step 6: Flush and wait for all records to be durably written
            logger.info("\nFlushing stream and waiting for durability...")
            await stream.flush()
            logger.info("✓ Stream flushed")

            # Optionally wait for all individual futures
            logger.info("Waiting for all records to be acknowledged...")
            await asyncio.gather(*futures)

            end_time = time.time()
            total_duration = end_time - start_time
            records_per_second = NUM_RECORDS / total_duration
            avg_latency_ms = (total_duration * 1000.0) / NUM_RECORDS

            logger.info("✓ All records durably written")

            # Step 7: Close the stream
            await stream.close()
            logger.info("✓ Stream closed")

            # Print summary
            print("\n" + "=" * 60)
            print("Ingestion Summary:")
            print(f"  Total records: {NUM_RECORDS}")
            print(f"  Submit time: {submit_duration:.2f} seconds")
            print(f"  Total time: {total_duration:.2f} seconds")
            print(f"  Throughput: {records_per_second:.2f} records/sec")
            print(f"  Average latency: {avg_latency_ms:.2f} ms/record")
            print(f"  Stream state: {stream.get_state()}")
            print(f"  Record type: JSON (explicit)")
            print("=" * 60)

        except Exception as e:
            logger.error(f"\n✗ Error during ingestion: {e}")
            await stream.close()
            raise

    except Exception as e:
        logger.error(f"\n✗ Failed to initialize stream: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main())
