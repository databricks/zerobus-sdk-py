"""
Synchronous Ingestion Example - Protobuf

This example demonstrates record ingestion using the synchronous API with protobuf serialization.

Use Case: Best for applications that don't use asyncio or prefer blocking I/O patterns.

Authentication:
  - Uses OAuth 2.0 Client Credentials (standard method)
  - Includes example of custom headers provider for advanced use cases

Note: Both sync and async APIs provide the same throughput and durability guarantees.
Choose based on your application's architecture, not performance requirements.
"""

import logging
import os
import time

# Import the generated protobuf module
import record_pb2

from zerobus.sdk.shared import (RecordType, StreamConfigurationOptions,
                                TableProperties)
from zerobus.sdk.shared.headers_provider import HeadersProvider
from zerobus.sdk.shared.tls_config import TlsConfig
from zerobus.sdk.sync import ZerobusSdk
import grpc

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
NUM_RECORDS = 100


def create_sample_record(index):
    """
    Creates a sample AirQuality record.

    You can customize this to create records with different data patterns.
    """
    return record_pb2.AirQuality(device_name=f"sensor-{index % 10}", temp=20 + (index % 15), humidity=50 + (index % 40))


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
        """
        Initialize custom TLS configuration.

        Args:
            root_certificates: PEM-encoded root certificates (optional)
            private_key: PEM-encoded private key for client certificate (optional)
            certificate_chain: PEM-encoded certificate chain for client certificate (optional)
        """
        self.root_certificates = root_certificates
        self.private_key = private_key
        self.certificate_chain = certificate_chain

    def to_channel_credentials(self) -> grpc.ChannelCredentials:
        """
        Convert TLS configuration to gRPC ChannelCredentials.

        Returns:
            grpc.ChannelCredentials: SSL channel credentials with custom settings
        """
        return grpc.ssl_channel_credentials(
            root_certificates=self.root_certificates,
            private_key=self.private_key,
            certificate_chain=self.certificate_chain
        )


def main():
    print("Starting synchronous ingestion example (Protobuf)...")
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

        # Step 2: Define table properties
        table_properties = TableProperties(TABLE_NAME, record_pb2.AirQuality.DESCRIPTOR)
        logger.info(f"✓ Table properties configured for: {TABLE_NAME}")

        # Step 3: Create stream configuration with protobuf record type
        options = StreamConfigurationOptions(
            record_type=RecordType.PROTO,
            max_inflight_records=1000,
            recovery=True,
            recovery_timeout_ms=15000,
            recovery_backoff_ms=2000,
            recovery_retries=3,
        )
        logger.info("✓ Stream configuration created (Protobuf mode)")

        # Step 4: Create a stream with OAuth 2.0 authentication
        #
        # Standard method: OAuth 2.0 Client Credentials with default TLS (SecureTlsConfig)
        # The SDK automatically:
        #   - Uses system CA certificates for TLS
        #   - Includes authorization header with OAuth token
        #   - Includes x-databricks-zerobus-table-name header
        stream = sdk.create_stream(CLIENT_ID, CLIENT_SECRET, table_properties, options)

        # Advanced: Custom TLS configuration (for special use cases only)
        # Uncomment to use custom TLS (e.g., custom CA certificates, mTLS):
        # custom_tls = CustomTlsConfig(root_certificates=your_ca_certs)
        # stream = sdk.create_stream(CLIENT_ID, CLIENT_SECRET, table_properties, options, custom_tls)

        # Advanced: Custom headers provider (for special use cases only)
        # Uncomment to use custom headers instead of OAuth:
        # custom_provider = CustomHeadersProvider(custom_token="your-custom-token")
        # stream = sdk.create_stream(CLIENT_ID, CLIENT_SECRET, table_properties, options, headers_provider=custom_provider)

        # Advanced: Both custom TLS and custom headers provider
        # Uncomment to use both:
        # custom_tls = CustomTlsConfig(root_certificates=your_ca_certs)
        # custom_provider = CustomHeadersProvider(custom_token="your-custom-token")
        # stream = sdk.create_stream(CLIENT_ID, CLIENT_SECRET, table_properties, options, custom_tls, custom_provider)

        logger.info(f"✓ Stream created: {stream.stream_id}")

        # Step 5: Ingest records synchronously
        logger.info(f"\nIngesting {NUM_RECORDS} records (blocking mode)...")
        start_time = time.time()
        success_count = 0

        try:
            for i in range(NUM_RECORDS):
                # Create a record
                record = create_sample_record(i)

                # Ingest and wait for acknowledgment
                ack = stream.ingest_record(record)

                # Wait for record to be durably written
                ack.wait_for_ack()

                success_count += 1

                # Progress indicator
                if (i + 1) % 10 == 0:
                    logger.info(f"  Ingested {i + 1} records")

            end_time = time.time()
            duration_seconds = end_time - start_time
            records_per_second = NUM_RECORDS / duration_seconds

            # Step 6: Flush and close the stream
            logger.info("\nFlushing stream...")
            stream.flush()
            logger.info("✓ Stream flushed")

            stream.close()
            logger.info("✓ Stream closed")

            # Print summary
            print("\n" + "=" * 60)
            print("Ingestion Summary:")
            print(f"  Total records: {NUM_RECORDS}")
            print(f"  Successful: {success_count}")
            print(f"  Failed: {NUM_RECORDS - success_count}")
            print(f"  Duration: {duration_seconds:.2f} seconds")
            print(f"  Throughput: {records_per_second:.2f} records/sec")
            print(f"  Stream state: {stream.get_state()}")
            print("  Record type: Protobuf")
            print("=" * 60)

        except Exception as e:
            logger.error(f"\n✗ Error during ingestion: {e}")
            stream.close()
            raise

    except Exception as e:
        logger.error(f"\n✗ Failed to initialize stream: {e}")
        raise


if __name__ == "__main__":
    main()
