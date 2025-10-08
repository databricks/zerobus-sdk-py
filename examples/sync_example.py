"""
Synchronous Ingestion Example

This example demonstrates blocking (synchronous) record ingestion where each record
is waited for before proceeding to the next one. This approach provides the strongest
durability guarantees but has lower throughput compared to asynchronous ingestion.

Use Case: Best for low-volume ingestion where durability is critical and you need
immediate confirmation of each write.
"""

import logging
import os
import time

from zerobus.sdk.shared import StreamConfigurationOptions
from zerobus.sdk.sync import ZerobusSdk

# You'll need to generate Python classes from record.proto:
# protoc --python_out=. record.proto
# Then uncomment the following line:
# import record_pb2



# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


# Configuration - update these with your values
SERVER_ENDPOINT = "your-shard-id.zerobus.region.cloud.databricks.com"
UNITY_CATALOG_ENDPOINT = "https://your-workspace.cloud.databricks.com"
TABLE_NAME = "catalog.schema.table"

# For OAuth authentication
CLIENT_ID = os.getenv("DATABRICKS_CLIENT_ID", "your-oauth-client-id")
CLIENT_SECRET = os.getenv("DATABRICKS_CLIENT_SECRET", "your-oauth-client-secret")

# Number of records to ingest
NUM_RECORDS = 1000


def create_sample_record(index):
    """
    Creates a sample AirQuality record.

    Replace this with your own record creation logic using your protobuf schema.
    """
    # Uncomment and use your protobuf message:
    # return record_pb2.AirQuality(
    #     device_name=f"sensor-{index % 10}",
    #     temp=20 + (index % 15),
    #     humidity=50 + (index % 40)
    # )

    # For demonstration purposes, we'll create a mock object
    # Replace this with actual protobuf message
    class MockRecord:
        def SerializeToString(self):
            return b"mock_data"

    return MockRecord()


def main():
    print("Starting synchronous ingestion example...")
    print("=" * 60)

    try:
        # Step 1: Initialize the SDK
        ZerobusSdk(SERVER_ENDPOINT, UNITY_CATALOG_ENDPOINT)
        logger.info("✓ SDK initialized")

        # Step 2: Define table properties
        # Note: Replace with your own protobuf message descriptor
        # table_properties = TableProperties(
        #     TABLE_NAME,
        #     record_pb2.AirQuality.DESCRIPTOR
        # )

        # For demonstration, create a mock table properties
        # Replace this with actual implementation
        logger.warning("Using mock table properties - update with your protobuf schema")

        # Step 3: Create stream configuration (optional)
        options = StreamConfigurationOptions(
            max_inflight_records=1000,
            recovery=True,
            recovery_timeout_ms=15000,
            recovery_backoff_ms=2000,
            recovery_retries=3,
        )

        # Step 4: Create a stream
        # stream = sdk.create_stream(
        #     CLIENT_ID,
        #     CLIENT_SECRET,
        #     table_properties,
        #     options
        # )
        # logger.info(f"✓ Stream created: {stream.stream_id}")

        # Step 5: Ingest records synchronously
        logger.info(f"\nIngesting {NUM_RECORDS} records (blocking mode)...")
        time.time()

        # Uncomment when using actual SDK:
        # try:
        #     for i in range(NUM_RECORDS):
        #         # Create a record
        #         record = create_sample_record(i)
        #
        #         # Ingest and wait for acknowledgment
        #         ack = stream.ingest_record(record)
        #
        #         # Wait for record to be durably written
        #         ack.wait_for_ack()
        #
        #         success_count += 1
        #
        #         # Progress indicator
        #         if (i + 1) % 100 == 0:
        #             logger.info(f"  Ingested {i + 1} records")
        #
        #     end_time = time.time()
        #     duration_seconds = end_time - start_time
        #     records_per_second = NUM_RECORDS / duration_seconds
        #
        #     # Step 6: Close the stream
        #     stream.close()
        #     logger.info("\n✓ Stream closed")
        #
        #     # Print summary
        #     print("\n" + "=" * 60)
        #     print("Ingestion Summary:")
        #     print(f"  Total records: {NUM_RECORDS}")
        #     print(f"  Successful: {success_count}")
        #     print(f"  Failed: {NUM_RECORDS - success_count}")
        #     print(f"  Duration: {duration_seconds:.2f} seconds")
        #     print(f"  Throughput: {records_per_second:.2f} records/sec")
        #     print("=" * 60)
        #
        # except Exception as e:
        #     logger.error(f"\n✗ Error during ingestion: {e}")
        #     stream.close()
        #     raise

        logger.info("Example completed! Update the code with your credentials and schema.")

    except Exception as e:
        logger.error(f"\n✗ Failed to initialize stream: {e}")
        raise


if __name__ == "__main__":
    main()
