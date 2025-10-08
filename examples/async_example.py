"""
Asynchronous Ingestion Example

This example demonstrates non-blocking (asynchronous) record ingestion for maximum
throughput. The SDK manages buffering and flow control automatically.

Use Case: Best for high-volume ingestion where maximum throughput is important.
Records are still durably written, but acknowledgments are handled asynchronously.
"""

import asyncio
import logging
import os
import time

from zerobus.sdk.aio import ZerobusSdk
from zerobus.sdk.shared import StreamConfigurationOptions

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
NUM_RECORDS = 100_000


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


def create_ack_callback():
    """
    Creates an acknowledgment callback that logs progress.

    The callback is invoked by the SDK whenever records are acknowledged by the server.
    """

    def callback(response):
        offset = response.durability_ack_up_to_offset
        # Log every 10000 records
        if offset % 10000 == 0:
            logger.info(f"  Acknowledged up to offset: {offset}")

    return callback


async def main():
    print("Starting asynchronous ingestion example...")
    print("=" * 60)

    try:
        # Step 1: Initialize the SDK
        ZerobusSdk(SERVER_ENDPOINT, UNITY_CATALOG_ENDPOINT)
        logger.info("✓ SDK initialized")

        # Step 2: Configure stream options with ack callback
        options = StreamConfigurationOptions(
            max_inflight_records=50_000,  # Allow 50k records in flight
            recovery=True,  # Enable automatic recovery
            ack_callback=create_ack_callback(),  # Track acknowledgments
        )
        logger.info("✓ Stream configuration created")

        # Step 3: Define table properties
        # Note: Replace with your own protobuf message descriptor
        # table_properties = TableProperties(
        #     TABLE_NAME,
        #     record_pb2.AirQuality.DESCRIPTOR
        # )

        # For demonstration, create a mock table properties
        logger.warning("Using mock configuration - update with your credentials and schema")

        # Step 4: Create a stream
        # stream = await sdk.create_stream(
        #     CLIENT_ID,
        #     CLIENT_SECRET,
        #     table_properties,
        #     options
        # )
        # logger.info(f"✓ Stream created: {stream.stream_id}")

        # Step 5: Ingest records asynchronously
        logger.info(f"\nIngesting {NUM_RECORDS} records (non-blocking mode)...")
        time.time()

        # Uncomment when using actual SDK:
        # try:
        #     # Store futures for later waiting
        #     futures = []
        #
        #     for i in range(NUM_RECORDS):
        #         # Create a record with varying data
        #         record = create_sample_record(i)
        #
        #         # Ingest record asynchronously
        #         future = await stream.ingest_record(record)
        #         futures.append(future)
        #
        #         # Progress indicator
        #         if (i + 1) % 10000 == 0:
        #             logger.info(f"  Submitted {i + 1} records")
        #
        #     submit_end_time = time.time()
        #     submit_duration = submit_end_time - start_time
        #     logger.info(f"\n✓ All records submitted in {submit_duration:.2f} seconds")
        #
        #     # Step 6: Flush and wait for all records to be durably written
        #     logger.info("\nFlushing stream and waiting for durability...")
        #     await stream.flush()
        #
        #     # Optionally wait for all individual futures
        #     await asyncio.gather(*futures)
        #
        #     end_time = time.time()
        #     total_duration = end_time - start_time
        #     records_per_second = NUM_RECORDS / total_duration
        #     avg_latency_ms = (total_duration * 1000.0) / NUM_RECORDS
        #
        #     logger.info("✓ All records durably written")
        #
        #     # Step 7: Close the stream
        #     await stream.close()
        #     logger.info("✓ Stream closed")
        #
        #     # Print summary
        #     print("\n" + "=" * 60)
        #     print("Ingestion Summary:")
        #     print(f"  Total records: {NUM_RECORDS}")
        #     print(f"  Submit time: {submit_duration:.2f} seconds")
        #     print(f"  Total time: {total_duration:.2f} seconds")
        #     print(f"  Throughput: {records_per_second:.2f} records/sec")
        #     print(f"  Average latency: {avg_latency_ms:.2f} ms/record")
        #     print("=" * 60)
        #
        # except Exception as e:
        #     logger.error(f"\n✗ Error during ingestion: {e}")
        #     await stream.close()
        #     raise

        logger.info("Example completed! Update the code with your credentials and schema.")

    except Exception as e:
        logger.error(f"\n✗ Failed to initialize stream: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main())
