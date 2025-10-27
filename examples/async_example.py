"""
Asynchronous Ingestion Example

This example demonstrates record ingestion using the asynchronous API with Python's asyncio framework.

Use Case: Best for applications already using asyncio, async web frameworks (FastAPI, aiohttp),
or when integrating ingestion with other asynchronous operations in an event loop.

Note: Both sync and async APIs provide the same throughput and durability guarantees.
Choose based on your application's architecture, not performance requirements.
"""

import asyncio
import logging
import os
import time

# Import the generated protobuf module
import record_pb2

from zerobus.sdk.aio import ZerobusSdk
from zerobus.sdk.shared import StreamConfigurationOptions, TableProperties

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


def create_sample_record(index):
    """
    Creates a sample AirQuality record.

    You can customize this to create records with different data patterns.
    """
    return record_pb2.AirQuality(device_name=f"sensor-{index % 10}", temp=20 + (index % 15), humidity=50 + (index % 40))


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
    print("Starting asynchronous ingestion example...")
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

        # Step 2: Configure stream options with ack callback
        options = StreamConfigurationOptions(
            max_inflight_records=10_000,  # Allow 10k records in flight
            recovery=True,  # Enable automatic recovery
            ack_callback=create_ack_callback(),  # Track acknowledgments
        )
        logger.info("✓ Stream configuration created")

        # Step 3: Define table properties
        table_properties = TableProperties(TABLE_NAME, record_pb2.AirQuality.DESCRIPTOR)
        logger.info(f"✓ Table properties configured for: {TABLE_NAME}")

        # Step 4: Create a stream
        stream = await sdk.create_stream(CLIENT_ID, CLIENT_SECRET, table_properties, options)
        logger.info(f"✓ Stream created: {stream.stream_id}")

        # Step 5: Ingest records asynchronously
        logger.info(f"\nIngesting {NUM_RECORDS} records (non-blocking mode)...")
        start_time = time.time()

        try:
            # Store futures for later waiting
            futures = []

            for i in range(NUM_RECORDS):
                # Create a record with varying data
                record = create_sample_record(i)

                # Ingest record asynchronously
                future = await stream.ingest_record(record)
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
