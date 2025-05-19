import os
import json
import time
import threading
import logging
import psycopg2
import pandas as pd
from datetime import datetime, timedelta
from google.cloud import pubsub_v1
from psycopg2.extras import execute_batch

# === CONFIGURATION ===
PROJECT_ID = "dataengineering-456318"
SUBSCRIPTION_ID = "trimet-breadcrumbs-sub"
CREDENTIAL_PATH = "/home/xiangqz/pubsub-key.json"
IDLE_TIMEOUT = 60  # 60 seconds

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = CREDENTIAL_PATH

DB_CONFIG = {
    "dbname": "XXXXX",
    "user": "XXXXXXX",
    "password": "XXXXXXX",
    "host": "XXXXXX",
    "port": "XXXX"
}

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class CollectorContext:
    def __init__(self):
        self.collected_messages = []
        self.last_message_time = time.time()
        self.total_received = 0
        self.subscriber = None
        self.streaming_future = None

ctx = CollectorContext()


# 1. Non-null trip_id
def assert_trip_id(record):
    assert record.get("EVENT_NO_TRIP") is not None, "Null trip_id"

# 2 and 3. Valid tstamp (OPD_DATE and ACT_TIME not null, and can be combined into a timestamp)
def assert_tstamp(record):
    assert record.get("OPD_DATE") is not None, "Missing OPD_DATE"
    assert record.get("ACT_TIME") is not None, "Missing ACT_TIME"

# 4. Latitude must be non-null and in bounds
def assert_latitude(record):
    lat = record.get("GPS_LATITUDE")
    assert lat is not None, "Latitude is None"
    assert 45.0 <= lat <= 46.0, f"Latitude {lat} out of range"

# 5. Longitude must be non-null and in bounds
def assert_longitude(record):
    lon = record.get("GPS_LONGITUDE")
    assert lon is not None, "Longitude is None"
    assert -124.0 <= lon <= -121.0, f"Longitude {lon} out of range"

# 6. HDOP must be present, ≤ 5.0
def assert_hdop(record):
    hdop = record.get("GPS_HDOP")
    assert hdop is not None, "HDOP is None"
    assert hdop <= 5.0, f"HDOP {hdop} too high"

# 7. Trip must have at least two readings
def assert_trip_length(trip_records):
    assert len(trip_records) >= 2, "Trip has less than 2 readings"

# 8. How many records will be inserted vs discarded.
def assert_log_counts(inserted, discarded):
    logger.info(f"Total valid records to insert: {inserted}")
    logger.info(f"Total discarded records: {discarded}")

assertion_funcs = [
    assert_trip_id,
    assert_tstamp,
    assert_latitude,
    assert_longitude,
    assert_hdop
]

# Pub/Sub callback
def callback(message):
    try:
        record = json.loads(message.data.decode("utf-8"))
        ctx.collected_messages.append(record)
        ctx.total_received += 1
        ctx.last_message_time = time.time()
    except Exception as e:
        logger.warning(f"Error decoding message: {e}")
    message.ack()

def idle_monitor():
    while True:
        time.sleep(60)
        idle_duration = time.time() - ctx.last_message_time
        if idle_duration > IDLE_TIMEOUT:
            logger.info("No messages received for 60 seconds. Shutting down subscriber...")
            if ctx.streaming_future:
                ctx.streaming_future.cancel()
            if ctx.subscriber:
                ctx.subscriber.close()
            break

def validate_and_transform():
    if not ctx.collected_messages:
        logger.info("No data collected. Exiting.")
        return []

    valid_records = []
    discarded = 0
    trip_dict = {}

    # --- Per-record validation ---
    for record in ctx.collected_messages:
        valid = True
        for func in assertion_funcs:
            try:
                func(record)
            except AssertionError as e:
                logger.error(f"Validation error: {e} | Record: {record}")
                valid = False
                break
        if valid:
            tid = record.get("EVENT_NO_TRIP")
            if tid not in trip_dict:
                trip_dict[tid] = []
            trip_dict[tid].append(record)
        else:
            discarded += 1

    # --- Inter-record/trip-level validation ---
    for trip_id, records in trip_dict.items():
        try:
            assert_trip_length(records)
            valid_records.extend(records)
        except AssertionError as e:
            logger.error(f"Trip-level validation error for trip {trip_id}: {e}")
            discarded += len(records)

    logger.info(f"Total valid records to insert: {len(valid_records)}")
    logger.info(f"Total discarded records: {discarded}")
    return valid_records

def transform_for_insert(records):
    # For each record, compute timestamp and speed if not already present
    df = pd.DataFrame(records)

    # Timestamp
    df['tstamp'] = pd.to_datetime(df['OPD_DATE'].str[:9], format="%d%b%Y") + pd.to_timedelta(df['ACT_TIME'], unit='s')

    # Sort and speed calculation
    df.sort_values(['EVENT_NO_TRIP', 'ACT_TIME'], inplace=True)
    df['prev_METERS'] = df.groupby('EVENT_NO_TRIP')['METERS'].shift()
    df['prev_ACT_TIME'] = df.groupby('EVENT_NO_TRIP')['ACT_TIME'].shift()
    df['speed'] = (df['METERS'] - df['prev_METERS']) / (df['ACT_TIME'] - df['prev_ACT_TIME'])
    df['speed'] = df['speed'].fillna(0).clip(lower=0).round(2)
    
    # 9 and 10 Speed must be realistic
    df = df[df['speed'].between(0, 30)]

    # Copy second record's speed to the first record
    trip_groups = df.groupby('EVENT_NO_TRIP')
    for trip_id, group in trip_groups:
        if len(group) >= 2:
            first_idx = group.index[0]
            second_idx = group.index[1]
            df.at[first_idx, 'speed'] = df.at[second_idx, 'speed']

    # Prepare final insert data
    insert_df = df[['tstamp', 'GPS_LATITUDE', 'GPS_LONGITUDE', 'speed', 'EVENT_NO_TRIP']].dropna()
    return insert_df

def insert_into_db(df):
    if df.empty:
        return 0
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        # Use in-memory CSV for copy_from
        output = io.StringIO()
        df.to_csv(output, sep=',', header=False, index=False)
        output.seek(0)
        cursor.copy_from(
            output,
            'breadcrumb_copy',
            sep=',',
            columns=('tstamp', 'latitude', 'longitude', 'speed', 'trip_id')
        )
        conn.commit()
        cursor.close()
        conn.close()
        logger.info(f"Inserted {len(df)} records into BreadCrumb_copy.")
        return len(df)
    except Exception as e:
        logger.error(f"DB insert error: {e}")
        return 0

def main():
    logger.info("Starting subscriber with idle timeout monitoring...")

    ctx.subscriber = pubsub_v1.SubscriberClient()
    subscription_path = ctx.subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)
    ctx.streaming_future = ctx.subscriber.subscribe(subscription_path, callback=callback)

    monitor_thread = threading.Thread(target=idle_monitor, daemon=True)
    monitor_thread.start()

    try:
        ctx.streaming_future.result()
        logger.info(f"Total messages received from topic: {ctx.total_received}")
        logger.info(f"Total records in collected_messages list: {len(ctx.collected_messages)}")
    except Exception as e:
        logger.info(f"Subscriber stopped: {e}")

    logger.info("Validating and transforming data for DB insert...")
    valid_records = validate_and_transform()

    if not valid_records:
        logger.info("No valid records after validation. Exiting.")
        return

    df = transform_for_insert(valid_records)
    inserted = insert_into_db(df)
    logger.info(f"Inserted {inserted} records into the database.")
    logger.info("Done.")

if __name__ == "__main__":
    main()
