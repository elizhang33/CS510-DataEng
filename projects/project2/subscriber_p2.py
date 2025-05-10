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
    "dbname": "postgres",
    "user": "",
    "password": "",
    "host": "localhost",
    "port": "5432"
}

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Shared data
collected_messages = []
last_message_time = time.time()
subscriber = None
streaming_future = None
total_received = 0

def callback(message):
    global last_message_time, total_received
    try:
        record = json.loads(message.data.decode("utf-8"))
        collected_messages.append(record)
        total_received += 1
        last_message_time = time.time()
    except Exception as e:
        logger.warning(f"Error decoding message: {e}")
    message.ack()

def idle_monitor():
    global streaming_future, subscriber
    while True:
        time.sleep(60)
        idle_duration = time.time() - last_message_time
        if idle_duration > IDLE_TIMEOUT:
            logger.info("No messages received for 60 seconds. Shutting down subscriber...")
            if streaming_future:
                streaming_future.cancel()
            if subscriber:
                subscriber.close()
            break

def validate_and_transform():
    if not collected_messages:
        logger.info("No data collected. Exiting.")
        return []

    df = pd.DataFrame(collected_messages)

    # Validation
    valid_mask = (
        df['EVENT_NO_TRIP'].notnull() &
        df['VEHICLE_ID'].gt(0) &
        df['GPS_LATITUDE'].between(45.0, 46.0) &
        df['GPS_LONGITUDE'].between(-124.0, -121.0) &
        df['GPS_HDOP'].le(5.0) &
        df['ACT_TIME'].between(0, 86400)
    )
    df = df[valid_mask]

    if df.empty:
        logger.info("No valid records after filtering.")
        return []

    trip_counts = df['EVENT_NO_TRIP'].value_counts()
    valid_trips = trip_counts[trip_counts >= 2].index
    df = df[df['EVENT_NO_TRIP'].isin(valid_trips)]

    if df.empty:
        logger.info("No trips with 2 or more records.")
        return []

    # Timestamp
    df['tstamp'] = pd.to_datetime(df['OPD_DATE'].str[:9], format="%d%b%Y") + pd.to_timedelta(df['ACT_TIME'], unit='s')

    # Sort and speed calculation
    df.sort_values(['EVENT_NO_TRIP', 'ACT_TIME'], inplace=True)
    df['prev_METERS'] = df.groupby('EVENT_NO_TRIP')['METERS'].shift()
    df['prev_ACT_TIME'] = df.groupby('EVENT_NO_TRIP')['ACT_TIME'].shift()
    df['speed'] = (df['METERS'] - df['prev_METERS']) / (df['ACT_TIME'] - df['prev_ACT_TIME'])
    df['speed'] = df['speed'].fillna(0).clip(lower=0).round(2)

    # Copy second record's speed to the first record
    trip_groups = df.groupby('EVENT_NO_TRIP')
    for trip_id, group in trip_groups:
        if len(group) >= 2:
            first_idx = group.index[0]
            second_idx = group.index[1]
            df.at[first_idx, 'speed'] = df.at[second_idx, 'speed']
    
    logger.info(f"Total records in DataFrame after validation: {len(df)}")
    logger.info(f"Number of trip groups dropped for having <2 records: {(trip_counts < 2).sum()}")

    # Prepare final insert data
    insert_df = df[['tstamp', 'GPS_LATITUDE', 'GPS_LONGITUDE', 'speed', 'EVENT_NO_TRIP']].dropna()
    logger.info(f"Total rows to insert after transformation: {len(insert_df)}")
    
    return insert_df.values.tolist()

def insert_into_db(rows):
    if not rows:
        return 0
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        execute_batch(
            cursor,
            """
            INSERT INTO BreadCrumb_copy (tstamp, latitude, longitude, speed, trip_id)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
            """,
            rows
        )
        conn.commit()
        cursor.close()
        conn.close()
        return len(rows)
    except Exception as e:
        logger.error(f"DB insert error: {e}")
        return 0

def main():
    global subscriber, streaming_future

    logger.info("Starting subscriber with idle timeout monitoring...")

    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)
    streaming_future = subscriber.subscribe(subscription_path, callback=callback)

    # Start idle monitoring thread
    monitor_thread = threading.Thread(target=idle_monitor, daemon=True)
    monitor_thread.start()

    try:
        streaming_future.result()
        logger.info(f"Total messages received from topic: {total_received}")
        logger.info(f"Total records in collected_messages list: {len(collected_messages)}")
    except Exception as e:
        logger.info(f"Subscriber stopped: {e}")

    logger.info("Transforming and inserting data into DB...")
    rows = validate_and_transform()
    inserted = insert_into_db(rows)
    logger.info(f"Inserted {inserted} records into the database.")
    logger.info("Done.")

if __name__ == "__main__":
    main()
