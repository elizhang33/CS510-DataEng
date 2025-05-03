import os
import json
import psycopg2
import logging
from datetime import datetime, timedelta
from google.cloud import pubsub_v1
from psycopg2.extras import execute_batch

# === CONFIGURATION ===
PROJECT_ID = "dataengineering-456318"
SUBSCRIPTION_ID = "trimet-breadcrumbs-sub"
CREDENTIAL_PATH = "/home/xiangqz/pubsub-key.json"

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = CREDENTIAL_PATH

# Database connection
conn = psycopg2.connect(
    dbname="postgres",
    user="postgres",
    password="XXXXXXXXXX",
    host="localhost",
    port="5432"
)
cursor = conn.cursor()

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Cache previous breadcrumb for each trip to calculate speed
last_record_by_trip = {}

# Batch buffer for bulk inserts
breadcrumb_batch = []
BATCH_SIZE = 500
records_flushed = 0

def flush_batch():
    global records_flushed
    if not breadcrumb_batch:
        return
    try:
        execute_batch(
            cursor,
            """
            INSERT INTO BreadCrumb (tstamp, latitude, longitude, speed, trip_id)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
            """,
            breadcrumb_batch
        )
        conn.commit()
        records_flushed += len(breadcrumb_batch)
        breadcrumb_batch.clear()
    except Exception:
        # Suppress individual flush errors from logging
        pass

def parse_opd_datetime(opd_date, act_time):
    return datetime.strptime(opd_date[:9], "%d%b%Y") + timedelta(seconds=int(act_time))

def validate(record):
    try:
        if not record.get("EVENT_NO_TRIP"):
            return False
        if record.get("GPS_LATITUDE") is None or not (45.0 <= record["GPS_LATITUDE"] <= 46.0):
            return False
        if record.get("GPS_LONGITUDE") is None or not (-124.0 <= record["GPS_LONGITUDE"] <= -121.0):
            return False
        if not isinstance(record.get("VEHICLE_ID"), int) or record["VEHICLE_ID"] <= 0:
            return False
        if record.get("GPS_HDOP") is None or record["GPS_HDOP"] > 5.0:
            return False
        if record.get("ACT_TIME") is None or not (0 <= record["ACT_TIME"] <= 86400):
            return False
    except KeyError:
        return False
    return True

def compute_speed(current, previous):
    d_meters = current["METERS"] - previous["METERS"]
    d_seconds = current["ACT_TIME"] - previous["ACT_TIME"]
    if d_seconds > 0:
        return round(d_meters / d_seconds, 2)
    return 0.0

def cache_and_insert_breadcrumb(timestamp, lat, lon, speed, trip_id):
    breadcrumb_batch.append((timestamp, lat, lon, speed, trip_id))
    if len(breadcrumb_batch) >= BATCH_SIZE:
        flush_batch()

def callback(message):
    try:
        record = json.loads(message.data.decode("utf-8"))
        if not validate(record):
            message.ack()
            return

        trip_id = record["EVENT_NO_TRIP"]
        vehicle_id = record["VEHICLE_ID"]
        timestamp = parse_opd_datetime(record["OPD_DATE"], record["ACT_TIME"])
        lat = record["GPS_LATITUDE"]
        lon = record["GPS_LONGITUDE"]

        if trip_id not in last_record_by_trip:
            # Cache first record only
            last_record_by_trip[trip_id] = {
                "record": record,
                "timestamp": timestamp,
                "lat": lat,
                "lon": lon,
                "vehicle_id": vehicle_id
            }
            message.ack()
            return

        # Compute speed with previous record
        prev_info = last_record_by_trip[trip_id]
        prev_record = prev_info["record"]
        speed = compute_speed(record, prev_record)

        # Insert both previous (first) and current (second or later) breadcrumb
        cache_and_insert_breadcrumb(
            prev_info["timestamp"],
            prev_info["lat"],
            prev_info["lon"],
            speed,
            trip_id
        )
        cache_and_insert_breadcrumb(timestamp, lat, lon, speed, trip_id)

        # Update trip cache
        last_record_by_trip[trip_id] = {
            "record": record,
            "timestamp": timestamp,
            "lat": lat,
            "lon": lon,
            "vehicle_id": vehicle_id
        }
        # logger.info(f"Buffered breadcrumb for trip {trip_id} at {timestamp}")

    except Exception:
        pass
    message.ack()

def main():
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)
    future = subscriber.subscribe(subscription_path, callback=callback)
    logger.info(f"Listening for messages on {subscription_path}...")

    try:
        future.result()
    except KeyboardInterrupt:
        flush_batch()  # Ensure all pending data is flushed
        logger.info(f"Pipeline finished. Total records flushed to DB: {records_flushed}")
        future.cancel()

if __name__ == "__main__":
    main()
