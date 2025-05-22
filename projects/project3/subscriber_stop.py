import os
import json
import time
import threading
import logging
import psycopg2
import io
import pandas as pd
from google.cloud import pubsub_v1
from collections import defaultdict

# === CONFIGURATION ===
PROJECT_ID = "dataengineering-456318"
SUBSCRIPTION_ID = "trimet-stop-events-topic-sub"
CREDENTIAL_PATH = "/home/xiangqz/pubsub-key.json"
IDLE_TIMEOUT = 60

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = CREDENTIAL_PATH

DB_CONFIG = {
    "dbname": "postgres",
    "user": "postgres",
    "password": "psql908243",
    "host": "localhost",
    "port": "5432"
}

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler("subscriber_stop.log"), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# === Error counters/summary ===
error_counters = defaultdict(int)
error_first_message = {}

# Use a context object to avoid global mutables
class StopContext:
    def __init__(self):
        self.collected_messages = []
        self.last_message_time = time.time()
        self.total_received = 0
        self.subscriber = None
        self.streaming_future = None

ctx = StopContext()

# === Assertion functions ===
# 1. Vehicle number must be present and a valid positive integer
def assert_vehicle_number(record):
    vnum = record.get("vehicle_number")
    assert vnum is not None, "vehicle_number is None"
    try:
        vnum_int = int(vnum)
    except Exception:
        raise AssertionError(f"vehicle_number {vnum} is not an integer")
    assert vnum_int > 0, f"vehicle_number {vnum_int} not positive"

# 2. Route number must be present and a valid positive integer
def assert_route_number(record):
    rnum = record.get("route_number")
    assert rnum is not None, "route_number is None"
    try:
        rnum_int = int(rnum)
    except Exception:
        raise AssertionError(f"route_number {rnum} is not an integer")
    assert rnum_int > 0, f"route_number {rnum_int} not positive"

# 3. stop_time must be present, an integer(seconds in a day)
def assert_stop_time(record):
    stop_time = record.get("stop_time")
    assert stop_time is not None, "stop_time is None"
    try:
        stop_time_int = int(stop_time)
    except Exception:
        raise AssertionError(f"stop_time {stop_time} is not an integer")

# 4. x_coordinate must be present, a number, and positive
def assert_x_coordinate(record):
    x = record.get("x_coordinate")
    assert x is not None, "x_coordinate is None"
    try:
        x_val = float(x)
    except Exception:
        raise AssertionError(f"x_coordinate {x} is not a number")
    assert x_val > 0, f"x_coordinate {x_val} not positive"

# 5. y_coordinate must be present, a number, and positive
def assert_y_coordinate(record):
    y = record.get("y_coordinate")
    assert y is not None, "y_coordinate is None"
    try:
        y_val = float(y)
    except Exception:
        raise AssertionError(f"y_coordinate {y} is not a number")
    assert y_val > 0, f"y_coordinate {y_val} not positive"

# 6. pdx_trip must be present (string or number is OK)
def assert_pdx_trip(record):
    pdx_trip = record.get("pdx_trip")
    assert pdx_trip is not None, "pdx_trip is None"

# 7. service_date must be present (string is OK)
def assert_service_date(record):
    sdate = record.get("service_date")
    assert sdate is not None, "service_date is None"

assertion_funcs = [
    assert_vehicle_number,
    assert_route_number,
    assert_stop_time,
    assert_x_coordinate,
    assert_y_coordinate,
    assert_pdx_trip,
    assert_service_date
]

def callback(message):
    try:
        record = json.loads(message.data.decode("utf-8"))
        ctx.collected_messages.append(record)
        ctx.total_received += 1
        ctx.last_message_time = time.time()
    except Exception as e:
        error_type = "decode_error"
        error_counters[error_type] += 1
        if error_type not in error_first_message:
            error_first_message[error_type] = f"Error decoding message: {e}"
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
        return pd.DataFrame()

    valid_records = []
    discarded = 0
    for record in ctx.collected_messages:
        valid = True
        for func in assertion_funcs:
            try:
                func(record)
            except AssertionError as e:
                err_type = str(e)
                error_counters[err_type] += 1
                if err_type not in error_first_message:
                    error_first_message[err_type] = f"Validation error: {err_type}"
                valid = False
                break
        if valid:
            valid_records.append(record)
        else:
            discarded += 1

    logger.info(f"Total valid records to insert: {len(valid_records)}")
    logger.info(f"Total discarded records: {discarded}")

    if not valid_records:
        logger.info("No valid records after validation.")
        return pd.DataFrame()

    df = pd.DataFrame(valid_records)
    logger.info(f"Valid records after filtering: {len(df)}")

    # Convert numeric fields if needed
    numeric_fields = ['vehicle_number', 'route_number', 'stop_time', 'x_coordinate', 'y_coordinate']
    for field in numeric_fields:
        df[field] = pd.to_numeric(df[field], errors='coerce')

    logger.info(f"Rows ready for Trip insertion: {len(df)}")
    return df

def insert_into_trip_db(df):
    """
    Insert one row per unique trip into the Trip table with correct enum mapping.
    """
    if df.empty:
        return 0
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        # Build unique trip dataframe
        trip_df = df.drop_duplicates(subset=['pdx_trip'])
        trip_df = trip_df[trip_df['pdx_trip'].notnull() & trip_df['vehicle_number'].notnull()]
        inserted = 0
        for _, row in trip_df.iterrows():
            trip_id = int(row['pdx_trip'])
            route_id = int(row['route_number']) if pd.notnull(row['route_number']) else None
            vehicle_id = int(row['vehicle_number']) if pd.notnull(row['vehicle_number']) else None
            # Map service_key
            sk = row.get('service_key')
            if sk == 'W':
                service_key = 'Weekday'
            elif sk == 'S':
                service_key = 'Saturday'
            elif sk == 'U':
                service_key = 'Sunday'
            else:
                service_key = None
            # Map direction
            direction_raw = str(row.get('direction'))
            if direction_raw == '0':
                direction = 'Out'
            elif direction_raw == '1':
                direction = 'Back'
            else:
                direction = None

            try:
                cursor.execute("""
                    INSERT INTO trip (trip_id, route_id, vehicle_id, service_key, direction)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                """, (trip_id, route_id, vehicle_id, service_key, direction))
                inserted += 1
            except Exception as e:
                error_type = "db_insert_error_trip"
                error_counters[error_type] += 1
                if error_type not in error_first_message:
                    error_first_message[error_type] = f"DB insert error (trip): {e}"

        conn.commit()
        cursor.close()
        conn.close()
        logger.info(f"Inserted {inserted} unique trips into Trip table.")
        return inserted
    except Exception as e:
        error_type = "db_insert_error_trip_outer"
        error_counters[error_type] += 1
        if error_type not in error_first_message:
            error_first_message[error_type] = f"DB insert error (trip, outer): {e}"
        return 0

# === OLD: insert into stopevent table (not used in Trip-mode, kept for debugging) ===
# def insert_into_stopevent_db(df):
#     if df.empty:
#         return 0
#     try:
#         conn = psycopg2.connect(**DB_CONFIG)
#         cursor = conn.cursor()
#         output = io.StringIO()
#         df.to_csv(output, sep=',', header=False, index=False)
#         output.seek(0)
#         cursor.copy_from(
#             output,
#             'stopevent',
#             sep=',',
#             columns=(
#                 'vehicle_number', 'leave_time', 'train', 'route_number', 'direction',
#                 'service_key', 'trip_number', 'stop_time', 'arrive_time', 'dwell',
#                 'location_id', 'door', 'lift', 'ons', 'offs', 'estimated_load',
#                 'maximum_speed', 'train_mileage', 'pattern_distance', 'location_distance',
#                 'x_coordinate', 'y_coordinate', 'data_source', 'schedule_status',
#                 'pdx_trip', 'service_date'
#             )
#         )
#         conn.commit()
#         cursor.close()
#         conn.close()
#         logger.info(f"Inserted {len(df)} records into StopEvent table.")
#         return len(df)
#     except Exception as e:
#         error_type = "db_insert_error"
#         error_counters[error_type] += 1
#         if error_type not in error_first_message:
#             error_first_message[error_type] = f"DB insert error: {e}"
#         return 0

def print_error_summary():
    logger.info("\nError summary:")
    print("\nError summary:")
    for err_type, count in error_counters.items():
        msg = error_first_message.get(err_type, err_type)
        logger.info(f"{msg} - Occurred {count} times")
        print(f"{msg} - Occurred {count} times")

def main():
    logger.info("Starting subscriber with idle timeout monitoring...")

    ctx.subscriber = pubsub_v1.SubscriberClient()
    subscription_path = ctx.subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)
    ctx.streaming_future = ctx.subscriber.subscribe(subscription_path, callback=callback)

    monitor_thread = threading.Thread(target=idle_monitor, daemon=True)
    monitor_thread.start()

    try:
        ctx.streaming_future.result()
        logger.info(f"Total messages received: {ctx.total_received}")
    except Exception as e:
        logger.info(f"Subscriber stopped: {e}")

    logger.info("Validating and transforming into DB...")
    df = validate_and_transform()
    inserted = insert_into_trip_db(df)  # <--- Insert into Trip table

    # Uncomment below to insert into stopevent table instead
    # inserted = insert_into_stopevent_db(df)

    print_error_summary()
    discarded = ctx.total_received - inserted
    logger.info(f"SUMMARY: Received: {ctx.total_received}, Inserted: {inserted}, Discarded: {discarded}")
    print(f"\nSUMMARY: Received: {ctx.total_received}, Inserted: {inserted}, Discarded: {discarded}")
    logger.info("Done.")

if __name__ == "__main__":
    main()
