import os
import io
import json
import time
import threading
import logging
import psycopg2
import pandas as pd
from datetime import datetime, timedelta
from google.cloud import pubsub_v1
from psycopg2.extras import execute_batch
from collections import defaultdict

class BreadcrumbSubscriber:
    def __init__(self, project_id, subscription_id, cred_path, db_config, idle_timeout=60):
        self.project_id = project_id
        self.subscription_id = subscription_id
        self.cred_path = cred_path
        self.db_config = db_config
        self.idle_timeout = idle_timeout

        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = self.cred_path
        self.logger = self._init_logger()
        self.subscriber = pubsub_v1.SubscriberClient()
        self.subscription_path = self.subscriber.subscription_path(self.project_id, self.subscription_id)

        self.collected_messages = []
        self.last_message_time = time.time()
        self.total_received = 0
        self.streaming_future = None

        self.error_counters = defaultdict(int)
        self.error_first_message = {}

    def _init_logger(self):
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        return logging.getLogger(__name__)

    def callback(self, message):
        # Process each message from Pub/Sub
        try:
            record = json.loads(message.data.decode("utf-8"))
            self.collected_messages.append(record)
            self.total_received += 1
            self.last_message_time = time.time()
        except Exception as e:
            err_type = "decode_error"
            self.error_counters[err_type] += 1
            if err_type not in self.error_first_message:
                self.error_first_message[err_type] = f"Error decoding message: {e}"
        message.ack()

    def idle_monitor(self):
        # Stop subscriber after idle timeout
        while True:
            time.sleep(60)
            idle = time.time() - self.last_message_time
            if idle > self.idle_timeout:
                self.logger.info("Idle timeout reached. Shutting down subscriber...")
                if self.streaming_future:
                    self.streaming_future.cancel()
                self.subscriber.close()
                break

    def run(self):
        # Start subscription and wait for data
        self.logger.info("Starting subscriber with idle timeout monitoring...")
        self.streaming_future = self.subscriber.subscribe(self.subscription_path, callback=self.callback)

        monitor_thread = threading.Thread(target=self.idle_monitor, daemon=True)
        monitor_thread.start()

        try:
            self.streaming_future.result()
        except Exception as e:
            self.logger.info(f"Subscriber stopped: {e}")

        self.logger.info(f"Total messages received: {self.total_received}")
        self.logger.info("Validating and transforming for DB insert...")

        valid_records = self.validate_and_transform()
        if not valid_records:
            self.logger.info("No valid records after validation. Exiting.")
            self.print_error_summary()
            return

        df = self.transform_for_insert(valid_records)
        inserted = self.insert_into_db(df)
        self.print_error_summary()

        discarded = self.total_received - inserted
        self.logger.info(f"SUMMARY: Received: {self.total_received}, Inserted: {inserted}, Discarded: {discarded}")

    def validate_and_transform(self):
        if not self.collected_messages:
            return []

        valid_records = []
        trip_dict = {}

        # --- Per-record validation ---
        for record in self.collected_messages:
            if self.is_valid_record(record):
                tid = record.get("EVENT_NO_TRIP")
                trip_dict.setdefault(tid, []).append(record)

        # --- Inter-record validation ---
        for trip_id, records in trip_dict.items():
            if len(records) >= 2:
                valid_records.extend(records)
            else:
                self.track_error("Trip has less than 2 readings")

        return valid_records

     def is_valid_record(self, record):
        # Validate each record using dedicated assertion functions
        try:
            assert self.check_trip_id(record), "Null trip_id"
            assert self.check_opd_date(record), "Missing OPD_DATE"
            assert self.check_act_time(record), "Missing ACT_TIME"
            assert self.check_latitude(record), f"Latitude {record.get('GPS_LATITUDE')} out of range"
            assert self.check_longitude(record), f"Longitude {record.get('GPS_LONGITUDE')} out of range"
            assert self.check_hdop(record), f"HDOP {record.get('GPS_HDOP')} too high"
            return True
        except AssertionError as e:
            self.track_error(str(e))
            return False

    def check_trip_id(self, record):
        # Check that the EVENT_NO_TRIP field exists and is not None
        return record.get("EVENT_NO_TRIP") is not None

    def check_opd_date(self, record):
        # Check that the OPD_DATE field exists and is not None
        return record.get("OPD_DATE") is not None

    def check_act_time(self, record):
        # Check that the ACT_TIME field exists and is not None
        return record.get("ACT_TIME") is not None

    def check_latitude(self, record):
        # Check that latitude is within valid Portland area bounds
        lat = record.get("GPS_LATITUDE")
        return lat is not None and 45.0 <= lat <= 46.0

    def check_longitude(self, record):
        # Check that longitude is within valid Portland area bounds
        lon = record.get("GPS_LONGITUDE")
        return lon is not None and -124.0 <= lon <= -121.0

    def check_hdop(self, record):
        # Check that the GPS_HDOP value is within accuracy threshold
        hdop = record.get("GPS_HDOP")
        return hdop is not None and hdop <= 5.0

    def transform_for_insert(self, records):
        df = pd.DataFrame(records)
        df['tstamp'] = pd.to_datetime(df['OPD_DATE'].str[:9], format="%d%b%Y") + pd.to_timedelta(df['ACT_TIME'], unit='s')
        df.sort_values(['EVENT_NO_TRIP', 'ACT_TIME'], inplace=True)
        df['prev_METERS'] = df.groupby('EVENT_NO_TRIP')['METERS'].shift()
        df['prev_ACT_TIME'] = df.groupby('EVENT_NO_TRIP')['ACT_TIME'].shift()
        df['speed'] = (df['METERS'] - df['prev_METERS']) / (df['ACT_TIME'] - df['prev_ACT_TIME'])
        df['speed'] = df['speed'].fillna(0).clip(lower=0).round(2)

        # Copy second speed to the first
        for tid, group in df.groupby('EVENT_NO_TRIP'):
            if len(group) >= 2:
                df.at[group.index[0], 'speed'] = df.at[group.index[1], 'speed']

        df = df[df['speed'] <= 30]  # Remove unrealistic speeds
        return df[['tstamp', 'GPS_LATITUDE', 'GPS_LONGITUDE', 'speed', 'EVENT_NO_TRIP']].dropna()

    def insert_into_db(self, df):
        if df.empty:
            return 0
        try:
            conn = psycopg2.connect(**self.db_config)
            cursor = conn.cursor()
            execute_batch(cursor,
                """
                INSERT INTO breadcrumb_copy (tstamp, latitude, longitude, speed, trip_id)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
                """,
                df.values.tolist()
            )
            conn.commit()
            cursor.close()
            conn.close()
            self.logger.info(f"Inserted {len(df)} records into breadcrumb_copy.")
            return len(df)
        except Exception as e:
            self.track_error(f"DB insert error: {e}")
            return 0

    def track_error(self, error_type):
        self.error_counters[error_type] += 1
        if error_type not in self.error_first_message:
            self.error_first_message[error_type] = error_type

    def print_error_summary(self):
        self.logger.info("Error summary:")
        for err_type, count in self.error_counters.items():
            msg = self.error_first_message.get(err_type, err_type)
            self.logger.info(f"{msg} - Occurred {count} times")

# === Execution ===
if __name__ == "__main__":
    subscriber = BreadcrumbSubscriber(
        project_id="XXXXXX",
        subscription_id="XXXXXX",
        cred_path="XXXXXX.json",
        db_config={
            "dbname": "XXXXXX",
            "user": "XXXXXX",
            "password": "XXXXXX",
            "host": "XXXXX",
            "port": "XXXXXX"
        }
    )
    subscriber.run()



    
