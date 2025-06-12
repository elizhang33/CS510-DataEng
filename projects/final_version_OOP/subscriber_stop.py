import os
import json
import time
import threading
import logging
import psycopg2
import pandas as pd
from google.cloud import pubsub_v1
from collections import defaultdict

class StopEventSubscriber:
    def __init__(self, project_id, subscription_id, cred_path, db_config, idle_timeout=60):
        """Initialize subscriber configuration."""
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
        """Configure logger."""
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        return logging.getLogger(__name__)

    def callback(self, message):
        """Callback function for processing incoming messages."""
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
        """Thread function that monitors for idle timeout."""
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
        """Run the subscriber."""
        self.logger.info("Starting StopEvent subscriber with idle timeout monitoring...")
        self.streaming_future = self.subscriber.subscribe(self.subscription_path, callback=self.callback)
        monitor_thread = threading.Thread(target=self.idle_monitor, daemon=True)
        monitor_thread.start()

        try:
            self.streaming_future.result()
        except Exception as e:
            self.logger.info(f"Subscriber stopped: {e}")

        self.logger.info(f"Total messages received: {self.total_received}")
        self.logger.info("Validating and transforming for DB insert...")

        df = self.validate_and_transform()
        inserted = self.insert_into_db(df)
        self.print_error_summary()

        discarded = self.total_received - inserted
        self.logger.info(f"SUMMARY: Received: {self.total_received}, Inserted: {inserted}, Discarded: {discarded}")

    def validate_and_transform(self):
        """Validate records using assertions and return clean DataFrame."""
        if not self.collected_messages:
            return pd.DataFrame()

        valid_records = []
        for record in self.collected_messages:
            if self.is_valid_record(record):
                valid_records.append(record)

        if not valid_records:
            return pd.DataFrame()

        df = pd.DataFrame(valid_records)
        for field in ['vehicle_number', 'route_number', 'stop_time', 'x_coordinate', 'y_coordinate']:
            df[field] = pd.to_numeric(df[field], errors='coerce')

        self.logger.info(f"Total valid records to insert: {len(df)}")
        return df

    def is_valid_record(self, record):
        """Run all individual assertions on a record."""
        try:
            self.assert_vehicle_number(record)
            self.assert_route_number(record)
            self.assert_stop_time(record)
            self.assert_x_coordinate(record)
            self.assert_y_coordinate(record)
            self.assert_pdx_trip(record)
            self.assert_service_date(record)
            return True
        except AssertionError as e:
            self.track_error(str(e))
            return False

    # === Individual assertion functions ===

    def assert_vehicle_number(self, record):
        vnum = record.get("vehicle_number")
        assert vnum is not None, "vehicle_number is None"
        assert int(vnum) > 0, f"vehicle_number {vnum} not positive"

    def assert_route_number(self, record):
        rnum = record.get("route_number")
        assert rnum is not None, "route_number is None"
        assert int(rnum) > 0, f"route_number {rnum} not positive"

    def assert_stop_time(self, record):
        stop_time = record.get("stop_time")
        assert stop_time is not None, "stop_time is None"
        int(stop_time)  # will raise if not an integer

    def assert_x_coordinate(self, record):
        x = record.get("x_coordinate")
        assert x is not None, "x_coordinate is None"
        assert float(x) > 0, f"x_coordinate {x} not positive"

    def assert_y_coordinate(self, record):
        y = record.get("y_coordinate")
        assert y is not None, "y_coordinate is None"
        assert float(y) > 0, f"y_coordinate {y} not positive"

    def assert_pdx_trip(self, record):
        assert record.get("pdx_trip") is not None, "pdx_trip is None"

    def assert_service_date(self, record):
        assert record.get("service_date") is not None, "service_date is None"

    def insert_into_db(self, df):
        """Insert validated DataFrame into the stopevent table."""
        if df.empty:
            return 0
        try:
            conn = psycopg2.connect(**self.db_config)
            cursor = conn.cursor()
            from io import StringIO
            output = StringIO()
            df.to_csv(output, sep=',', header=False, index=False)
            output.seek(0)
            cursor.copy_from(
                output,
                'stopevent',
                sep=',',
                columns=[
                    'vehicle_number', 'leave_time', 'train', 'route_number', 'direction',
                    'service_key', 'trip_number', 'stop_time', 'arrive_time', 'dwell',
                    'location_id', 'door', 'lift', 'ons', 'offs', 'estimated_load',
                    'maximum_speed', 'train_mileage', 'pattern_distance', 'location_distance',
                    'x_coordinate', 'y_coordinate', 'data_source', 'schedule_status',
                    'pdx_trip', 'service_date'
                ]
            )
            conn.commit()
            cursor.close()
            conn.close()
            self.logger.info(f"Inserted {len(df)} records into stopevent.")
            return len(df)
        except Exception as e:
            self.track_error(f"DB insert error: {e}")
            return 0

    def track_error(self, error_type):
        """Track validation or insertion errors."""
        self.error_counters[error_type] += 1
        if error_type not in self.error_first_message:
            self.error_first_message[error_type] = error_type

    def print_error_summary(self):
        """Print a summary of all errors encountered."""
        self.logger.info("Error summary:")
        for err_type, count in self.error_counters.items():
            msg = self.error_first_message.get(err_type, err_type)
            self.logger.info(f"{msg} - Occurred {count} times")

if __name__ == "__main__":
    subscriber = StopEventSubscriber(
        project_id="XXXXXX",
        subscription_id="XXXXXX",
        cred_path="XXXXXX.json",
        db_config={
            "dbname": "XXXXXX",
            "user": "XXXXXX",
            "password": "XXXXXX",
            "host": "XXXXXX",
            "port": "XXXXXX"
        }
    )
    subscriber.run()
