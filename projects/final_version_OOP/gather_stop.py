# Refactored OOP-based TriMet Breadcrumb Gathering Pipeline with Comments

import os
import json
import urllib.request
import datetime
import time
from concurrent.futures import wait, FIRST_COMPLETED, as_completed
from google.cloud import pubsub_v1

class BreadcrumbGatherPipeline:
    def __init__(self, vehicle_file, topic_id, project_id, cred_path):
        self.vehicle_file = vehicle_file
        self.topic_id = topic_id
        self.project_id = project_id
        self.cred_path = cred_path
        self.daily_file = f"breadcrumbs_{datetime.datetime.now().strftime('%Y%m%d')}.json"

        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = self.cred_path
        self.publisher = pubsub_v1.PublisherClient()
        self.topic_path = self.publisher.topic_path(self.project_id, self.topic_id)
        self.log_file = "breadcrumb_pipeline.log"

    def log(self, msg, scope="main"):
        # Logging function with timestamp and scope-based log file separation
        ts = datetime.datetime.now().strftime("[%m-%d-%Y--%H:%M:%S.%f]")[:-3]
        log_msg = f"{ts} {msg}"
        print(log_msg)
        with open(self.log_file, "a") as f:
            f.write(log_msg + "\n")

    def load_vehicle_ids(self):
        # Load list of vehicle IDs from the specified file
        try:
            with open(self.vehicle_file, "r") as f:
                return [line.strip() for line in f if line.strip()]
        except Exception as e:
            self.log(f"Error reading vehicle file: {e}", "gather")
            return []

    def gather_data(self):
        # Download new records from PSU Breadcrumb API for each vehicle
        self.log("Gathering from PSU API...", "gather")
        vehicle_ids = self.load_vehicle_ids()

        existing_records = set()
        if os.path.exists(self.daily_file):
            with open(self.daily_file, "r") as f:
                for line in f:
                    try:
                        rec = json.loads(line)
                        existing_records.add(json.dumps(rec, sort_keys=True))
                    except json.JSONDecodeError:
                        continue

        new_records = []
        for vid in vehicle_ids:
            url = f"https://busdata.cs.pdx.edu/api/getBreadCrumbs?vehicle_id={vid}"
            try:
                with urllib.request.urlopen(url) as response:
                    if response.status == 200:
                        records = json.loads(response.read())
                        for record in records:
                            record_str = json.dumps(record, sort_keys=True)
                            if record_str not in existing_records:
                                new_records.append(record)
                                existing_records.add(record_str)
            except Exception as e:
                self.log(f"Fetch error for vehicle {vid}: {e}", "gather")

        if new_records:
            with open(self.daily_file, "a") as f:
                for record in new_records:
                    json.dump(record, f)
                    f.write("\n")
            self.log(f"Appended {len(new_records)} new records to {self.daily_file}", "gather")
        else:
            self.log("No new records fetched today.", "gather")

        return new_records

    def publish_data(self, records, max_in_flight=500):
        # Publish records to the breadcrumb Pub/Sub topic
        self.log("Starting publishing...", "publish")
        published = 0
        skipped = 0
        futures = []

        def future_callback(fut, idx):
            try:
                fut.result()
            except Exception as e:
                nonlocal skipped
                skipped += 1
                self.log(f"[ERROR] Record {idx} failed to publish: {e}", "publish")

        for i, record in enumerate(records, start=1):
            try:
                message = json.dumps(record).encode("utf-8")
                future = self.publisher.publish(self.topic_path, message)
                future.add_done_callback(lambda fut, idx=i: future_callback(fut, idx))
                futures.append(future)
                published += 1
                if len(futures) >= max_in_flight:
                    done, not_done = wait(futures, return_when=FIRST_COMPLETED)
                    futures = list(not_done)
            except Exception as e:
                skipped += 1
                self.log(f"[ERROR] Record {i} failed to publish: {e}", "publish")

        self.log(f"Waiting for {len(futures)} publishes to complete...", "publish")
        for fut in as_completed(futures):
            pass

        self.log(f"[DONE] Published: {published}, Skipped: {skipped}", "publish")

    def run(self):
        # Run the complete pipeline
        start = time.time()
        self.log("Starting full breadcrumb pipeline...")
        new_records = self.gather_data()
        if new_records:
            time.sleep(10)  # slight delay for any system sync
            self.publish_data(new_records)
        end = time.time()
        self.log(f"Pipeline finished in {end - start:.2f} seconds.")

# Main execution
if __name__ == "__main__":
    pipeline = BreadcrumbGatherPipeline(
        vehicle_file="vehicle_ids.txt",
        topic_id="XXXXXX",
        project_id="XXXXXX",
        cred_path="XXXXXX.json"
    )
    pipeline.run()
