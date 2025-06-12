# Refactored OOP-based TriMet StopEvent Pipeline with Comments

import os
import json
import time
import datetime
import urllib.request
from concurrent.futures import wait, FIRST_COMPLETED, as_completed
from google.cloud import pubsub_v1
from bs4 import BeautifulSoup

class StopEventPipeline:
    def __init__(self, vehicle_file, topic_id, project_id, cred_path):
        # Initialize configuration and setup
        self.vehicle_file = vehicle_file
        self.topic_id = topic_id
        self.project_id = project_id
        self.cred_path = cred_path
        self.daily_file = f"stop_events_{datetime.datetime.now().strftime('%Y%m%d')}.json"

        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = self.cred_path
        self.publisher = pubsub_v1.PublisherClient()
        self.topic_path = self.publisher.topic_path(self.project_id, self.topic_id)
        self.logfile = "stop_pipeline.log"

    def log(self, msg):
        # Log messages with timestamps to both console and file
        ts = datetime.datetime.now().strftime("[%m-%d-%Y--%H:%M:%S.%f]")[:-3]
        log_msg = f"{ts} {msg}"
        print(log_msg)
        with open(self.logfile, "a") as f:
            f.write(log_msg + "\n")

    def load_vehicle_ids(self):
        # Load vehicle IDs from file
        try:
            with open(self.vehicle_file, "r") as f:
                return [line.strip() for line in f if line.strip()]
        except Exception as e:
            self.log(f"Error reading vehicle file: {e}")
            return []

    def parse_html(self, html_text):
        # Parse StopEvent HTML into structured JSON records
        soup = BeautifulSoup(html_text, "html.parser")
        records = []
        h1 = soup.find("h1")
        service_date = h1.text.strip().split()[-1] if h1 and "stop data for" in h1.text else None

        for h2 in soup.find_all("h2"):
            if not h2.text.startswith("Stop events for PDX_TRIP"):
                continue
            pdx_trip = h2.text.strip().split()[-1]
            table = h2.find_next("table")
            if not table:
                continue
            headers = [th.text.strip() for th in table.find("tr").find_all("th")]
            for tr in table.find_all("tr")[1:]:
                values = [td.text.strip() for td in tr.find_all("td")]
                if len(values) != len(headers):
                    continue
                record = dict(zip(headers, values))
                record["pdx_trip"] = pdx_trip
                record["service_date"] = service_date
                records.append(record)
        return records

    def gather_data(self):
        # Main data gathering logic
        self.log("Gathering StopEvent data...")
        vehicle_ids = self.load_vehicle_ids()

        existing = set()
        if os.path.exists(self.daily_file):
            with open(self.daily_file, "r") as f:
                for line in f:
                    try:
                        rec = json.loads(line)
                        existing.add(json.dumps(rec, sort_keys=True))
                    except json.JSONDecodeError:
                        continue

        new_records = []
        for vid in vehicle_ids:
            url = f"https://busdata.cs.pdx.edu/api/getStopEvents?vehicle_num={vid}"
            try:
                with urllib.request.urlopen(url) as response:
                    raw = response.read().decode("utf-8")
                    if not raw.strip():
                        continue
                    parsed = self.parse_html(raw)
                    for record in parsed:
                        rec_str = json.dumps(record, sort_keys=True)
                        if rec_str not in existing:
                            new_records.append(record)
                            existing.add(rec_str)
            except Exception as e:
                self.log(f"Vehicle {vid} fetch error: {e}")

        if new_records:
            with open(self.daily_file, "a") as f:
                for rec in new_records:
                    json.dump(rec, f)
                    f.write("\n")
            self.log(f"Appended {len(new_records)} new StopEvent records.")
        else:
            self.log("No new StopEvent records fetched.")

        return new_records

    def publish_data(self, records, max_in_flight=500):
        # Publish validated StopEvent records to Pub/Sub topic
        self.log("Publishing StopEvent data...")
        published = 0
        skipped = 0
        futures = []

        def callback(fut):
            try:
                fut.result()
            except Exception:
                nonlocal skipped
                skipped += 1

        for i, record in enumerate(records, 1):
            try:
                msg = json.dumps(record).encode("utf-8")
                future = self.publisher.publish(self.topic_path, msg)
                future.add_done_callback(callback)
                futures.append(future)
                published += 1
                if len(futures) >= max_in_flight:
                    done, not_done = wait(futures, return_when=FIRST_COMPLETED)
                    futures = list(not_done)
            except Exception as e:
                skipped += 1
                self.log(f"[ERROR] Record {i} publish failed: {e}")

        self.log(f"Waiting for {len(futures)} publish tasks...")
        for fut in as_completed(futures):
            pass

        self.log(f"[DONE] Published: {published}, Skipped: {skipped}")

    def run(self):
        # Entry point for running the entire pipeline
        start = time.time()
        self.log("Starting full StopEvent pipeline...")
        records = self.gather_data()
        if records:
            time.sleep(10)  # Optional wait before publishing
            self.publish_data(records)
        end = time.time()
        self.log(f"Pipeline finished in {end - start:.2f} seconds.")

# Execution entry point
if __name__ == "__main__":
    pipeline = StopEventPipeline(
        vehicle_file="vehicle_ids.txt",
        topic_id="xxxxxx",
        project_id="xxxxxx",
        cred_path="xxxxxx.json"
    )
    pipeline.run()
