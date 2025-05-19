import os
import json
import urllib.request
import datetime
import time
from concurrent.futures import wait
from concurrent.futures import as_completed
from google.cloud import pubsub_v1

# === CONFIGURATION ===
PROJECT_ID = "dataengineering-456318"
TOPIC_ID = "trimet-breadcrumbs"
CREDENTIAL_PATH = "/home/xiangqz/pubsub-key.json"
VEHICLE_FILE = "vehicle_ids.txt"
DAILY_FILE = f"breadcrumbs_{datetime.datetime.now().strftime('%Y%m%d')}.json"

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = CREDENTIAL_PATH
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

def log(msg, scope="main"):
    ts = datetime.datetime.now().strftime("[%m-%d-%Y--%H:%M:%S.%f]")[:-3]
    full_msg = f"{ts} {msg}"
    print(full_msg)
    log_file = {
        "gather": "data_gather.log",
        "publish": "data_pub.log"
    }.get(scope, "pipeline.log")
    with open(log_file, "a") as f:
        f.write(full_msg + "\n")

def future_callback(fut, idx):
    try:
        fut.result()
    except Exception as e:
        log(f"[ERROR] Record {idx} failed to publish: {e}", "publish")

def gather_data():
    log("Gathering from PSU API...", "gather")
    try:
        with open(VEHICLE_FILE, "r") as f:
            vehicle_ids = [line.strip() for line in f if line.strip()]
    except Exception as e:
        log(f"Error reading vehicle file: {e}", "gather")
        return []

    existing_records = set()
    if os.path.exists(DAILY_FILE):
        with open(DAILY_FILE, "r") as f:
            for line in f:
                try:
                    rec = json.loads(line)
                    existing_records.add(json.dumps(rec, sort_keys=True))
                except json.JSONDecodeError:
                    log(f"JSON decode error in {DAILY_FILE}: {e}", "gather")
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
            log(f"Fetch error for vehicle {vid}: {e}", "gather")

    if new_records:
        with open(DAILY_FILE, "a") as f:
            for record in new_records:
                json.dump(record, f)
                f.write("\n")
        log(f"Appended {len(new_records)} new records to {DAILY_FILE}", "gather")
    else:
        log("No new records fetched today.", "gather")

    return new_records

def publish_data(records):
    log("Starting publishing...", "publish")
    published = 0
    skipped = 0
    futures = []

    for i, record in enumerate(records, start=1):
        try:
            message = json.dumps(record).encode("utf-8")
            future = publisher.publish(topic_path, message)
            future.add_done_callback(lambda fut, idx=i: future_callback(fut, idx))
            futures.append(future)
            published += 1
            if published % 50000 == 0:
                log(f"{published} records queued...", "publish")
        except Exception as e:
            skipped += 1
            log(f"[ERROR] Record {i} failed to publish: {e}", "publish")

    log(f"Waiting for {len(futures)} publishes to complete...", "publish")
    for fut in as_completed(futures):
        pass

    log(f"[DONE] Published: {published}, Skipped: {skipped}", "publish")

def main():
    start = time.time()
    log("Starting full pipeline...")

    new_records = gather_data()

    if new_records:
        log("Waiting 10 seconds before publishing...", "publish")
        time.sleep(10)
        publish_data(new_records)

    end = time.time()
    log(f"Pipeline finished in {end - start:.2f} seconds.")

if __name__ == "__main__":
    main()
