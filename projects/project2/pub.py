import os
import json
import urllib.request
import datetime
import time
from concurrent import futures
from google.cloud import pubsub_v1

# === CONFIGURATION ===
PROJECT_ID = "dataengineering-456318"
TOPIC_ID = "trimet-breadcrumbs"
CREDENTIAL_PATH = "/home/xiangqz/pubsub-key.json"
VEHICLE_FILE = "vehicle_ids.txt"
DAILY_FILE = f"breadcrumbs_{datetime.datetime.now().strftime('%Y%m%d')}.json"
ALL_FILE = "bcsample.json"

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = CREDENTIAL_PATH
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

def log(msg, scope = "main"):
    ts = datetime.datetime.now().strftime("[%m-%d-%Y--%H:%M:%S.%f]")[:-3]
    full_msg = f"{ts} {msg}"
    print(full_msg)

    if scope == "gather":
        with open("data_gather.log", "a") as f:
            f.write(full_msg + "\n")
    elif scope == "publish":
        with open("data_pub.log", "a") as f:
            f.write(full_msg + "\n")
    else:
        with open("pipeline.log", "a") as f:
            f.write(full_msg + "\n")

def gather_data():
    log("Gathering from PSU API...")
    try:
        with open(VEHICLE_FILE, "r") as f:
            vehicle_ids = [line.strip() for line in f if line.strip()]
    except Exception as e:
        log(f"Error reading vehicle file: {e}")
        return []

    # Load today's file if it exists
    if os.path.exists(DAILY_FILE):
        with open(DAILY_FILE, "r") as f:
            today_data = json.load(f)
        record_set = {json.dumps(rec, sort_keys=True) for rec in today_data}
    else:
        today_data = []
        record_set = set()

    new_data = []

    for vid in vehicle_ids:
        url = f"https://busdata.cs.pdx.edu/api/getBreadCrumbs?vehicle_id={vid}"
        try:
            with urllib.request.urlopen(url) as response:
                if response.status == 200:
                    records = json.loads(response.read())
                    for record in records:
                        record_str = json.dumps(record, sort_keys=True)
                        if record_str not in record_set:
                            today_data.append(record)
                            new_data.append(record)
                            record_set.add(record_str)
        except Exception as e:
            log(f"Fetch error for vehicle {vid}: {e}")

    # Save updated daily file
    if new_data:
        with open(DAILY_FILE, "w") as f:
             for record in today_data:
                 json.dump(record, f)
                 f.write("\n")
                 #json.dump(today_data, f, indent=2)
        log(f"Updated daily file: {DAILY_FILE} with {len(new_data)} new records")

        # Append to cumulative file
        #if os.path.exists(ALL_FILE):
        #    with open(ALL_FILE, "r") as f:
        #        all_data = json.load(f)
        #else:
        #    all_data = []

        #all_data.extend(new_data)

        #with open(ALL_FILE, "w") as f:
        #    json.dump(all_data, f, indent=2)

        #log(f"Appended to {ALL_FILE}. Total records: {len(all_data)}")

    else:
        log("No new records fetched today.")

    return new_data

def publish_data(records):
    log("Starting publishing...")
    futures_list = []
    count = 0

    def future_callback(fut):
        try:
            fut.result()
        except Exception as e:
            log(f"Publish error: {e}")

    for record in records:
        try:
            data = json.dumps(record).encode("utf-8")
            future = publisher.publish(topic_path, data)
            future.add_done_callback(future_callback)
            futures_list.append(future)
            count += 1

            if count % 50000 == 0:
                log(f"{count} records queued for publish...")

        except Exception as e:
            log(f"Unexpected publish error: {e}")

    for fut in futures.as_completed(futures_list):
        pass

    log(f"Publishing complete. Total published: {count}")

def main():
    start = time.time()
    log("Starting full pipeline...")

    new_records = gather_data()

    
    if new_records:
        log("Waiting 60 seconds before publishing...", scope="publish")
        time.sleep(60)
        publish_data(new_records)

    end = time.time()
    log(f"Pipeline finished in {end - start:.2f} seconds.")

if __name__ == "__main__":
    main()
