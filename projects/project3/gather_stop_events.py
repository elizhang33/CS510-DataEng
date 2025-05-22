import os
import json
import urllib.request
import datetime
import time
from concurrent.futures import as_completed, wait, FIRST_COMPLETED
from google.cloud import pubsub_v1
from bs4 import BeautifulSoup  # <-- HTML parser

# === CONFIGURATION ===
PROJECT_ID = "dataengineering-456318"
TOPIC_ID = "trimet-stop-events-topic"
CREDENTIAL_PATH = "/home/xiangqz/pubsub-key.json"
VEHICLE_FILE = "vehicle_ids.txt"
DAILY_FILE = f"stop_events_{datetime.datetime.now().strftime('%Y%m%d')}.json"

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = CREDENTIAL_PATH
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

def log(msg, scope="main"):
    ts = datetime.datetime.now().strftime("[%m-%d-%Y--%H:%M:%S.%f]")[:-3]
    full_msg = f"{ts} {msg}"
    print(full_msg)
    log_file = {
        "gather": "stop_gather.log",
        "publish": "stop_pub.log"
    }.get(scope, "stop_pipeline.log")
    with open(log_file, "a") as f:
        f.write(full_msg + "\n")

def parse_stop_events_html(html_text):
    soup = BeautifulSoup(html_text, "html.parser")
    records = []

    # Extract service date from <h1>
    h1 = soup.find("h1")
    service_date = None
    if h1 and "stop data for" in h1.text:
        service_date = h1.text.strip().split()[-1]

    # Loop through all <h2> and <table> pairs
    for h2 in soup.find_all("h2"):
        if not h2.text.startswith("Stop events for PDX_TRIP"):
            continue
        pdx_trip = h2.text.strip().split()[-1]

        table = h2.find_next("table")
        if not table:
            continue

        header_row = table.find("tr")
        headers = [th.text.strip() for th in header_row.find_all("th")]

        for tr in table.find_all("tr")[1:]:  # skip header row
            values = [td.text.strip() for td in tr.find_all("td")]
            if len(values) != len(headers):
                continue
            record = dict(zip(headers, values))
            record["pdx_trip"] = pdx_trip
            record["service_date"] = service_date
            records.append(record)

    return records

def gather_data():
    log("Gathering StopEvent data from PSU API...", "gather")
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
                    continue

    new_records = []

    for vid in vehicle_ids:
        url = f"https://busdata.cs.pdx.edu/api/getStopEvents?vehicle_num={vid}"
        try:
            with urllib.request.urlopen(url) as response:
                raw = response.read().decode("utf-8")
                if not raw.strip():
                    log(f"No data returned for vehicle {vid}. Skipping.", "gather")
                    continue

                records = parse_stop_events_html(raw)
                #log(f"Vehicle {vid}: Parsed {len(records)} records", "gather")

                for record in records:
                    record_str = json.dumps(record, sort_keys=True)
                    if record_str not in existing_records:
                        new_records.append(record)
                        existing_records.add(record_str)
        except urllib.error.HTTPError as e:
            log(f"Fetch error for vehicle {vid}: {e}", "gather")
        except Exception as e:
            log(f"Unexpected error for vehicle {vid}: {e}", "gather")

    if new_records:
        with open(DAILY_FILE, "a") as f:
            for record in new_records:
                json.dump(record, f)
                f.write("\n")
        log(f"Appended {len(new_records)} new StopEvent records to {DAILY_FILE}", "gather")
    else:
        log("No new StopEvent records fetched today.", "gather")

    return new_records

def publish_data(records, max_in_flight = 500):
    log("Starting publishing of StopEvent data...", "publish")
    published = 0
    skipped_counter = {"count": 0}
    futures = []

    def future_callback(fut):
        try:
            fut.result()
        except Exception:
            skipped_counter["count"] += 1

    for i, record in enumerate(records, start=1):
        try:
            message = json.dumps(record).encode("utf-8")
            future = publisher.publish(topic_path, message)
            future.add_done_callback(future_callback)
            futures.append(future)
            published += 1
            if published % 50000 == 0:
                log(f"{published} records queued...", "publish")
            # Throttle after max_in_flight outstanding
            if len(futures) >= max_in_flight:
                done, not_done = wait(futures, return_when = FIRST_COMPLETED)
                futures = list(not_done)
        except Exception as e:
            skipped_counter["count"] += 1
            log(f"[ERROR] Record {i} failed to publish: {e}", "publish")

    log(f"Waiting for {len(futures)} publishes to complete...", "publish")
    for fut in as_completed(futures):
        pass

    log(f"[DONE] Published: {published}, Skipped: {skipped_counter['count']}", "publish")

def main():
    start = time.time()
    log("Starting full StopEvent pipeline...")

    new_records = gather_data()

    if new_records:
        log("Waiting 10 seconds before publishing...", "publish")
        time.sleep(10)
        publish_data(new_records)

    end = time.time()
    log(f"Pipeline finished in {end - start:.2f} seconds.")

if __name__ == "__main__":
    main()
