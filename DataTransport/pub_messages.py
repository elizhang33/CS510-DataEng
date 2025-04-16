import urllib.request
import json
import os
import datetime
from google.cloud import pubsub_v1
import logging
import time
import csv

# Set GCP project and topic
PROJECT_ID = "dataengineering-456318"
TOPIC_ID = "my-topic"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

# Read vehicle IDs
with open("lab_vehicle_ids.txt", "r") as f:
    vehicle_ids = [line.strip() for line in f if line.strip()]

# Setup filenames
today_str = datetime.datetime.now().strftime("%Y%m%d")
daily_file = f"breadcrumbs_{today_str}.json"
all_file = "bcsample.json"

# Load today's data if it exists
if os.path.exists(daily_file):
    with open(daily_file, "r") as f:
        today_data = json.load(f)
    today_record_set = {json.dumps(rec, sort_keys=True) for rec in today_data}
else:
    today_data = []
    today_record_set = set()

# Track if anything new was fetched today
new_today_data = []

logging.basicConfig(
    filename="lab_pubsub_publish.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

# Start the timer
start_time = time.time()

total_message_published = 0

# Fetch data
for vid in vehicle_ids:
    url = f"https://busdata.cs.pdx.edu/api/getBreadCrumbs?vehicle_id={vid}"
    count = 0
    try:
        with urllib.request.urlopen(url) as response:
            if response.status == 200:
                raw = response.read()
                new_data = json.loads(raw)
                for record in new_data:
                    record_str = json.dumps(record, sort_keys=True)
                    if record_str not in today_record_set:
                        today_data.append(record)
                        count += 1
                        new_today_data.append(record)
                        today_record_set.add(record_str)
                        try:
                            pub_data = json.dumps(record).encode("utf-8")
                            future = publisher.publish(topic_path, pub_data)
                            #print(future.result())
                            msg_id = future.result()
                            logging.info(f"Published message ID: {msg_id}")
                        except Exception as pub_error:
                            logging.error(f"Pub/Sub error for vehicle {vid}: {pub_error}")

                print(f"Vehicle {vid}: {len(new_data)} fetched, {count} new today.")
        total_message_published += count
    except Exception as e:
        print(f"Error fetching vehicle {vid}: {e}")

# End the timer
end_time = time.time()
print(f"Publishing today's {total_message_published} records took {end_time - start_time:.2f} seconds.")

# Save today's data if there are new records
if new_today_data:
    with open(daily_file, "w") as f:
        json.dump(today_data, f, indent=2)
    print(f"Updated today's temp file: {daily_file}")

    print(f"Number of new records to publish for {vehicle_ids}: {len(new_today_data)}")
    # Append to all-time archive
    if os.path.exists(all_file):
        with open(all_file, "r") as f:
            all_data = json.load(f)
    else:
        all_data = []

    all_data.extend(new_today_data)

    # Save combined archive
    with open(all_file, "w") as f:
        json.dump(all_data, f, indent=2)

    print(f"Appended {len(new_today_data)} new records to {all_file} on {today_str}")
else:
    print(f"No new records today {today_str} — skipping update to archive.")
