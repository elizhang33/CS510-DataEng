import os
import json
import time
import threading
import datetime
from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1

# === CONFIGURATION ===
PROJECT_ID = "dataengineering-456318"
SUBSCRIPTION_ID = "trimet-breadcrumbs-sub"
CREDENTIAL_PATH = "/home/xiangqz/pubsub-key.json"
STOP_AFTER_IDLE_SECONDS = 30

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = CREDENTIAL_PATH
subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)

# === SHARED STATE ===
message_count = 0
first_message_time = None
last_message_time = None
streaming_pull_future = None


# === LOGGING ===
def log(msg):
    ts = datetime.datetime.now().strftime("[%m-%d-%Y--%H:%M:%S.%f]")[:-3]
    full_msg = f"{ts} {msg}"
    print(full_msg)
    with open("subscriber.log", "a") as f:
        f.write(full_msg + "\n")


# === CALLBACK ===
def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    global message_count, first_message_time, last_message_time
    current_time = time.time()

    message_count += 1
    if first_message_time is None:
        first_message_time = current_time
    last_message_time = current_time

    # Save the message data to a daily file
    try:
        data = json.loads(message.data.decode("utf-8"))
        today_str = datetime.datetime.now().strftime("%Y%m%d")
        filename = f"received_breadcrumbs_{today_str}.json"

        with open(filename, "a") as f:
            json.dump(data, f)
            f.write("\n")

    except Exception as e:
        log(f"Error writing message to file: {e}",)

    message.ack()

# === MAIN ===
def main():
    global streaming_pull_future

    log(f"Starting subscriber. Listening on {subscription_path}...")

    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)

    # Launch idle timeout monitor
    # threading.Thread(target=monitor_idle, daemon=True).start()

    # Start listening
    with subscriber:
        try:
            streaming_pull_future.result()
        except TimeoutError:
            streaming_pull_future.cancel()
            streaming_pull_future.result()

    # Summary
    if first_message_time and last_message_time:
        duration = last_message_time - first_message_time
        log(f"Received {message_count} messages over {duration:.2f} seconds.")
    else:
        log("No messages received.")


if __name__ == "__main__":
    main()
