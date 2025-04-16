from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1
import logging
import time
import threading

# Setup logging
logging.basicConfig(
    filename="subscriber_log.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

# TODO(developer)
project_id = "dataengineering-456318"
subscription_id = "my-topic-sub"
# Number of seconds the subscriber should listen for messages
# timeout = 1000.0

subscriber = pubsub_v1.SubscriberClient()
# The `subscription_path` method creates a fully qualified identifier
# in the form `projects/{project_id}/subscriptions/{subscription_id}`
subscription_path = subscriber.subscription_path(project_id, subscription_id)

message_count = 0
first_message_time = None
last_message_time = None
stop_after_idle_seconds = 10

def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    #logging.info(f" Received message: {message.data.decode('utf-8')}")
    global message_count, first_message_time, last_message_time
    message_count += 1
    current_time = time.time()

    if first_message_time is None:
        first_message_time = current_time

    last_message_time = current_time

    message.ack()
    #print(f"[{message_count}] Received message: {message.data.decode('utf-8')}")


# Background thread to stop subscriber after inactivity
def monitor_idle():
    global last_received_time
    while True:
        time.sleep(1)
        if last_message_time and (time.time() - last_message_time > stop_after_idle_seconds):
            print(f"No messages for {stop_after_idle_seconds} seconds. Shutting down...")
            streaming_pull_future.cancel()
            break

streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
# print(f"Listening for messages on {subscription_path}...\n")

threading.Thread(target=monitor_idle, daemon=True).start()

# Start the timer
start_time = time.time()

# Wrap subscriber in a 'with' block to automatically call close() when done.
with subscriber:
    try:
        # When `timeout` is not set, result() will block indefinitely,
        # unless an exception is encountered first.
        streaming_pull_future.result()
    except TimeoutError:
        streaming_pull_future.cancel()  # Trigger the shutdown.
        streaming_pull_future.result()  # Block until the shutdown is complete.
        #logging.warning("Subscriber timed out and shutdown is complete.")

end_time = time.time()

if first_message_time and last_message_time:
    total_duration = last_message_time - first_message_time
    print(f"Total time to receive all messages with per-message print is: {total_duration: .2f} seconds.")
else:
    print(f"No Messages received.")

print(f"Total messages received: {message_count}")
