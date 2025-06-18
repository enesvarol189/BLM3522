import asyncio
import json
import time
from datetime import datetime

from google.cloud import pubsub_v1
from google.cloud import bigquery

PROJECT_ID = "upheld-chalice-459519-s3"
SUBSCRIPTION_ID = "data-topic-sub"
TABLE_ID = "my_dataset.my_table"

async def consume_pubsub_messages(callback):
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)

    def pubsub_callback(message: pubsub_v1.subscriber.message.Message):
        try:
            data = message.data.decode("utf-8")
            print(f"Received message from Pub/Sub: {data}")
            callback(data)
            message.ack()
        except Exception as e:
            print(f"Error processing message: {e}")

    streaming_pull_future = subscriber.subscribe(subscription_path, callback=pubsub_callback)
    print(f"Listening for messages on {subscription_path}...")

    try:
        await streaming_pull_future.result()
    except Exception as e:
        streaming_pull_future.cancel()
        await streaming_pull_future.wait()
        print(f"Error consuming messages: {e}")

class BigQueryWriter:
    def __init__(self, project_id, table_id):
        self.client = bigquery.Client(project=project_id)
        self.table_ref = self.client.dataset(table_id.split(".")[0]).table(table_id.split(".")[1])
        self.schema = [
            bigquery.SchemaField("id", "STRING"),
            bigquery.SchemaField("message", "STRING"),
            bigquery.SchemaField("timestamp", "TIMESTAMP"),
        ]

    def write_row(self, row_data):
        rows_to_insert = [row_data]
        errors = self.client.insert_rows(self.table_ref, rows_to_insert, self.schema)
        if errors == []:
            print(f"Inserted row: {row_data}")
        else:
            print(f"Errors inserting row: {errors}")

def process_message(message_data, bigquery_writer):
    try:
        message_json = json.loads(message_data)
        message_id = message_json.get("id", "default-id")
    except json.JSONDecodeError:
        message_id = "invalid-json"
        message_json = None

    timestamp = datetime.utcnow().isoformat()

    row_to_insert = {
        "id": message_id,
        "message": message_data,
        "timestamp": timestamp,
    }
    print(f"Writing to BigQuery: {row_to_insert}")
    bigquery_writer.write_row(row_to_insert)

async def main():
    bigquery_writer = BigQueryWriter(PROJECT_ID, TABLE_ID)

    def message_received(message):
        process_message(message, bigquery_writer)

    await consume_pubsub_messages(message_received)

if __name__ == "__main__":
    asyncio.run(main())