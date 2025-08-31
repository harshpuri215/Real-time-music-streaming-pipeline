import snowflake.connector
import json
import os

# Load Snowflake config from JSON or environment variables
with open('snowflake/config/snowflake_config.json', 'r') as f:
    config = json.load(f)

conn = snowflake.connector.connect(
    user=config['user'],
    password=config['password'],
    account=config['account'],
    warehouse=config['warehouse'],
    database=config['database'],
    schema=config['schema']
)

def ingest_data(records):
    cursor = conn.cursor()
    try:
        for record in records:
            cursor.execute(
                "INSERT INTO music_streaming_events (user_id, song, event_time, listen_duration) VALUES (%s, %s, %s, %s)",
                (record['user_id'], record['song'], record['event_time'], record['listen_duration'])
            )
    finally:
        cursor.close()

# Example usage could be reading processed data from GCS or passed from Spark job
if __name__ == "__main__":
    sample_records = [
        {
            "user_id": "User1",
            "song": "Song A",
            "event_time": "2025-08-31 12:00:00",
            "listen_duration": 180
        }
    ]
    ingest_data(sample_records)
