import json
import time
from kafka import KafkaProducer
from random import choice, randint
from datetime import datetime

# Kafka configuration
KAFKA_TOPIC = 'music-events'
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'

producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

songs = ['Song A', 'Song B', 'Song C', 'Song D']
users = ['User1', 'User2', 'User3', 'User4']

def generate_event():
    event = {
        'user_id': choice(users),
        'song': choice(songs),
        'timestamp': datetime.utcnow().isoformat(),
        'listen_duration': randint(30, 300)  # seconds
    }
    return event

if __name__ == "__main__":
    while True:
        event = generate_event()
        producer.send(KAFKA_TOPIC, value=event)
        print(f"Sent event {event}")
        time.sleep(1)
