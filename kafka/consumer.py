from kafka import KafkaConsumer
import json

# Kafka configuration
KAFKA_TOPIC = 'music-events'
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'

# Create Kafka consumer
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    auto_offset_reset='earliest',   # Start reading at the earliest message
    enable_auto_commit=True,        # Automatically commit offsets
    group_id='music-consumer-group',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

print(f"Listening to Kafka topic: {KAFKA_TOPIC}")

# Consume messages
for message in consumer:
    event = message.value
    print(f"Received event: {event}")
