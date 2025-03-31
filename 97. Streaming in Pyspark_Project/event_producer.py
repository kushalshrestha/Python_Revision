from kafka import KafkaProducer
import json
import time
import random


# Kafka producer setup
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',  # Connect to Kafka running in Docker
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Simulate producing events
event_types = ['purchase', 'view', 'add_to_cart']

while True:
    event = {
        "user_id": random.randint(1, 100),
        "event_type": random.choice(event_types),
        "timestamp": time.time()
    }
    producer.send('real_time_events', value=event)
    print(f"Sent: {event}")
    time.sleep(1)  # Simulate real-time event every second
