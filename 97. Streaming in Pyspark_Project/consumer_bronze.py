import os
import json
import pandas as pd
from confluent_kafka import Consumer, KafkaException, KafkaError

# File path for the Bronze layer storage (CSV format)
bronze_file_path = "bronze_layer_events.csv"

# Kafka configuration
conf = {
    'bootstrap.servers': 'localhost:29092',  # Kafka broker
    'group.id': 'python-consumer-group',  # Consumer group ID
    'auto.offset.reset': 'earliest',  # Start consuming from the earliest message if no offset is stored
}

# Create Consumer instance
consumer = Consumer(conf)

# Subscribe to the 'events' topic
consumer.subscribe(['events'])

# Function to append event to the Bronze layer (CSV file)
def store_bronze_layer(event):
    try:
        # If the file doesn't exist, create it and write headers
        if not os.path.exists(bronze_file_path):
            df = pd.DataFrame([event])
            df.to_csv(bronze_file_path, mode='w', index=False, header=True)
        else:
            # If file exists, append data
            df = pd.DataFrame([event])
            df.to_csv(bronze_file_path, mode='a', index=False, header=False)
        print(f"Stored event in Bronze Layer: {event}")
    except Exception as e:
        print(f"Error storing event in Bronze Layer: {e}")

# Function to process each message and store it in the Bronze layer
def process_message(message):
    try:
        # Deserialize message from JSON
        event = json.loads(message.value().decode('utf-8'))
        print(f"Consumed event: {event}")

        # Store the event in the Bronze layer
        store_bronze_layer(event)

        return event

    except Exception as e:
        print(f"Error processing message: {e}")
        return None

# Consume messages continuously from Kafka
try:
    while True:
        msg = consumer.poll(timeout=1.0)  # 1 second timeout

        if msg is None:
            continue
        elif msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print(f"End of partition reached: {msg.topic()} [{msg.partition()}] @ {msg.offset()}")
            else:
                raise KafkaException(msg.error())
        else:
            # Process the consumed message
            event = process_message(msg)
            if event:
                # Additional logic can be added here, if necessary
                pass

except KeyboardInterrupt:
    print("Consumer stopped")

finally:
    # Close the consumer gracefully
    consumer.close()
