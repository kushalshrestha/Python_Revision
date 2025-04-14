from confluent_kafka import Consumer, KafkaException, KafkaError
import json

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


# Function to process each message
def process_message(message):
    try:
        # Deserialize message from JSON
        event = json.loads(message.value().decode('utf-8'))
        print(f"Consumed event: {event}")

        # Here we can process the event, e.g., storing it into a raw (Bronze) layer, etc.
        # For now, let's just print the event.

        return event

    except Exception as e:
        print(f"Error processing message: {e}")
        return None

# Consume messages continuously
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
                # Save event to Bronze layer, or do something else
                pass

except KeyboardInterrupt:
    print("Consumer stopped")

finally:
    # Close the consumer gracefully
    consumer.close()
