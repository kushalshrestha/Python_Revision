from confluent_kafka import Producer
import json
import random
import time

# Kafka configuration
conf = {
    'bootstrap.servers': 'localhost:29092',  # Kafka broker
    'client.id': 'python-producer'
}

# Create Producer instance
producer = Producer(conf)

# Callback function to handle delivery reports
def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}] @ {msg.offset()}")

# Function to simulate user events
def create_event(user_id, action):
    event = {
        'user_id': user_id,
        'action': action,
        'timestamp': int(time.time()),  # Current timestamp
        'product_id': random.randint(1, 100),  # Example: a random product ID for product-related actions
        'session_id': random.randint(1000, 9999)  # Random session ID
    }
    return event

# Send a message to the 'events' topic continuously (simulating real-time streaming)
topic = 'events'

try:
    while True:
        # Simulating a user event (view_product or added_to_cart)
        user_id = random.randint(1, 50)  # Simulating a user ID
        action = random.choice(['view_product', 'added_to_cart', 'checkout', 'remove_from_cart'])
        
        # Create an event
        event = create_event(user_id, action)
        event_message = json.dumps(event)  # Convert event dict to JSON string
        
        # Print event to console for debugging
        print(f"Producing event: {event_message}")
        
        # Produce the message to the 'events' topic
        producer.produce(topic, event_message.encode('utf-8'), callback=delivery_report)
        
        # Poll for any outstanding messages to be delivered
        producer.poll(0)
        
        # Simulate real-time by waiting for 1 second before sending another event
        time.sleep(1)

except KeyboardInterrupt:
    print("\nProducer stopped.")

# Flush the producer to make sure all messages are sent
producer.flush()

print("All events have been sent.")
