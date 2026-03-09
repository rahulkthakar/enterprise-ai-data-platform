# ingestion/event_generator/generator.py

import time
import random
import json
from datetime import datetime
from faker import Faker

# Initialize Faker
fake = Faker()

# Define constants
PRODUCTS = ["Laptop", "Smartphone", "Headphones", "Camera", "Smartwatch"]
EVENT_TYPES = ["view", "cart", "purchase"]
DEVICES = ["mobile", "desktop", "tablet"]
LOCATIONS = ["USA", "Canada", "UK", "Germany", "India"]


# Function to generate a single event
def generate_event():
    event = {
        "user_id": fake.uuid4(),
        "product_id": random.choice(PRODUCTS),
        "event_type": random.choice(EVENT_TYPES),
        "device": random.choice(DEVICES),
        "location": random.choice(LOCATIONS),
        "price": round(random.uniform(10.0, 2000.0), 2),
        "timestamp": datetime.utcnow().isoformat(),
    }
    return event


# Main loop to generate events every second
if __name__ == "__main__":
    print("Starting event generator. Press Ctrl+C to stop...")
    try:
        while True:
            for _ in range(100):
                event = generate_event()
                event["event_id"] = fake.uuid4()
                print(json.dumps(event))  # Replace with Kafka producer later
                with open("events.jsonl", "a") as f:
                    f.write(json.dumps(event) + "\n")
            time.sleep(1)  # Run every second
    except KeyboardInterrupt:
        print("Event generator stopped.")
