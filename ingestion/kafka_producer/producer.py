# ingestion/kafka_producer/producer.py

import json
import time
from kafka import KafkaProducer
from faker import Faker
import random
from datetime import datetime

from data_contracts.validators.event_validator import validate_event

fake = Faker()

# Kafka configuration
KAFKA_BROKER = "ubuntu.hom:9092"
TOPIC_NAME = "user_events"

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

# Data simulation fields
PRODUCTS = ["Laptop", "Smartphone", "Headphones", "Camera", "Smartwatch"]
EVENT_TYPES = ["view", "cart", "purchase"]
DEVICES = ["mobile", "desktop", "tablet"]
LOCATIONS = ["USA", "Canada", "UK", "Germany", "India"]


# Generate event
def generate_event():
    event = {
        "user_id": fake.uuid4(),
        "product_id": random.choice(PRODUCTS),
        "event_type": random.choice(EVENT_TYPES),
        "device": random.choice(DEVICES),
        "location": random.choice(LOCATIONS),
        "price": round(random.uniform(10, 2000), 2),
        "timestamp": datetime.now().isoformat(),
    }
    return event


def stream_events():

    print("Streaming events to Kafka topic:", TOPIC_NAME)

    while True:

        event = generate_event()

        if validate_event(event):
            producer.send(TOPIC_NAME, event)
            print(f"Valid event sent: {event}")
        else:
            print(f"Invalid event skipped {event}")

        time.sleep(1)


if __name__ == "__main__":
    stream_events()
