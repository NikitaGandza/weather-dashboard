#!/usr/bin/env python3

import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DummyProducer:
    def __init__(self, bootstrap_servers='redpanda:9092', topic='dummy-data'):
        self.topic = topic
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None
        )
        logger.info(f"Producer initialized for topic: {topic}")

    def generate_dummy_data(self):
        """Generate dummy JSON data"""
        return {
            "id": random.randint(1000, 9999),
            "timestamp": datetime.now().isoformat(),
            "user_id": f"user_{random.randint(1, 100)}",
            "event_type": random.choice(["click", "view", "purchase", "login", "logout"]),
            "product_id": f"prod_{random.randint(1, 50)}",
            "price": round(random.uniform(10.0, 500.0), 2),
            "quantity": random.randint(1, 5),
            "location": random.choice(["US", "UK", "DE", "FR", "JP", "CA"]),
            "device": random.choice(["mobile", "desktop", "tablet"]),
            "session_id": f"session_{random.randint(10000, 99999)}"
        }

    def send_message(self, data):
        """Send message to Kafka topic"""
        try:
            future = self.producer.send(
                self.topic,
                key=str(data['id']),
                value=data
            )
            # Wait for the message to be sent
            record_metadata = future.get(timeout=10)
            logger.info(f"Message sent to {record_metadata.topic} partition {record_metadata.partition} offset {record_metadata.offset}")
            return True
        except Exception as e:
            logger.error(f"Failed to send message: {e}")
            return False

    def run(self, interval=2):
        """Run the producer, sending messages every interval seconds"""
        logger.info(f"Starting dummy producer, sending messages every {interval} seconds...")
        
        try:
            while True:
                dummy_data = self.generate_dummy_data()
                logger.info(f"Sending: {dummy_data}")
                
                if self.send_message(dummy_data):
                    logger.info("Message sent successfully")
                else:
                    logger.error("Failed to send message")
                
                time.sleep(interval)
                
        except KeyboardInterrupt:
            logger.info("Producer stopped by user")
        except Exception as e:
            logger.error(f"Producer error: {e}")
        finally:
            self.producer.close()
            logger.info("Producer closed")

if __name__ == "__main__":
    producer = DummyProducer()
    producer.run(interval=2)