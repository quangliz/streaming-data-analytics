import json
import time
import logging
import os
import requests
from kafka import KafkaProducer
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load env variables
load_dotenv()

# Kafka config
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'user_data')
BATCH_SIZE = int(os.getenv('BATCH_SIZE', '10'))
INTERVAL_SECONDS = int(os.getenv('INTERVAL_SECONDS', '5'))

RANDOM_USER_API = 'https://randomuser.me/api/'

def create_kafka_producer():
    # create a Kafka producer instance
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda v: json.dumps(v).encode('utf-8') if v else None
        )
        logger.info(f"Connected to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
        return producer
    except Exception as e:
        logger.error(f"Failed to create Kafka producer: {e}")
        raise

def fetch_random_users(batch_size=10):
    # fetch data
    try:
        params = {
            'results': batch_size,
            'inc': 'name,gender,dob,location,email,login,phone,nat'
        }
        response = requests.get(RANDOM_USER_API, params=params)
        response.raise_for_status()
        return response.json()['results']
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching data from RandomUser.me API: {e}")
        return []

def send_to_kafka(producer, topic, users):
    # send data to kafka topic
    try:
        for user in users:
            # Use user's login UUID as key for potential partitioning
            key = {'uuid': user['login']['uuid']} if 'login' in user and 'uuid' in user['login'] else None
            
            # send message
            future = producer.send(topic, key=key, value=user)
            # wait for the message to be delivered
            record_metadata = future.get(timeout=10)
            
            logger.debug(f"Sent user data to {record_metadata.topic}:{record_metadata.partition}:{record_metadata.offset}")
        
        # Flush to ensure all messages are sent
        producer.flush()
        logger.info(f"Successfully sent {len(users)} user records to Kafka topic '{topic}'")
    except Exception as e:
        logger.error(f"Error sending data to Kafka: {e}")

def main():
    # main function
    logger.info("Starting Random User Data Producer")
    
    # Create Kafka producer
    producer = create_kafka_producer()
    
    try:
        while True:
            users = fetch_random_users(BATCH_SIZE)
            
            if users:
                send_to_kafka(producer, KAFKA_TOPIC, users)
            
            # Wait for the next interval
            time.sleep(INTERVAL_SECONDS)
    except KeyboardInterrupt:
        logger.info("Stopping the producer...")
    except Exception as e:
        logger.error(f"error: {e}")
    finally:
        if producer:
            producer.close()
            logger.info("Producer closed")

if __name__ == "__main__":
    main() 