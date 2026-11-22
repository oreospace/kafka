from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_BROKERS = "127.0.0.1:9092,127.0.0.1:9093,127.0.0.1:9094"
TOPIC_NAME = "financial_transactions"
NUM_PARTITIONS = 5
REPLICATION_FACTOR = 3

def create_topic(topic_name, num_partitions, replication_factor):

    admin_client = AdminClient({'bootstrap.servers': KAFKA_BROKERS})
    try:
        metadata = admin_client.list_topics(timeout=10)
        if topic_name not in metadata.topics:
            new_topic = NewTopic(topic_name, num_partitions=num_partitions, replication_factor=replication_factor)
            fs = admin_client.create_topics([new_topic])
            for topic, future in fs.items():
                try:
                    future.result()  # Wait for the topic creation to complete
                    logger.info(f"Topic '{topic_name}' created successfully.")
                except Exception as e:
                    logger.error(f"Failed to create topic with error: {e}")
        else:
            logger.info(f"Topic '{topic_name}' already exists.")

    except Exception as e:
        logger.error(f"Error occurred while creating topic: {e}")

if __name__ == "__main__":
    create_topic(TOPIC_NAME, NUM_PARTITIONS, REPLICATION_FACTOR)