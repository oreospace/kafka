import json
import threading
import uuid
import random
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
import logging
import time

from concurrent.futures import ThreadPoolExecutor


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_BROKERS = "127.0.0.1:9092,127.0.0.1:9093,127.0.0.1:9094"
TOPIC_NAME = "financial_transactions"
NUM_PARTITIONS = 5
REPLICATION_FACTOR = 3

producer_conf = {
    'bootstrap.servers': KAFKA_BROKERS,
    'queue.buffering.max.messages': 10000,
    'queue.buffering.max.kbytes': 512000,
    'batch.num.messages': 1000,
    'acks': 1,
    'linger.ms': 10,
    'compression.type': 'gzip'
}

producer = Producer(producer_conf)

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

def generate_transactions():
    return dict (
        transactionid = str(uuid.uuid4()),
        userid = f"user_{random.randint(1, 5000)}",
        amount = round(random.uniform(5000.0, 15000.0), 2),
        transactionTime = int(time.time()),
        merchantID = random.choice(['merchant_1', 'merchant_2', 'merchant_3', 'merchant_4', 'merchant_5']),
        transactionType = random.choice(['purchase', 'refund', 'withdrawal', 'deposit']),
        location = f'location_random_{random.randint(1, 100)}',
        paymentMethod = random.choice(['credit_card', 'debit_card', 'paypal', 'bank_transfer']),
        isInternational = random.choice([True, False]),
        currency = random.choice(['USD', 'EUR', 'GBP', 'JPY', 'AUD'])
    )

def delivery_report(err, msg):
    if err is not None:
        logger.error(f"Delivery failed for record {msg.key()}: {err}")
    else:
        logger.info(f"Record successfully produced to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

def produce_transactions(thread_id):
    while True:
            transaction = generate_transactions()
    
            try:
                producer.produce(
                    topic=TOPIC_NAME,
                    key=transaction['userid'],
                    value=json.dumps(transaction).encode('utf-8'),
                    on_delivery=delivery_report
                )
                print(f" Thread ID {thread_id} --> Produced transaction: {transaction}")
                producer.flush()
            except Exception as e:
                logger.error(f"Failed to produce transaction with error: {e}")

def produce_data_in_parallel(num_threads):

    threads = []
    try:
        for i in range(num_threads):
            thread = threading.Thread(target=produce_transactions, args=(i,))
            thread.daemon = True
            thread.start()
            threads.append(thread)

        for thread in threads:
            thread.join()
    except Exception as e:
        print(f"Error occured while creating a thread: {e}")


if __name__ == "__main__":
    create_topic(TOPIC_NAME, NUM_PARTITIONS, REPLICATION_FACTOR)
    produce_data_in_parallel(3)

    