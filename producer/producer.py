import time
import uuid
import random
import threading
from datetime import datetime
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer
from faker import Faker
import json
from schema import TRANSACTION_SCHEMA_STR

fake = Faker()

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "transactions-raw"
SCHEMA_REGISTRY_URL = "http://localhost:8081"

USER_POOL = [f"user-{i:04d}" for i in range(1, 501)]

schema_registry_conf = {"url": SCHEMA_REGISTRY_URL}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

avro_schema_dict = json.loads(TRANSACTION_SCHEMA_STR)
avro_serializer = AvroSerializer(schema_registry_client, json.dumps(avro_schema_dict))
string_serializer = StringSerializer("utf-8")


def generate_transaction():
    user_id = random.choice(USER_POOL)

    # 20% chance of fraudulent pattern
    is_fraud = random.random() < 0.2

    if is_fraud:
        # Fraudulent transactions: higher amounts, rapid succession
        amount = round(random.uniform(5000.0, 15000.0), 2)
    else:
        # Normal transactions: lower amounts
        amount = round(random.uniform(10.0, 500.0), 2)

    return {
        "transaction_id": str(uuid.uuid4()),
        "user_id": user_id,
        "amount": amount,
        "currency": random.choice(["USD", "EUR", "MXN", "GBP"]),
        "merchant": fake.company(),
        "category": random.choice(
            ["electronics", "groceries", "travel", "entertainment", None]
        ),
        "timestamp": int(datetime.now().timestamp() * 1000),
        "country": fake.country_code(),
        "device_type": random.choice(["mobile", "desktop", "tablet"]),
        "is_fraud": is_fraud,
    }


def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")


def producer_thread(thread_id, num_messages):
    conf = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "key.serializer": string_serializer,
        "value.serializer": avro_serializer,
        "acks": "all",  # Wait for all replicas
        "enable.idempotence": True,
        "retries": 5,
        "compression.type": "snappy",
    }
    producer = SerializingProducer(conf)

    for i in range(num_messages):
        transaction = generate_transaction()
        key = transaction["user_id"]  # Partition by user_id
        producer.produce(
            topic=TOPIC, key=key, value=transaction, on_delivery=delivery_report
        )
        producer.poll(0)  # Non-blocking

        # Small delay to spread transactions over time (for windowing)
        # Every 10 messages, add a tiny delay
        if i % 10 == 0:
            time.sleep(0.01)

    producer.flush()


if __name__ == "__main__":
    num_threads = 4  # Scale for higher throughput (e.g., 10k+ msgs/sec total)
    messages_per_thread = 1000

    threads = []
    start_time = time.time()

    for i in range(num_threads):
        t = threading.Thread(target=producer_thread, args=(i, messages_per_thread))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    elapsed = time.time() - start_time
    total_msgs = num_threads * messages_per_thread
    print(
        f"Produced {total_msgs} messages in {elapsed:.2f}s ({total_msgs / elapsed:.2f} msgs/sec)"
    )
