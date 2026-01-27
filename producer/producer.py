import time
import uuid
import random
import threading
from datetime import datetime
from confluent_kafka import SerializingProducer
from faker import Faker
import fastavro
from io import BytesIO
from schema import TRANSACTION_SCHEMA_STR

fake = Faker()

BOOTSTRAP_SERVERS = "localhost:9092"  # Host view (kafka-1:29092 inside container)
TOPIC = "transactions-raw"
SCHEMA_REGISTRY_URL = "http://localhost:8081"


def avro_serializer(value, schema_str):
    schema = fastavro.parse_schema(schema_str)
    bytes_writer = BytesIO()
    fastavro.schemaless_writer(bytes_writer, schema, value)
    return bytes_writer.getvalue()


def generate_transaction():
    is_fraud = random.random() < 0.08  # ~8% fraud rate
    return {
        "transaction_id": str(uuid.uuid4()),
        "user_id": fake.uuid4(),
        "amount": round(random.uniform(1.0, 5000.0), 2),
        "currency": random.choice(["USD", "EUR", "MXN", "GBP"]),
        "merchant": fake.company(),
        "category": random.choice(
            ["electronics", "groceries", "travel", "entertainment", None]
        ),
        "timestamp": int(datetime.now().timestamp() * 1000),  # millis
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
        "key.serializer": lambda v: v.encode("utf-8"),  # Simple string key (user_id)
        "value.serializer": lambda v: avro_serializer(v, TRANSACTION_SCHEMA_STR),
        "acks": "all",  # Wait for all replicas
        "enable.idempotence": True,
        "retries": 5,
        "compression.type": "snappy",  # For efficiency
    }
    producer = SerializingProducer(conf)

    for _ in range(num_messages):
        transaction = generate_transaction()
        key = transaction["user_id"]  # Partition by user_id
        producer.produce(
            topic=TOPIC, key=key, value=transaction, on_delivery=delivery_report
        )
        producer.poll(0)  # Non-blocking
        time.sleep(0.001)  # Throttle for ~1k msgs/sec per thread; adjust for rate

    producer.flush()


if __name__ == "__main__":
    num_threads = 4  # Scale for higher throughput (e.g., 10k+ msgs/sec total)
    messages_per_thread = 250  # Total: 1000 msgs; increase for benchmarks

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
