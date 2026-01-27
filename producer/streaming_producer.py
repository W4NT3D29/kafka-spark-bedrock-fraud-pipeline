import time
import uuid
import random
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

USER_POOL = [f"user-{i:04d}" for i in range(1, 201)]

schema_registry_conf = {'url': SCHEMA_REGISTRY_URL}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)
avro_schema_dict = json.loads(TRANSACTION_SCHEMA_STR)
avro_serializer = AvroSerializer(schema_registry_client, json.dumps(avro_schema_dict))
string_serializer = StringSerializer('utf-8')


def generate_normal_transaction(user_id):
    return {
        "transaction_id": str(uuid.uuid4()),
        "user_id": user_id,
        "amount": round(random.uniform(10.0, 500.0), 2),
        "currency": random.choice(["USD", "EUR", "MXN", "GBP"]),
        "merchant": fake.company(),
        "category": random.choice(["electronics", "groceries", "travel", "entertainment"]),
        "timestamp": int(datetime.now().timestamp() * 1000),
        "country": fake.country_code(),
        "device_type": random.choice(["mobile", "desktop", "tablet"]),
        "is_fraud": False,
    }


def generate_fraudulent_burst(user_id, num_transactions=5):
    """Generate a burst of fraudulent transactions for velocity detection"""
    transactions = []
    for _ in range(num_transactions):
        transactions.append({
            "transaction_id": str(uuid.uuid4()),
            "user_id": user_id,
            "amount": round(random.uniform(3000.0, 8000.0), 2),
            "currency": "USD",
            "merchant": fake.company(),
            "category": "electronics",
            "timestamp": int(datetime.now().timestamp() * 1000),
            "country": random.choice(["US", "CN", "RU"]),
            "device_type": "mobile",
            "is_fraud": True,
        })
    return transactions


def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Delivery failed: {err}")
    else:
        print(f"✓ Message delivered to {msg.topic()}[{msg.partition()}] at offset {msg.offset()}")


def stream_transactions(duration_minutes=5, transactions_per_minute=20):
    """
    Stream transactions continuously
    
    Args:
        duration_minutes: How long to produce (default 5 minutes)
        transactions_per_minute: Rate of normal transactions
    """
    conf = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "key.serializer": string_serializer,
        "value.serializer": avro_serializer,
        "acks": "all",
        "enable.idempotence": True,
        "retries": 5,
        "compression.type": "snappy",
    }
    producer = SerializingProducer(conf)
    
    start_time = time.time()
    end_time = start_time + (duration_minutes * 60)
    transaction_count = 0
    fraud_burst_count = 0
    
    print(f"🚀 Starting streaming producer for {duration_minutes} minutes")
    print(f"📊 Rate: ~{transactions_per_minute} transactions/minute")
    print(f"⏰ Will run until {datetime.fromtimestamp(end_time).strftime('%H:%M:%S')}")
    print("-" * 70)
    
    try:
        while time.time() < end_time:
            # Generate normal transactions
            user = random.choice(USER_POOL)
            transaction = generate_normal_transaction(user)
            
            producer.produce(
                topic=TOPIC,
                key=transaction["user_id"],
                value=transaction,
                on_delivery=delivery_report
            )
            transaction_count += 1
            
            # Occasionally generate fraudulent bursts (every ~30 seconds)
            if random.random() < 0.05:  # 5% chance
                fraud_user = random.choice(USER_POOL)
                print(f"\n🚨 GENERATING FRAUD BURST for {fraud_user} 🚨")
                fraud_transactions = generate_fraudulent_burst(fraud_user, num_transactions=random.randint(4, 7))
                
                for fraud_tx in fraud_transactions:
                    producer.produce(
                        topic=TOPIC,
                        key=fraud_tx["user_id"],
                        value=fraud_tx,
                        on_delivery=delivery_report
                    )
                    transaction_count += 1
                    time.sleep(0.5)  # Small delay between burst transactions
                
                fraud_burst_count += 1
                print(f"💥 Fraud burst #{fraud_burst_count} completed\n")
            
            producer.poll(0)
            
            # Sleep to maintain desired rate
            delay = 60.0 / transactions_per_minute
            time.sleep(delay)
            
            # Status update every minute
            if transaction_count % transactions_per_minute == 0:
                elapsed = time.time() - start_time
                print(f"\n📈 Status: {transaction_count} transactions in {elapsed:.0f}s | {fraud_burst_count} fraud bursts")
        
        producer.flush()
        elapsed = time.time() - start_time
        print("\n" + "=" * 70)
        print(f"✅ Streaming complete!")
        print(f"📊 Total: {transaction_count} transactions in {elapsed:.1f}s ({transaction_count/elapsed:.1f} msgs/sec)")
        print(f"🚨 Fraud bursts: {fraud_burst_count}")
        print("=" * 70)
        
    except KeyboardInterrupt:
        print("\n\n⚠️  Interrupted by user")
        producer.flush()
        print(f"📊 Produced {transaction_count} transactions before stopping")


if __name__ == "__main__":
    import sys
    
    # Parse command line args
    duration = int(sys.argv[1]) if len(sys.argv) > 1 else 5
    rate = int(sys.argv[2]) if len(sys.argv) > 2 else 20
    
    print(f"""
╔════════════════════════════════════════════════════════════════════╗
║          STREAMING FRAUD DETECTION PRODUCER                        ║
╚════════════════════════════════════════════════════════════════════╝

This producer generates transactions over time to demonstrate
windowed fraud detection with velocity analysis.

Usage: python streaming_producer.py [duration_minutes] [rate_per_minute]
Example: python streaming_producer.py 10 30

""")
    
    stream_transactions(duration_minutes=duration, transactions_per_minute=rate)
