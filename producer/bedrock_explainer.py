import json
import os
import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import boto3
from botocore.exceptions import ClientError, BotoCoreError
from confluent_kafka import Consumer, Producer, KafkaException
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("BedrockExplainer")

# Configuration from Environment Variables
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
INPUT_TOPIC = os.getenv("INPUT_TOPIC", "flagged-transactions")
OUTPUT_TOPIC = os.getenv("OUTPUT_TOPIC", "explained-transactions")
DLQ_TOPIC = os.getenv("DLQ_TOPIC", "flagged-dlq")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "10"))
CONSUMER_GROUP = os.getenv("CONSUMER_GROUP", "bedrock-explainer-group")

# AWS Configuration (loaded securely from environment or IAM role)
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
BEDROCK_MODEL_ID = os.getenv("BEDROCK_MODEL_ID", "anthropic.claude-3-haiku-20240307-v1:0")

# Initialize AWS Session safely
# This respects AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN environment variables
# or IAM roles if running on EC2/ECS/EKS safely.
session = boto3.Session()
bedrock_client = session.client('bedrock-runtime', region_name=AWS_REGION)

# Initialize Kafka Producer
producer_conf = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'client.id': 'bedrock-explainer-producer',
    'acks': 'all'
}
producer = Producer(producer_conf)

def delivery_report(err, msg):
    if err is not None:
        logger.error(f"Failed to deliver message: {err}")
    else:
        logger.debug(f"Message delivered to {msg.topic()} [{msg.partition()}]")

@retry(
    wait=wait_exponential(multiplier=1, min=2, max=10),
    stop=stop_after_attempt(3),
    retry=retry_if_exception_type((ClientError, BotoCoreError)),
    reraise=True
)
def invoke_bedrock(transaction):
    """
    Calls Bedrock to generate a human-readable explanation for the flagged transaction.
    Uses Tenacity for automatic retries with exponential backoff on AWS API errors.
    """
    prompt = (
        f"You are a fraud analyst. A real-time system flagged the following transaction data "
        f"as suspicious. Read the data and provide a concise, 2-sentence explanation of why "
        f"it might be fraudulent based on the 'fraud_reason' and other signals.\n\n"
        f"Data: {json.dumps(transaction, indent=2)}\n\n"
        f"Explanation:"
    )

    request_body = {
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": 150,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.1
    }

    response = bedrock_client.invoke_model(
        modelId=BEDROCK_MODEL_ID,
        body=json.dumps(request_body),
        accept="application/json",
        contentType="application/json"
    )

    response_body = json.loads(response.get("body").read())
    explanation = response_body.get('content', [])[0].get('text', 'No explanation provided.')
    return explanation

def process_transaction(transaction):
    """
    Worker function to process a single transaction via Bedrock.
    """
    try:
        explanation = invoke_bedrock(transaction)
        transaction['llm_explanation'] = explanation
        transaction['status'] = 'SUCCESS'
        return transaction
    except Exception as e:
        logger.error(f"Bedrock invocation failed for transaction {transaction.get('user_id')}: {e}")
        transaction['status'] = 'FAILED'
        transaction['error'] = str(e)
        return transaction

def consume_and_process():
    consumer_conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': CONSUMER_GROUP,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False
    }
    consumer = Consumer(consumer_conf)
    consumer.subscribe([INPUT_TOPIC])

    logger.info(f"Started consuming from {INPUT_TOPIC}...")

    try:
        while True:
            # Batching logic
            msgs = consumer.consume(num_messages=BATCH_SIZE, timeout=2.0)
            if not msgs:
                continue
                
            batch_transactions = []
            valid_msgs = []
            
            for msg in msgs:
                if msg.error():
                    if msg.error().code() == KafkaException._PARTITION_EOF:
                        continue
                    else:
                        logger.error(f"Consumer error: {msg.error()}")
                        continue
                
                try:
                    val = json.loads(msg.value().decode('utf-8'))
                    batch_transactions.append(val)
                    valid_msgs.append(msg)
                except Exception as e:
                    logger.error(f"Failed to decode message: {e}")
            
            if not batch_transactions:
                continue
                
            logger.info(f"Processing batch of {len(batch_transactions)} flagged transactions...")
            
            # Asynchronous concurrent calls to Bedrock to optimize throughput
            processed_results = []
            with ThreadPoolExecutor(max_workers=BATCH_SIZE) as executor:
                futures = {executor.submit(process_transaction, tx): tx for tx in batch_transactions}
                for future in as_completed(futures):
                    processed_results.append(future.result())

            # Produce results back to Kafka
            for tx in processed_results:
                if tx['status'] == 'SUCCESS':
                    producer.produce(OUTPUT_TOPIC, key=str(tx.get('user_id', '')).encode('utf-8'), value=json.dumps(tx).encode('utf-8'), on_delivery=delivery_report)
                else:
                    # Dead Letter Queue for failed cases
                    logger.warning(f"Sending to DLQ: {tx.get('user_id')}")
                    producer.produce(DLQ_TOPIC, key=str(tx.get('user_id', '')).encode('utf-8'), value=json.dumps(tx).encode('utf-8'), on_delivery=delivery_report)
            
            producer.flush()
            
            # Commit offsets only after entire batch is successfully pushed
            consumer.commit()
            
    except KeyboardInterrupt:
        logger.info("Shutdown requested.")
    finally:
        consumer.close()

if __name__ == "__main__":
    consume_and_process()
