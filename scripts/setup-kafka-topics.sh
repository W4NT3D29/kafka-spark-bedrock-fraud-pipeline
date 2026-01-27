#!/bin/bash

# Script to create required Kafka topics for fraud detection pipeline
# Usage: ./scripts/setup-kafka-topics.sh

echo "Setting up Kafka topics..."

# Check if Kafka is running
if ! docker ps | grep -q kafka-1; then
  echo "Error: Kafka container (kafka-1) is not running."
  echo "Please start the Docker stack first: docker compose -f docker/docker-compose.yml up -d"
  exit 1
fi

echo "Creating 'transactions-raw' topic..."
docker exec kafka-1 kafka-topics \
  --bootstrap-server localhost:9092 \
  --create \
  --topic transactions-raw \
  --partitions 3 \
  --replication-factor 1 \
  --if-not-exists

echo "Creating 'flagged-transactions' topic..."
docker exec kafka-1 kafka-topics \
  --bootstrap-server localhost:9092 \
  --create \
  --topic flagged-transactions \
  --partitions 3 \
  --replication-factor 1 \
  --if-not-exists

echo ""
echo "Topics created successfully! Listing all topics:"
docker exec kafka-1 kafka-topics \
  --bootstrap-server localhost:9092 \
  --list

echo ""
echo "Topic details:"
docker exec kafka-1 kafka-topics \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic transactions-raw

docker exec kafka-1 kafka-topics \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic flagged-transactions

echo ""
echo "✓ Kafka topics setup complete!"
echo "You can view topics in Kafka UI at: http://localhost:8080"
