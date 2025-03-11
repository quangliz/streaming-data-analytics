#!/bin/bash
# Script to initialize Kafka topic

echo "Waiting for Kafka to be ready..."
sleep 10

# Create the Kafka topic
kafka-topics --create \
  --bootstrap-server kafka:29092 \
  --replication-factor 1 \
  --partitions 1 \
  --topic user_data \
  --if-not-exists

echo "Kafka topic 'user_data' created successfully" 