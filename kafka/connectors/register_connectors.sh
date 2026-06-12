#!/bin/bash
# Register all Kafka Connect connectors

CONNECT_URL="http://localhost:8083"

echo "Waiting for Kafka Connect to be ready..."
until curl -s "$CONNECT_URL/connectors" > /dev/null; do
  sleep 2
done
echo "Kafka Connect is ready."

echo "Registering Debezium source connector..."
curl -X POST "$CONNECT_URL/connectors" \
  -H "Content-Type: application/json" \
  -d @connectors/debezium-source.json

echo "Registering Snowflake sink connector..."
curl -X POST "$CONNECT_URL/connectors" \
  -H "Content-Type: application/json" \
  -d @connectors/snowflake-sink.json

echo "All connectors registered."