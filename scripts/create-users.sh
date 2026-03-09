#!/bin/bash
# Run AFTER brokers are up.
# Creates SCRAM-SHA-512 credentials for users inside the Kafka cluster.

set -e

CONTAINER="kafka1"
KAFKA_BIN="/opt/kafka/bin"
BOOTSTRAP_CFG="/opt/kafka/config/bootstrap-client.properties"

echo "==> Waiting for kafka1 to be ready..."
until docker exec "$CONTAINER" \
  "$KAFKA_BIN/kafka-broker-api-versions.sh" \
  --bootstrap-server kafka1:9092 \
  --command-config "$BOOTSTRAP_CFG" \
  > /dev/null 2>&1; do
  echo "   ...not ready yet, retrying in 3s"
  sleep 3
done

echo "==> Creating SCRAM-SHA-512 user: admin ..."
docker exec "$CONTAINER" \
  "$KAFKA_BIN/kafka-configs.sh" \
  --bootstrap-server kafka1:9092 \
  --command-config "$BOOTSTRAP_CFG" \
  --alter \
  --add-config 'SCRAM-SHA-512=[iterations=8192,password=admin-secret]' \
  --entity-type users --entity-name admin

echo "==> Creating SCRAM-SHA-512 user: appuser ..."
docker exec "$CONTAINER" \
  "$KAFKA_BIN/kafka-configs.sh" \
  --bootstrap-server kafka1:9092 \
  --command-config "$BOOTSTRAP_CFG" \
  --alter \
  --add-config 'SCRAM-SHA-512=[iterations=8192,password=appuser-secret]' \
  --entity-type users --entity-name appuser

echo ""
echo "==> Done. Users created:"
echo "  admin   / admin-secret"
echo "  appuser / appuser-secret"
echo ""
echo "Bootstrap servers for your connector:"
echo "  kafka1:9093,kafka2:9093,kafka3:9093   (internal SASL_SSL)"
echo "  localhost:19093,localhost:29093,localhost:39093  (external SASL_SSL)"