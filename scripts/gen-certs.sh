#!/bin/bash
set -e

CERTS_DIR="$(cd "$(dirname "$0")/.." && pwd)/certs"
PASSWORD="changeit"
VALIDITY=3650
CA_CN="KafkaCA"
HOSTNAME_LIST=("kafka1" "kafka2" "kafka3" "localhost")

echo "==> Generating certs in: $CERTS_DIR"
mkdir -p "$CERTS_DIR"
cd "$CERTS_DIR"

# ---- 1. CA ----
echo "==> Generating CA key + cert..."
openssl req -new -x509 -keyout ca.key -out ca.crt -days $VALIDITY \
  -passout pass:"$PASSWORD" \
  -subj "/CN=$CA_CN/OU=Kafka/O=Local/L=Local/S=Local/C=US"

# ---- 2. Per-broker keystores ----
for BROKER in kafka1 kafka2 kafka3; do
  echo "==> Generating keystore for $BROKER..."

  # Generate keystore
  keytool -genkey -noprompt \
    -alias "$BROKER" \
    -dname "CN=$BROKER,OU=Kafka,O=Local,L=Local,S=Local,C=US" \
    -keystore "$BROKER.keystore.jks" \
    -keyalg RSA -keysize 2048 \
    -validity $VALIDITY \
    -storepass "$PASSWORD" -keypass "$PASSWORD"

  # Generate CSR
  keytool -certreq -noprompt \
    -alias "$BROKER" \
    -keystore "$BROKER.keystore.jks" \
    -file "$BROKER.csr" \
    -storepass "$PASSWORD"

  # SAN extension
  cat > "$BROKER.ext" <<EOF
[req]
req_extensions = v3_req
distinguished_name = req_distinguished_name
[req_distinguished_name]
[v3_req]
subjectAltName = @alt_names
[alt_names]
DNS.1 = $BROKER
DNS.2 = localhost
IP.1  = 127.0.0.1
EOF

  # Sign with CA
  openssl x509 -req \
    -CA ca.crt -CAkey ca.key \
    -in "$BROKER.csr" -out "$BROKER.crt" \
    -days $VALIDITY -CAcreateserial \
    -passin pass:"$PASSWORD" \
    -extfile "$BROKER.ext" -extensions v3_req

  # Import CA cert into keystore
  keytool -import -noprompt \
    -alias CARoot \
    -file ca.crt \
    -keystore "$BROKER.keystore.jks" \
    -storepass "$PASSWORD"

  # Import signed broker cert
  keytool -import -noprompt \
    -alias "$BROKER" \
    -file "$BROKER.crt" \
    -keystore "$BROKER.keystore.jks" \
    -storepass "$PASSWORD"

  echo "  -> $BROKER.keystore.jks done"
done

# ---- 3. Single truststore (all clients + UI) ----
echo "==> Generating truststore..."
keytool -import -noprompt \
  -alias CARoot \
  -file ca.crt \
  -keystore truststore.jks \
  -storepass "$PASSWORD"

# Also export a PEM version for easy curl / openssl usage
cp ca.crt ca-bundle.pem

# ---- 4. Copy truststore to client certs path ----
# The connector expects ./certs/cloudstudio.truststore.jks
cp truststore.jks cloudstudio.truststore.jks

echo ""
echo "==> Done! Files in $CERTS_DIR:"
ls -lh "$CERTS_DIR"
echo ""
echo "Truststore / Keystore password: $PASSWORD"