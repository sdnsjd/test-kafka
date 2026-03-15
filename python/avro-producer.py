"""
Kafka Avro Producer — запись сообщений с Avro-сериализацией через Schema Registry.

python не поддерживает jks
если нет ca-cert.pem а только jks то выполнить:

openssl pkcs12 \                                       
  -in ./certs/cloudstudio.truststore.jks \
  -nokeys \
  -out ./certs/ca-cert.pem \
  -passin pass:changeit
  

Зависимости:
  pip install confluent-kafka[avro] fastavro requests
"""

import logging
import sys
import time
from confluent_kafka import KafkaException
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka import Producer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Schema Registry
# ---------------------------------------------------------------------------
SCHEMA_REGISTRY_CONFIG = {
    # "url": "http://schema-registry:8081",   # внутри Docker-сети
    "url": "http://localhost:8081",        # снаружи Docker-сети
}

# ---------------------------------------------------------------------------
# Avro-схема (пример: событие пользователя)
# Зарегистрируется автоматически при первой отправке.
# ---------------------------------------------------------------------------
USER_EVENT_SCHEMA = """
{
  "type": "record",
  "name": "UserEvent",
  "namespace": "com.example",
  "fields": [
    {"name": "user_id",   "type": "int"},
    {"name": "action",    "type": "string"},
    {"name": "timestamp", "type": "double"},
    {"name": "amount",    "type": ["null", "double"], "default": null}
  ]
}
"""

# ---------------------------------------------------------------------------
# Конфигурация продюсера
# ---------------------------------------------------------------------------

# ── Внутри Docker-сети (SASL_PLAINTEXT / PLAIN) ──────────────────────────────
INTERNAL_CONFIG = {
    "bootstrap.servers": "kafka1:9092,kafka2:9092,kafka3:9092",
    "security.protocol": "SASL_PLAINTEXT",
    "sasl.mechanism": "PLAIN",
    "sasl.username": "broker",
    "sasl.password": "broker-secret",
    "acks": "all",
    "enable.idempotence": True,
    "retries": 5,
    "retry.backoff.ms": 300,
    "linger.ms": 5,
}

# ── Снаружи Docker-сети (SASL_SSL / SCRAM-SHA-512) ───────────────────────────
EXTERNAL_CONFIG = {
    "bootstrap.servers": "localhost:19093,localhost:29093,localhost:39093",
    "security.protocol": "SASL_SSL",
    "sasl.mechanism": "SCRAM-SHA-512",
    "sasl.username": "admin",        # ← замените
    "sasl.password": "admin-secret",   # ← замените
    "ssl.ca.location": "./certs/ca-cert.pem",
    "acks": "all",
    "enable.idempotence": True,
    "retries": 5,
    "retry.backoff.ms": 300,
    "linger.ms": 5,
}

CONFIG = EXTERNAL_CONFIG   # ← переключите при необходимости
TOPIC  = "user-events"

# ---------------------------------------------------------------------------
# Маппинг Python-dict → Avro-объект (требуется для AvroSerializer)
# ---------------------------------------------------------------------------

def user_event_to_dict(event: dict, ctx) -> dict:
    """Конвертер: просто возвращаем словарь как есть."""
    return event

# ---------------------------------------------------------------------------
# Delivery callback
# ---------------------------------------------------------------------------

def delivery_report(err, msg):
    if err:
        log.error("Ошибка доставки: %s", err)
    else:
        log.info(
            "Доставлено → topic=%s partition=%d offset=%d key=%s",
            msg.topic(), msg.partition(), msg.offset(),
            msg.key().decode() if msg.key() else None,
        )

# ---------------------------------------------------------------------------
# Основная логика
# ---------------------------------------------------------------------------

def main():
    # Клиент Schema Registry
    sr_client = SchemaRegistryClient(SCHEMA_REGISTRY_CONFIG)

    # Avro-сериализатор (регистрирует схему автоматически)
    avro_serializer = AvroSerializer(
        schema_registry_client=sr_client,
        schema_str=USER_EVENT_SCHEMA,
        to_dict=user_event_to_dict,
    )

    producer = Producer(CONFIG)
    log.info("Avro Producer запущен. Топик: %s", TOPIC)

    events = [
        {"user_id": 1,  "action": "login",    "timestamp": time.time(), "amount": None},
        {"user_id": 2,  "action": "purchase",  "timestamp": time.time(), "amount": 149.99},
        {"user_id": 1,  "action": "logout",   "timestamp": time.time(), "amount": None},
        {"user_id": 3,  "action": "register", "timestamp": time.time(), "amount": None},
        {"user_id": 2,  "action": "purchase",  "timestamp": time.time(), "amount": 49.0},
    ]

    try:
        for event in events:
            key = str(event["user_id"])

            # Сериализуем value в Avro (Schema ID вшивается в начало байтов)
            avro_value = avro_serializer(
                event,
                SerializationContext(TOPIC, MessageField.VALUE),
            )

            producer.produce(
                topic=TOPIC,
                key=key.encode("utf-8"),
                value=avro_value,
                on_delivery=delivery_report,
            )
            producer.poll(0)

        remaining = producer.flush(timeout=30)
        if remaining > 0:
            log.warning("Не отправлено %d сообщений", remaining)
        else:
            log.info("Все Avro-сообщения успешно отправлены.")

    except KafkaException as exc:
        log.exception("Ошибка Kafka: %s", exc)
        sys.exit(1)
    except KeyboardInterrupt:
        producer.flush(10)


if __name__ == "__main__":
    main()