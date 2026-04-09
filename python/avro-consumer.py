"""
Kafka Avro Consumer — чтение и десериализация Avro-сообщений через Schema Registry.

Схему НЕ нужно знать заранее — она подтягивается из Schema Registry
по Schema ID, встроенному в каждое сообщение.

python не поддерживает jks
если нет ca-cert.pem а только jks то выполнить:

openssl pkcs12 \                                       
  -in ./certs/cloudstudio.truststore.jks \
  -nokeys \
  -out ./certs/ca-cert.pem \
  -passin pass:changeit

python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install --upgrade pip

Зависимости:
  pip install 'confluent-kafka[avro]' fastavro requests
"""

import json
import logging
import signal
import sys
from confluent_kafka import Consumer, KafkaError, KafkaException
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField

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
# Avro-схема для десериализации.
# Можно передать None — тогда AvroDeserializer вернёт dict автоматически,
# но явная схема даёт валидацию и документацию.
# ---------------------------------------------------------------------------
USER_EVENT_SCHEMA = """
{
  "type": "record",
  "name": "UserEvent",
  "namespace": "com-example",
  "fields": [
    {"name": "user_id",   "type": "int"},
    {"name": "action",    "type": "string"},
    {"name": "timestamp", "type": "double"},
    {"name": "amount",    "type": ["null", "double"], "default": null}
  ]
}
"""

# ---------------------------------------------------------------------------
# Конфигурация консьюмера
# ---------------------------------------------------------------------------

INTERNAL_CONFIG = {
    "bootstrap.servers": "kafka1:9092,kafka2:9092,kafka3:9092",
    "security.protocol": "SASL_PLAINTEXT",
    "sasl.mechanism": "PLAIN",
    "sasl.username": "broker",
    "sasl.password": "broker-secret",
    "group.id": "avro-consumer-group",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
    "max.poll.interval.ms": 300000,
    "session.timeout.ms": 45000,
}

EXTERNAL_CONFIG = {
    "bootstrap.servers": "localhost:19093,localhost:29093,localhost:39093",
    "security.protocol": "SASL_SSL",
    "sasl.mechanism": "SCRAM-SHA-512",
    "sasl.username": "admin",        # ← замените
    "sasl.password": "admin-secret",   # ← замените
    "ssl.ca.location": "./certs/ca-cert.pem",
    "group.id": "avro-consumer-group",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
    "max.poll.interval.ms": 300000,
    "session.timeout.ms": 45000,
}

CONFIG  = EXTERNAL_CONFIG
TOPICS  = ["user-events"]
TIMEOUT = 1.0

# ---------------------------------------------------------------------------
# Graceful shutdown
# ---------------------------------------------------------------------------
running = True

def _stop_handler(sig, frame):
    global running
    log.info("Сигнал %s — останавливаю консьюмер…", signal.Signals(sig).name)
    running = False

signal.signal(signal.SIGINT,  _stop_handler)
signal.signal(signal.SIGTERM, _stop_handler)

# ---------------------------------------------------------------------------
# Маппинг Avro-объекта → Python-объект
# ---------------------------------------------------------------------------

def dict_to_user_event(data: dict, ctx) -> dict:
    """Можно добавить валидацию или конвертацию типов здесь."""
    return data

# ---------------------------------------------------------------------------
# Callbacks
# ---------------------------------------------------------------------------

def on_assign(consumer, partitions):
    log.info("Назначены партиции: %s",
             [(p.topic, p.partition) for p in partitions])

def on_revoke(consumer, partitions):
    log.info("Отозваны партиции: %s",
             [(p.topic, p.partition) for p in partitions])
    try:
        consumer.commit(asynchronous=False)
    except KafkaException:
        pass

# ---------------------------------------------------------------------------
# Основная логика
# ---------------------------------------------------------------------------

def main():
    sr_client = SchemaRegistryClient(SCHEMA_REGISTRY_CONFIG)

    # Десериализатор: Schema ID из сообщения → запрос схемы в Registry → decode
    avro_deserializer = AvroDeserializer(
        schema_registry_client=sr_client,
        schema_str=None,   # передайте None для auto-схемы
        from_dict=dict_to_user_event,
    )

    consumer = Consumer(CONFIG)
    consumer.subscribe(TOPICS, on_assign=on_assign, on_revoke=on_revoke)
    log.info("Avro Consumer запущен. Топики: %s  group: %s",
             TOPICS, CONFIG["group.id"])

    processed = 0

    try:
        while running:
            msg = consumer.poll(timeout=TIMEOUT)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    log.debug("EOF: topic=%s partition=%d offset=%d",
                              msg.topic(), msg.partition(), msg.offset())
                else:
                    raise KafkaException(msg.error())
                continue

            # ── Десериализация Avro ──────────────────────────────────────────
            try:
                event = avro_deserializer(
                    msg.value(),
                    SerializationContext(msg.topic(), MessageField.VALUE),
                )
            except Exception as exc:
                log.error("Ошибка десериализации offset=%d: %s",
                          msg.offset(), exc)
                continue

            key = msg.key().decode("utf-8") if msg.key() else None

            log.info(
                "Получено ← topic=%s partition=%d offset=%d key=%s",
                msg.topic(), msg.partition(), msg.offset(), key,
            )
            log.info("  event: %s", json.dumps(event, ensure_ascii=False))

            # ── Ваша бизнес-логика ───────────────────────────────────────────
            # process_event(event)

            processed += 1

            # Ручной коммит каждые 100 сообщений
            if processed % 100 == 0:
                consumer.commit(asynchronous=False)
                log.info("Закоммичено %d сообщений", processed)

    except KafkaException as exc:
        log.exception("Ошибка Kafka: %s", exc)
        sys.exit(1)
    finally:
        log.info("Закрываю консьюмер…")
        try:
            consumer.commit(asynchronous=False)
        except KafkaException:
            pass
        consumer.close()
        log.info("Остановлен. Обработано: %d сообщений.", processed)


if __name__ == "__main__":
    main()