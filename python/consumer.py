"""
Kafka Consumer — чтение сообщений из топика.

Поддерживает:
  - Подключение изнутри Docker-сети (SASL_PLAINTEXT / PLAIN)
  - Подключение снаружи (SASL_SSL / SCRAM-SHA-512)
  - Consumer Group с авто-коммитом и ручным коммитом
  - Десериализацию JSON и plain-text
  - Graceful shutdown по Ctrl+C

python не поддерживает jks
если нет ca-cert.pem а только jks то выполнить:

openssl pkcs12 \                                       
  -in ./certs/cloudstudio.truststore.jks \
  -nokeys \
  -out ./certs/ca-cert.pem \
  -passin pass:changeit
  

Зависимости:
  pip install confluent-kafka
"""

import json
import logging
import signal
import sys
from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition
from typing import Optional, Union, Tuple

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Конфигурации подключения
# ---------------------------------------------------------------------------

# ── Вариант 1: внутри Docker-сети (SASL_PLAINTEXT / PLAIN) ──────────────────
INTERNAL_CONFIG = {
    "bootstrap.servers": "kafka1:9092,kafka2:9092,kafka3:9092",
    "security.protocol": "SASL_PLAINTEXT",
    "sasl.mechanism": "PLAIN",
    "sasl.username": "broker",
    "sasl.password": "broker-secret",
    # Consumer Group
    "group.id": "my-consumer-group",
    "auto.offset.reset": "earliest",     # начать с начала, если нет сохранённого offset
    "enable.auto.commit": False,         # ручной коммит — надёжнее
    "max.poll.interval.ms": 300000,
    "session.timeout.ms": 45000,
    "heartbeat.interval.ms": 3000,
}

# ── Вариант 2: снаружи Docker-сети (SASL_SSL / SCRAM-SHA-512) ───────────────
EXTERNAL_CONFIG = {
    "bootstrap.servers": "localhost:19093,localhost:29093,localhost:39093",
    "security.protocol": "SASL_SSL",
    "sasl.mechanism": "SCRAM-SHA-512",
    "sasl.username": "admin",        # ← замените
    "sasl.password": "admin-secret",   # ← замените
    "ssl.ca.location": "./certs/ca-cert.pem", # ← путь до корневого сертификата
    "group.id": "my-consumer-group",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
    "max.poll.interval.ms": 300000,
    "session.timeout.ms": 45000,
    "heartbeat.interval.ms": 3000,
}

# ---------------------------------------------------------------------------
# Выбор конфигурации
# ---------------------------------------------------------------------------
CONFIG  = EXTERNAL_CONFIG   # ← переключите на EXTERNAL_CONFIG при необходимости
TOPICS  = ["user-events"]      # ← список топиков для подписки
TIMEOUT = 1.0               # секунды ожидания нового сообщения

# ---------------------------------------------------------------------------
# Graceful shutdown
# ---------------------------------------------------------------------------
running = True

def _stop_handler(sig, frame):
    global running
    log.info("Получен сигнал %s, останавливаю консьюмер…", signal.Signals(sig).name)
    running = False

signal.signal(signal.SIGINT,  _stop_handler)
signal.signal(signal.SIGTERM, _stop_handler)

# ---------------------------------------------------------------------------
# Вспомогательные функции
# ---------------------------------------------------------------------------

def decode_message(msg) -> Tuple[Optional[str], Union[dict, str]]:
    """Декодировать key и value. Value пробуется как JSON, иначе — строка."""
    key = msg.key().decode("utf-8") if msg.key() else None
    raw = msg.value()
    try:
        value = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        value = raw.decode("utf-8") if raw else ""
    return key, value

def print_headers(msg):
    headers = msg.headers()
    if headers:
        decoded = {k: v.decode("utf-8") if isinstance(v, bytes) else v
                   for k, v in headers}
        log.info("  headers: %s", decoded)

# ---------------------------------------------------------------------------
# Callbacks назначения партиций
# ---------------------------------------------------------------------------

def on_assign(consumer, partitions):
    log.info("Назначены партиции: %s",
             [(p.topic, p.partition) for p in partitions])

def on_revoke(consumer, partitions):
    log.info("Отозваны партиции: %s",
             [(p.topic, p.partition) for p in partitions])
    # Закоммитить текущие offset'ы перед отзывом
    try:
        consumer.commit(asynchronous=False)
    except KafkaException:
        pass

# ---------------------------------------------------------------------------
# Основная логика
# ---------------------------------------------------------------------------

def main():
    consumer = Consumer(CONFIG)
    consumer.subscribe(TOPICS, on_assign=on_assign, on_revoke=on_revoke)
    log.info("Консьюмер запущен. Подписка: %s  group: %s",
             TOPICS, CONFIG["group.id"])

    processed = 0

    try:
        while running:
            msg = consumer.poll(timeout=TIMEOUT)

            if msg is None:
                # Нет новых сообщений в течение таймаута
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # Достигнут конец партиции — это нормально
                    log.debug(
                        "EOF: topic=%s partition=%d offset=%d",
                        msg.topic(), msg.partition(), msg.offset()
                    )
                else:
                    raise KafkaException(msg.error())
                continue

            # ── Обработка сообщения ─────────────────────────────────────────
            key, value = decode_message(msg)

            log.info(
                "Получено ← topic=%s partition=%d offset=%d key=%s",
                msg.topic(), msg.partition(), msg.offset(), key,
            )
            print_headers(msg)

            if isinstance(value, dict):
                log.info("  payload (JSON): %s", json.dumps(value, ensure_ascii=False))
            else:
                log.info("  payload (text): %s", value)

            # ── Ваша бизнес-логика здесь ────────────────────────────────────
            # process(key, value)

            processed += 1

            # ── Ручной коммит каждые 100 сообщений (или сразу — зависит от задачи)
            if processed % 100 == 0:
                consumer.commit(asynchronous=False)
                log.info("Закоммичено %d сообщений", processed)

    except KafkaException as exc:
        log.exception("Ошибка Kafka: %s", exc)
        sys.exit(1)
    finally:
        log.info("Закрываю консьюмер, закоммичено offset'ов…")
        try:
            consumer.commit(asynchronous=False)
        except KafkaException:
            pass
        consumer.close()
        log.info("Консьюмер остановлен. Всего обработано: %d сообщений.", processed)


if __name__ == "__main__":
    main()