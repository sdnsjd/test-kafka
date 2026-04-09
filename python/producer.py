"""
Kafka Producer — запись сообщений в топик.

Поддерживает:
  - Подключение изнутри Docker-сети (SASL_PLAINTEXT, механизм PLAIN)
  - Подключение снаружи (SASL_SSL, механизм SCRAM-SHA-512)
  - Сериализацию в JSON и plain-text
  - Заголовки сообщений, явный partition/key

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
  pip install confluent-kafka
"""

import json
import logging
import sys
import time
from confluent_kafka import Producer, KafkaException

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
    "sasl.username": "broker",          # пользователь из kafka-jaas.properties
    "sasl.password": "broker-secret",   # пароль из kafka-jaas.properties
    # Надёжная доставка
    "acks": "all",                       # ждать подтверждения от всех ISR
    "enable.idempotence": True,          # exactly-once на уровне продюсера
    "retries": 5,
    "retry.backoff.ms": 300,
    "linger.ms": 5,                      # небольшая пауза для батчинга
    "batch.size": 16384,
}

# ── Вариант 2: снаружи Docker-сети (SASL_SSL / SCRAM-SHA-512) ───────────────
EXTERNAL_CONFIG = {
    "bootstrap.servers": "localhost:19093,localhost:29093,localhost:39093",
    "security.protocol": "SASL_SSL",
    "sasl.mechanism": "SCRAM-SHA-512",
    "sasl.username": "your-user",        # ← замените
    "sasl.password": "your-password",   # ← замените
    "ssl.ca.location": "./certs/ca.crt", # ← путь до корневого сертификата
    "acks": "all",
    "enable.idempotence": True,
    "retries": 5,
    "retry.backoff.ms": 300,
    "linger.ms": 5,
    "batch.size": 16384,
}

# ---------------------------------------------------------------------------
# Выбор конфигурации
# ---------------------------------------------------------------------------
CONFIG = INTERNAL_CONFIG   # ← переключите на EXTERNAL_CONFIG при необходимости
TOPIC  = "my-topic"        # ← имя топика

# ---------------------------------------------------------------------------
# Callback подтверждения доставки
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
# Вспомогательные функции
# ---------------------------------------------------------------------------

def send_text(producer: Producer, topic: str, value: str, key: str | None = None):
    """Отправить строку."""
    producer.produce(
        topic,
        value=value.encode("utf-8"),
        key=key.encode("utf-8") if key else None,
        on_delivery=delivery_report,
    )

def send_json(producer: Producer, topic: str, payload: dict, key: str | None = None,
              headers: dict | None = None):
    """Отправить JSON-объект с опциональными заголовками."""
    producer.produce(
        topic,
        value=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        key=key.encode("utf-8") if key else None,
        headers=list(headers.items()) if headers else None,
        on_delivery=delivery_report,
    )

# ---------------------------------------------------------------------------
# Основная логика
# ---------------------------------------------------------------------------

def main():
    producer = Producer(CONFIG)
    log.info("Продюсер запущен. Топик: %s", TOPIC)

    try:
        # ── Пример 1: простые текстовые сообщения ───────────────────────────
        for i in range(5):
            send_text(producer, TOPIC, f"Привет, Kafka! Сообщение #{i}", key=str(i))
            producer.poll(0)   # дать шанс сработать callback'ам

        # ── Пример 2: JSON-сообщения с заголовками ──────────────────────────
        events = [
            {"user_id": 42, "action": "login",   "ts": time.time()},
            {"user_id": 42, "action": "purchase", "ts": time.time(), "amount": 99.9},
            {"user_id": 7,  "action": "logout",  "ts": time.time()},
        ]
        for event in events:
            send_json(
                producer, TOPIC, event,
                key=str(event["user_id"]),
                headers={"source": "producer.py", "env": "dev"},
            )
            producer.poll(0)

        # ── Пример 3: flush — дождаться отправки всего буфера ───────────────
        remaining = producer.flush(timeout=30)
        if remaining > 0:
            log.warning("Не удалось отправить %d сообщений за таймаут", remaining)
        else:
            log.info("Все сообщения успешно отправлены.")

    except KafkaException as exc:
        log.exception("Ошибка Kafka: %s", exc)
        sys.exit(1)
    except KeyboardInterrupt:
        log.info("Прерван пользователем. Выполняю flush…")
        producer.flush(10)


if __name__ == "__main__":
    main()