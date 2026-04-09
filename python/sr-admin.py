"""
Schema Registry Admin — управление Avro-схемами.

Возможности:
  - Зарегистрировать схему вручную
  - Просмотреть все субъекты и версии
  - Получить схему по субъекту / версии / ID
  - Проверить совместимость новой схемы
  - Удалить субъект или версию

python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install --upgrade pip

Зависимости:
  pip install confluent-kafka[avro] requests
"""

import json
import logging
from confluent_kafka.schema_registry import SchemaRegistryClient, Schema

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# SR_URL = "http://schema-registry:8081"   # внутри Docker-сети
SR_URL = "http://localhost:8081"        # снаружи Docker-сети

client = SchemaRegistryClient({"url": SR_URL})

# ---------------------------------------------------------------------------
# Регистрация схемы
# ---------------------------------------------------------------------------

def register_schema(subject: str, schema_str: str) -> int:
    """Зарегистрировать схему. Возвращает Schema ID."""
    schema = Schema(schema_str, schema_type="AVRO")
    schema_id = client.register_schema(subject, schema)
    log.info("Схема зарегистрирована: subject=%s  id=%d", subject, schema_id)
    return schema_id

# ---------------------------------------------------------------------------
# Просмотр субъектов и версий
# ---------------------------------------------------------------------------

def list_subjects():
    subjects = client.get_subjects()
    log.info("Субъекты (%d):", len(subjects))
    for s in sorted(subjects):
        log.info("  %s", s)
    return subjects

def list_versions(subject: str):
    versions = client.get_versions(subject)
    log.info("Версии субъекта '%s': %s", subject, versions)
    return versions

# ---------------------------------------------------------------------------
# Получение схемы
# ---------------------------------------------------------------------------

def get_schema_by_id(schema_id: int):
    schema = client.get_schema(schema_id)
    log.info("Схема id=%d:\n%s",
             schema_id,
             json.dumps(json.loads(schema.schema_str), indent=2, ensure_ascii=False))
    return schema

def get_latest_schema(subject: str):
    registered = client.get_latest_version(subject)
    log.info("Последняя версия '%s': version=%d  id=%d\n%s",
             subject, registered.version, registered.schema_id,
             json.dumps(json.loads(registered.schema.schema_str), indent=2, ensure_ascii=False))
    return registered

# ---------------------------------------------------------------------------
# Проверка совместимости
# ---------------------------------------------------------------------------

def check_compatibility(subject: str, new_schema_str: str) -> bool:
    schema = Schema(new_schema_str, schema_type="AVRO")
    compatible = client.test_compatibility(subject, schema)
    status = "✓ совместима" if compatible else "✗ НЕСОВМЕСТИМА"
    log.info("Совместимость схемы с '%s': %s", subject, status)
    return compatible

# ---------------------------------------------------------------------------
# Удаление
# ---------------------------------------------------------------------------

def delete_subject(subject: str, permanent: bool = False):
    """permanent=True — физическое удаление (hard delete)."""
    versions = client.delete_subject(subject, permanent=permanent)
    log.info("Субъект '%s' удалён. Версии: %s", subject, versions)

# ---------------------------------------------------------------------------
# Demo
# ---------------------------------------------------------------------------

USER_EVENT_SCHEMA_V1 = """
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

# Обратно-совместимое изменение: добавлено поле с default
USER_EVENT_SCHEMA_V2 = """
{
  "type": "record",
  "name": "UserEvent",
  "namespace": "com.example",
  "fields": [
    {"name": "user_id",   "type": "int"},
    {"name": "action",    "type": "string"},
    {"name": "timestamp", "type": "double"},
    {"name": "amount",    "type": ["null", "double"], "default": null},
    {"name": "ip_address","type": ["null", "string"], "default": null}
  ]
}
"""

if __name__ == "__main__":
    subject = "user-events-value"

    print("\n=== Регистрация схемы v1 ===")
    register_schema(subject, USER_EVENT_SCHEMA_V1)

    print("\n=== Список субъектов ===")
    list_subjects()

    print("\n=== Последняя версия ===")
    get_latest_schema(subject)

    print("\n=== Проверка совместимости v2 ===")
    check_compatibility(subject, USER_EVENT_SCHEMA_V2)

    print("\n=== Регистрация схемы v2 ===")
    register_schema(subject, USER_EVENT_SCHEMA_V2)

    print("\n=== Версии субъекта ===")
    list_versions(subject)