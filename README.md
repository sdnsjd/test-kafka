# Kafka KRaft Cluster – 3 Brokers, SASL_SSL + SCRAM-SHA-512

## Структура

```
kafka-kraft/
├── docker-compose.yml
├── certs/                        ← генерируются скриптом
│   ├── kafka1.keystore.jks
│   ├── kafka2.keystore.jks
│   ├── kafka3.keystore.jks
│   ├── truststore.jks
│   └── cloudstudio.truststore.jks  ← это и есть файл для вашего коннектора
├── config/
│   ├── kafka-jaas.properties     ← JAAS для брокеров
│   └── client-jaas.properties    ← JAAS для CLI-утилит внутри контейнеров
└── scripts/
    ├── gen-certs.sh              ← генерация TLS-сертификатов
    └── create-users.sh           ← создание SCRAM-SHA-512 пользователей
```

---

## Быстрый старт

### 1. Генерация сертификатов

> Требуется: `openssl`, `keytool` (входит в JDK).

```bash
chmod +x scripts/gen-certs.sh scripts/create-users.sh
./scripts/gen-certs.sh
```

После выполнения в папке `certs/` появятся все JKS-файлы.  
`cloudstudio.truststore.jks` — это копия `truststore.jks`, именно его указывает ваш коннектор.

---

### 2. Запуск кластера

```bash
docker compose up -d
```

Дождитесь когда все три брокера станут healthy:

```bash
docker compose ps
```

---

### 3. Создание SCRAM-пользователей

После того как кластер поднялся (все контейнеры healthy):

```bash
./scripts/create-users.sh
```

Это создаёт двух пользователей внутри Kafka:

| Пользователь | Пароль         | Назначение              |
|-------------|----------------|-------------------------|
| `admin`     | `admin-secret` | Kafka UI, администрация |
| `appuser`   | `appuser-secret` | Ваш коннектор         |

---
Проверить 

docker exec kafka1 /opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server kafka1:9092 \
  --describe \
  --entity-type users

### 4. Kafka UI

Открыть в браузере: **http://localhost:8080**

Kafka UI подключается к кластеру через `SASL_SSL` с пользователем `admin`.

---

## Порты

| Брокер  | PLAINTEXT (внутр.) | SASL_SSL (внешний) |
|---------|--------------------|--------------------|
| kafka1  | —                  | `localhost:19093`  |
| kafka2  | —                  | `localhost:29093`  |
| kafka3  | —                  | `localhost:39093`  |
| Kafka UI | —                 | `localhost:8080`   |

---

## Полезные команды

```bash
# Список топиков
docker exec kafka1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka1:9092 \
  --command-config /opt/kafka/config/client-jaas.properties \
  --list

# Создать топик
docker exec kafka1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka1:9092 \
  --command-config /opt/kafka/config/client-jaas.properties \
  --create --topic my-topic \
  --partitions 3 --replication-factor 3

# Описание топика
docker exec kafka1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka1:9092 \
  --command-config /opt/kafka/config/client-jaas.properties \
  --describe --topic my-topic

# Посмотреть consumer groups
docker exec kafka1 /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka1:9092 \
  --command-config /opt/kafka/config/client-jaas.properties \
  --list

# Список SCRAM-пользователей
docker exec kafka1 /opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server kafka1:9092 \
  --command-config /opt/kafka/config/client-jaas.properties \
  --describe --entity-type users

# Остановить кластер (данные сохраняются в volumes)
docker compose down

# Полный сброс (удалить данные)
docker compose down -v
```

---

## Пароли

| Что              | Значение     |
|------------------|--------------|
| Keystore/Truststore password | `changeit` |
| Kafka user `admin`   | `admin-secret`   |
| Kafka user `appuser` | `appuser-secret` |

> В продакшне замените все пароли и используйте Docker Secrets или Vault.

---

## Архитектура

```
┌─────────────────────────────────────────────────────┐
│                  kafka-net (bridge)                  │
│                                                      │
│  ┌──────────┐   ┌──────────┐   ┌──────────┐        │
│  │  kafka1  │   │  kafka2  │   │  kafka3  │        │
│  │ node.id=1│   │ node.id=2│   │ node.id=3│        │
│  │ broker + │   │ broker + │   │ broker + │        │
│  │controller│   │controller│   │controller│        │
│  └────┬─────┘   └────┬─────┘   └────┬─────┘        │
│       │              │              │                │
│       └──────── KRaft Quorum ───────┘                │
│              (port 9094, PLAINTEXT)                  │
│                                                      │
│  ┌──────────┐                                        │
│  │ kafka-ui │ → SASL_SSL → kafka1:9093               │
│  │ :8080    │                                        │
│  └──────────┘                                        │
└─────────────────────────────────────────────────────┘
         ↑                    ↑
   SASL_SSL              SASL_SSL
  :19093/:29093/:39093  (внешние порты)
         ↑
   Ваш коннектор (cloudstudio)
```



kafka-topics.sh --bootstrap-server localhost:19093 \
    --command-config ~/kafka-kraft/config/local-client.properties \
    --list



kafka-console-consumer.sh --bootstrap-server localhost:19093 \
    --topic <имя_топика> \
    --consumer.config ~/kafka-kraft/config/local-client.properties \
    --from-beginning


kafka-leader-election.sh --bootstrap-server localhost:19093 \
    --command-config ~/kafka-kraft/config/local-client.properties \
    --election-type preferred \
    --all-topic-partitions

  
sudo nano /etc/hosts

127.0.0.1 kafka1
127.0.0.1 kafka2
127.0.0.1 kafka3