# Kafka KRaft Cluster – 3 Brokers, SASL_SSL + SCRAM-SHA-512

## Быстрый старт

### 1. Генерация сертификатов(Если их нет)

> Требуется: `openssl`, `keytool` (входит в JDK).

```bash
chmod +x scripts/gen-certs.sh scripts/create-users.sh
./scripts/gen-certs.sh
```
---

### 2. Запуск кластера

```bash
docker compose up -d
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

### 4. Kafka UI

Открыть в браузере: **http://localhost:8080**

Kafka UI подключается к кластеру через `SASL_SSL` с пользователем `admin`.

---

## Порты

| Брокер  | PLAINTEXT (внутр.) | SASL_SSL (внешний) |
|---------|--------------------|--------------------|
| kafka1  | — kafka:9092       | `localhost:19093`  |
| kafka2  | — kafka:9092       | `localhost:29093`  |
| kafka3  | — kafka:9092       | `localhost:39093`  |
| Kafka UI| —                  | `localhost:8080`   |

---

## Пароли

| Что              | Значение     |
|------------------|--------------|
| Keystore/Truststore password | `changeit` |
| Kafka user `admin`   | `admin-secret`   |
| Kafka user `appuser` | `appuser-secret` |

---

## Команды

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