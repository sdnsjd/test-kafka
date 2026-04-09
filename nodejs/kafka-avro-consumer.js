/**
 * Kafka Avro Consumer — чтение и десериализация Avro-сообщений через Schema Registry.
 *
 * Схему НЕ нужно знать заранее — она подтягивается из Schema Registry
 * по Schema ID, встроенному в каждое сообщение.
 *
 * Зависимости:
 *   npm install kafkajs @kafkajs/confluent-schema-registry 
 *
 * Для SSL (EXTERNAL_CONFIG):
 *   npm install kafkajs @kafkajs/confluent-schema-registry
 *   Файл ca-cert.pem получить так (если есть только jks):
 *     openssl pkcs12 \
 *       -in ./certs/cloudstudio.truststore.jks \
 *       -nokeys \
 *       -out ./certs/ca-cert.pem \
 *       -passin pass:changeit
 */

const { Kafka, logLevel } = require('kafkajs');
const { SchemaRegistry } = require('@kafkajs/confluent-schema-registry');
const fs = require('fs');

// ---------------------------------------------------------------------------
// Schema Registry
// ---------------------------------------------------------------------------
const schemaRegistry = new SchemaRegistry({
  host: 'http://localhost:8081', // снаружи Docker-сети
  // host: 'http://schema-registry:8081', // внутри Docker-сети
});

// ---------------------------------------------------------------------------
// Конфигурация клиента Kafka
// ---------------------------------------------------------------------------

const INTERNAL_CONFIG = {
  clientId: 'avro-consumer-node',
  brokers: ['kafka1:9092', 'kafka2:9092', 'kafka3:9092'],
  sasl: {
    mechanism: 'plain',
    username: 'broker',
    password: 'broker-secret',
  },
  ssl: false,
  logLevel: logLevel.WARN,
};

const EXTERNAL_CONFIG = {
  clientId: 'avro-consumer-node',
  brokers: ['localhost:19093', 'localhost:29093', 'localhost:39093'],
  sasl: {
    mechanism: 'scram-sha-512',
    username: 'admin',       // ← замените
    password: 'admin-secret', // ← замените
  },
  ssl: {
    rejectUnauthorized: true,
    ca: [fs.readFileSync('../certs/ca-cert.pem')],
  },
  logLevel: logLevel.WARN,
};

const KAFKA_CONFIG = EXTERNAL_CONFIG;
const TOPICS       = ['user-events'];
const GROUP_ID     = 'avro-consumer-group1';
const COMMIT_EVERY = 100; // ручной коммит каждые N сообщений

// ---------------------------------------------------------------------------
// Маппинг Avro-объекта → нужный формат (опционально)
// ---------------------------------------------------------------------------
function mapUserEvent(data) {
  // Добавьте здесь валидацию или конвертацию типов при необходимости
  return data;
}

// ---------------------------------------------------------------------------
// Ваша бизнес-логика
// ---------------------------------------------------------------------------
async function processEvent(event) {
  // TODO: обработка события
  // console.log('Processing:', event);
}

// ---------------------------------------------------------------------------
// Graceful shutdown
// ---------------------------------------------------------------------------
let running = true;

function setupSignalHandlers(consumer) {
  const stop = async (signal) => {
    if (!running) return;
    running = false;
    console.log(`\n[${signal}] Останавливаю consumer...`);
    await consumer.disconnect();
  };
  process.on('SIGINT',  () => stop('SIGINT'));
  process.on('SIGTERM', () => stop('SIGTERM'));
}

// ---------------------------------------------------------------------------
// Основная логика
// ---------------------------------------------------------------------------
async function main() {
  const kafka    = new Kafka(KAFKA_CONFIG);
  const consumer = kafka.consumer({
    groupId:               GROUP_ID,
    sessionTimeout:        45000,
    maxWaitTimeInMs:       1000,
    // Аналог max.poll.interval.ms — задаётся на уровне heartbeat/rebalance
  });

  setupSignalHandlers(consumer);

  await consumer.connect();
  console.log(`[INFO] Avro Consumer запущен. Топики: ${TOPICS}  group: ${GROUP_ID}`);

  await consumer.subscribe({
    topics:    TOPICS,
    fromBeginning: true, // auto.offset.reset: earliest
  });

  let processed   = 0;
  let batch       = null; // хранит текущий batch для ручного коммита

  await consumer.run({
    autoCommit: false,         // enable.auto.commit: false
    eachBatchAutoResolve: true,

    eachBatch: async ({ batch: currentBatch, resolveOffset, heartbeat, isRunning, isStale }) => {
      batch = currentBatch;

      console.log(
        `[INFO] Назначена партиция: topic=${currentBatch.topic} partition=${currentBatch.partition}`
      );

      for (const message of currentBatch.messages) {
        if (!isRunning() || isStale()) break;

        // ── Десериализация Avro ───────────────────────────────────────────
        let event;
        try {
          event = await schemaRegistry.decode(message.value);
          event = mapUserEvent(event);
        } catch (err) {
          console.error(`[ERROR] Ошибка десериализации offset=${message.offset}:\n`, err.stack ?? err);
          resolveOffset(message.offset);
          continue;
        }

        const key = message.key ? message.key.toString('utf8') : null;

        console.log(
          `[INFO] Получено ← topic=${currentBatch.topic} partition=${currentBatch.partition} offset=${message.offset} key=${key}`
        );
        console.log(`[INFO]   event: ${JSON.stringify(event)}`);

        // ── Ваша бизнес-логика ────────────────────────────────────────────
        await processEvent(event);

        resolveOffset(message.offset);
        processed++;

        // Ручной коммит каждые COMMIT_EVERY сообщений
        if (processed % COMMIT_EVERY === 0) {
          await consumer.commitOffsets([{
            topic:     currentBatch.topic,
            partition: currentBatch.partition,
            offset:    (BigInt(message.offset) + 1n).toString(),
          }]);
          console.log(`[INFO] Закоммичено ${processed} сообщений`);
        }

        await heartbeat();
      }
    },
  });

  // Финальный коммит при остановке
  if (batch) {
    try {
      const lastMsg = batch.messages.at(-1);
      if (lastMsg) {
        await consumer.commitOffsets([{
          topic:     batch.topic,
          partition: batch.partition,
          offset:    (BigInt(lastMsg.offset) + 1n).toString(),
        }]);
      }
    } catch (_) {}
  }

  console.log(`[INFO] Остановлен. Обработано: ${processed} сообщений.`);
}

main().catch((err) => {
  console.error('[FATAL]', err.stack ?? err);
  process.exit(1);
});