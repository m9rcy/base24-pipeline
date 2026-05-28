# base24-pipeline

Spring Boot consumer pipeline for the Base24 EPS Real-Time Feed (xEE-SE451 module).

## What it does

Consumes XML `RTFDataRq` Data fragments from a Kafka topic, filters to `PTLFX` record
types, tokenises the digital PAN via `cards-tokenisation-service`, then saves the
event via `transactions/save`.

```
Kafka Topic → Parse XML → Filter PTLFX + TVN/TCN/ACN → Tokenise DPAN → Save Event
```

## Prerequisites

- Java 21+
- Maven 3.8+
- Kafka (for running the app; not required for tests)

---

## Running Tests

```bash
mvn test
```

Most tests are pure unit tests — no Spring context, no Kafka, no HTTP calls.
PostgreSQL deduplication and Kafka end-to-end scenarios use Testcontainers and
are skipped automatically when Docker is not available.

### Testcontainers And Docker Desktop

This project uses Testcontainers `2.0.5`. As of 2026-05-28, Maven Central and
the Testcontainers Java release page list `2.0.5` as the latest version.

Docker Desktop with Docker Engine 29 can fail with older Testcontainers versions.
The practical 1.x floor reported by the community is `1.21.4`; the project now
uses `2.0.5`, which also works with Docker Engine 29.

On this workstation, `~/.testcontainers.properties` points at Docker Desktop's
raw socket:

```properties
docker.host=unix:///Users/m9rcy/Library/Containers/com.docker.docker/Data/docker.raw.sock
```

That lets the JVM connect to Docker, but Ryuk then tries to mount
`docker.raw.sock` into its cleanup container and Docker rejects the mount. The
symptom is:

```text
error while creating mount source path '.../docker.raw.sock': operation not supported
```

Use this command when running Testcontainers tests locally:

```bash
TESTCONTAINERS_RYUK_DISABLED=true \
TESTCONTAINERS_DOCKER_CLIENT_STRATEGY=org.testcontainers.dockerclient.EnvironmentAndSystemPropertyClientProviderStrategy \
DOCKER_HOST=unix:///Users/m9rcy/.docker/run/docker.sock \
TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=/var/run/docker.sock \
mvn test -q
```

References:

- Testcontainers Java Docker Engine 29 issue: https://github.com/testcontainers/testcontainers-java/issues/11235
- Docker forum note on Testcontainers 1.x and Docker 29: https://forums.docker.com/t/could-not-find-a-valid-docker-environment/151396/2

| Test Class | What it covers |
|---|---|
| `MessageFilterTest` | PTLFX filter + TVN/TCN/ACN actionable rules |
| `Base24XmlParserTest` | XML parsing, field mapping, error cases |
| `AbstractKafkaConsumerTest` | Generic map/dedupe/orchestrate/ack outcomes |
| `ProcessorRoutingOrchestratorTest` | Single/multi processor routing with a made-up consumer event |
| `TokeniseAndSaveTransactionProcessorTest` | Tokenise + save processor behavior |
| `FingerprintHasherTest` | Stable SHA/HMAC hashing |
| `JdbcDeduplicationServiceTest` | PostgreSQL dedupe, stale event handling, hash changes |
| `Base24KafkaConsumerTest` | Topic-specific consumer retry classification |
| `TokenisationAdapterTest` | HTTP adapter with MockRestServiceServer |
| `TransactionAdapterTest` | HTTP adapter with MockRestServiceServer |
| `StubPipelineSmokeTest` | Full pipeline wired with real stubs (no mocks) |
| `AbstractKafkaIntegrationTest` | Shared Testcontainers Kafka, MockServer, topic, publish, DLQ, and offset helpers |
| `Base24KafkaIntegrationTest` | Kafka + HTTP end-to-end behavior with Testcontainers |

---

## Running Locally (stub mode — no real services needed)

```bash
mvn spring-boot:run -Dspring-boot.run.profiles=stub
```

Stubs log every tokenisation and save event at DEBUG level so you can see
the full consumer processing flow.

To send a test message, use the Kafka console producer:

```bash
kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic base24-eps-realtime
```

Then paste a sample PTLFX message:

```xml
<Data>
    <MessageType>TVN</MessageType>
    <RecordType>PTLFX</RecordType>
    <TransactionId>TXN-20240101-001</TransactionId>
    <DigitalPan>4111111111111111</DigitalPan>
    <Amount>123.45</Amount>
    <CurrencyCode>NZD</CurrencyCode>
    <ResponseCode>00</ResponseCode>
    <Timestamp>2024-01-15T10:30:00</Timestamp>
</Data>
```

---

## Running Kafka Only And Debugging In IntelliJ IDEA

Start only Kafka:

```bash
docker compose -f docker-compose.kafka.yml up -d
```

Kafka is available to your host JVM at:

```text
localhost:9092
```

Create an IntelliJ run configuration for `Base24PipelineApplication` with:

```text
Active profile: stub
Environment:
  BASE24_KAFKA_BOOTSTRAP_SERVERS=localhost:9092
  BASE24_KAFKA_TOPIC=base24-eps-realtime
  BASE24_KAFKA_GROUP_ID=base24-consumer-group
```

The `stub` profile keeps tokenisation and transaction saving in-process, so you
can debug the consumer pipeline without the downstream HTTP services.

Stop Kafka when finished:

```bash
docker compose -f docker-compose.kafka.yml down
```

---

## Publishing Test Messages From A Folder

`FolderKafkaPublisher` reads UTF-8 files from a folder and publishes each file to
the configured Kafka topic. It publishes existing files once, and with `--watch`
it keeps running and publishes new or modified files.

Create a local input folder:

```bash
mkdir -p tmp/base24-inbox
```

Add a sample message:

```bash
cat > tmp/base24-inbox/tvn.xml <<'XML'
<Data>
    <MessageType>TVN</MessageType>
    <RecordType>PTLFX</RecordType>
    <TransactionId>TXN-LOCAL-001</TransactionId>
    <DigitalPan>4111111111111111</DigitalPan>
    <Amount>123.45</Amount>
    <CurrencyCode>NZD</CurrencyCode>
    <ResponseCode>00</ResponseCode>
    <Timestamp>2024-01-15T10:30:00</Timestamp>
</Data>
XML
```

Publish plaintext:

```bash
mvn -DskipTests compile exec:java \
  -Dexec.mainClass=com.commercial.cards.base24.tools.FolderKafkaPublisher \
  -Dexec.args="--folder tmp/base24-inbox --bootstrap-server localhost:9092 --topic base24-eps-realtime"
```

Or run the same main class directly in IntelliJ:

```text
Main class:
  com.commercial.cards.base24.tools.FolderKafkaPublisher
Program arguments:
  --folder tmp/base24-inbox --bootstrap-server localhost:9092 --topic base24-eps-realtime
```

For continuous publishing while you edit or add files:

```text
--folder tmp/base24-inbox --bootstrap-server localhost:9092 --topic base24-eps-realtime --watch
```

---

## Publishing And Consuming Encrypted Test Messages

The repo includes a small AES-GCM string serializer/deserializer for local
symmetric encryption testing:

```text
com.commercial.cards.base24.kafka.crypto.AesGcmStringSerializer
com.commercial.cards.base24.kafka.crypto.AesGcmStringDeserializer
```

The encrypted wire format is text with an `aes-gcm-v1:` prefix. The deserializer
passes plaintext through unchanged, so the app can consume both plaintext and
messages encrypted by this local serializer.

Use a 16, 24, or 32 byte key. For local testing, this 16 byte value is valid:

```bash
export KAFKA_SYMMETRIC_KEY=0123456789abcdef
```

In IntelliJ, run `Base24PipelineApplication` with the same `stub` profile and
Kafka environment values from above, plus:

```text
BASE24_KAFKA_VALUE_DESERIALIZER=com.commercial.cards.base24.kafka.crypto.AesGcmStringDeserializer
KAFKA_SYMMETRIC_KEY=0123456789abcdef
```

Publish encrypted files:

```bash
mvn -DskipTests compile exec:java \
  -Dexec.mainClass=com.commercial.cards.base24.tools.FolderKafkaPublisher \
  -Dexec.args="--folder tmp/base24-inbox --bootstrap-server localhost:9092 --topic base24-eps-realtime --secure --key 0123456789abcdef"
```

You can also use a base64 encoded AES key:

```bash
mvn -DskipTests compile exec:java \
  -Dexec.mainClass=com.commercial.cards.base24.tools.FolderKafkaPublisher \
  -Dexec.args="--folder tmp/base24-inbox --bootstrap-server localhost:9092 --topic base24-eps-realtime --secure --key-base64 MDEyMzQ1Njc4OWFiY2RlZg=="
```

If your real environment uses a different organisation-provided symmetric Kafka
serde, set the app deserializer to that class instead:

```text
BASE24_KAFKA_VALUE_DESERIALIZER=com.yourcompany.kafka.crypto.YourDeserializer
```

Then configure that serde using its required environment variables or JVM
properties.

---

## Running With Docker Compose

Build and start Kafka, PostgreSQL, and the pipeline in stub mode:

```bash
docker compose up --build
```

Send a test message from another terminal:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server kafka:9092 \
  --topic base24-eps-realtime
```

Then paste the sample XML above. The app runs with `SPRING_PROFILES_ACTIVE=stub`,
so tokenisation and transaction saving are handled by local stubs and no external
HTTP services are required. Database deduplication is enabled in this compose
file and uses the local `postgres` service.

Stop everything with:

```bash
docker compose down
```

---

## Running Against Real Services

Update `src/main/resources/application.yml`:

```yaml
base24:
  kafka:
    bootstrap-servers: your-kafka-broker:9092
    topic: base24-eps-realtime
    group-id: base24-consumer-group
  services:
    tokenisation-url: http://cards-tokenisation-service
    transaction-url:  http://transactions-service
  dedupe:
    enabled: true
    hmac-secret: your-secret
spring:
  datasource:
    url: jdbc:postgresql://your-postgres-host:5432/base24
    username: your-user
    password: your-password
```

Then run:

```bash
mvn spring-boot:run
```

---

## Project Structure

```
src/main/java/com/commercial/cards/base24/
├── Base24PipelineApplication.java
├── model/
│   ├── MessageType.java          ← TAR | TVN | TCN | ACN | UNKNOWN
│   ├── RecordType.java           ← PTLFX | OTHER
│   ├── Base24Message.java        ← domain model (pure data)
│   └── RtfData.java              ← JAXB binding (update from spec)
├── dto/
│   └── SaveTransactionRequest.java
├── exception/
│   ├── TokenisationException.java
│   └── TransactionSaveException.java
├── port/
│   ├── TokenisationPort.java     ← interface
│   └── TransactionPort.java      ← interface
├── parser/
│   └── Base24XmlParser.java
├── pipeline/
│   └── MessageFilter.java
├── mapper/
│   └── Base24EventMapper.java
├── orchestrator/
│   └── Base24TransactionOrchestrator.java
├── processor/
│   └── TokeniseAndSaveTransactionProcessor.java
├── orchestration/                  ← generic mapper/orchestrator/processor contracts
│   ├── EventMapper.java
│   ├── EventOrchestrator.java
│   ├── EventProcessor.java
│   ├── BaseProcessor.java
│   ├── ProcessorRoutingMode.java
│   └── ProcessorRoutingOrchestrator.java
├── dedupe/                         ← no-op + PostgreSQL-backed dedupe
│   ├── DeduplicationService.java
│   ├── NoOpDeduplicationService.java
│   ├── JdbcDeduplicationService.java
│   ├── EventFingerprint.java
│   ├── FingerprintHasher.java
│   └── Base24EventFingerprint.java
├── consumer/
│   ├── AbstractKafkaConsumer.java   ← generic consume/map/dedupe/orchestrate flow
│   ├── Base24KafkaConsumer.java
│   └── KafkaProcessingRetryException.java
├── adapter/                      ← @Profile("!stub") — real HTTP + CB + Retry
│   ├── TokenisationAdapter.java
│   └── TransactionAdapter.java
├── stub/                         ← @Profile("stub") — local dev only
│   ├── TokenisationStub.java
│   └── TransactionStub.java
└── config/
    ├── KafkaConfig.java
    ├── RestClientConfig.java
    ├── DeduplicationConfig.java
    └── DedupeDataSourceConfig.java
```

---

## Adding A New Kafka Consumer

The consumer architecture is intentionally split so new topics do not copy the
Base24 processing pipeline.

To add a new consumer:

1. Create a DTO/domain object for the new topic's payload.
2. Implement `EventMapper<I, D>` to map the raw Kafka value into that DTO. Return
   `Optional.empty()` for malformed or unsupported input that should be skipped.
3. Implement `DeduplicationService<D>` if the consumer needs domain-specific
   duplicate detection. For PostgreSQL-backed dedupe, provide an
   `EventFingerprint<D>` that exposes the domain, dedupe key, event time, and
   interesting fields used for hashing.
4. Implement one or more `EventProcessor<D>` classes. Extend `BaseProcessor<D>`
   when the processor needs a local `shouldProcess(dto)` filter.
5. Add an `EventOrchestrator<D>`. Prefer `ProcessorRoutingOrchestrator<D>` when
   routing can be expressed as one or more processors selected by
   `shouldProcess(dto)`.
6. Add a concrete Kafka consumer that extends `AbstractKafkaConsumer<I, D>`.
   The class should only bind `@KafkaListener` properties and classify retryable
   domain exceptions in `isRetriableException`.
7. Add configuration properties for the topic, group id, retry settings, DLQ
   topic, downstream URLs, and dedupe settings.
8. Add unit tests for mapper, dedupe fingerprint, processors, orchestrator, and
   concrete retry classification.
9. Add a Kafka integration test that extends `AbstractKafkaIntegrationTest`.
   Reuse `newScenario(...)`, `startApplication(...)`, `publish(...)`,
   `consumeOne(...)`, and `committedOffset(...)` instead of duplicating Kafka
   setup code.

The shared consumer flow is:

```text
Kafka record -> mapper -> optional orchestrator pre-filter -> dedupe isConsumable -> orchestrator -> markProcessed -> ack
```

`AbstractKafkaConsumer` also owns trace setup. It reads `trace-id` or `traceId`
from Kafka headers when present; otherwise it generates a UUID. After mapping,
a concrete consumer can override `traceId(dto)` to use a trace ID carried inside
the event DTO. The trace ID is stored in MDC under `trace-id`, included in logs,
and copied by the shared `RestClient` into outgoing HTTP requests as the
`trace-id` header.

Retry is exception-driven. Concrete consumers decide which exceptions are
retryable; retryable failures are wrapped in `KafkaProcessingRetryException` and
handled by Spring Kafka's `DefaultErrorHandler`. After retries are exhausted,
the handler publishes to the configured DLQ and commits the recovered offset.

---

## Deduplication

Deduplication is disabled by default:

```yaml
base24:
  dedupe:
    enabled: false
```

When enabled, the app uses PostgreSQL table `event_deduplication` keyed by:

```text
domain + dedupe_key
```

For Base24 transactions:

```text
domain = base24-transaction
dedupe_key = transactionId
```

The stored hash is computed from the domain-relevant fingerprint:

```text
messageType
recordType
transactionId
digitalPan
amount
currencyCode
responseCode
```

`timestamp` is not part of the hash. It is used only for event ordering:

```text
older timestamp than stored last_event_time -> skipped as stale
same hash with newer timestamp -> skipped and last_event_time is advanced
changed hash with newer timestamp -> processed
```

If `base24.dedupe.hmac-secret` is set, fingerprints are hashed with HMAC-SHA256.
Otherwise they use SHA-256.

For multi-instance deployments, producers should set the Kafka message key to
the dedupe key, usually `transactionId`, so all events for the same transaction
stay on the same Kafka partition.

---

## Resilience

Both adapters use Resilience4j with the following defaults (configure in `application.yml`):

| Setting | Value |
|---|---|
| Circuit breaker window | 10 calls |
| Open threshold | 50% failure rate |
| Open wait | 10 seconds |
| Retry attempts | 3 |
| Retry backoff | 500ms, exponential ×2 |

When the circuit is open or retries are exhausted, the adapter throws a domain
exception. The abstract Kafka consumer asks the concrete consumer whether that
exception is retryable; Base24 currently retries tokenisation and transaction
save failures. Retryable exceptions are wrapped in `KafkaProcessingRetryException`
and handled by Spring Kafka's `DefaultErrorHandler`.

Kafka retry defaults:

| Setting | Value |
|---|---|
| Retry interval | 1000ms |
| Retry attempts | 3 |

After retry exhaustion, the current handler logs the exhausted record and
publishes it to a DLQ. If `base24.kafka.dlq-topic` is blank, the default DLQ
topic is the source topic plus `.DLQ`.

---

## Updating XML Field Mappings

Once the full xEE-SE451 spec is confirmed, update element names in:

```
src/main/java/com/commercial/cards/base24/model/RtfData.java
```

Change the `@XmlElement(name = "...")` annotations to match the real field names.
No other files need to change.
