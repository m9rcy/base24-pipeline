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

## Running Tests (no Kafka or real services needed)

```bash
mvn test
```

All tests are pure unit tests — no Spring context, no Kafka, no HTTP calls.

| Test Class | What it covers |
|---|---|
| `MessageFilterTest` | PTLFX filter + TVN/TCN/ACN actionable rules |
| `Base24XmlParserTest` | XML parsing, field mapping, error cases |
| `Base24MessagePipelineTest` | All pipeline stage outcomes (skip/retry/success) |
| `Base24KafkaConsumerTest` | Ack/no-ack decisions per ProcessingResult |
| `TokenisationAdapterTest` | HTTP adapter with MockRestServiceServer |
| `TransactionAdapterTest` | HTTP adapter with MockRestServiceServer |
| `StubPipelineSmokeTest` | Full pipeline wired with real stubs (no mocks) |

---

## Running Locally (stub mode — no real services needed)

```bash
mvn spring-boot:run -Dspring-boot.run.profiles=stub
```

Stubs log every tokenisation and save event at DEBUG level so you can see
the full pipeline processing messages.

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

Build and start Kafka plus the pipeline in stub mode:

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
HTTP services are required.

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
│   ├── ProcessingResult.java     ← SUCCESS | SKIPPED | RETRY
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
│   ├── MessageFilter.java
│   └── Base24MessagePipeline.java
├── consumer/
│   └── Base24KafkaConsumer.java
├── adapter/                      ← @Profile("!stub") — real HTTP + CB + Retry
│   ├── TokenisationAdapter.java
│   └── TransactionAdapter.java
├── stub/                         ← @Profile("stub") — local dev only
│   ├── TokenisationStub.java
│   └── TransactionStub.java
└── config/
    ├── KafkaConfig.java
    └── RestClientConfig.java
```

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

When the circuit is open or retries are exhausted, the fallback re-throws so the
pipeline returns `RETRY` and Kafka does **not** commit the offset — the message
will be redelivered when the downstream service recovers.

---

## Updating XML Field Mappings

Once the full xEE-SE451 spec is confirmed, update element names in:

```
src/main/java/com/commercial/cards/base24/model/RtfData.java
```

Change the `@XmlElement(name = "...")` annotations to match the real field names.
No other files need to change.
