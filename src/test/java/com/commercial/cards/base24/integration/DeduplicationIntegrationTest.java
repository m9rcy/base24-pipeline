package com.commercial.cards.base24.integration;

import com.sun.net.httpserver.HttpServer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.context.ConfigurableApplicationContext;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.net.InetSocketAddress;
import java.sql.Connection;
import java.sql.DriverManager;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * End-to-end deduplication test — parameterised over increasing transaction counts.
 *
 * Stubs tokenise and save with an in-process JDK HttpServer and AtomicInteger counters
 * rather than MockServer, eliminating request-log overhead and the socket-timeout issues
 * that MockServer exhibits under high request volumes.
 *
 * Per-transaction event sequence (offsets are ordered by round, not interleaved):
 *   round 1 — initial         amount=100.00 ts=T+5  → PROCESSED  (no existing row)
 *   round 2 — stale duplicate amount=100.00 ts=T+3  → SKIPPED    (T+3 < T+5, stale)
 *   round 3 — newer duplicate amount=100.00 ts=T+7  → SKIPPED    (same hash; advances time to T+7)
 *   round 4 — update          amount=200.00 ts=T+9  → PROCESSED  (new hash, T+9 > T+7)
 *   round 5 — stale update    amount=200.00 ts=T+8  → SKIPPED    (T+8 < T+9, stale)
 */
@Testcontainers(disabledWithoutDocker = true)
class DeduplicationIntegrationTest extends AbstractKafkaIntegrationTest {

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine");

    private static HttpServer stubServer;
    private static final AtomicInteger tokeniseCount = new AtomicInteger();
    private static final AtomicInteger saveCount     = new AtomicInteger();
    private static int stubServerPort;

    private static final String STUB_PATH = "/scenario/dedupe-bulk";
    private static final DateTimeFormatter TS_FORMAT = DateTimeFormatter.ISO_LOCAL_DATE_TIME;
    private static final LocalDateTime BASE_TIME = LocalDateTime.of(2026, 1, 1, 12, 0, 0);
    private static final int ROUNDS = 5;

    @BeforeAll
    static void startStubServer() throws Exception {
        stubServer = HttpServer.create(new InetSocketAddress(0), 0);

        stubServer.createContext(STUB_PATH + "/cards/tokenise", exchange -> {
            tokeniseCount.incrementAndGet();
            byte[] body = "{\"tokenisedPan\":\"TOK-IT-1111\"}".getBytes();
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, body.length);
            try (var out = exchange.getResponseBody()) { out.write(body); }
        });

        stubServer.createContext(STUB_PATH + "/transactions/save", exchange -> {
            saveCount.incrementAndGet();
            exchange.sendResponseHeaders(200, -1);
            exchange.close();
        });

        stubServer.setExecutor(Executors.newFixedThreadPool(4));
        stubServer.start();
        stubServerPort = ((InetSocketAddress) stubServer.getAddress()).getPort();
    }

    @AfterAll
    static void stopStubServer() {
        if (stubServer != null) stubServer.stop(0);
    }

    @ParameterizedTest(name = "txnCount={0}")
    @ValueSource(ints = {20, 100, 500, 1000, 2000})
    void shouldDeduplicateEvents(int txnCount) throws Exception {
        int totalEvents       = txnCount * ROUNDS;
        int expectedProcessed = txnCount * 2;
        Duration timeout = Duration.ofSeconds(Math.max(30L, (long) txnCount * 300 / 2000));

        String serviceBaseUrl = "http://localhost:" + stubServerPort + STUB_PATH;
        KafkaScenario kafka = newScenario("base24-dedupe-it", "base24-dedupe-it-group");

        tokeniseCount.set(0);
        saveCount.set(0);

        try (ConfigurableApplicationContext ignored = startDedupeApplication(kafka, serviceBaseUrl)) {
            truncateDedupeTable();
            publishInRounds(kafka.topic(), txnCount);

            await().atMost(timeout).untilAsserted(() -> {
                assertEquals(totalEvents, committedOffset(kafka.groupId(), kafka.topic()),
                        "all events must be consumed and acked");
                assertEquals(expectedProcessed, tokeniseCount.get(), "tokenise call count");
                assertEquals(expectedProcessed, saveCount.get(),     "save call count");
                assertEquals(txnCount, countDedupeRows(),
                        "one dedupe row per unique transaction ID");
            });
        }
    }

    // Publish all rounds sequentially; within each round all transactions are sent in parallel.
    // Waiting for each round's futures before the next ensures round-N offsets are all below round-(N+1).
    private static void publishInRounds(String topic, int txnCount) throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 131_072);

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (int round = 1; round <= ROUNDS; round++) {
                List<Future<RecordMetadata>> sent = new ArrayList<>(txnCount);
                for (int i = 1; i <= txnCount; i++) {
                    String txnId = "TXN-%06d".formatted(i);
                    sent.add(producer.send(new ProducerRecord<>(topic, txnId, xmlFor(round, txnId))));
                }
                for (Future<RecordMetadata> f : sent) {
                    f.get();
                }
            }
        }
    }

    private static String xmlFor(int round, String txnId) {
        return switch (round) {
            case 1 -> xml(txnId, "100.00", BASE_TIME.plusHours(5));
            case 2 -> xml(txnId, "100.00", BASE_TIME.plusHours(3));
            case 3 -> xml(txnId, "100.00", BASE_TIME.plusHours(7));
            case 4 -> xml(txnId, "200.00", BASE_TIME.plusHours(9));
            case 5 -> xml(txnId, "200.00", BASE_TIME.plusHours(8));
            default -> throw new IllegalArgumentException("unexpected round: " + round);
        };
    }

    private static String xml(String txnId, String amount, LocalDateTime ts) {
        return """
                <Data>
                    <MessageType>TVN</MessageType>
                    <RecordType>PTLFX</RecordType>
                    <TransactionId>%s</TransactionId>
                    <DigitalPan>4111111111111111</DigitalPan>
                    <Amount>%s</Amount>
                    <CurrencyCode>NZD</CurrencyCode>
                    <ResponseCode>00</ResponseCode>
                    <Timestamp>%s</Timestamp>
                </Data>
                """.formatted(txnId, amount, ts.format(TS_FORMAT));
    }

    private int countDedupeRows() throws Exception {
        try (Connection conn = DriverManager.getConnection(
                POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())) {
            var rs = conn.createStatement().executeQuery("select count(*) from event_deduplication");
            rs.next();
            return rs.getInt(1);
        }
    }

    private void truncateDedupeTable() throws Exception {
        try (Connection conn = DriverManager.getConnection(
                POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())) {
            conn.createStatement().execute("truncate table event_deduplication");
        }
    }

    private static ConfigurableApplicationContext startDedupeApplication(
            KafkaScenario kafka, String serviceBaseUrl) {
        return startApplication(
                "--server.port=0",
                "--base24.kafka.topic=" + kafka.topic(),
                "--base24.kafka.group-id=" + kafka.groupId(),
                "--base24.kafka.bootstrap-servers=" + kafkaBootstrapServers(),
                "--base24.kafka.max-poll-records=500",
                "--base24.kafka.retry.interval-ms=100",
                "--base24.kafka.retry.max-attempts=1",
                "--base24.kafka.dlq-topic=" + kafka.dlqTopic(),
                "--base24.services.tokenisation-url=" + serviceBaseUrl,
                "--base24.services.transaction-url=" + serviceBaseUrl,
                "--resilience4j.retry.instances.tokenisation.max-attempts=1",
                "--resilience4j.retry.instances.transaction.max-attempts=1",
                "--resilience4j.circuitbreaker.instances.tokenisation.failure-rate-threshold=100",
                "--resilience4j.circuitbreaker.instances.transaction.failure-rate-threshold=100",
                "--base24.dedupe.enabled=true",
                "--spring.datasource.url=" + POSTGRES.getJdbcUrl(),
                "--spring.datasource.username=" + POSTGRES.getUsername(),
                "--spring.datasource.password=" + POSTGRES.getPassword(),
                "--spring.flyway.enabled=true"
        );
    }
}
