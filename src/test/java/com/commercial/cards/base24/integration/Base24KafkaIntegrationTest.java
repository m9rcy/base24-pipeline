package com.commercial.cards.base24.integration;

import com.commercial.cards.base24.Base24PipelineApplication;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.mockserver.client.MockServerClient;
import org.mockserver.model.MediaType;
import org.mockserver.verify.VerificationTimes;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.net.HttpURLConnection;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

@Testcontainers(disabledWithoutDocker = true)
@Execution(ExecutionMode.SAME_THREAD)
class Base24KafkaIntegrationTest {

    @Container
    static final ConfluentKafkaContainer KAFKA = new ConfluentKafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));

    @Container
    static final GenericContainer<?> MOCK_SERVER = new GenericContainer<>(
            DockerImageName.parse("mockserver/mockserver:5.15.0"))
            .withExposedPorts(1080)
            .withStartupAttempts(3)
            .waitingFor(Wait.forListeningPort().withStartupTimeout(Duration.ofSeconds(90)));

    static MockServerClient mockServerClient;

    @BeforeAll
    static void waitForMockServerApi() {
        await().atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofMillis(250))
                .until(Base24KafkaIntegrationTest::mockServerStatusIsReady);
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("providerMethod")
    void shouldHandleKafkaMessageScenario(String testName, Scenario scenario) throws Exception {
        String topic = "base24-it-" + UUID.randomUUID();
        String groupId = "base24-it-group-" + UUID.randomUUID();
        String scenarioPath = "/scenario/" + scenarioName(testName) + "-" + UUID.randomUUID();

        mockServerClient = mockServerClient();
        scenario.stubInitialApiState(scenarioPath);
        createTopic(topic);

        try (ConfigurableApplicationContext ignored =
                     startApplication(topic, groupId, mockServerEndpoint() + scenarioPath)) {
            publish(topic, messageXml(scenario.transactionId(), scenario.messageType()));

            if (scenario.commitsImmediately()) {
                await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
                    verifyTokenisationCalled(scenarioPath, VerificationTimes.exactly(1));
                    verifyTransactionCalled(scenarioPath, VerificationTimes.exactly(1));
                    assertEquals(1L, committedOffset(groupId, topic));
                });
                return;
            }

            await().atMost(Duration.ofSeconds(20))
                    .untilAsserted(() -> scenario.verifyFailureReached(scenarioPath));

            await().during(Duration.ofSeconds(3))
                    .atMost(Duration.ofSeconds(5))
                    .untilAsserted(() -> assertEquals(0L, committedOffset(groupId, topic)));
        }
    }

    private static ConfigurableApplicationContext startApplication(String topic, String groupId, String mockServerEndpoint) {
        return new SpringApplicationBuilder(Base24PipelineApplication.class)
                .run(
                        "--server.port=0",
                        "--spring.profiles.active=test",
                        "--base24.kafka.topic=" + topic,
                        "--base24.kafka.group-id=" + groupId,
                        "--base24.kafka.bootstrap-servers=" + KAFKA.getBootstrapServers(),
                        "--base24.services.tokenisation-url=" + mockServerEndpoint,
                        "--base24.services.transaction-url=" + mockServerEndpoint,
                        "--resilience4j.retry.instances.tokenisation.max-attempts=1",
                        "--resilience4j.retry.instances.transaction.max-attempts=1"
                );
    }

    private static String mockServerEndpoint() {
        return "http://" + MOCK_SERVER.getHost() + ":" + MOCK_SERVER.getMappedPort(1080);
    }

    private static MockServerClient mockServerClient() {
        if (mockServerClient == null) {
            mockServerClient = new MockServerClient(MOCK_SERVER.getHost(), MOCK_SERVER.getMappedPort(1080));
        }
        return mockServerClient;
    }

    private static boolean mockServerStatusIsReady() {
        try {
            HttpURLConnection connection = (HttpURLConnection) URI.create(mockServerEndpoint() + "/status")
                    .toURL()
                    .openConnection();
            connection.setRequestMethod("PUT");
            connection.setConnectTimeout(500);
            connection.setReadTimeout(500);
            return connection.getResponseCode() == 200;
        } catch (Exception ignored) {
            return false;
        }
    }

    static Stream<Arguments> providerMethod() {
        return Stream.of(
                Arguments.of("happy path commits TVN message",
                        Scenario.success("TXN-HAPPY-TVN-001", "TVN")),
                Arguments.of("happy path commits TCN message",
                        Scenario.success("TXN-HAPPY-TCN-001", "TCN")),
                Arguments.of("transaction API unavailable does not commit Kafka offset",
                        Scenario.transactionUnavailable("TXN-FAIL-TXN-API-001", "TVN")),
                Arguments.of("tokenisation API unavailable does not commit Kafka offset",
                        Scenario.tokenisationUnavailable("TXN-FAIL-TOKEN-API-001", "TVN"))
        );
    }

    private static String scenarioName(String testName) {
        return testName.toLowerCase()
                .replaceAll("[^a-z0-9]+", "-")
                .replaceAll("(^-|-$)", "");
    }

    private static void verifyTokenisationCalled(String scenarioPath, VerificationTimes times) {
        mockServerClient.verify(request().withMethod("POST").withPath(scenarioPath + "/cards/tokenise"), times);
    }

    private static void verifyTransactionCalled(String scenarioPath, VerificationTimes times) {
        mockServerClient.verify(request().withMethod("POST").withPath(scenarioPath + "/transactions/save"), times);
    }

    private record Scenario(
            String transactionId,
            String messageType,
            FailurePoint failurePoint
    ) {

        static Scenario success(String transactionId, String messageType) {
            return new Scenario(transactionId, messageType, null);
        }

        static Scenario transactionUnavailable(String transactionId, String messageType) {
            return new Scenario(transactionId, messageType, FailurePoint.TRANSACTION);
        }

        static Scenario tokenisationUnavailable(String transactionId, String messageType) {
            return new Scenario(transactionId, messageType, FailurePoint.TOKENISATION);
        }

        boolean commitsImmediately() {
            return failurePoint == null;
        }

        void stubInitialApiState(String scenarioPath) {
            if (failurePoint == FailurePoint.TOKENISATION) {
                stubTokenisationUnavailable(scenarioPath);
                stubTransactionSuccess(scenarioPath);
                return;
            }

            stubTokenisationSuccess(scenarioPath);
            if (failurePoint == FailurePoint.TRANSACTION) {
                stubTransactionUnavailable(scenarioPath);
            } else {
                stubTransactionSuccess(scenarioPath);
            }
        }

        void verifyFailureReached(String scenarioPath) {
            if (failurePoint == FailurePoint.TOKENISATION) {
                verifyTokenisationCalled(scenarioPath, VerificationTimes.atLeast(1));
                verifyTransactionCalled(scenarioPath, VerificationTimes.exactly(0));
            } else {
                verifyTransactionCalled(scenarioPath, VerificationTimes.atLeast(1));
            }
        }
    }

    private enum FailurePoint {
        TOKENISATION,
        TRANSACTION
    }

    private static void stubTokenisationSuccess(String scenarioPath) {
        mockServerClient.when(request()
                        .withMethod("POST")
                        .withPath(scenarioPath + "/cards/tokenise"))
                .respond(response()
                        .withStatusCode(200)
                        .withContentType(MediaType.APPLICATION_JSON)
                        .withBody("{\"tokenisedPan\":\"TOK-IT-1111\"}"));
    }

    private static void stubTokenisationUnavailable(String scenarioPath) {
        mockServerClient.when(request()
                        .withMethod("POST")
                        .withPath(scenarioPath + "/cards/tokenise"))
                .respond(response()
                        .withStatusCode(503)
                        .withBody("tokenisation service unavailable"));
    }

    private static void stubTransactionSuccess(String scenarioPath) {
        mockServerClient.when(request()
                        .withMethod("POST")
                        .withPath(scenarioPath + "/transactions/save"))
                .respond(response().withStatusCode(200));
    }

    private static void stubTransactionUnavailable(String scenarioPath) {
        mockServerClient.when(request()
                        .withMethod("POST")
                        .withPath(scenarioPath + "/transactions/save"))
                .respond(response()
                        .withStatusCode(503)
                        .withBody("transaction service unavailable"));
    }

    private static void createTopic(String topic) throws Exception {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        try (AdminClient adminClient = AdminClient.create(props)) {
            adminClient.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get();
        }
    }

    private static void publish(String topic, String payload) throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            producer.send(new ProducerRecord<>(topic, null, payload)).get();
        }
    }

    private static long committedOffset(String groupId, String topic) throws Exception {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        try (AdminClient adminClient = AdminClient.create(props)) {
            Map<TopicPartition, OffsetAndMetadata> offsets = adminClient
                    .listConsumerGroupOffsets(groupId)
                    .partitionsToOffsetAndMetadata()
                    .get();
            OffsetAndMetadata offset = offsets.get(new TopicPartition(topic, 0));
            return offset == null ? 0L : offset.offset();
        }
    }

    private static String messageXml(String transactionId, String messageType) {
        return """
                <Data>
                    <MessageType>%s</MessageType>
                    <RecordType>PTLFX</RecordType>
                    <TransactionId>%s</TransactionId>
                    <DigitalPan>4111111111111111</DigitalPan>
                    <Amount>123.45</Amount>
                    <CurrencyCode>NZD</CurrencyCode>
                    <ResponseCode>00</ResponseCode>
                    <Timestamp>2024-01-15T10:30:00</Timestamp>
                </Data>
                """.formatted(messageType, transactionId);
    }
}
