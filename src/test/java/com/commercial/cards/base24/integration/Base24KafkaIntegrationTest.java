package com.commercial.cards.base24.integration;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockserver.model.MediaType;
import org.mockserver.verify.VerificationTimes;
import org.springframework.context.ConfigurableApplicationContext;

import java.time.Duration;
import java.util.UUID;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

@Execution(ExecutionMode.SAME_THREAD)
class Base24KafkaIntegrationTest extends AbstractKafkaIntegrationTest {

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("providerMethod")
    void shouldHandleKafkaMessageScenario(String testName, Scenario scenario) throws Exception {
        KafkaScenario kafka = newScenario("base24-it", "base24-it-group");
        String scenarioPath = "/scenario/" + scenarioName(testName) + "-" + UUID.randomUUID();

        scenario.stubInitialApiState(scenarioPath);

        try (ConfigurableApplicationContext ignored =
                     startBase24Application(kafka, mockServerEndpoint() + scenarioPath)) {
            publish(kafka.topic(), scenario.transactionId(), messageXml(scenario.transactionId(), scenario.messageType()));

            if (scenario.commitsImmediately()) {
                await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
                    verifyTokenisationCalled(scenarioPath, VerificationTimes.exactly(1));
                    verifyTransactionCalled(scenarioPath, VerificationTimes.exactly(1));
                    assertEquals(1L, committedOffset(kafka.groupId(), kafka.topic()));
                });
                return;
            }

            await().atMost(Duration.ofSeconds(20))
                    .untilAsserted(() -> scenario.verifyFailureReached(scenarioPath));

            await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
                ConsumerRecord<String, String> dlqRecord = consumeOne(kafka.dlqTopic());
                assertEquals(scenario.transactionId(), dlqRecord.key());
                assertEquals(1L, committedOffset(kafka.groupId(), kafka.topic()));
            });
        }
    }

    private static ConfigurableApplicationContext startBase24Application(
            KafkaScenario kafka,
            String mockServerEndpoint
    ) {
        return startApplication(
                "--server.port=0",
                "--spring.profiles.active=test",
                "--base24.kafka.topic=" + kafka.topic(),
                "--base24.kafka.group-id=" + kafka.groupId(),
                "--base24.kafka.bootstrap-servers=" + kafkaBootstrapServers(),
                "--base24.kafka.retry.interval-ms=100",
                "--base24.kafka.retry.max-attempts=1",
                "--base24.kafka.dlq-topic=" + kafka.dlqTopic(),
                "--base24.services.tokenisation-url=" + mockServerEndpoint,
                "--base24.services.transaction-url=" + mockServerEndpoint,
                "--resilience4j.retry.instances.tokenisation.max-attempts=1",
                "--resilience4j.retry.instances.transaction.max-attempts=1"
        );
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

    private static void verifyTokenisationCalled(String scenarioPath, VerificationTimes times) {
        mockServerClient().verify(request().withMethod("POST").withPath(scenarioPath + "/cards/tokenise"), times);
    }

    private static void verifyTransactionCalled(String scenarioPath, VerificationTimes times) {
        mockServerClient().verify(request().withMethod("POST").withPath(scenarioPath + "/transactions/save"), times);
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
        mockServerClient().when(request()
                        .withMethod("POST")
                        .withPath(scenarioPath + "/cards/tokenise"))
                .respond(response()
                        .withStatusCode(200)
                        .withContentType(MediaType.APPLICATION_JSON)
                        .withBody("{\"tokenisedPan\":\"TOK-IT-1111\"}"));
    }

    private static void stubTokenisationUnavailable(String scenarioPath) {
        mockServerClient().when(request()
                        .withMethod("POST")
                        .withPath(scenarioPath + "/cards/tokenise"))
                .respond(response()
                        .withStatusCode(503)
                        .withBody("tokenisation service unavailable"));
    }

    private static void stubTransactionSuccess(String scenarioPath) {
        mockServerClient().when(request()
                        .withMethod("POST")
                        .withPath(scenarioPath + "/transactions/save"))
                .respond(response().withStatusCode(200));
    }

    private static void stubTransactionUnavailable(String scenarioPath) {
        mockServerClient().when(request()
                        .withMethod("POST")
                        .withPath(scenarioPath + "/transactions/save"))
                .respond(response()
                        .withStatusCode(503)
                        .withBody("transaction service unavailable"));
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
