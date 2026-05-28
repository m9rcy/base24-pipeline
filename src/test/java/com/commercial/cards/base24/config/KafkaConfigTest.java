package com.commercial.cards.base24.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;

class KafkaConfigTest {

    private KafkaConfig config;

    @BeforeEach
    void setUp() {
        config = new KafkaConfig();
    }

    // resolveDlqTopic

    @ParameterizedTest
    @CsvSource({"'',my-topic.DLQ", "'   ',my-topic.DLQ"})
    void shouldAppendDlqSuffixWhenConfigIsBlankOrWhitespace(String dlqConfig, String expected) {
        assertEquals(expected, KafkaConfig.resolveDlqTopic("my-topic", dlqConfig));
    }

    @Test
    void shouldAppendDlqSuffixWhenConfigIsNull() {
        assertEquals("my-topic.DLQ", KafkaConfig.resolveDlqTopic("my-topic", null));
    }

    @Test
    void shouldUseConfiguredDlqTopicWhenSet() {
        assertEquals("custom-dlq", KafkaConfig.resolveDlqTopic("my-topic", "custom-dlq"));
    }

    @Test
    void shouldIncludeSourceTopicInDefaultDlqName() {
        assertEquals("base24-eps-realtime.DLQ", KafkaConfig.resolveDlqTopic("base24-eps-realtime", ""));
    }

    // buildConsumerFactory

    @Test
    void shouldSetMaxPollRecordsOnConsumerFactory() {
        ConsumerFactory<String, String> factory = config.buildConsumerFactory(
                "localhost:9092", "test-group",
                "org.apache.kafka.common.serialization.StringDeserializer",
                "org.apache.kafka.common.serialization.StringDeserializer",
                50, "", "");

        assertEquals(50, factory.getConfigurationProperties().get(ConsumerConfig.MAX_POLL_RECORDS_CONFIG));
    }

    @Test
    void shouldDisableAutoCommitOnConsumerFactory() {
        ConsumerFactory<String, String> factory = config.buildConsumerFactory(
                "localhost:9092", "test-group",
                "org.apache.kafka.common.serialization.StringDeserializer",
                "org.apache.kafka.common.serialization.StringDeserializer",
                1, "", "");

        assertEquals(false, factory.getConfigurationProperties().get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG));
    }

    @Test
    void shouldSetEarliestOffsetResetOnConsumerFactory() {
        ConsumerFactory<String, String> factory = config.buildConsumerFactory(
                "localhost:9092", "test-group",
                "org.apache.kafka.common.serialization.StringDeserializer",
                "org.apache.kafka.common.serialization.StringDeserializer",
                1, "", "");

        assertEquals("earliest", factory.getConfigurationProperties().get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG));
    }

    @Test
    void shouldHonourDifferentPollRecordsPerConsumer() {
        ConsumerFactory<String, String> topic1Factory = config.buildConsumerFactory(
                "localhost:9092", "group1",
                "org.apache.kafka.common.serialization.StringDeserializer",
                "org.apache.kafka.common.serialization.StringDeserializer",
                50, "", "");

        ConsumerFactory<String, String> topic2Factory = config.buildConsumerFactory(
                "localhost:9092", "group2",
                "org.apache.kafka.common.serialization.StringDeserializer",
                "org.apache.kafka.common.serialization.StringDeserializer",
                1, "", "");

        assertEquals(50, topic1Factory.getConfigurationProperties().get(ConsumerConfig.MAX_POLL_RECORDS_CONFIG));
        assertEquals(1, topic2Factory.getConfigurationProperties().get(ConsumerConfig.MAX_POLL_RECORDS_CONFIG));
    }

    // buildContainerFactory

    @Test
    void shouldSetManualImmediateAckModeOnContainerFactory() {
        ConsumerFactory<String, String> consumerFactory = config.buildConsumerFactory(
                "localhost:9092", "test-group",
                "org.apache.kafka.common.serialization.StringDeserializer",
                "org.apache.kafka.common.serialization.StringDeserializer",
                1, "", "");

        @SuppressWarnings("unchecked")
        KafkaTemplate<String, String> template = mock(KafkaTemplate.class);
        DefaultErrorHandler errorHandler = config.buildErrorHandler(template, 100L, 1L, "");

        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                config.buildContainerFactory(consumerFactory, errorHandler);

        assertEquals(ContainerProperties.AckMode.MANUAL_IMMEDIATE,
                factory.getContainerProperties().getAckMode());
    }

    // buildErrorHandler

    @Test
    void shouldBuildErrorHandlerWithoutThrowingForAnyRetryConfig() {
        @SuppressWarnings("unchecked")
        KafkaTemplate<String, String> template = mock(KafkaTemplate.class);

        DefaultErrorHandler handler1 = config.buildErrorHandler(template, 1000L, 3L, "");
        DefaultErrorHandler handler2 = config.buildErrorHandler(template, 2000L, 1L, "my-dlq");

        assertNotNull(handler1);
        assertNotNull(handler2);
    }

    @Test
    void shouldBuildIndependentErrorHandlersForTwoConsumers() {
        @SuppressWarnings("unchecked")
        KafkaTemplate<String, String> template = mock(KafkaTemplate.class);

        DefaultErrorHandler base24Handler = config.buildErrorHandler(template, 1000L, 3L, "");
        DefaultErrorHandler swiftHandler  = config.buildErrorHandler(template, 2000L, 1L, "swift.DLQ");

        assertNotNull(base24Handler);
        assertNotNull(swiftHandler);
        // Each is its own instance — independent retry policies per consumer.
        assertEquals(false, base24Handler == swiftHandler);
    }

    @Test
    void shouldRouteFailedRecordToDlqViaResolvedTopic() {
        // Verifies the DLQ topic resolution logic used inside the recoverer lambda
        // is correct for both the auto-suffix and the explicit-topic paths.
        assertEquals("base24-eps-realtime.DLQ",
                KafkaConfig.resolveDlqTopic("base24-eps-realtime", null));
        assertEquals("swift-messages.DLQ",
                KafkaConfig.resolveDlqTopic("swift-messages", ""));
        assertEquals("override-dlq",
                KafkaConfig.resolveDlqTopic("any-topic", "override-dlq"));
    }
}
