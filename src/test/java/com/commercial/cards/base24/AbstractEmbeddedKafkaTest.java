package com.commercial.cards.base24;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.opentest4j.AssertionFailedError;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.ActiveProfiles;

import java.io.IOException;
import java.net.ServerSocket;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * A generic EmbeddedKafkaTest for testing with built-in Spring Embedded Kafka
 */
@ActiveProfiles("test")
@EmbeddedKafka(partitions = 1, topics = {"test-topic"}, brokerProperties = {"listeners=${spring.kafka.bootstrap-servers}"})
@Slf4j
public abstract class AbstractEmbeddedKafkaTest {

    private static final Map<String, Function<Integer, String>> SYSTEM_PROPS = Map.of(
            "spring.kafka.bootstrap-servers", p -> String.format("PLAINTEXT://localhost:%d", p),
            "KAFKA_BOOTSTRAP_SERVERS", p -> String.format("localhost:%d", p),
            "KAFKA_SCRAM_USERNAME", p -> "",
            "KAFKA_SCRAM_PASSWORD", p -> "",
            "KAFKA_SCRAM_MECHANISM", p -> "PLAIN",
            "KAFKA_SECURITY_PROTOCOL", p-> "PLAINTEXT");

    @Autowired
    @Getter(AccessLevel.PROTECTED)
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @BeforeAll
    protected static void initEmbeddedKafkaBootstrapServer() {
        int availablePort = findAvailablePort();
        log.info("Embedded Kafka will use port: {}", availablePort);
        SYSTEM_PROPS.forEach((k, v) -> {
            String value = v.apply(availablePort);
            System.setProperty(k, value);
            log.info("Set system property: {}={}", k, value);
        });

    }

    @AfterAll
    static void cleanupEmbeddedKafkaBootstrapServer() {
        SYSTEM_PROPS.keySet().forEach(System::clearProperty);
    }

    protected static <V> V pollFor(Supplier<V> supplier, Predicate<V> predicate, Duration timeLimit) {
        Instant failTime = Instant.now().plus(timeLimit);
        log.debug("Started polling; timeout is {}", failTime);
        V value = null;
        while(Instant.now().isBefore(failTime)) {
            value = supplier.get();
            log.trace("Polled value: {}", value);
            if (predicate.test(value)) {
                log.debug("Polling successful; value: {}", value);
                return value;
            }
            try {
                TimeUnit.MILLISECONDS.sleep(200);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionFailedError("Polling interrupted", e);
            }
        }
        log.warn ("Didn't meet condition within time limit {}; last value: {}", timeLimit, value);
        throw new AssertionFailedError("Condition not met within time limit: " + timeLimit);
    }

    protected void configuredKafkaTopics(String... topics) {
        configureKafkaTopics(Arrays.asList(topics));
    }

    protected void configureKafkaTopics(Collection<String> topics) {
        Set<String> additionalTopics = new TreeSet<>(topics);
        additionalTopics.removeAll(embeddedKafkaBroker.getTopics());

        if (!additionalTopics.isEmpty()) {
            log.info("Existing Kafka topics: {}", new TreeSet<>(embeddedKafkaBroker.getTopics()));
            log.info("Creating additional Kafka topics: {}", additionalTopics);

            Map<String, Exception> results = embeddedKafkaBroker.addTopicsWithResults(additionalTopics.toArray(new String[0]));


            Set<String> unExpectedFailures = new TreeSet<>();
            for (Map.Entry<String, Exception> entry : results.entrySet()) {
                if (entry.getValue() != null) {
                    TopicExistsException topicExistsException = ExceptionUtils.throwableOfType(entry.getValue(), TopicExistsException.class);
                    if (topicExistsException != null) {
                        log.warn("Topic already exists: {}", entry.getKey());
                    } else {
                        unExpectedFailures.add(entry.getKey());
                        log.error("Failed to create topic: {}", entry.getKey(), entry.getValue());
                    }
                }
            }

            if (!unExpectedFailures.isEmpty()) {
                throw new RuntimeException(String.format("Failed to create topics: %s",unExpectedFailures));
            }
        }
    }

    protected <D extends Deserializer<V>, V> Consumer<String, V> createConsumer(String topic, Class<D> deserializerClass) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, deserializerClass);

        return createConsumer(topic, props);
    }

    protected <V> Consumer<String, V> createConsumer(String topic, Map<String, Object> consumerProps) {
        Map<String, Object> props = KafkaTestUtils.consumerProps(
                String.format("%s-%s", getClass().getName(), UUID.randomUUID()), "false", embeddedKafkaBroker);
        props.putAll(consumerProps);

        DefaultKafkaConsumerFactory<String, V> consumerFactory = new DefaultKafkaConsumerFactory<>(props);
        Consumer<String, V> consumer = consumerFactory.createConsumer();

        embeddedKafkaBroker.consumeFromAnEmbeddedTopic(consumer, true, topic);
        awaitSeekToEnd(consumer, topic);
        return consumer;
    }

    protected void awaitSeekToEnd(Consumer<String, ?> consumer, String topic) {
          final List<TopicPartition> topicPartitions = consumer.partitionsFor(topic).stream()
                .map(info -> new TopicPartition(info.topic(), info.partition()))
                .toList();

        // Requesting the position of each partition will force lazy seekToEnd() operation to complete
        topicPartitions.forEach(consumer::position);
    }

    private static int findAvailablePort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            socket.setReuseAddress(true);
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new IllegalStateException("No available ports", e);
        }
    }

}
