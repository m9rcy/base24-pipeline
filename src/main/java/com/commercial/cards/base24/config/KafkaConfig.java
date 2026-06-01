package com.commercial.cards.base24.config;

import com.commercial.cards.base24.consumer.KafkaProcessingRetryException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka infrastructure configuration.
 *
 * <p>Each consumer gets its own {@link ConsumerFactory}, {@link DefaultErrorHandler}, and
 * {@link ConcurrentKafkaListenerContainerFactory}. The three package-private builder helpers
 * ({@link #buildConsumerFactory}, {@link #buildErrorHandler}, {@link #buildContainerFactory})
 * eliminate boilerplate when adding a second consumer — add {@code @Value} fields for the new
 * consumer's property prefix, then declare three {@code @Bean} methods that delegate to those
 * helpers.</p>
 *
 * <h3>Adding a new consumer</h3>
 * <pre>{@code
 * // 1. Add @Value fields for the new prefix (e.g. swift.kafka.*)
 * @Value("${swift.kafka.group-id}")         private String swiftGroupId;
 * @Value("${swift.kafka.max-poll-records:1}") private int    swiftMaxPollRecords;
 * @Value("${swift.kafka.retry.interval-ms:2000}") private long swiftRetryIntervalMs;
 * @Value("${swift.kafka.retry.max-attempts:1}")  private long swiftRetryMaxAttempts;
 * @Value("${swift.kafka.dlq-topic:}")        private String swiftDlqTopic;
 *
 * // 2. Three @Bean methods — boilerplate stays inside the helpers
 * @Bean public ConsumerFactory<String,String> swiftConsumerFactory() {
 *     return buildConsumerFactory(bootstrapServers, swiftGroupId,
 *         "org.apache.kafka.common.serialization.StringDeserializer",
 *         "org.apache.kafka.common.serialization.StringDeserializer",
 *         swiftMaxPollRecords, "", "");
 * }
 * @Bean public DefaultErrorHandler swiftKafkaErrorHandler(KafkaTemplate<String,String> t) {
 *     return buildErrorHandler(t, swiftRetryIntervalMs, swiftRetryMaxAttempts, swiftDlqTopic);
 * }
 * @Bean public ConcurrentKafkaListenerContainerFactory<String,String> swiftKafkaListenerContainerFactory() {
 *     return buildContainerFactory(swiftConsumerFactory(), swiftKafkaErrorHandler(base24KafkaTemplate()));
 * }
 *
 * // 3. In the consumer, reference the new factory by name:
 * // @KafkaListener(topics="${swift.kafka.topic}", containerFactory="swiftKafkaListenerContainerFactory")
 * }</pre>
 */
@Slf4j
@Configuration
public class KafkaConfig {

    // Shared

    @Value("${base24.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${base24.crypto.key-base64:}")
    private String cryptoKeyBase64;

    @Value("${base24.crypto.key:}")
    private String cryptoKey;

    // Base24 consumer

    @Value("${base24.kafka.group-id}")
    private String base24GroupId;

    @Value("${base24.kafka.key-deserializer:org.apache.kafka.common.serialization.StringDeserializer}")
    private String base24KeyDeserializer;

    @Value("${base24.kafka.value-deserializer:org.apache.kafka.common.serialization.StringDeserializer}")
    private String base24ValueDeserializer;

    @Value("${base24.kafka.max-poll-records:500}")
    private int base24MaxPollRecords;

    @Value("${base24.kafka.retry.interval-ms:1000}")
    private long base24RetryIntervalMs;

    @Value("${base24.kafka.retry.max-attempts:3}")
    private long base24RetryMaxAttempts;

    @Value("${base24.kafka.dlq-topic:}")
    private String base24DlqTopic;

    // Shared producer beans

    @Bean
    public ProducerFactory<String, String> base24ProducerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String, String> base24KafkaTemplate() {
        return new KafkaTemplate<>(base24ProducerFactory());
    }

    // Base24 consumer beans

    @Bean
    public ConsumerFactory<String, String> base24ConsumerFactory() {
        return buildConsumerFactory(
                bootstrapServers, base24GroupId,
                base24KeyDeserializer, base24ValueDeserializer,
                base24MaxPollRecords,
                cryptoKeyBase64, cryptoKey);
    }

    @Bean
    public DefaultErrorHandler base24KafkaErrorHandler(KafkaTemplate<String, String> kafkaTemplate) {
        return buildErrorHandler(kafkaTemplate, base24RetryIntervalMs, base24RetryMaxAttempts, base24DlqTopic);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> base24KafkaListenerContainerFactory() {
        return buildContainerFactory(base24ConsumerFactory(), base24KafkaErrorHandler(base24KafkaTemplate()));
    }

    // Reusable builder helpers (package-private for testability)

    /**
     * Builds a {@link ConsumerFactory} with manual-ack, auto-offset-reset=earliest, and the
     * supplied {@code maxPollRecords} limit. Use this as the single entry point for consumer
     * factory construction so all consumers share the same defaults.
     */
    ConsumerFactory<String, String> buildConsumerFactory(
            String bootstrapServers,
            String groupId,
            String keyDeserializer,
            String valueDeserializer,
            int maxPollRecords,
            String cryptoKeyBase64,
            String cryptoKey
    ) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // Manual ack: offset only commits after processing completes or the error handler recovers.
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        // Limit how many records Kafka fetches per poll — tune per consumer for memory/throughput.
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);
        putIfNotBlank(props, "base24.crypto.key-base64", cryptoKeyBase64);
        putIfNotBlank(props, "base24.crypto.key", cryptoKey);
        return new DefaultKafkaConsumerFactory<>(props);
    }

    /**
     * Builds a {@link DefaultErrorHandler} with fixed-backoff retry and dead-letter publishing.
     * DLQ topic resolves to {@code <sourceTopic>.DLQ} when {@code dlqTopicConfig} is blank.
     */
    DefaultErrorHandler buildErrorHandler(
            KafkaTemplate<String, String> kafkaTemplate,
            long retryIntervalMs,
            long retryMaxAttempts,
            String dlqTopicConfig
    ) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate,
                (record, exception) ->
                        new TopicPartition(resolveDlqTopic(record.topic(), dlqTopicConfig), record.partition()));

        DefaultErrorHandler errorHandler = new DefaultErrorHandler((record, exception) -> {
            log.error("Kafka processing retries exhausted [topic={} partition={} offset={} key={} reason={}]",
                    record.topic(), record.partition(), record.offset(),
                    record.key(), exception.getMessage());
            recoverer.accept(record, exception);
        }, new FixedBackOff(retryIntervalMs, retryMaxAttempts));

        // Only KafkaProcessingRetryException triggers retry; everything else (programming errors,
        // permanent data failures, unexpected exceptions) goes straight to the DLQ on first delivery.
        // BinaryExceptionClassifier resolves the most-specific match first, so the explicit
        // KafkaProcessingRetryException entry wins over the catch-all Exception.class entry.
        errorHandler.addRetryableExceptions(KafkaProcessingRetryException.class);
        errorHandler.addNotRetryableExceptions(Exception.class);
        errorHandler.setAckAfterHandle(true);
        errorHandler.setCommitRecovered(true);
        errorHandler.setRetryListeners((record, exception, deliveryAttempt) ->
                log.warn("Retrying Kafka message [topic={} partition={} offset={} key={} attempt={} reason={}]",
                        record.topic(), record.partition(), record.offset(),
                        record.key(), deliveryAttempt, exception.getMessage()));
        return errorHandler;
    }

    /**
     * Builds a {@link ConcurrentKafkaListenerContainerFactory} with {@code MANUAL_IMMEDIATE} ack
     * mode and the supplied error handler. All consumers use the same ack semantics.
     */
    ConcurrentKafkaListenerContainerFactory<String, String> buildContainerFactory(
            ConsumerFactory<String, String> consumerFactory,
            DefaultErrorHandler errorHandler
    ) {
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        // MANUAL_IMMEDIATE: ack() commits the offset synchronously right away.
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.setCommonErrorHandler(errorHandler);
        return factory;
    }

    /**
     * Returns the DLQ topic name. Falls back to {@code <sourceTopic>.DLQ} when the configured
     * value is absent so each consumer gets an automatically namespaced dead-letter topic.
     */
    static String resolveDlqTopic(String sourceTopic, String dlqTopicConfig) {
        return dlqTopicConfig == null || dlqTopicConfig.isBlank()
                ? sourceTopic + ".DLQ"
                : dlqTopicConfig;
    }

    private void putIfNotBlank(Map<String, Object> props, String key, String value) {
        if (value != null && !value.isBlank()) {
            props.put(key, value);
        }
    }
}
