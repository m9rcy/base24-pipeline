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

@Slf4j
@Configuration
public class KafkaConfig {

    @Value("${base24.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${base24.kafka.group-id}")
    private String groupId;

    @Value("${base24.kafka.key-deserializer:org.apache.kafka.common.serialization.StringDeserializer}")
    private String keyDeserializer;

    @Value("${base24.kafka.value-deserializer:org.apache.kafka.common.serialization.StringDeserializer}")
    private String valueDeserializer;

    @Value("${base24.crypto.key-base64:}")
    private String cryptoKeyBase64;

    @Value("${base24.crypto.key:}")
    private String cryptoKey;

    @Value("${base24.kafka.retry.interval-ms:1000}")
    private long retryIntervalMs;

    @Value("${base24.kafka.retry.max-attempts:3}")
    private long retryMaxAttempts;

    @Value("${base24.kafka.dlq-topic:}")
    private String dlqTopic;

    @Bean
    public ConsumerFactory<String, String> base24ConsumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // Manual ack: offset only commits after processing completes or the error handler recovers the record.
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        putIfNotBlank(props, "base24.crypto.key-base64", cryptoKeyBase64);
        putIfNotBlank(props, "base24.crypto.key", cryptoKey);
        return new DefaultKafkaConsumerFactory<>(props);
    }

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

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String>
    base24KafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(base24ConsumerFactory());
        // MANUAL_IMMEDIATE: ack() commits the offset synchronously right away
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.setCommonErrorHandler(base24KafkaErrorHandler(base24KafkaTemplate()));
        return factory;
    }

    @Bean
    public DefaultErrorHandler base24KafkaErrorHandler(KafkaTemplate<String, String> kafkaTemplate) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate,
                (record, exception) -> new TopicPartition(dlqTopic(record.topic()), record.partition()));

        DefaultErrorHandler errorHandler = new DefaultErrorHandler((record, exception) -> {
                log.error("Kafka processing retries exhausted [topic={} partition={} offset={} key={} reason={}]",
                        record.topic(),
                        record.partition(),
                        record.offset(),
                        record.key(),
                        exception.getMessage());
                recoverer.accept(record, exception);
        }, new FixedBackOff(retryIntervalMs, retryMaxAttempts));

        errorHandler.addRetryableExceptions(KafkaProcessingRetryException.class);
        errorHandler.setAckAfterHandle(true);
        errorHandler.setCommitRecovered(true);
        errorHandler.setRetryListeners((record, exception, deliveryAttempt) ->
                log.warn("Retrying Kafka message [topic={} partition={} offset={} key={} attempt={} reason={}]",
                        record.topic(),
                        record.partition(),
                        record.offset(),
                        record.key(),
                        deliveryAttempt,
                        exception.getMessage()));
        return errorHandler;
    }

    private String dlqTopic(String sourceTopic) {
        return dlqTopic == null || dlqTopic.isBlank() ? sourceTopic + ".DLQ" : dlqTopic;
    }

    private void putIfNotBlank(Map<String, Object> props, String key, String value) {
        if (value != null && !value.isBlank()) {
            props.put(key, value);
        }
    }
}
