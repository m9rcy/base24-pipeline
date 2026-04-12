package com.commercial.cards.base24.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;

import java.util.HashMap;
import java.util.Map;

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

    @Bean
    public ConsumerFactory<String, String> base24ConsumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // Manual ack — offset only committed after pipeline confirms SUCCESS or SKIPPED
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        putIfNotBlank(props, "base24.crypto.key-base64", cryptoKeyBase64);
        putIfNotBlank(props, "base24.crypto.key", cryptoKey);
        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String>
    base24KafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(base24ConsumerFactory());
        // MANUAL_IMMEDIATE: ack() commits the offset synchronously right away
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        return factory;
    }

    private void putIfNotBlank(Map<String, Object> props, String key, String value) {
        if (value != null && !value.isBlank()) {
            props.put(key, value);
        }
    }
}
