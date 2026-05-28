package com.commercial.cards.base24.integration;

import com.commercial.cards.base24.Base24PipelineApplication;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeAll;
import org.mockserver.client.MockServerClient;
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

import static org.awaitility.Awaitility.await;

@Testcontainers(disabledWithoutDocker = true)
abstract class AbstractKafkaIntegrationTest {

    @Container
    static final ConfluentKafkaContainer KAFKA = new ConfluentKafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));

    @Container
    static final GenericContainer<?> MOCK_SERVER = new GenericContainer<>(
            DockerImageName.parse("mockserver/mockserver:5.15.0"))
            .withExposedPorts(1080)
            .withStartupAttempts(3)
            .waitingFor(Wait.forListeningPort().withStartupTimeout(Duration.ofSeconds(90)));

    private static MockServerClient mockServerClient;

    @BeforeAll
    static void waitForMockServerApi() {
        await().atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofMillis(250))
                .until(AbstractKafkaIntegrationTest::mockServerStatusIsReady);
    }

    protected static KafkaScenario newScenario(String topicPrefix, String groupPrefix) throws Exception {
        String topic = topicPrefix + "-" + UUID.randomUUID();
        String dlqTopic = topic + ".DLQ";
        String groupId = groupPrefix + "-" + UUID.randomUUID();
        createTopic(topic);
        createTopic(dlqTopic);
        return new KafkaScenario(topic, dlqTopic, groupId);
    }

    protected static ConfigurableApplicationContext startApplication(String... properties) {
        return new SpringApplicationBuilder(Base24PipelineApplication.class).run(properties);
    }

    protected static String kafkaBootstrapServers() {
        return KAFKA.getBootstrapServers();
    }

    protected static String mockServerEndpoint() {
        return "http://" + MOCK_SERVER.getHost() + ":" + MOCK_SERVER.getMappedPort(1080);
    }

    protected static MockServerClient mockServerClient() {
        if (mockServerClient == null) {
            mockServerClient = new MockServerClient(MOCK_SERVER.getHost(), MOCK_SERVER.getMappedPort(1080));
        }
        return mockServerClient;
    }

    protected static void publish(String topic, String key, String payload) throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            producer.send(new ProducerRecord<>(topic, key, payload)).get();
        }
    }

    protected static long committedOffset(String groupId, String topic) throws Exception {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers());
        try (AdminClient adminClient = AdminClient.create(props)) {
            Map<TopicPartition, OffsetAndMetadata> offsets = adminClient
                    .listConsumerGroupOffsets(groupId)
                    .partitionsToOffsetAndMetadata()
                    .get();
            OffsetAndMetadata offset = offsets.get(new TopicPartition(topic, 0));
            return offset == null ? 0L : offset.offset();
        }
    }

    protected static ConsumerRecord<String, String> consumeOne(String topic) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            TopicPartition partition = new TopicPartition(topic, 0);
            consumer.assign(List.of(partition));
            consumer.seekToBeginning(List.of(partition));
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(2));
            if (records.isEmpty()) {
                throw new AssertionError("No Kafka record available for topic " + topic);
            }
            return records.iterator().next();
        }
    }

    protected static String scenarioName(String testName) {
        return testName.toLowerCase()
                .replaceAll("[^a-z0-9]+", "-")
                .replaceAll("(^-|-$)", "");
    }

    private static void createTopic(String topic) throws Exception {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers());
        try (AdminClient adminClient = AdminClient.create(props)) {
            adminClient.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get();
        }
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

    protected record KafkaScenario(String topic, String dlqTopic, String groupId) {
    }
}
