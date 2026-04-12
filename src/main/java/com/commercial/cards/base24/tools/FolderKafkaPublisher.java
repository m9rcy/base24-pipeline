package com.commercial.cards.base24.tools;

import com.commercial.cards.base24.kafka.crypto.AesGcmStringSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.time.Duration;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public final class FolderKafkaPublisher {

    private FolderKafkaPublisher() {
    }

    public static void main(String[] args) throws Exception {
        Options options = Options.parse(args);
        Properties props = producerProperties(options);

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            publishExistingFiles(options, producer);
            if (options.watch) {
                watchFolder(options, producer);
            }
        }
    }

    private static Properties producerProperties(Options options) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, options.bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                options.secure ? AesGcmStringSerializer.class.getName() : StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "base24-folder-publisher");

        if (options.keyBase64 != null) {
            props.put("base24.crypto.key-base64", options.keyBase64);
        }
        if (options.keyText != null) {
            props.put("base24.crypto.key", options.keyText);
        }
        return props;
    }

    private static void publishExistingFiles(Options options, KafkaProducer<String, String> producer)
            throws IOException, ExecutionException, InterruptedException {
        try (var files = Files.list(options.folder)) {
            for (Path file : files.filter(Files::isRegularFile).sorted().toList()) {
                publish(file, options, producer);
            }
        }
    }

    private static void watchFolder(Options options, KafkaProducer<String, String> producer)
            throws IOException, ExecutionException, InterruptedException {
        try (WatchService watchService = options.folder.getFileSystem().newWatchService()) {
            options.folder.register(watchService,
                    StandardWatchEventKinds.ENTRY_CREATE,
                    StandardWatchEventKinds.ENTRY_MODIFY);

            Set<Path> recentlyPublished = new HashSet<>();
            while (true) {
                WatchKey key = watchService.take();
                for (WatchEvent<?> event : key.pollEvents()) {
                    if (event.kind() == StandardWatchEventKinds.OVERFLOW) {
                        continue;
                    }
                    Path file = options.folder.resolve((Path) event.context());
                    if (Files.isRegularFile(file) && recentlyPublished.add(file)) {
                        Thread.sleep(Duration.ofMillis(250));
                        publish(file, options, producer);
                    }
                }
                recentlyPublished.clear();
                if (!key.reset()) {
                    throw new IllegalStateException("Folder watch key is no longer valid: " + options.folder);
                }
            }
        }
    }

    private static void publish(Path file, Options options, KafkaProducer<String, String> producer)
            throws IOException, ExecutionException, InterruptedException {
        String payload = Files.readString(file, StandardCharsets.UTF_8);
        String key = stripExtension(file.getFileName().toString());
        var metadata = producer.send(new ProducerRecord<>(options.topic, key, payload)).get();
        System.out.printf("Published %s to %s-%d offset %d (%s)%n",
                file, metadata.topic(), metadata.partition(), metadata.offset(),
                options.secure ? "encrypted" : "plaintext");
    }

    private static String stripExtension(String filename) {
        int index = filename.lastIndexOf('.');
        return index > 0 ? filename.substring(0, index) : filename;
    }

    private record Options(
            Path folder,
            String bootstrapServers,
            String topic,
            boolean secure,
            boolean watch,
            String keyBase64,
            String keyText
    ) {
        static Options parse(String[] args) {
            Path folder = null;
            String bootstrapServers = "localhost:9092";
            String topic = "base24-eps-realtime";
            boolean secure = false;
            boolean watch = false;
            String keyBase64 = null;
            String keyText = null;

            for (int i = 0; i < args.length; i++) {
                switch (args[i]) {
                    case "--folder" -> folder = Path.of(requireValue(args, ++i, "--folder"));
                    case "--bootstrap-server", "--bootstrap-servers" ->
                            bootstrapServers = requireValue(args, ++i, args[i - 1]);
                    case "--topic" -> topic = requireValue(args, ++i, "--topic");
                    case "--secure" -> secure = true;
                    case "--watch" -> watch = true;
                    case "--key-base64" -> keyBase64 = requireValue(args, ++i, "--key-base64");
                    case "--key" -> keyText = requireValue(args, ++i, "--key");
                    case "--help", "-h" -> {
                        printUsage();
                        System.exit(0);
                    }
                    default -> throw new IllegalArgumentException("Unknown argument: " + args[i]);
                }
            }

            if (folder == null) {
                throw new IllegalArgumentException("--folder is required\n\n" + usage());
            }
            if (!Files.isDirectory(folder)) {
                throw new IllegalArgumentException("--folder must be an existing directory: " + folder);
            }
            if (secure && keyBase64 == null && keyText == null
                    && System.getenv("KAFKA_SYMMETRIC_KEY_BASE64") == null
                    && System.getenv("KAFKA_SYMMETRIC_KEY") == null) {
                throw new IllegalArgumentException("--secure requires --key-base64, --key, "
                        + "KAFKA_SYMMETRIC_KEY_BASE64, or KAFKA_SYMMETRIC_KEY");
            }

            return new Options(folder, bootstrapServers, topic, secure, watch, keyBase64, keyText);
        }

        private static String requireValue(String[] args, int index, String name) {
            if (index >= args.length || args[index].startsWith("--")) {
                throw new IllegalArgumentException(name + " requires a value");
            }
            return args[index];
        }

        private static void printUsage() {
            System.out.println(usage());
        }

        private static String usage() {
            return """
                    Usage:
                      FolderKafkaPublisher --folder <path> [--bootstrap-server localhost:9092]
                          [--topic base24-eps-realtime] [--watch] [--secure]
                          [--key-base64 <base64-aes-key> | --key <16|24|32 byte text key>]
                    """;
        }
    }
}
