package com.commercial.cards.base24.consumer;

import com.commercial.cards.base24.model.ProcessingResult;
import com.commercial.cards.base24.pipeline.Base24MessagePipeline;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * Kafka infrastructure layer — thin by design.
 *
 * <p>Responsibilities:</p>
 * <ul>
 *   <li>Receive raw XML string from the Base24 EPS topic</li>
 *   <li>Delegate all business logic to {@link Base24MessagePipeline}</li>
 *   <li>Own the ack/no-ack decision based on {@link ProcessingResult}</li>
 * </ul>
 *
 * <p>This class has zero knowledge of XML structure, PTLFX, PANs, or message types.</p>
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class Base24KafkaConsumer {

    private final Base24MessagePipeline pipeline;

    @KafkaListener(
            topics                = "${base24.kafka.topic}",
            groupId               = "${base24.kafka.group-id}",
            containerFactory      = "base24KafkaListenerContainerFactory"
    )
    public void consume(ConsumerRecord<String, String> record, Acknowledgment ack) {
        log.debug("Received message [topic={} partition={} offset={}]",
                record.topic(), record.partition(), record.offset());

        ProcessingResult result = pipeline.process(record.value());

        switch (result) {
            case SUCCESS, SKIPPED -> {
                ack.acknowledge();
                log.debug("Acked [offset={} result={}]", record.offset(), result);
            }
            case RETRY -> log.warn(
                    "Not acking — will redeliver [topic={} partition={} offset={}]",
                    record.topic(), record.partition(), record.offset());
        }
    }
}
