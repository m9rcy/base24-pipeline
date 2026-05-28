package com.commercial.cards.base24.consumer;

import com.commercial.cards.base24.dedupe.DeduplicationService;
import com.commercial.cards.base24.exception.TokenisationException;
import com.commercial.cards.base24.exception.TransactionSaveException;
import com.commercial.cards.base24.model.Base24Message;
import com.commercial.cards.base24.orchestration.EventMapper;
import com.commercial.cards.base24.orchestration.EventOrchestrator;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * Kafka infrastructure layer — thin by design.
 *
 * <p>Responsibilities:</p>
 * <ul>
 *   <li>Receive raw XML string from the Base24 EPS topic</li>
 *   <li>Delegate generic map/dedupe/orchestrate behavior to {@link AbstractKafkaConsumer}</li>
 *   <li>Classify domain exceptions that should be retried by Kafka infrastructure</li>
 * </ul>
 *
 * <p>This class has zero knowledge of XML structure, PTLFX, PANs, or message types.</p>
 */
@Component
public class Base24KafkaConsumer extends AbstractKafkaConsumer<String, Base24Message> {

    public Base24KafkaConsumer(
            EventMapper<String, Base24Message> mapper,
            @Qualifier("base24DeduplicationService") DeduplicationService<Base24Message> deduplicationService,
            EventOrchestrator<Base24Message> orchestrator
    ) {
        super(mapper, deduplicationService, orchestrator);
    }

    @KafkaListener(
            topics                = "${base24.kafka.topic}",
            groupId               = "${base24.kafka.group-id}",
            containerFactory      = "base24KafkaListenerContainerFactory"
    )
    @Override
    public void consume(ConsumerRecord<String, String> record, Acknowledgment ack) {
        super.consume(record, ack);
    }

    @Override
    protected boolean isRetriableException(RuntimeException exception) {
        return exception instanceof TokenisationException
                || exception instanceof TransactionSaveException;
    }
}
