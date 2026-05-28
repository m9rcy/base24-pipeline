package com.commercial.cards.base24.consumer;

import com.commercial.cards.base24.dedupe.DeduplicationService;
import com.commercial.cards.base24.orchestration.EventMapper;
import com.commercial.cards.base24.orchestration.EventOrchestrator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.MDC;
import org.springframework.kafka.support.Acknowledgment;

import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.UUID;

@Slf4j
@RequiredArgsConstructor
public abstract class AbstractKafkaConsumer<I, D> {

    public static final String TRACE_ID_MDC_KEY = "trace-id";

    private final EventMapper<I, D> mapper;
    private final DeduplicationService<D> deduplicationService;
    private final EventOrchestrator<D> orchestrator;

    public void consume(ConsumerRecord<String, I> record, Acknowledgment ack) {
        String traceId = traceId(record).orElseGet(() -> UUID.randomUUID().toString());
        try (MDC.MDCCloseable ignored = MDC.putCloseable(TRACE_ID_MDC_KEY, traceId)) {
            log.debug("Received message [topic={} partition={} offset={} key={}]",
                    record.topic(), record.partition(), record.offset(), record.key());
            process(record.value());
            ack.acknowledge();
            log.info("Consumed and acked Kafka message [topic={} partition={} offset={} key={}]",
                    record.topic(), record.partition(), record.offset(), record.key());
        } catch (RuntimeException e) {
            if (isRetriableException(e)) {
                throw new KafkaProcessingRetryException(
                        "Retriable processing failure for topic=%s partition=%d offset=%d key=%s"
                                .formatted(record.topic(), record.partition(), record.offset(), record.key()), e);
            }
            throw e;
        }
    }

    protected void process(I input) {
        Optional<D> mapped = mapper.map(input);
        if (mapped.isEmpty()) {
            log.warn("Message could not be mapped - skipping");
            return;
        }

        D dto = mapped.get();
        traceId(dto).ifPresent(traceId -> MDC.put(TRACE_ID_MDC_KEY, traceId));

        if (!orchestrator.shouldOrchestrate(dto)) {
            log.debug("Skipping non-orchestratable event [{}]", dto);
            return;
        }

        if (!deduplicationService.isConsumable(dto)) {
            return;
        }

        orchestrator.orchestrate(dto);
        deduplicationService.markProcessed(dto);
    }

    protected boolean isRetriableException(RuntimeException exception) {
        return false;
    }

    protected Optional<String> traceId(D dto) {
        return Optional.empty();
    }

    private Optional<String> traceId(ConsumerRecord<String, I> record) {
        return Optional.ofNullable(record.headers().lastHeader(TRACE_ID_MDC_KEY))
                .or(() -> Optional.ofNullable(record.headers().lastHeader("traceId")))
                .map(Header::value)
                .filter(value -> value != null && value.length > 0)
                .map(value -> new String(value, StandardCharsets.UTF_8))
                .map(String::trim)
                .filter(value -> !value.isBlank());
    }
}
