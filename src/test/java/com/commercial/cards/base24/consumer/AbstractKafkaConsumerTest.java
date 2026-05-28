package com.commercial.cards.base24.consumer;

import com.commercial.cards.base24.dedupe.DeduplicationService;
import com.commercial.cards.base24.model.Base24Message;
import com.commercial.cards.base24.model.MessageType;
import com.commercial.cards.base24.model.RecordType;
import com.commercial.cards.base24.orchestration.EventMapper;
import com.commercial.cards.base24.orchestration.EventOrchestrator;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.slf4j.MDC;
import org.springframework.kafka.support.Acknowledgment;

import java.nio.charset.StandardCharsets;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class AbstractKafkaConsumerTest {

    private EventMapper<String, Base24Message> mapper;
    private DeduplicationService<Base24Message> deduplicationService;
    private EventOrchestrator<Base24Message> orchestrator;
    private Acknowledgment ack;
    private TestKafkaConsumer consumer;

    @BeforeEach
    void setUp() {
        mapper = mock(EventMapper.class);
        deduplicationService = mock(DeduplicationService.class);
        orchestrator = mock(EventOrchestrator.class);
        ack = mock(Acknowledgment.class);

        consumer = new TestKafkaConsumer(mapper, deduplicationService, orchestrator);
    }

    @Test
    void shouldAckWhenMessageCannotBeMapped() {
        when(mapper.map(any())).thenReturn(Optional.empty());

        consumer.consume(aRecord("<bad-xml/>"), ack);

        verify(ack).acknowledge();
        verifyNoInteractions(deduplicationService, orchestrator);
    }

    @Test
    void shouldAckWhenEventShouldNotBeOrchestrated() {
        Base24Message msg = aMessage();
        when(mapper.map(any())).thenReturn(Optional.of(msg));
        when(orchestrator.shouldOrchestrate(msg)).thenReturn(false);

        consumer.consume(aRecord("<xml/>"), ack);

        verify(ack).acknowledge();
        verifyNoInteractions(deduplicationService);
    }

    @Test
    void shouldAckWhenDedupeSkipsEvent() {
        Base24Message msg = setupPassingPreDedupe();
        when(deduplicationService.isConsumable(msg)).thenReturn(false);

        consumer.consume(aRecord("<xml/>"), ack);

        verify(ack).acknowledge();
        verify(orchestrator, never()).orchestrate(any());
    }

    @Test
    void shouldThrowRetryExceptionWhenDedupeFailsWithRetriableException() {
        Base24Message msg = setupPassingPreDedupe();
        doThrow(new TestRetriableException("db unavailable")).when(deduplicationService).isConsumable(msg);

        assertThrows(KafkaProcessingRetryException.class, () -> consumer.consume(aRecord("<xml/>"), ack));

        verify(ack, never()).acknowledge();
        verify(orchestrator, never()).orchestrate(any());
    }

    @Test
    void shouldThrowRetryExceptionWhenOrchestratorThrowsRetriableException() {
        Base24Message msg = setupPassingDedupe();
        doThrow(new TestRetriableException("downstream unavailable")).when(orchestrator).orchestrate(msg);

        assertThrows(KafkaProcessingRetryException.class, () -> consumer.consume(aRecord("<xml/>"), ack));

        verify(ack, never()).acknowledge();
        verify(deduplicationService, never()).markProcessed(any());
    }

    @Test
    void shouldPropagateNonRetriableExceptionWithoutAck() {
        Base24Message msg = setupPassingDedupe();
        doThrow(new IllegalArgumentException("configuration bug")).when(orchestrator).orchestrate(msg);

        assertThrows(IllegalArgumentException.class, () -> consumer.consume(aRecord("<xml/>"), ack));

        verify(ack, never()).acknowledge();
    }

    @Test
    void shouldAckSuccessAndMarkProcessedWhenAllStagesPass() {
        Base24Message msg = setupPassingDedupe();

        consumer.consume(aRecord("<xml/>"), ack);

        verify(ack).acknowledge();
        verify(deduplicationService).markProcessed(msg);
    }

    @Test
    void shouldGenerateTraceIdWhenRecordHasNoTraceId() {
        Base24Message msg = setupPassingDedupe();
        doAnswer(invocation -> {
            assertNotNull(MDC.get(AbstractKafkaConsumer.TRACE_ID_MDC_KEY));
            return null;
        }).when(orchestrator).orchestrate(msg);

        consumer.consume(aRecord("<xml/>"), ack);

        assertNull(MDC.get(AbstractKafkaConsumer.TRACE_ID_MDC_KEY));
    }

    @Test
    void shouldUseTraceIdFromKafkaHeader() {
        Base24Message msg = setupPassingDedupe();
        ConsumerRecord<String, String> record = aRecord("<xml/>");
        record.headers().add(AbstractKafkaConsumer.TRACE_ID_MDC_KEY, "trace-from-header".getBytes(StandardCharsets.UTF_8));
        doAnswer(invocation -> {
            assertEquals("trace-from-header", MDC.get(AbstractKafkaConsumer.TRACE_ID_MDC_KEY));
            return null;
        }).when(orchestrator).orchestrate(msg);

        consumer.consume(record, ack);

        assertNull(MDC.get(AbstractKafkaConsumer.TRACE_ID_MDC_KEY));
    }

    @Test
    void shouldGenerateTraceIdWhenHeaderValueBytesAreNull() {
        Base24Message msg = setupPassingDedupe();
        ConsumerRecord<String, String> record = aRecord("<xml/>");
        // Header present but with null value bytes — must not NPE, must fall back to UUID
        record.headers().add(AbstractKafkaConsumer.TRACE_ID_MDC_KEY, null);
        doAnswer(invocation -> {
            assertNotNull(MDC.get(AbstractKafkaConsumer.TRACE_ID_MDC_KEY));
            return null;
        }).when(orchestrator).orchestrate(msg);

        consumer.consume(record, ack);

        verify(ack).acknowledge();
    }

    @Test
    void shouldGenerateTraceIdWhenHeaderValueBytesAreEmpty() {
        Base24Message msg = setupPassingDedupe();
        ConsumerRecord<String, String> record = aRecord("<xml/>");
        record.headers().add(AbstractKafkaConsumer.TRACE_ID_MDC_KEY, new byte[0]);
        doAnswer(invocation -> {
            assertNotNull(MDC.get(AbstractKafkaConsumer.TRACE_ID_MDC_KEY));
            return null;
        }).when(orchestrator).orchestrate(msg);

        consumer.consume(record, ack);

        verify(ack).acknowledge();
    }

    @Test
    void shouldGenerateTraceIdWhenHeaderValueIsWhitespaceOnly() {
        Base24Message msg = setupPassingDedupe();
        ConsumerRecord<String, String> record = aRecord("<xml/>");
        record.headers().add(AbstractKafkaConsumer.TRACE_ID_MDC_KEY, "   ".getBytes(StandardCharsets.UTF_8));
        doAnswer(invocation -> {
            assertNotNull(MDC.get(AbstractKafkaConsumer.TRACE_ID_MDC_KEY));
            return null;
        }).when(orchestrator).orchestrate(msg);

        consumer.consume(record, ack);

        verify(ack).acknowledge();
    }

    @Test
    void shouldUseTraceIdFromDtoWhenConsumerExtractsIt() {
        Base24Message msg = setupPassingDedupe();
        consumer = new TraceAwareKafkaConsumer(mapper, deduplicationService, orchestrator);
        doAnswer(invocation -> {
            assertEquals("trace-from-dto", MDC.get(AbstractKafkaConsumer.TRACE_ID_MDC_KEY));
            return null;
        }).when(orchestrator).orchestrate(msg);

        consumer.consume(aRecord("<xml/>"), ack);

        assertNull(MDC.get(AbstractKafkaConsumer.TRACE_ID_MDC_KEY));
    }

    @ParameterizedTest
    @EnumSource(value = MessageType.class, names = {"TVN", "TCN", "ACN"})
    void shouldSucceedForAllActionableMessageTypes(MessageType type) {
        Base24Message msg = Base24Message.builder()
                .messageType(type)
                .recordType(RecordType.PTLFX)
                .transactionId("TXN-" + type)
                .digitalPan("4111111111111111")
                .build();

        when(mapper.map(any())).thenReturn(Optional.of(msg));
        when(orchestrator.shouldOrchestrate(msg)).thenReturn(true);
        when(deduplicationService.isConsumable(msg)).thenReturn(true);

        consumer.consume(aRecord("<xml/>"), ack);

        verify(ack).acknowledge();
        verify(deduplicationService).markProcessed(msg);
    }

    @Test
    void shouldThrowRetryExceptionWhenMarkProcessedFailsAfterSuccess() {
        Base24Message msg = setupPassingDedupe();
        doThrow(new TestRetriableException("db timeout")).when(deduplicationService).markProcessed(msg);

        assertThrows(KafkaProcessingRetryException.class, () -> consumer.consume(aRecord("<xml/>"), ack));

        verify(ack, never()).acknowledge();
    }

    private Base24Message setupPassingPreDedupe() {
        Base24Message msg = aMessage();
        when(mapper.map(any())).thenReturn(Optional.of(msg));
        when(orchestrator.shouldOrchestrate(msg)).thenReturn(true);
        return msg;
    }

    private Base24Message setupPassingDedupe() {
        Base24Message msg = setupPassingPreDedupe();
        when(deduplicationService.isConsumable(msg)).thenReturn(true);
        return msg;
    }

    private ConsumerRecord<String, String> aRecord(String value) {
        return new ConsumerRecord<>("base24-eps-realtime", 0, 100L, "TXN-001", value);
    }

    private Base24Message aMessage() {
        return Base24Message.builder()
                .messageType(MessageType.TVN)
                .recordType(RecordType.PTLFX)
                .transactionId("TXN-001")
                .digitalPan("4111111111111111")
                .build();
    }

    private static final class TraceAwareKafkaConsumer extends TestKafkaConsumer {
        private TraceAwareKafkaConsumer(
                EventMapper<String, Base24Message> mapper,
                DeduplicationService<Base24Message> deduplicationService,
                EventOrchestrator<Base24Message> orchestrator
        ) {
            super(mapper, deduplicationService, orchestrator);
        }

        @Override
        protected Optional<String> traceId(Base24Message dto) {
            return Optional.of("trace-from-dto");
        }
    }

    private static class TestKafkaConsumer extends AbstractKafkaConsumer<String, Base24Message> {
        private TestKafkaConsumer(
                EventMapper<String, Base24Message> mapper,
                DeduplicationService<Base24Message> deduplicationService,
                EventOrchestrator<Base24Message> orchestrator
        ) {
            super(mapper, deduplicationService, orchestrator);
        }

        @Override
        protected boolean isRetriableException(RuntimeException exception) {
            return exception instanceof TestRetriableException;
        }
    }

    private static final class TestRetriableException extends RuntimeException {
        private TestRetriableException(String message) {
            super(message);
        }
    }
}
