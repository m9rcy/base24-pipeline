package com.commercial.cards.base24.consumer;

import com.commercial.cards.base24.dedupe.DeduplicationService;
import com.commercial.cards.base24.exception.TokenisationException;
import com.commercial.cards.base24.exception.TransactionSaveException;
import com.commercial.cards.base24.model.Base24Message;
import com.commercial.cards.base24.orchestration.EventMapper;
import com.commercial.cards.base24.orchestration.EventOrchestrator;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.Acknowledgment;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class Base24KafkaConsumerTest {

    private EventMapper<String, Base24Message> mapper;
    private DeduplicationService<Base24Message> deduplicationService;
    private EventOrchestrator<Base24Message> orchestrator;
    private Acknowledgment ack;
    private Base24KafkaConsumer consumer;

    @BeforeEach
    void setUp() {
        mapper = mock(EventMapper.class);
        deduplicationService = mock(DeduplicationService.class);
        orchestrator = mock(EventOrchestrator.class);
        ack = mock(Acknowledgment.class);
        consumer = new Base24KafkaConsumer(mapper, deduplicationService, orchestrator);
    }

    @Test
    void shouldAckWhenProcessingSucceeds() {
        Base24Message message = aMessage();
        when(mapper.map(any())).thenReturn(Optional.of(message));
        when(orchestrator.shouldOrchestrate(message)).thenReturn(true);
        when(deduplicationService.isConsumable(message)).thenReturn(true);

        consumer.consume(aRecord("<xml/>"), ack);

        verify(ack).acknowledge();
    }

    @Test
    void shouldAckWhenProcessingSkips() {
        when(mapper.map(any())).thenReturn(Optional.empty());

        consumer.consume(aRecord("<xml/>"), ack);

        verify(ack).acknowledge();
    }

    @Test
    void shouldThrowRetryExceptionAndNotAckWhenProcessingRetries() {
        Base24Message message = aMessage();
        when(mapper.map(any())).thenReturn(Optional.of(message));
        when(orchestrator.shouldOrchestrate(message)).thenReturn(true);
        when(deduplicationService.isConsumable(message)).thenReturn(true);
        doThrow(new TokenisationException("service unavailable")).when(orchestrator).orchestrate(message);

        assertThrows(KafkaProcessingRetryException.class, () -> consumer.consume(aRecord("<xml/>"), ack));

        verify(ack, never()).acknowledge();
    }

    @Test
    void shouldThrowRetryExceptionForTransactionSaveException() {
        Base24Message message = aMessage();
        when(mapper.map(any())).thenReturn(Optional.of(message));
        when(orchestrator.shouldOrchestrate(message)).thenReturn(true);
        when(deduplicationService.isConsumable(message)).thenReturn(true);
        doThrow(new TransactionSaveException("db timeout")).when(orchestrator).orchestrate(message);

        assertThrows(KafkaProcessingRetryException.class, () -> consumer.consume(aRecord("<xml/>"), ack));

        verify(ack, never()).acknowledge();
    }

    @Test
    void shouldRethrowNonRetriableExceptionDirectly() {
        Base24Message message = aMessage();
        when(mapper.map(any())).thenReturn(Optional.of(message));
        when(orchestrator.shouldOrchestrate(message)).thenReturn(true);
        when(deduplicationService.isConsumable(message)).thenReturn(true);
        RuntimeException nonRetriable = new IllegalStateException("unexpected state");
        doThrow(nonRetriable).when(orchestrator).orchestrate(message);

        IllegalStateException thrown = assertThrows(IllegalStateException.class,
                () -> consumer.consume(aRecord("<xml/>"), ack));
        assertSame(nonRetriable, thrown);
        verify(ack, never()).acknowledge();
    }

    @Test
    void shouldDelegateRawXmlToMapper() {
        String rawXml = "<Data><MessageType>TVN</MessageType></Data>";
        when(mapper.map(rawXml)).thenReturn(Optional.empty());

        consumer.consume(aRecord(rawXml), ack);

        verify(mapper).map(rawXml);
    }

    private ConsumerRecord<String, String> aRecord(String value) {
        return new ConsumerRecord<>("base24-eps-realtime", 0, 100L, "TXN-001", value);
    }

    private Base24Message aMessage() {
        return Base24Message.builder()
                .transactionId("TXN-001")
                .build();
    }
}
