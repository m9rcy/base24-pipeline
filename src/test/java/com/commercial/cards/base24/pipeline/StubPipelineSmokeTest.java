package com.commercial.cards.base24.pipeline;

import com.commercial.cards.base24.consumer.Base24KafkaConsumer;
import com.commercial.cards.base24.dedupe.NoOpDeduplicationService;
import com.commercial.cards.base24.mapper.Base24EventMapper;
import com.commercial.cards.base24.model.Base24Message;
import com.commercial.cards.base24.model.MessageType;
import com.commercial.cards.base24.model.RecordType;
import com.commercial.cards.base24.orchestrator.Base24TransactionOrchestrator;
import com.commercial.cards.base24.processor.TokeniseAndSaveTransactionProcessor;
import com.commercial.cards.base24.stub.TokenisationStub;
import com.commercial.cards.base24.stub.TransactionStub;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.Acknowledgment;

import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Integration-style smoke test for the stub profile.
 * Wires the real stubs into the consumer flow to verify the full flow
 * without any Spring context or real HTTP calls.
 */
class StubPipelineSmokeTest {

    @Test
    void shouldProcessFullPipelineEndToEndWithStubs() {
        // Arrange — real stubs, mock parser only (XML not the focus here)
        var parser        = mock(com.commercial.cards.base24.parser.Base24XmlParser.class);
        var filter        = new MessageFilter();
        var tokenisation  = new TokenisationStub();
        var transaction   = new TransactionStub();

        Base24Message message = Base24Message.builder()
                .messageType(MessageType.TVN)
                .recordType(RecordType.PTLFX)
                .transactionId("TXN-SMOKE-001")
                .digitalPan("4111111111111111")
                .amount(new BigDecimal("250.00"))
                .currencyCode("NZD")
                .responseCode("00")
                .build();

        when(parser.parse(any())).thenReturn(Optional.of(message));

        var processor = new TokeniseAndSaveTransactionProcessor(tokenisation, transaction);
        var orchestrator = new Base24TransactionOrchestrator(filter, List.of(processor), "single-match");
        var consumer = new Base24KafkaConsumer(
                new Base24EventMapper(parser),
                new NoOpDeduplicationService<>(),
                orchestrator);
        Acknowledgment ack = mock(Acknowledgment.class);

        consumer.consume(aRecord("<xml/>"), ack);

        verify(ack).acknowledge();
    }

    @Test
    void shouldSkipTarMessageWithRealFilter() {
        var parser       = mock(com.commercial.cards.base24.parser.Base24XmlParser.class);
        var filter       = new MessageFilter();
        var tokenisation = new TokenisationStub();
        var transaction  = new TransactionStub();

        Base24Message tarMessage = Base24Message.builder()
                .messageType(MessageType.TAR)
                .recordType(RecordType.PTLFX)
                .transactionId("TXN-TAR-001")
                .digitalPan("4111111111111111")
                .build();

        when(parser.parse(any())).thenReturn(Optional.of(tarMessage));

        var processor = new TokeniseAndSaveTransactionProcessor(tokenisation, transaction);
        var orchestrator = new Base24TransactionOrchestrator(filter, List.of(processor), "single-match");
        var consumer = new Base24KafkaConsumer(
                new Base24EventMapper(parser),
                new NoOpDeduplicationService<>(),
                orchestrator);
        Acknowledgment ack = mock(Acknowledgment.class);

        consumer.consume(aRecord("<xml/>"), ack);

        verify(ack).acknowledge();
    }

    private ConsumerRecord<String, String> aRecord(String value) {
        return new ConsumerRecord<>("base24-eps-realtime", 0, 100L, "TXN-001", value);
    }
}
