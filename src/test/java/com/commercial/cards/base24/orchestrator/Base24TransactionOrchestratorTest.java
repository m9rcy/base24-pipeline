package com.commercial.cards.base24.orchestrator;

import com.commercial.cards.base24.model.Base24Message;
import com.commercial.cards.base24.model.MessageType;
import com.commercial.cards.base24.model.RecordType;
import com.commercial.cards.base24.orchestration.EventProcessor;
import com.commercial.cards.base24.pipeline.MessageFilter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

class Base24TransactionOrchestratorTest {

    private Base24TransactionOrchestrator orchestrator;

    @BeforeEach
    void setUp() {
        @SuppressWarnings("unchecked")
        EventProcessor<Base24Message> processor = mock(EventProcessor.class);
        orchestrator = new Base24TransactionOrchestrator(
                new MessageFilter(),
                List.of(processor),
                "single-match");
    }

    @ParameterizedTest
    @EnumSource(value = MessageType.class, names = {"TVN", "TCN", "ACN"})
    void shouldOrchestrateWhenPtlfxAndActionableMessageType(MessageType type) {
        assertTrue(orchestrator.shouldOrchestrate(ptlfxMessage(type)));
    }

    @Test
    void shouldNotOrchestrateWhenRecordTypeIsNotPtlfx() {
        Base24Message msg = Base24Message.builder()
                .messageType(MessageType.TVN)
                .recordType(RecordType.OTHER)
                .build();
        assertFalse(orchestrator.shouldOrchestrate(msg));
    }

    @ParameterizedTest
    @EnumSource(value = MessageType.class, names = {"TAR", "UNKNOWN"})
    void shouldNotOrchestrateWhenMessageTypeIsNotActionable(MessageType type) {
        assertFalse(orchestrator.shouldOrchestrate(ptlfxMessage(type)));
    }

    @Test
    void shouldNotOrchestrateWhenMessageIsNull() {
        assertFalse(orchestrator.shouldOrchestrate(null));
    }

    @Test
    void shouldNotOrchestrateWhenMessageTypeIsNull() {
        Base24Message msg = Base24Message.builder()
                .recordType(RecordType.PTLFX)
                .build();
        assertFalse(orchestrator.shouldOrchestrate(msg));
    }

    private Base24Message ptlfxMessage(MessageType type) {
        return Base24Message.builder()
                .messageType(type)
                .recordType(RecordType.PTLFX)
                .transactionId("TXN-001")
                .build();
    }
}
