package com.commercial.cards.base24.orchestration;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ProcessorRoutingOrchestratorTest {

    @Test
    void singleMatchShouldProcessOneMatchingProcessorForMadeUpConsumer() {
        TestProcessor matching = new TestProcessor("A");
        TestProcessor other = new TestProcessor("B");
        TestOrchestrator orchestrator = new TestOrchestrator(
                List.of(matching, other),
                ProcessorRoutingMode.SINGLE_MATCH);

        orchestrator.orchestrate(new TestEvent("A"));

        assertEquals(1, matching.calls);
        assertEquals(0, other.calls);
    }

    @Test
    void singleMatchShouldThrowWhenMultipleProcessorsMatch() {
        TestProcessor first = new TestProcessor("A");
        TestProcessor second = new TestProcessor("A");
        TestOrchestrator orchestrator = new TestOrchestrator(
                List.of(first, second),
                ProcessorRoutingMode.SINGLE_MATCH);

        assertThrows(IllegalStateException.class, () -> orchestrator.orchestrate(new TestEvent("A")));

        assertEquals(0, first.calls);
        assertEquals(0, second.calls);
    }

    @Test
    void multiMatchShouldRunAllMatchingProcessors() {
        TestProcessor first = new TestProcessor("A");
        TestProcessor second = new TestProcessor("A");
        TestOrchestrator orchestrator = new TestOrchestrator(
                List.of(first, second),
                ProcessorRoutingMode.MULTI_MATCH);

        orchestrator.orchestrate(new TestEvent("A"));

        assertEquals(1, first.calls);
        assertEquals(1, second.calls);
    }

    @Test
    void multiMatchShouldPropagateProcessorException() {
        TestProcessor first = new TestProcessor("A");
        TestProcessor second = new TestProcessor("A", new IllegalStateException("processor failed"));
        TestOrchestrator orchestrator = new TestOrchestrator(
                List.of(first, second),
                ProcessorRoutingMode.MULTI_MATCH);

        assertThrows(IllegalStateException.class, () -> orchestrator.orchestrate(new TestEvent("A")));

        assertEquals(1, first.calls);
        assertEquals(1, second.calls);
    }

    private record TestEvent(String type) {
    }

    private static final class TestOrchestrator extends ProcessorRoutingOrchestrator<TestEvent> {
        private TestOrchestrator(List<EventProcessor<TestEvent>> processors, ProcessorRoutingMode routingMode) {
            super(processors, routingMode);
        }

        @Override
        public boolean shouldOrchestrate(TestEvent dto) {
            return true;
        }
    }

    private static final class TestProcessor implements EventProcessor<TestEvent> {
        private final String supportedType;
        private final RuntimeException exception;
        private int calls;

        private TestProcessor(String supportedType) {
            this(supportedType, null);
        }

        private TestProcessor(String supportedType, RuntimeException exception) {
            this.supportedType = supportedType;
            this.exception = exception;
        }

        @Override
        public boolean supports(TestEvent dto) {
            return supportedType.equals(dto.type());
        }

        @Override
        public void process(TestEvent dto) {
            calls++;
            if (exception != null) {
                throw exception;
            }
        }
    }
}
