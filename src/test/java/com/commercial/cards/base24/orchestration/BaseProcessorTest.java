package com.commercial.cards.base24.orchestration;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class BaseProcessorTest {

    @Test
    void shouldSkipWhenProcessorSpecificFilterRejectsEvent() {
        TestProcessor processor = new TestProcessor(false);

        processor.process("event");

        assertEquals(0, processor.processCalls);
    }

    @Test
    void shouldProcessWhenProcessorSpecificFilterAcceptsEvent() {
        TestProcessor processor = new TestProcessor(true);

        processor.process("event");

        assertEquals(1, processor.processCalls);
    }

    private static final class TestProcessor extends BaseProcessor<String> {
        private final boolean shouldProcess;
        private int processCalls;

        private TestProcessor(boolean shouldProcess) {
            this.shouldProcess = shouldProcess;
        }

        @Override
        public boolean supports(String dto) {
            return true;
        }

        @Override
        protected boolean shouldProcess(String dto) {
            return shouldProcess;
        }

        @Override
        protected void doProcess(String dto) {
            processCalls++;
        }
    }
}
