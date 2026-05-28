package com.commercial.cards.base24.consumer;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class KafkaProcessingRetryExceptionTest {

    @Test
    void shouldCreateExceptionWithMessageOnly() {
        KafkaProcessingRetryException ex = new KafkaProcessingRetryException("processing failed");
        assertEquals("processing failed", ex.getMessage());
        assertNull(ex.getCause());
    }

    @Test
    void shouldCreateExceptionWithMessageAndCause() {
        Throwable cause = new RuntimeException("root cause");
        KafkaProcessingRetryException ex = new KafkaProcessingRetryException("processing failed", cause);
        assertEquals("processing failed", ex.getMessage());
        assertEquals(cause, ex.getCause());
    }
}
