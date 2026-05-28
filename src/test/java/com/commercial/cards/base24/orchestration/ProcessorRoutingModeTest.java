package com.commercial.cards.base24.orchestration;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ProcessorRoutingModeTest {

    @ParameterizedTest
    @ValueSource(strings = {"single-match", "SINGLE-MATCH", "SINGLE_MATCH", "single_match"})
    void shouldParseSingleMatch(String value) {
        assertEquals(ProcessorRoutingMode.SINGLE_MATCH, ProcessorRoutingMode.from(value));
    }

    @ParameterizedTest
    @ValueSource(strings = {"multi-match", "MULTI-MATCH", "MULTI_MATCH", "multi_match"})
    void shouldParseMultiMatch(String value) {
        assertEquals(ProcessorRoutingMode.MULTI_MATCH, ProcessorRoutingMode.from(value));
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "   "})
    void shouldDefaultToSingleMatchForBlankInput(String value) {
        assertEquals(ProcessorRoutingMode.SINGLE_MATCH, ProcessorRoutingMode.from(value));
    }

    @Test
    void shouldDefaultToSingleMatchForNullInput() {
        assertEquals(ProcessorRoutingMode.SINGLE_MATCH, ProcessorRoutingMode.from(null));
    }

    @Test
    void shouldThrowForUnrecognisedMode() {
        assertThrows(IllegalArgumentException.class, () -> ProcessorRoutingMode.from("unknown-mode"));
    }
}
