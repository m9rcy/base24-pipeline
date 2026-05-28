package com.commercial.cards.base24.model;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.assertEquals;

class RecordTypeTest {

    @Test
    void shouldReturnOtherForNullInput() {
        assertEquals(RecordType.OTHER, RecordType.from(null));
    }

    @ParameterizedTest
    @CsvSource({"PTLFX,PTLFX", "ptlfx,PTLFX", "Ptlfx,PTLFX"})
    void shouldParsePtlfxCaseInsensitive(String input, String expected) {
        assertEquals(RecordType.valueOf(expected), RecordType.from(input));
    }

    @Test
    void shouldReturnOtherForUnrecognisedValue() {
        assertEquals(RecordType.OTHER, RecordType.from("UNKNOWN_TYPE"));
    }

    @Test
    void shouldTrimWhitespaceBeforeParsing() {
        assertEquals(RecordType.PTLFX, RecordType.from("  PTLFX  "));
    }

    @Test
    void shouldReturnOtherForEmptyString() {
        assertEquals(RecordType.OTHER, RecordType.from(""));
    }
}
