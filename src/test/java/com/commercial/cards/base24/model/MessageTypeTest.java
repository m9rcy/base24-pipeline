package com.commercial.cards.base24.model;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.assertEquals;

class MessageTypeTest {

    @ParameterizedTest
    @CsvSource({"TVN,TVN", "tvn,TVN", "TCN,TCN", "tcn,TCN", "ACN,ACN", "acn,ACN", "TAR,TAR", "tar,TAR"})
    void shouldParseKnownTypeCaseInsensitive(String input, String expected) {
        assertEquals(MessageType.valueOf(expected), MessageType.from(input));
    }

    @Test
    void shouldReturnUnknownForNullInput() {
        assertEquals(MessageType.UNKNOWN, MessageType.from(null));
    }

    @Test
    void shouldReturnUnknownForUnrecognisedValue() {
        assertEquals(MessageType.UNKNOWN, MessageType.from("XYZ"));
    }

    @Test
    void shouldReturnUnknownForEmptyString() {
        assertEquals(MessageType.UNKNOWN, MessageType.from(""));
    }

    @Test
    void shouldTrimWhitespaceBeforeParsing() {
        assertEquals(MessageType.TVN, MessageType.from("  TVN  "));
    }
}
