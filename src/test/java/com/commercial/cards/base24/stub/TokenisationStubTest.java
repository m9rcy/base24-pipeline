package com.commercial.cards.base24.stub;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TokenisationStubTest {

    private final TokenisationStub stub = new TokenisationStub();

    @Test
    void shouldTokeniseUsingLast4DigitsOfPan() {
        assertEquals("TOK-STUB-1111", stub.tokenise("4111111111111111"));
    }

    @Test
    void shouldReturnDefaultTokenWhenPanIsNull() {
        assertEquals("TOK-STUB-0000", stub.tokenise(null));
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "123"})
    void shouldReturnDefaultTokenWhenPanIsTooShort(String pan) {
        assertEquals("TOK-STUB-0000", stub.tokenise(pan));
    }

    @Test
    void shouldUseExactlyLast4Characters() {
        assertEquals("TOK-STUB-5559", stub.tokenise("5500005555555559"));
    }
}
