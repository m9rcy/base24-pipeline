package com.commercial.cards.base24.dedupe;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class FingerprintHasherTest {

    @Test
    void shouldCreateStableHashForEquivalentMapOrdering() {
        FingerprintHasher hasher = new FingerprintHasher(new ObjectMapper(), "");

        String first = hasher.hash(Map.of("b", "two", "a", "one"));
        String second = hasher.hash(Map.of("a", "one", "b", "two"));

        assertEquals(first, second);
    }

    @Test
    void shouldUseHmacSecretWhenConfigured() {
        Object fingerprint = Map.of("transactionId", "TXN-001");

        String plainHash = new FingerprintHasher(new ObjectMapper(), "").hash(fingerprint);
        String hmacHash = new FingerprintHasher(new ObjectMapper(), "secret").hash(fingerprint);

        assertNotEquals(plainHash, hmacHash);
    }
}
