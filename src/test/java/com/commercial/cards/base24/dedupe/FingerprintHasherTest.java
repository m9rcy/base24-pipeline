package com.commercial.cards.base24.dedupe;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

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

    @Test
    void shouldProduceDifferentHashesForDifferentFingerprints() {
        FingerprintHasher hasher = new FingerprintHasher(new ObjectMapper(), "");

        String hash1 = hasher.hash(Map.of("transactionId", "TXN-001"));
        String hash2 = hasher.hash(Map.of("transactionId", "TXN-002"));

        assertNotEquals(hash1, hash2);
    }

    @Test
    void shouldHandleNullHmacSecret() {
        FingerprintHasher hasher = new FingerprintHasher(new ObjectMapper(), null);
        assertNotNull(hasher.hash(Map.of("key", "value")));
    }

    @Test
    void shouldThrowIllegalStateWhenFingerprintCannotBeSerialized() {
        FingerprintHasher hasher = new FingerprintHasher(new ObjectMapper(), "");

        // Jackson calls getters during serialization; a getter that throws causes
        // JsonMappingException (extends JsonProcessingException) → IllegalStateException.
        Object unserializable = new Object() {
            @SuppressWarnings("unused")
            public String getField() { throw new UnsupportedOperationException("not serializable"); }
        };

        assertThrows(IllegalStateException.class, () -> hasher.hash(unserializable));
    }
}
