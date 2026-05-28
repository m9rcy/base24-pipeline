package com.commercial.cards.base24.dedupe;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

class EventFingerprintTest {

    @Test
    void defaultEventTimeShouldReturnEmpty() {
        EventFingerprint<String> fingerprint = new EventFingerprint<>() {
            @Override
            public String domain() { return "test"; }

            @Override
            public String key(String dto) { return dto; }

            @Override
            public String version() { return "v1"; }

            @Override
            public Object fingerprint(String dto) { return dto; }
        };

        assertTrue(fingerprint.eventTime("any").isEmpty());
    }
}
