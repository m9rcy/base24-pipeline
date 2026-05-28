package com.commercial.cards.base24.dedupe;

import com.commercial.cards.base24.model.Base24Message;
import com.commercial.cards.base24.model.MessageType;
import com.commercial.cards.base24.model.RecordType;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class Base24EventFingerprintTest {

    private final Base24EventFingerprint fingerprint = new Base24EventFingerprint();

    @Test
    void shouldReturnCorrectDomainAndVersion() {
        assertEquals(Base24EventFingerprint.DOMAIN, fingerprint.domain());
        assertEquals(Base24EventFingerprint.VERSION, fingerprint.version());
    }

    @Test
    void shouldReturnTrimmedTransactionIdAsKey() {
        Base24Message msg = Base24Message.builder().transactionId(" TXN-001 ").build();
        assertEquals("TXN-001", fingerprint.key(msg));
    }

    @Test
    void shouldReturnNullKeyWhenTransactionIdIsNull() {
        Base24Message msg = Base24Message.builder().build();
        assertNull(fingerprint.key(msg));
    }

    @Test
    void shouldReturnEventTimeWhenTimestampIsPresent() {
        LocalDateTime ts = LocalDateTime.of(2026, 1, 15, 10, 30, 0);
        Base24Message msg = Base24Message.builder().timestamp(ts).build();
        assertEquals(Optional.of(ts), fingerprint.eventTime(msg));
    }

    @Test
    void shouldReturnEmptyEventTimeWhenTimestampIsNull() {
        Base24Message msg = Base24Message.builder().build();
        assertTrue(fingerprint.eventTime(msg).isEmpty());
    }

    @Test
    void shouldNormalizeDomainFingerprintFields() {
        Base24EventFingerprint fingerprint = new Base24EventFingerprint();

        Object result = fingerprint.fingerprint(Base24Message.builder()
                .messageType(MessageType.TVN)
                .recordType(RecordType.PTLFX)
                .transactionId(" TXN-001 ")
                .digitalPan(" 4111111111111111 ")
                .amount(new BigDecimal("123.4500"))
                .currencyCode("nzd")
                .responseCode(" 00 ")
                .build());

        assertEquals(new Base24TransactionFingerprint(
                "TVN",
                "PTLFX",
                "TXN-001",
                "4111111111111111",
                new BigDecimal("123.45"),
                "NZD",
                "00"
        ), result);
    }

    @Test
    void shouldHandleNullFingerprintFields() {
        Object result = fingerprint.fingerprint(Base24Message.builder().build());

        assertEquals(new Base24TransactionFingerprint(null, null, null, null, null, null, null), result);
    }
}
