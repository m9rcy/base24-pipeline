package com.commercial.cards.base24.dedupe;

import com.commercial.cards.base24.model.Base24Message;
import com.commercial.cards.base24.model.MessageType;
import com.commercial.cards.base24.model.RecordType;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.assertEquals;

class Base24EventFingerprintTest {

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
}
