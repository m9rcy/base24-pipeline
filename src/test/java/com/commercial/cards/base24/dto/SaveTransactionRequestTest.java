package com.commercial.cards.base24.dto;

import com.commercial.cards.base24.model.Base24Message;
import com.commercial.cards.base24.model.MessageType;
import com.commercial.cards.base24.model.RecordType;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class SaveTransactionRequestTest {

    @Test
    void shouldMapAllFieldsFromMessage() {
        LocalDateTime ts = LocalDateTime.of(2026, 1, 15, 10, 30, 0);
        Base24Message msg = Base24Message.builder()
                .messageType(MessageType.TVN)
                .recordType(RecordType.PTLFX)
                .transactionId("TXN-001")
                .amount(new BigDecimal("123.45"))
                .currencyCode("NZD")
                .responseCode("00")
                .timestamp(ts)
                .build();

        SaveTransactionRequest request = SaveTransactionRequest.from(msg, "TOK-1111");

        assertEquals("TVN", request.messageType());
        assertEquals("TXN-001", request.transactionId());
        assertEquals("TOK-1111", request.tokenisedPan());
        assertEquals(new BigDecimal("123.45"), request.amount());
        assertEquals("NZD", request.currencyCode());
        assertEquals("00", request.responseCode());
        assertEquals(ts.toString(), request.timestamp());
    }

    @Test
    void shouldMapNullTimestampAsNull() {
        Base24Message msg = Base24Message.builder()
                .messageType(MessageType.TVN)
                .transactionId("TXN-001")
                .build();

        SaveTransactionRequest request = SaveTransactionRequest.from(msg, "TOK-1111");

        assertNull(request.timestamp());
    }
}
