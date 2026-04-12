package com.commercial.cards.base24.dto;

import com.commercial.cards.base24.model.Base24Message;

import java.math.BigDecimal;

public record SaveTransactionRequest(
        String     messageType,
        String     transactionId,
        String     tokenisedPan,
        BigDecimal amount,
        String     currencyCode,
        String     responseCode,
        String     timestamp
) {
    public static SaveTransactionRequest from(Base24Message msg, String tokenisedPan) {
        return new SaveTransactionRequest(
                msg.getMessageType().name(),
                msg.getTransactionId(),
                tokenisedPan,
                msg.getAmount(),
                msg.getCurrencyCode(),
                msg.getResponseCode(),
                msg.getTimestamp() != null ? msg.getTimestamp().toString() : null
        );
    }
}
