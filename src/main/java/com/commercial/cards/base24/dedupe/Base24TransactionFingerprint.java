package com.commercial.cards.base24.dedupe;

import java.math.BigDecimal;

public record Base24TransactionFingerprint(
        String messageType,
        String recordType,
        String transactionId,
        String digitalPan,
        BigDecimal amount,
        String currencyCode,
        String responseCode
) {
    public static Base24TransactionFingerprint of(
            String messageType,
            String recordType,
            String transactionId,
            String digitalPan,
            BigDecimal amount,
            String currencyCode,
            String responseCode
    ) {
        return new Base24TransactionFingerprint(
                messageType,
                recordType,
                transactionId,
                digitalPan,
                amount,
                currencyCode,
                responseCode
        );
    }
}
