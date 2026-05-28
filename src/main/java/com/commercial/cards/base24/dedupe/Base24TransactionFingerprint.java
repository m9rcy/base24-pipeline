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
}
