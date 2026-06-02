package com.commercial.cards.base24.model;

import lombok.Builder;
import lombok.ToString;
import lombok.Value;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Value
@Builder
@ToString(exclude = "digitalPan")
public class Base24Message {

    MessageType   messageType;
    RecordType    recordType;
    String        transactionId;
    String        digitalPan;
    BigDecimal    amount;
    String        currencyCode;
    String        responseCode;
    LocalDateTime timestamp;

    public static Base24Message of(
            MessageType messageType,
            RecordType recordType,
            String transactionId,
            String digitalPan,
            BigDecimal amount,
            String currencyCode,
            String responseCode,
            LocalDateTime timestamp
    ) {
        return Base24Message.builder()
                .messageType(messageType)
                .recordType(recordType)
                .transactionId(transactionId)
                .digitalPan(digitalPan)
                .amount(amount)
                .currencyCode(currencyCode)
                .responseCode(responseCode)
                .timestamp(timestamp)
                .build();
    }

    public static Base24Message from(RtfData rtfData, BigDecimal amount, LocalDateTime timestamp) {
        return of(
                MessageType.from(rtfData.getMessageType()),
                RecordType.from(rtfData.getRecordType()),
                rtfData.getTransactionId(),
                rtfData.getDigitalPan(),
                amount,
                rtfData.getCurrencyCode(),
                rtfData.getResponseCode(),
                timestamp
        );
    }
}
