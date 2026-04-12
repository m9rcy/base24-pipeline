package com.commercial.cards.base24.model;

import lombok.Builder;
import lombok.Data;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Data
@Builder
public class Base24Message {

    private MessageType   messageType;
    private RecordType    recordType;
    private String        transactionId;
    private String        digitalPan;
    private BigDecimal    amount;
    private String        currencyCode;
    private String        responseCode;
    private LocalDateTime timestamp;
}
