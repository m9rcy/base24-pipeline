package com.commercial.cards.base24.consumer;

public class KafkaProcessingRetryException extends RuntimeException {

    public KafkaProcessingRetryException(String message) {
        super(message);
    }

    public KafkaProcessingRetryException(String message, Throwable cause) {
        super(message, cause);
    }
}
