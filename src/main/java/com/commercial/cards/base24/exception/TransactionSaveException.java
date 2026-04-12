package com.commercial.cards.base24.exception;

public class TransactionSaveException extends RuntimeException {

    public TransactionSaveException(String message) {
        super(message);
    }

    public TransactionSaveException(String message, Throwable cause) {
        super(message, cause);
    }
}
