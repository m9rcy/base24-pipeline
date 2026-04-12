package com.commercial.cards.base24.exception;

public class TokenisationException extends RuntimeException {

    public TokenisationException(String message) {
        super(message);
    }

    public TokenisationException(String message, Throwable cause) {
        super(message, cause);
    }
}
