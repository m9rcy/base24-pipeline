package com.commercial.cards.base24.model;

public enum MessageType {
    TAR,
    TVN,
    TCN,
    ACN,
    UNKNOWN;

    public static MessageType from(String value) {
        if (value == null) return UNKNOWN;
        return switch (value.trim().toUpperCase()) {
            case "TAR" -> TAR;
            case "TVN" -> TVN;
            case "TCN" -> TCN;
            case "ACN" -> ACN;
            default    -> UNKNOWN;
        };
    }
}
