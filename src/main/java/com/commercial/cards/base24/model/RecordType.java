package com.commercial.cards.base24.model;

public enum RecordType {
    PTLFX,
    OTHER;

    public static RecordType from(String value) {
        if (value == null) return OTHER;
        return switch (value.trim().toUpperCase()) {
            case "PTLFX" -> PTLFX;
            default      -> OTHER;
        };
    }
}
