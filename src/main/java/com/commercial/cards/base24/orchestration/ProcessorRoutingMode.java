package com.commercial.cards.base24.orchestration;

public enum ProcessorRoutingMode {
    SINGLE_MATCH,
    MULTI_MATCH;

    public static ProcessorRoutingMode from(String value) {
        if (value == null || value.isBlank()) {
            return SINGLE_MATCH;
        }

        return ProcessorRoutingMode.valueOf(value.trim().toUpperCase().replace('-', '_'));
    }
}
