package com.commercial.cards.base24.model;

public enum ProcessingResult {
    /** Message successfully tokenised and saved — ack offset. */
    SUCCESS,
    /** Message filtered out (not PTLFX or not actionable) — ack offset, discard cleanly. */
    SKIPPED,
    /** Transient downstream failure — do NOT ack, let Kafka redeliver. */
    RETRY
}
