package com.commercial.cards.base24.dedupe;

public interface DeduplicationService<D> {

    /**
     * Atomically decides whether this event should be processed and, if so, records that
     * decision in the same transaction before any downstream side-effects occur.
     * Returns true if the caller should proceed with downstream orchestration.
     */
    boolean isConsumable(D dto);
}
