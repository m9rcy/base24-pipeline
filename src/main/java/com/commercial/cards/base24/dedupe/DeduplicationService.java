package com.commercial.cards.base24.dedupe;

public interface DeduplicationService<D> {

    boolean isConsumable(D dto);

    void markProcessed(D dto);
}
