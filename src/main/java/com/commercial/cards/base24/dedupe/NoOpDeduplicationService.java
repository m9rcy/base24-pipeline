package com.commercial.cards.base24.dedupe;

public class NoOpDeduplicationService<D> implements DeduplicationService<D> {

    @Override
    public boolean isConsumable(D dto) {
        return true;
    }
}
