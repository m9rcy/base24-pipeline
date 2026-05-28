package com.commercial.cards.base24.orchestration;

public interface EventOrchestrator<D> {

    boolean shouldOrchestrate(D dto);

    void orchestrate(D dto);
}
