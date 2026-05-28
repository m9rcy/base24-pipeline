package com.commercial.cards.base24.orchestration;

public interface EventProcessor<D> {

    boolean supports(D dto);

    void process(D dto);
}
