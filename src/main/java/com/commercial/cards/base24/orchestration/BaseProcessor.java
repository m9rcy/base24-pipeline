package com.commercial.cards.base24.orchestration;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class BaseProcessor<D> implements EventProcessor<D> {

    @Override
    public final void process(D dto) {
        if (!shouldProcess(dto)) {
            log.debug("Skipping event in processor-specific filter [processor={} dto={}]",
                    getClass().getSimpleName(), dto);
            return;
        }

        doProcess(dto);
    }

    protected boolean shouldProcess(D dto) {
        return true;
    }

    protected abstract void doProcess(D dto);
}
