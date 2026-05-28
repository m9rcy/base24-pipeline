package com.commercial.cards.base24.orchestration;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

@Slf4j
@RequiredArgsConstructor
public abstract class ProcessorRoutingOrchestrator<D> implements EventOrchestrator<D> {

    private final List<EventProcessor<D>> processors;
    private final ProcessorRoutingMode routingMode;

    @Override
    public void orchestrate(D dto) {
        List<EventProcessor<D>> matchingProcessors = processors.stream()
                .filter(processor -> processor.supports(dto))
                .toList();

        if (matchingProcessors.isEmpty()) {
            log.debug("No processor matched [{}]", dto);
            return;
        }

        switch (routingMode) {
            case SINGLE_MATCH -> processSingleMatch(dto, matchingProcessors);
            case MULTI_MATCH  -> processMultiMatch(dto, matchingProcessors);
        }
    }

    private void processSingleMatch(D dto, List<EventProcessor<D>> matchingProcessors) {
        if (matchingProcessors.size() > 1) {
            throw new IllegalStateException("Multiple processors matched in SINGLE_MATCH mode [matches=%d dto=%s]"
                    .formatted(matchingProcessors.size(), dto));
        }

        matchingProcessors.getFirst().process(dto);
    }

    private void processMultiMatch(D dto, List<EventProcessor<D>> matchingProcessors) {
        for (EventProcessor<D> processor : matchingProcessors) {
            processor.process(dto);
        }
    }
}
