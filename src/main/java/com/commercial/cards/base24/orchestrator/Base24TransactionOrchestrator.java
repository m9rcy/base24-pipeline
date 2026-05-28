package com.commercial.cards.base24.orchestrator;

import com.commercial.cards.base24.model.Base24Message;
import com.commercial.cards.base24.orchestration.EventProcessor;
import com.commercial.cards.base24.orchestration.ProcessorRoutingMode;
import com.commercial.cards.base24.orchestration.ProcessorRoutingOrchestrator;
import com.commercial.cards.base24.pipeline.MessageFilter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class Base24TransactionOrchestrator extends ProcessorRoutingOrchestrator<Base24Message> {

    private final MessageFilter filter;

    public Base24TransactionOrchestrator(
            MessageFilter filter,
            List<EventProcessor<Base24Message>> processors,
            @Value("${base24.orchestration.processor-routing-mode:single-match}") String routingMode
    ) {
        super(processors, ProcessorRoutingMode.from(routingMode));
        this.filter = filter;
    }

    @Override
    public boolean shouldOrchestrate(Base24Message dto) {
        return filter.shouldPublish(dto, MessageFilter.IS_PTLFX, MessageFilter.IS_ACTIONABLE);
    }
}
