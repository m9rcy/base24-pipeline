package com.commercial.cards.base24.pipeline;

import com.commercial.cards.base24.model.Base24Message;
import com.commercial.cards.base24.model.MessageType;
import com.commercial.cards.base24.model.RecordType;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.function.Predicate;

@Component
public class MessageFilter {

    public static final Predicate<Base24Message> IS_PTLFX =
            message -> message != null && RecordType.PTLFX == message.getRecordType();

    public static final Predicate<Base24Message> IS_ACTIONABLE =
            message -> message != null && message.getMessageType() != null && switch (message.getMessageType()) {
                case TVN, TCN, ACN -> true;
                default            -> false;
            };

    @SuppressWarnings("unchecked")
    public boolean shouldPublish(Base24Message message, Predicate<Base24Message>... predicates) {
        return predicates != null
                && Arrays.stream(predicates).allMatch(predicate -> predicate != null && predicate.test(message));
    }
}
