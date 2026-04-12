package com.commercial.cards.base24.pipeline;

import com.commercial.cards.base24.model.Base24Message;
import com.commercial.cards.base24.model.MessageType;
import com.commercial.cards.base24.model.RecordType;
import org.springframework.stereotype.Component;

@Component
public class MessageFilter {

    /**
     * Returns true only if this message is a PTLFX record type.
     */
    public boolean isPtlfx(Base24Message message) {
        return RecordType.PTLFX == message.getRecordType();
    }

    /**
     * Returns true for message types we act on: TVN, TCN, ACN.
     * TAR and UNKNOWN are not actionable and will be discarded.
     */
    public boolean isActionable(Base24Message message) {
        return switch (message.getMessageType()) {
            case TVN, TCN, ACN -> true;
            default            -> false;
        };
    }
}
