package com.commercial.cards.base24.filter;

import com.commercial.cards.base24.model.Base24Message;
import com.commercial.cards.base24.model.MessageType;
import com.commercial.cards.base24.model.RecordType;
import com.commercial.cards.base24.pipeline.MessageFilter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MessageFilterTest {

    // No mocks, no Spring — pure unit test
    private final MessageFilter filter = new MessageFilter();

    // ── isPtlfx ──────────────────────────────────────────────────────────────

    @Test
    void shouldPassPtlfxRecords() {
        assertTrue(filter.isPtlfx(messageWith(RecordType.PTLFX)));
    }

    @Test
    void shouldRejectNonPtlfxRecords() {
        assertFalse(filter.isPtlfx(messageWith(RecordType.OTHER)));
    }

    @Test
    void shouldRejectNullRecordType() {
        Base24Message msg = Base24Message.builder().recordType(null).build();
        assertFalse(filter.isPtlfx(msg));
    }

    // ── isActionable ─────────────────────────────────────────────────────────

    @ParameterizedTest
    @EnumSource(value = MessageType.class, names = {"TVN", "TCN", "ACN"})
    void shouldAllowActionableMessageTypes(MessageType type) {
        assertTrue(filter.isActionable(messageWith(type)));
    }

    @ParameterizedTest
    @EnumSource(value = MessageType.class, names = {"TAR", "UNKNOWN"})
    void shouldBlockNonActionableMessageTypes(MessageType type) {
        assertFalse(filter.isActionable(messageWith(type)));
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    private Base24Message messageWith(RecordType recordType) {
        return Base24Message.builder().recordType(recordType).build();
    }

    private Base24Message messageWith(MessageType messageType) {
        return Base24Message.builder().messageType(messageType).build();
    }
}
