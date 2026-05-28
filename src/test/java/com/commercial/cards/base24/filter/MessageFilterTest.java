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

    @Test
    void shouldReturnFalseWhenMessageIsNull() {
        assertFalse(filter.shouldPublish(null, MessageFilter.IS_PTLFX));
    }

    @Test
    void shouldPassPtlfxRecords() {
        assertTrue(filter.shouldPublish(messageWith(RecordType.PTLFX), MessageFilter.IS_PTLFX));
    }

    @Test
    void shouldRejectNonPtlfxRecords() {
        assertFalse(filter.shouldPublish(messageWith(RecordType.OTHER), MessageFilter.IS_PTLFX));
    }

    @Test
    void shouldRejectNullRecordType() {
        Base24Message msg = Base24Message.builder().recordType(null).build();
        assertFalse(filter.shouldPublish(msg, MessageFilter.IS_PTLFX));
    }

    @Test
    void shouldReturnFalseWhenNullMessageIsTestedAgainstActionable() {
        assertFalse(filter.shouldPublish(null, MessageFilter.IS_ACTIONABLE));
    }

    @Test
    void shouldReturnFalseWhenMessageTypeIsNull() {
        Base24Message msg = Base24Message.builder().build();
        assertFalse(filter.shouldPublish(msg, MessageFilter.IS_ACTIONABLE));
    }

    @ParameterizedTest
    @EnumSource(value = MessageType.class, names = {"TVN", "TCN", "ACN"})
    void shouldAllowActionableMessageTypes(MessageType type) {
        assertTrue(filter.shouldPublish(messageWith(type), MessageFilter.IS_ACTIONABLE));
    }

    @ParameterizedTest
    @EnumSource(value = MessageType.class, names = {"TAR", "UNKNOWN"})
    void shouldBlockNonActionableMessageTypes(MessageType type) {
        assertFalse(filter.shouldPublish(messageWith(type), MessageFilter.IS_ACTIONABLE));
    }

    @Test
    void shouldPublishOnlyWhenAllPredicatesPass() {
        Base24Message msg = Base24Message.builder()
                .recordType(RecordType.PTLFX)
                .messageType(MessageType.TVN)
                .build();

        assertTrue(filter.shouldPublish(msg, MessageFilter.IS_PTLFX, MessageFilter.IS_ACTIONABLE));
    }

    @Test
    void shouldRejectWhenAnyPredicateFails() {
        Base24Message msg = Base24Message.builder()
                .recordType(RecordType.OTHER)
                .messageType(MessageType.TVN)
                .build();

        assertFalse(filter.shouldPublish(msg, MessageFilter.IS_PTLFX, MessageFilter.IS_ACTIONABLE));
    }

    // Helpers

    private Base24Message messageWith(RecordType recordType) {
        return Base24Message.builder().recordType(recordType).build();
    }

    private Base24Message messageWith(MessageType messageType) {
        return Base24Message.builder().messageType(messageType).build();
    }
}
