package com.commercial.cards.base24.pipeline;

import com.commercial.cards.base24.exception.TokenisationException;
import com.commercial.cards.base24.exception.TransactionSaveException;
import com.commercial.cards.base24.model.Base24Message;
import com.commercial.cards.base24.model.MessageType;
import com.commercial.cards.base24.model.ProcessingResult;
import com.commercial.cards.base24.model.RecordType;
import com.commercial.cards.base24.parser.Base24XmlParser;
import com.commercial.cards.base24.port.TokenisationPort;
import com.commercial.cards.base24.port.TransactionPort;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

class Base24MessagePipelineTest {

    // All deps injected via constructor — zero Spring context needed
    private Base24XmlParser    parser;
    private MessageFilter      filter;
    private TokenisationPort   tokenisationPort;
    private TransactionPort    transactionPort;

    private Base24MessagePipeline pipeline;

    @BeforeEach
    void setUp() {
        parser           = mock(Base24XmlParser.class);
        filter           = mock(MessageFilter.class);
        tokenisationPort = mock(TokenisationPort.class);
        transactionPort  = mock(TransactionPort.class);

        pipeline = new Base24MessagePipeline(parser, filter, tokenisationPort, transactionPort);
    }

    // ── Stage 1: Parse failures ───────────────────────────────────────────────

    @Test
    void shouldSkipWhenXmlCannotBeParsed() {
        when(parser.parse(any())).thenReturn(Optional.empty());

        ProcessingResult result = pipeline.process("<bad-xml/>");

        assertEquals(ProcessingResult.SKIPPED, result);
        verifyNoInteractions(filter, tokenisationPort, transactionPort);
    }

    // ── Stage 2: Filter ──────────────────────────────────────────────────────

    @Test
    void shouldSkipWhenRecordTypeIsNotPtlfx() {
        Base24Message msg = aMessage();
        when(parser.parse(any())).thenReturn(Optional.of(msg));
        whenShouldPublish(msg, false);

        ProcessingResult result = pipeline.process("<xml/>");

        assertEquals(ProcessingResult.SKIPPED, result);
        verifyNoInteractions(tokenisationPort, transactionPort);
    }

    @Test
    void shouldSkipWhenMessageTypeIsNotActionable() {
        Base24Message msg = aMessage();
        when(parser.parse(any())).thenReturn(Optional.of(msg));
        whenShouldPublish(msg, false);

        ProcessingResult result = pipeline.process("<xml/>");

        assertEquals(ProcessingResult.SKIPPED, result);
        verifyNoInteractions(tokenisationPort, transactionPort);
    }

    // ── Stage 3: Tokenisation failures ───────────────────────────────────────

    @Test
    void shouldRetryWhenTokenisationFails() {
        setupPassingFilter();
        when(tokenisationPort.tokenise(anyString()))
                .thenThrow(new TokenisationException("service unavailable"));

        ProcessingResult result = pipeline.process("<xml/>");

        assertEquals(ProcessingResult.RETRY, result);
        verifyNoInteractions(transactionPort);
    }

    // ── Stage 4: Save failures ────────────────────────────────────────────────

    @Test
    void shouldRetryWhenTransactionSaveFails() {
        setupPassingFilter();
        when(tokenisationPort.tokenise(anyString())).thenReturn("TOK-STUB-1111");
        doThrow(new TransactionSaveException("db timeout"))
                .when(transactionPort).save(any());

        ProcessingResult result = pipeline.process("<xml/>");

        assertEquals(ProcessingResult.RETRY, result);
    }

    // ── Happy path ────────────────────────────────────────────────────────────

    @Test
    void shouldReturnSuccessWhenAllStagesPass() {
        setupPassingFilter();
        when(tokenisationPort.tokenise(anyString())).thenReturn("TOK-STUB-1111");

        ProcessingResult result = pipeline.process("<xml/>");

        assertEquals(ProcessingResult.SUCCESS, result);
        verify(transactionPort).save(any());
    }

    @ParameterizedTest
    @EnumSource(value = MessageType.class, names = {"TVN", "TCN", "ACN"})
    void shouldSucceedForAllActionableMessageTypes(MessageType type) {
        Base24Message msg = Base24Message.builder()
                .messageType(type)
                .recordType(RecordType.PTLFX)
                .transactionId("TXN-" + type)
                .digitalPan("4111111111111111")
                .build();

        when(parser.parse(any())).thenReturn(Optional.of(msg));
        whenShouldPublish(msg, true);
        when(tokenisationPort.tokenise(anyString())).thenReturn("TOK-STUB-1111");

        ProcessingResult result = pipeline.process("<xml/>");

        assertEquals(ProcessingResult.SUCCESS, result);
    }

    @Test
    void shouldNotCallSaveWhenTokenisedPanIsObtained_thenSaveReceivesTokenisedPan() {
        setupPassingFilter();
        when(tokenisationPort.tokenise("4111111111111111")).thenReturn("TOK-STUB-1111");

        pipeline.process("<xml/>");

        verify(transactionPort).save(argThat(req ->
                "TOK-STUB-1111".equals(req.tokenisedPan()) &&
                "TVN".equals(req.messageType()) &&
                "TXN-001".equals(req.transactionId())
        ));
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private void setupPassingFilter() {
        Base24Message msg = aMessage();
        when(parser.parse(any())).thenReturn(Optional.of(msg));
        whenShouldPublish(msg, true);
    }

    private void whenShouldPublish(Base24Message msg, boolean result) {
        when(filter.shouldPublish(msg, MessageFilter.IS_PTLFX, MessageFilter.IS_ACTIONABLE)).thenReturn(result);
    }

    private Base24Message aMessage() {
        return Base24Message.builder()
                .messageType(MessageType.TVN)
                .recordType(RecordType.PTLFX)
                .transactionId("TXN-001")
                .digitalPan("4111111111111111")
                .build();
    }
}
