package com.commercial.cards.base24.pipeline;

import com.commercial.cards.base24.dto.SaveTransactionRequest;
import com.commercial.cards.base24.model.Base24Message;
import com.commercial.cards.base24.model.MessageType;
import com.commercial.cards.base24.model.ProcessingResult;
import com.commercial.cards.base24.model.RecordType;
import com.commercial.cards.base24.stub.TokenisationStub;
import com.commercial.cards.base24.stub.TransactionStub;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Integration-style smoke test for the stub profile.
 * Wires the real stubs into the pipeline to verify the full flow
 * without any Spring context or real HTTP calls.
 */
class StubPipelineSmokeTest {

    @Test
    void shouldProcessFullPipelineEndToEndWithStubs() {
        // Arrange — real stubs, mock parser only (XML not the focus here)
        var parser        = mock(com.commercial.cards.base24.parser.Base24XmlParser.class);
        var filter        = new MessageFilter();
        var tokenisation  = new TokenisationStub();
        var transaction   = new TransactionStub();

        Base24Message message = Base24Message.builder()
                .messageType(MessageType.TVN)
                .recordType(RecordType.PTLFX)
                .transactionId("TXN-SMOKE-001")
                .digitalPan("4111111111111111")
                .amount(new BigDecimal("250.00"))
                .currencyCode("NZD")
                .responseCode("00")
                .build();

        when(parser.parse(any())).thenReturn(Optional.of(message));

        var pipeline = new Base24MessagePipeline(parser, filter, tokenisation, transaction);

        // Act
        ProcessingResult result = pipeline.process("<xml/>");

        // Assert
        assertEquals(ProcessingResult.SUCCESS, result);
    }

    @Test
    void shouldSkipTarMessageWithRealFilter() {
        var parser       = mock(com.commercial.cards.base24.parser.Base24XmlParser.class);
        var filter       = new MessageFilter();
        var tokenisation = new TokenisationStub();
        var transaction  = new TransactionStub();

        Base24Message tarMessage = Base24Message.builder()
                .messageType(MessageType.TAR)
                .recordType(RecordType.PTLFX)
                .transactionId("TXN-TAR-001")
                .digitalPan("4111111111111111")
                .build();

        when(parser.parse(any())).thenReturn(Optional.of(tarMessage));

        var pipeline = new Base24MessagePipeline(parser, filter, tokenisation, transaction);

        assertEquals(ProcessingResult.SKIPPED, pipeline.process("<xml/>"));
    }
}
