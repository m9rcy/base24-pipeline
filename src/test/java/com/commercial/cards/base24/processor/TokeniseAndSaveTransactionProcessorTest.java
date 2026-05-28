package com.commercial.cards.base24.processor;

import com.commercial.cards.base24.exception.TokenisationException;
import com.commercial.cards.base24.exception.TransactionSaveException;
import com.commercial.cards.base24.model.Base24Message;
import com.commercial.cards.base24.model.MessageType;
import com.commercial.cards.base24.model.RecordType;
import com.commercial.cards.base24.port.TokenisationPort;
import com.commercial.cards.base24.port.TransactionPort;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.*;

class TokeniseAndSaveTransactionProcessorTest {

    private TokenisationPort tokenisationPort;
    private TransactionPort transactionPort;
    private TokeniseAndSaveTransactionProcessor processor;

    @BeforeEach
    void setUp() {
        tokenisationPort = mock(TokenisationPort.class);
        transactionPort = mock(TransactionPort.class);
        processor = new TokeniseAndSaveTransactionProcessor(tokenisationPort, transactionPort);
    }

    @Test
    void shouldSaveTransactionWithTokenisedPan() {
        when(tokenisationPort.tokenise("4111111111111111")).thenReturn("TOK-STUB-1111");

        processor.process(aMessage());

        verify(transactionPort).save(argThat(req ->
                "TOK-STUB-1111".equals(req.tokenisedPan()) &&
                "TVN".equals(req.messageType()) &&
                "TXN-001".equals(req.transactionId()) &&
                "00".equals(req.responseCode())
        ));
    }

    @Test
    void shouldRetryWhenTokenisationFails() {
        when(tokenisationPort.tokenise(anyString()))
                .thenThrow(new TokenisationException("service unavailable"));

        assertThrows(TokenisationException.class, () -> processor.process(aMessage()));

        verifyNoInteractions(transactionPort);
    }

    @Test
    void shouldRetryWhenTransactionSaveFails() {
        when(tokenisationPort.tokenise(anyString())).thenReturn("TOK-STUB-1111");
        doThrow(new TransactionSaveException("db timeout"))
                .when(transactionPort).save(any());

        assertThrows(TransactionSaveException.class, () -> processor.process(aMessage()));
    }

    private Base24Message aMessage() {
        return Base24Message.builder()
                .messageType(MessageType.TVN)
                .recordType(RecordType.PTLFX)
                .transactionId("TXN-001")
                .digitalPan("4111111111111111")
                .responseCode("00")
                .build();
    }
}
