package com.commercial.cards.base24.adapter;

import com.commercial.cards.base24.dto.SaveTransactionRequest;
import com.commercial.cards.base24.exception.TransactionSaveException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.RestClient;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.*;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.*;
import static org.springframework.test.web.client.response.MockRestResponseCreators.*;

class TransactionAdapterTest {

    private MockRestServiceServer mockServer;
    private TransactionAdapter    adapter;

    @BeforeEach
    void setUp() {
        RestClient.Builder builder = RestClient.builder();
        mockServer = MockRestServiceServer.bindTo(builder).build();
        RestClient restClient = builder.build();

        adapter = new TransactionAdapter(restClient);
        try {
            var field = TransactionAdapter.class.getDeclaredField("baseUrl");
            field.setAccessible(true);
            field.set(adapter, "http://transactions-service");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void shouldSaveSuccessfullyOnOkResponse() {
        mockServer.expect(requestTo("http://transactions-service/transactions/save"))
                .andExpect(method(HttpMethod.POST))
                .andExpect(content().contentType(MediaType.APPLICATION_JSON))
                .andRespond(withSuccess());

        assertDoesNotThrow(() -> adapter.save(aRequest()));
        mockServer.verify();
    }

    @Test
    void shouldThrowOnServerError() {
        mockServer.expect(requestTo("http://transactions-service/transactions/save"))
                .andRespond(withServerError());

        assertThrows(Exception.class, () -> adapter.save(aRequest()));
    }

    @Test
    void shouldWrapCauseInTransactionSaveExceptionFromFallback() throws Exception {
        Method fallback = TransactionAdapter.class
                .getDeclaredMethod("saveFallback", SaveTransactionRequest.class, Exception.class);
        fallback.setAccessible(true);

        RuntimeException cause = new RuntimeException("circuit open");
        TransactionSaveException thrown = assertThrows(TransactionSaveException.class, () -> {
            try {
                fallback.invoke(adapter, aRequest(), cause);
            } catch (InvocationTargetException e) {
                throw (RuntimeException) e.getCause();
            }
        });

        assertEquals("Transaction save unavailable", thrown.getMessage());
        assertSame(cause, thrown.getCause());
    }

    private SaveTransactionRequest aRequest() {
        return SaveTransactionRequest.of(
                "TVN",
                "TXN-001",
                "TOK-STUB-1111",
                new BigDecimal("123.45"),
                "NZD",
                "00",
                "2024-01-15T10:30:00"
        );
    }
}
