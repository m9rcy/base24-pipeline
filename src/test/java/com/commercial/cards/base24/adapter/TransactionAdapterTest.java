package com.commercial.cards.base24.adapter;

import com.commercial.cards.base24.dto.SaveTransactionRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.RestClient;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
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

    private SaveTransactionRequest aRequest() {
        return new SaveTransactionRequest(
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
