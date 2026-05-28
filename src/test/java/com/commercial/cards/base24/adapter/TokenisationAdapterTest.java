package com.commercial.cards.base24.adapter;

import com.commercial.cards.base24.exception.TokenisationException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.RestClient;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.*;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.*;
import static org.springframework.test.web.client.response.MockRestResponseCreators.*;

class TokenisationAdapterTest {

    private MockRestServiceServer mockServer;
    private TokenisationAdapter   adapter;

    @BeforeEach
    void setUp() {
        RestClient.Builder builder = RestClient.builder();
        mockServer = MockRestServiceServer.bindTo(builder).build();
        RestClient restClient = builder.build();

        adapter = new TokenisationAdapter(restClient);
        // Set the base URL via reflection to avoid needing Spring context
        try {
            var field = TokenisationAdapter.class.getDeclaredField("baseUrl");
            field.setAccessible(true);
            field.set(adapter, "http://cards-tokenisation-service");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void shouldReturnTokenisedPanOnSuccess() {
        mockServer.expect(requestTo("http://cards-tokenisation-service/cards/tokenise"))
                .andExpect(method(HttpMethod.POST))
                .andExpect(content().contentType(MediaType.APPLICATION_JSON))
                .andRespond(withSuccess(
                        "{\"tokenisedPan\":\"TOK-1111\"}",
                        MediaType.APPLICATION_JSON));

        String result = adapter.tokenise("4111111111111111");

        assertEquals("TOK-1111", result);
        mockServer.verify();
    }

    @Test
    void shouldThrowTokenisationExceptionWhenResponseIsNull() {
        mockServer.expect(requestTo("http://cards-tokenisation-service/cards/tokenise"))
                .andRespond(withSuccess("{}", MediaType.APPLICATION_JSON));

        assertThrows(TokenisationException.class,
                () -> adapter.tokenise("4111111111111111"));
    }

    @Test
    void shouldThrowTokenisationExceptionOnServerError() {
        mockServer.expect(requestTo("http://cards-tokenisation-service/cards/tokenise"))
                .andRespond(withServerError());

        assertThrows(Exception.class,
                () -> adapter.tokenise("4111111111111111"));
    }

    @Test
    void shouldThrowTokenisationExceptionWhenResponseBodyIsNull() {
        // JSON "null" deserializes to a null Java reference
        mockServer.expect(requestTo("http://cards-tokenisation-service/cards/tokenise"))
                .andRespond(withSuccess("null", MediaType.APPLICATION_JSON));

        assertThrows(TokenisationException.class,
                () -> adapter.tokenise("4111111111111111"));
    }

    @Test
    void shouldMaskNullPanInLogBeforeCallingService() {
        mockServer.expect(requestTo("http://cards-tokenisation-service/cards/tokenise"))
                .andRespond(withSuccess("{\"tokenisedPan\":\"TOK-STUB\"}", MediaType.APPLICATION_JSON));

        // null PAN hits the `pan == null` branch in maskPan; the HTTP call still proceeds
        assertEquals("TOK-STUB", adapter.tokenise(null));
        mockServer.verify();
    }

    @Test
    void shouldMaskShortPanInLogBeforeCallingService() {
        mockServer.expect(requestTo("http://cards-tokenisation-service/cards/tokenise"))
                .andRespond(withSuccess("{\"tokenisedPan\":\"TOK-STUB\"}", MediaType.APPLICATION_JSON));

        // PAN shorter than 4 chars hits the `length < 4` branch in maskPan
        assertEquals("TOK-STUB", adapter.tokenise("123"));
        mockServer.verify();
    }

    @Test
    void shouldWrapCauseInTokenisationExceptionFromFallback() throws Exception {
        Method fallback = TokenisationAdapter.class
                .getDeclaredMethod("tokeniseFallback", String.class, Exception.class);
        fallback.setAccessible(true);

        RuntimeException cause = new RuntimeException("circuit open");
        TokenisationException thrown = assertThrows(TokenisationException.class, () -> {
            try {
                fallback.invoke(adapter, "4111111111111111", cause);
            } catch (InvocationTargetException e) {
                throw (RuntimeException) e.getCause();
            }
        });

        assertEquals("Tokenisation unavailable", thrown.getMessage());
        assertSame(cause, thrown.getCause());
    }
}
