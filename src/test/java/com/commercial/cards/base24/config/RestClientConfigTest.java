package com.commercial.cards.base24.config;

import com.commercial.cards.base24.consumer.AbstractKafkaConsumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;
import org.springframework.http.HttpMethod;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.RestClient;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.springframework.test.web.client.ExpectedCount.once;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.header;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.method;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess;

class RestClientConfigTest {

    @AfterEach
    void tearDown() {
        MDC.clear();
    }

    @Test
    void shouldPropagateTraceIdFromMdcToOutgoingRequests() {
        MDC.put(AbstractKafkaConsumer.TRACE_ID_MDC_KEY, "trace-123");
        RestClient.Builder builder = new RestClientConfig().addTraceIdInterceptor(RestClient.builder());
        MockRestServiceServer server = MockRestServiceServer.bindTo(builder).build();
        RestClient restClient = builder.build();
        server.expect(once(), requestTo("http://downstream.example/test"))
                .andExpect(method(HttpMethod.GET))
                .andExpect(header(AbstractKafkaConsumer.TRACE_ID_MDC_KEY, "trace-123"))
                .andRespond(withSuccess());

        restClient.get()
                .uri("http://downstream.example/test")
                .retrieve()
                .toBodilessEntity();

        server.verify();
    }

    @Test
    void shouldNotAddTraceIdHeaderWhenMdcHasNoEntry() {
        // MDC is empty — no trace-id key at all
        RestClient.Builder builder = new RestClientConfig().addTraceIdInterceptor(RestClient.builder());
        MockRestServiceServer server = MockRestServiceServer.bindTo(builder).build();
        RestClient restClient = builder.build();
        server.expect(once(), requestTo("http://downstream.example/test"))
                .andExpect(method(HttpMethod.GET))
                .andExpect(request -> assertNull(
                        request.getHeaders().getFirst(AbstractKafkaConsumer.TRACE_ID_MDC_KEY),
                        "trace-id header must be absent when MDC has no entry"))
                .andRespond(withSuccess());

        restClient.get().uri("http://downstream.example/test").retrieve().toBodilessEntity();

        server.verify();
    }

    @Test
    void shouldNotAddTraceIdHeaderWhenMdcValueIsBlank() {
        MDC.put(AbstractKafkaConsumer.TRACE_ID_MDC_KEY, "   ");
        RestClient.Builder builder = new RestClientConfig().addTraceIdInterceptor(RestClient.builder());
        MockRestServiceServer server = MockRestServiceServer.bindTo(builder).build();
        RestClient restClient = builder.build();
        server.expect(once(), requestTo("http://downstream.example/test"))
                .andExpect(method(HttpMethod.GET))
                .andExpect(request -> assertNull(
                        request.getHeaders().getFirst(AbstractKafkaConsumer.TRACE_ID_MDC_KEY),
                        "trace-id header must be absent when MDC value is blank"))
                .andRespond(withSuccess());

        restClient.get().uri("http://downstream.example/test").retrieve().toBodilessEntity();

        server.verify();
    }
}
