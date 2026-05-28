package com.commercial.cards.base24.config;

import com.commercial.cards.base24.consumer.AbstractKafkaConsumer;
import org.slf4j.MDC;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.web.client.RestClient;

@Configuration
public class RestClientConfig {

    /**
     * Shared RestClient bean — adapters inject this.
     * Only needed when NOT on the stub profile (stubs make no HTTP calls).
     */
    @Bean
    @Profile("!stub")
    public RestClient restClient() {
        return addTraceIdInterceptor(RestClient.builder())
                .build();
    }

    RestClient.Builder addTraceIdInterceptor(RestClient.Builder builder) {
        return builder
                .requestInterceptor((request, body, execution) -> {
                    String traceId = MDC.get(AbstractKafkaConsumer.TRACE_ID_MDC_KEY);
                    if (traceId != null && !traceId.isBlank()) {
                        request.getHeaders().set(AbstractKafkaConsumer.TRACE_ID_MDC_KEY, traceId);
                    }
                    return execution.execute(request, body);
                });
    }
}
