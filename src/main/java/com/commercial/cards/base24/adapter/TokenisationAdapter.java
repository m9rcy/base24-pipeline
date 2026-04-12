package com.commercial.cards.base24.adapter;

import com.commercial.cards.base24.exception.TokenisationException;
import com.commercial.cards.base24.port.TokenisationPort;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.retry.annotation.Retry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

@Slf4j
@Component
@Profile("!stub")
@RequiredArgsConstructor
public class TokenisationAdapter implements TokenisationPort {

    private final RestClient restClient;

    @Value("${base24.services.tokenisation-url}")
    private String baseUrl;

    @CircuitBreaker(name = "tokenisation", fallbackMethod = "tokeniseFallback")
    @Retry(name = "tokenisation")
    @Override
    public String tokenise(String digitalPan) {
        log.debug("Calling tokenisation service for PAN ending in {}",
                maskPan(digitalPan));

        TokeniseResponse response = restClient.post()
                .uri(baseUrl + "/cards/tokenise")
                .contentType(MediaType.APPLICATION_JSON)
                .body(new TokeniseRequest(digitalPan))
                .retrieve()
                .body(TokeniseResponse.class);

        if (response == null || response.tokenisedPan() == null) {
            throw new TokenisationException("Null or empty response from tokenisation service");
        }

        return response.tokenisedPan();
    }

    // Fallback re-throws so the pipeline returns RETRY and Kafka does not ack
    private String tokeniseFallback(String digitalPan, Exception ex) {
        log.error("Tokenisation circuit open or retries exhausted [reason={}]", ex.getMessage());
        throw new TokenisationException("Tokenisation unavailable", ex);
    }

    private String maskPan(String pan) {
        if (pan == null || pan.length() < 4) return "****";
        return "*".repeat(pan.length() - 4) + pan.substring(pan.length() - 4);
    }

    record TokeniseRequest(String digitalPan) {}
    record TokeniseResponse(String tokenisedPan) {}
}
