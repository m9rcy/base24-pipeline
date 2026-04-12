package com.commercial.cards.base24.adapter;

import com.commercial.cards.base24.dto.SaveTransactionRequest;
import com.commercial.cards.base24.exception.TransactionSaveException;
import com.commercial.cards.base24.port.TransactionPort;
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
public class TransactionAdapter implements TransactionPort {

    private final RestClient restClient;

    @Value("${base24.services.transaction-url}")
    private String baseUrl;

    @CircuitBreaker(name = "transaction", fallbackMethod = "saveFallback")
    @Retry(name = "transaction")
    @Override
    public void save(SaveTransactionRequest request) {
        log.debug("Calling transaction save service [txn={}]", request.transactionId());

        restClient.post()
                .uri(baseUrl + "/transactions/save")
                .contentType(MediaType.APPLICATION_JSON)
                .body(request)
                .retrieve()
                .toBodilessEntity();

        log.debug("Transaction save successful [txn={}]", request.transactionId());
    }

    // Fallback re-throws so the pipeline returns RETRY and Kafka does not ack
    private void saveFallback(SaveTransactionRequest request, Exception ex) {
        log.error("Transaction save circuit open or retries exhausted [txn={} reason={}]",
                request.transactionId(), ex.getMessage());
        throw new TransactionSaveException("Transaction save unavailable", ex);
    }
}
