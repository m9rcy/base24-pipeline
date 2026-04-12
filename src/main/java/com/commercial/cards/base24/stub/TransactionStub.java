package com.commercial.cards.base24.stub;

import com.commercial.cards.base24.dto.SaveTransactionRequest;
import com.commercial.cards.base24.port.TransactionPort;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

/**
 * Stub implementation of {@link TransactionPort} for local development.
 *
 * <p>Active when {@code spring.profiles.active=stub}.
 * Logs the save request and returns successfully without making any HTTP calls.</p>
 */
@Slf4j
@Component
@Profile("stub")
public class TransactionStub implements TransactionPort {

    @Override
    public void save(SaveTransactionRequest request) {
        log.info("[STUB] Saving transaction [type={} txn={} tokenisedPan={} amount={} {}]",
                request.messageType(),
                request.transactionId(),
                request.tokenisedPan(),
                request.amount(),
                request.currencyCode());
    }
}
