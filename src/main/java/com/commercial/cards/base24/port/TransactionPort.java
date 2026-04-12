package com.commercial.cards.base24.port;

import com.commercial.cards.base24.dto.SaveTransactionRequest;

public interface TransactionPort {

    /**
     * Persist a processed transaction event via the transactions API.
     *
     * @param request the fully populated save request (with tokenised PAN)
     * @throws com.commercial.cards.base24.exception.TransactionSaveException on failure
     */
    void save(SaveTransactionRequest request);
}
