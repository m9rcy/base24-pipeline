package com.commercial.cards.base24.processor;

import com.commercial.cards.base24.dto.SaveTransactionRequest;
import com.commercial.cards.base24.model.Base24Message;
import com.commercial.cards.base24.orchestration.BaseProcessor;
import com.commercial.cards.base24.port.TokenisationPort;
import com.commercial.cards.base24.port.TransactionPort;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class TokeniseAndSaveTransactionProcessor extends BaseProcessor<Base24Message> {

    private final TokenisationPort tokenisationPort;
    private final TransactionPort transactionPort;

    @Override
    public boolean supports(Base24Message dto) {
        return dto != null;
    }

    @Override
    protected void doProcess(Base24Message dto) {
        String tokenisedPan = tokenisationPort.tokenise(dto.getDigitalPan());
        log.debug("Tokenisation successful [txn={}]", dto.getTransactionId());

        transactionPort.save(SaveTransactionRequest.from(dto, tokenisedPan));
        log.info("Successfully processed [{} txn={}]", dto.getMessageType(), dto.getTransactionId());
    }
}
