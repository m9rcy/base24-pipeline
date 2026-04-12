package com.commercial.cards.base24.pipeline;

import com.commercial.cards.base24.dto.SaveTransactionRequest;
import com.commercial.cards.base24.exception.TokenisationException;
import com.commercial.cards.base24.exception.TransactionSaveException;
import com.commercial.cards.base24.model.Base24Message;
import com.commercial.cards.base24.model.ProcessingResult;
import com.commercial.cards.base24.parser.Base24XmlParser;
import com.commercial.cards.base24.port.TokenisationPort;
import com.commercial.cards.base24.port.TransactionPort;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Optional;

/**
 * Orchestrates the full Base24 EPS PTLFX message processing pipeline.
 *
 * <p>This class has zero Kafka imports — it returns a {@link ProcessingResult}
 * so the Kafka consumer owns the ack/no-ack decision.</p>
 *
 * <p>Stage order:</p>
 * <ol>
 *   <li>Parse XML → {@link Base24Message}</li>
 *   <li>Filter: PTLFX record type + actionable message type (TVN/TCN/ACN)</li>
 *   <li>Tokenise digital PAN via {@link TokenisationPort}</li>
 *   <li>Save event via {@link TransactionPort}</li>
 * </ol>
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class Base24MessagePipeline {

    private final Base24XmlParser  parser;
    private final MessageFilter    filter;
    private final TokenisationPort tokenisationPort;
    private final TransactionPort  transactionPort;

    public ProcessingResult process(String xmlData) {

        // ── Stage 1: Parse ────────────────────────────────────────────────
        Optional<Base24Message> parsed = parser.parse(xmlData);
        if (parsed.isEmpty()) {
            log.warn("Message could not be parsed — skipping (malformed XML)");
            return ProcessingResult.SKIPPED;
        }

        Base24Message message = parsed.get();

        // ── Stage 2: Filter ───────────────────────────────────────────────
        if (!filter.shouldPublish(message, MessageFilter.IS_PTLFX, MessageFilter.IS_ACTIONABLE)) {
            log.debug("Skipping non-publishable message [recordType={} msgType={}]",
                    message.getRecordType(), message.getMessageType());
            return ProcessingResult.SKIPPED;
        }

        // ── Stage 3: Tokenise ─────────────────────────────────────────────
        String tokenisedPan;
        try {
            tokenisedPan = tokenisationPort.tokenise(message.getDigitalPan());
            log.debug("Tokenisation successful [txn={}]", message.getTransactionId());
        } catch (TokenisationException e) {
            log.error("Tokenisation failed [txn={} reason={}] — will retry",
                    message.getTransactionId(), e.getMessage());
            return ProcessingResult.RETRY;
        }

        // ── Stage 4: Save ─────────────────────────────────────────────────
        try {
            transactionPort.save(SaveTransactionRequest.from(message, tokenisedPan));
            log.info("Successfully processed [{} txn={}]",
                    message.getMessageType(), message.getTransactionId());
            return ProcessingResult.SUCCESS;
        } catch (TransactionSaveException e) {
            log.error("Save failed [txn={} reason={}] — will retry",
                    message.getTransactionId(), e.getMessage());
            return ProcessingResult.RETRY;
        }
    }
}
