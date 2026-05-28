package com.commercial.cards.base24.dedupe;

import com.commercial.cards.base24.model.Base24Message;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Optional;

@Component
public class Base24EventFingerprint implements EventFingerprint<Base24Message> {

    static final String DOMAIN = "base24-transaction";
    static final String VERSION = "v1";

    @Override
    public String domain() {
        return DOMAIN;
    }

    @Override
    public String key(Base24Message dto) {
        return normalize(dto.getTransactionId());
    }

    @Override
    public String version() {
        return VERSION;
    }

    @Override
    public Object fingerprint(Base24Message dto) {
        return new Base24TransactionFingerprint(
                dto.getMessageType() == null ? null : dto.getMessageType().name(),
                dto.getRecordType() == null ? null : dto.getRecordType().name(),
                normalize(dto.getTransactionId()),
                normalize(dto.getDigitalPan()),
                normalize(dto.getAmount()),
                normalizeUpper(dto.getCurrencyCode()),
                normalize(dto.getResponseCode())
        );
    }

    @Override
    public Optional<LocalDateTime> eventTime(Base24Message dto) {
        return Optional.ofNullable(dto.getTimestamp());
    }

    private String normalize(String value) {
        return value == null ? null : value.trim();
    }

    private String normalizeUpper(String value) {
        String normalized = normalize(value);
        return normalized == null ? null : normalized.toUpperCase();
    }

    private BigDecimal normalize(BigDecimal value) {
        return value == null ? null : value.stripTrailingZeros();
    }
}
