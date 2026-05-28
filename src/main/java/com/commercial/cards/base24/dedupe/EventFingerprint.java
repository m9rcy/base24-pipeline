package com.commercial.cards.base24.dedupe;

import java.time.LocalDateTime;
import java.util.Optional;

public interface EventFingerprint<D> {

    String domain();

    String key(D dto);

    String version();

    Object fingerprint(D dto);

    default Optional<LocalDateTime> eventTime(D dto) {
        return Optional.empty();
    }
}
