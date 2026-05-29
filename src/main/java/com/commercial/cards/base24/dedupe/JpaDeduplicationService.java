package com.commercial.cards.base24.dedupe;

import lombok.extern.slf4j.Slf4j;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.Optional;

@Slf4j
public class JpaDeduplicationService<D> implements DeduplicationService<D> {

    private final EventDeduplicationRepository repository;
    private final EventFingerprint<D> eventFingerprint;
    private final FingerprintHasher fingerprintHasher;

    public JpaDeduplicationService(
            EventDeduplicationRepository repository,
            EventFingerprint<D> eventFingerprint,
            FingerprintHasher fingerprintHasher
    ) {
        this.repository = repository;
        this.eventFingerprint = eventFingerprint;
        this.fingerprintHasher = fingerprintHasher;
    }

    @Override
    @Transactional
    public boolean isConsumable(D dto) {
        String domain = eventFingerprint.domain();
        String key = eventFingerprint.key(dto);
        String currentHash = currentHash(dto);
        Optional<LocalDateTime> incomingEventTime = eventFingerprint.eventTime(dto);

        Optional<EventDeduplicationEntity> existing = repository.findById(new EventDeduplicationId(domain, key));
        if (existing.isEmpty()) {
            return true;
        }

        EventDeduplicationEntity row = existing.get();
        if (isStale(incomingEventTime, row.getLastEventTime())) {
            log.info("Skipping stale event [domain={} key={} incomingEventTime={} lastEventTime={}]",
                    domain, key, incomingEventTime.orElse(null), row.getLastEventTime());
            return false;
        }

        if (currentHash.equals(row.getLastHash())) {
            advanceEventTimeWhenNewer(domain, key, incomingEventTime, row.getLastEventTime());
            log.info("Skipping duplicate event [domain={} key={}]", domain, key);
            return false;
        }

        return true;
    }

    @Override
    @Transactional
    public void markProcessed(D dto) {
        String domain = eventFingerprint.domain();
        String key = eventFingerprint.key(dto);
        String version = eventFingerprint.version();
        String hash = currentHash(dto);
        LocalDateTime eventTime = eventFingerprint.eventTime(dto).orElse(null);

        repository.upsert(domain, key, version, hash, eventTime);
    }

    private String currentHash(D dto) {
        return fingerprintHasher.hash(eventFingerprint.fingerprint(dto));
    }

    private boolean isStale(Optional<LocalDateTime> incomingEventTime, LocalDateTime existingEventTime) {
        return incomingEventTime.isPresent()
                && existingEventTime != null
                && incomingEventTime.get().isBefore(existingEventTime);
    }

    private void advanceEventTimeWhenNewer(
            String domain,
            String key,
            Optional<LocalDateTime> incomingEventTime,
            LocalDateTime existingEventTime
    ) {
        if (incomingEventTime.isEmpty()) {
            return;
        }

        LocalDateTime incoming = incomingEventTime.get();
        if (existingEventTime != null && !incoming.isAfter(existingEventTime)) {
            return;
        }

        repository.updateEventTime(domain, key, incoming);
    }
}
