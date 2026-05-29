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
    @Transactional(readOnly = true)
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

        if (!row.getHashVersion().equals(eventFingerprint.version())) {
            log.warn("Hash version changed, re-processing [domain={} key={} stored={} current={}]",
                    domain, key, row.getHashVersion(), eventFingerprint.version());
            return true;
        }

        if (isStale(incomingEventTime, row.getLastEventTime())) {
            log.info("Skipping stale event [domain={} key={} incomingEventTime={} lastEventTime={}]",
                    domain, key, incomingEventTime.orElse(null), row.getLastEventTime());
            return false;
        }

        if (currentHash.equals(row.getLastHash())) {
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

    @Override
    @Transactional
    public void afterRejected(D dto) {
        eventFingerprint.eventTime(dto).ifPresent(incomingTime -> {
            String domain = eventFingerprint.domain();
            String key = eventFingerprint.key(dto);
            repository.advanceEventTimeIfNewer(domain, key, incomingTime);
        });
    }

    private String currentHash(D dto) {
        return fingerprintHasher.hash(eventFingerprint.fingerprint(dto));
    }

    private boolean isStale(Optional<LocalDateTime> incomingEventTime, LocalDateTime existingEventTime) {
        return incomingEventTime.isPresent()
                && existingEventTime != null
                && incomingEventTime.get().isBefore(existingEventTime);
    }
}
