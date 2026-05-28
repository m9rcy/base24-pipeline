package com.commercial.cards.base24.dedupe;

import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

@Slf4j
public class JdbcDeduplicationService<D> implements DeduplicationService<D> {

    private static final String TABLE_NAME = "event_deduplication";

    private final JdbcTemplate jdbcTemplate;
    private final EventFingerprint<D> eventFingerprint;
    private final FingerprintHasher fingerprintHasher;

    public JdbcDeduplicationService(
            JdbcTemplate jdbcTemplate,
            EventFingerprint<D> eventFingerprint,
            FingerprintHasher fingerprintHasher
    ) {
        this.jdbcTemplate = jdbcTemplate;
        this.eventFingerprint = eventFingerprint;
        this.fingerprintHasher = fingerprintHasher;
    }

    public void initializeSchema() {
        jdbcTemplate.execute("""
                create table if not exists event_deduplication (
                    domain varchar(100) not null,
                    dedupe_key varchar(255) not null,
                    hash_version varchar(20) not null,
                    last_hash varchar(128) not null,
                    last_event_time timestamp null,
                    updated_at timestamp not null,
                    primary key (domain, dedupe_key)
                )
                """);
    }

    @Override
    public boolean isConsumable(D dto) {
        String domain = eventFingerprint.domain();
        String key = eventFingerprint.key(dto);
        String currentHash = currentHash(dto);
        Optional<LocalDateTime> incomingEventTime = eventFingerprint.eventTime(dto);

        Optional<DedupeRow> existing = findExisting(domain, key);
        if (existing.isEmpty()) {
            return true;
        }

        DedupeRow row = existing.get();
        if (isStale(incomingEventTime, row.lastEventTime())) {
            log.info("Skipping stale event [domain={} key={} incomingEventTime={} lastEventTime={}]",
                    domain, key, incomingEventTime.orElse(null), row.lastEventTime());
            return false;
        }

        if (currentHash.equals(row.lastHash())) {
            advanceEventTimeWhenNewer(domain, key, incomingEventTime, row.lastEventTime());
            log.info("Skipping duplicate event [domain={} key={}]", domain, key);
            return false;
        }

        return true;
    }

    @Override
    public void markProcessed(D dto) {
        String domain = eventFingerprint.domain();
        String key = eventFingerprint.key(dto);
        String version = eventFingerprint.version();
        String hash = currentHash(dto);
        LocalDateTime eventTime = eventFingerprint.eventTime(dto).orElse(null);

        jdbcTemplate.update("""
                insert into event_deduplication (
                    domain, dedupe_key, hash_version, last_hash, last_event_time, updated_at
                )
                values (?, ?, ?, ?, ?, current_timestamp)
                on conflict (domain, dedupe_key)
                do update set
                    hash_version = excluded.hash_version,
                    last_hash = excluded.last_hash,
                    last_event_time = coalesce(excluded.last_event_time, event_deduplication.last_event_time),
                    updated_at = current_timestamp
                """,
                domain,
                key,
                version,
                hash,
                eventTime == null ? null : Timestamp.valueOf(eventTime));
    }

    private String currentHash(D dto) {
        return fingerprintHasher.hash(eventFingerprint.fingerprint(dto));
    }

    private Optional<DedupeRow> findExisting(String domain, String key) {
        List<DedupeRow> rows = jdbcTemplate.query("""
                        select last_hash, last_event_time
                        from event_deduplication
                        where domain = ? and dedupe_key = ?
                        """,
                (rs, rowNum) -> new DedupeRow(
                        rs.getString("last_hash"),
                        rs.getTimestamp("last_event_time") == null
                                ? null
                                : rs.getTimestamp("last_event_time").toLocalDateTime()
                ),
                domain,
                key);

        return rows.stream().findFirst();
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

        jdbcTemplate.update("""
                update event_deduplication
                set last_event_time = ?, updated_at = current_timestamp
                where domain = ? and dedupe_key = ?
                """,
                Timestamp.valueOf(incoming),
                domain,
                key);
    }

    private record DedupeRow(String lastHash, LocalDateTime lastEventTime) {
    }
}
