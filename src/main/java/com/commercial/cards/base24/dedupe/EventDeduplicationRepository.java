package com.commercial.cards.base24.dedupe;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.LocalDateTime;

public interface EventDeduplicationRepository extends JpaRepository<EventDeduplicationEntity, EventDeduplicationId> {

    @Modifying(clearAutomatically = true)
    @Query(nativeQuery = true, value = """
            insert into event_deduplication (domain, dedupe_key, hash_version, last_hash, last_event_time, updated_at)
            values (:domain, :dedupeKey, :hashVersion, :lastHash, :lastEventTime, current_timestamp)
            on conflict (domain, dedupe_key)
            do update set
                hash_version = excluded.hash_version,
                last_hash = excluded.last_hash,
                last_event_time = coalesce(excluded.last_event_time, event_deduplication.last_event_time),
                updated_at = current_timestamp
            """)
    void upsert(
            @Param("domain") String domain,
            @Param("dedupeKey") String dedupeKey,
            @Param("hashVersion") String hashVersion,
            @Param("lastHash") String lastHash,
            @Param("lastEventTime") LocalDateTime lastEventTime
    );

    @Modifying(clearAutomatically = true)
    @Query(nativeQuery = true, value = """
            update event_deduplication
            set last_event_time = :lastEventTime, updated_at = current_timestamp
            where domain = :domain and dedupe_key = :dedupeKey
            """)
    void updateEventTime(
            @Param("domain") String domain,
            @Param("dedupeKey") String dedupeKey,
            @Param("lastEventTime") LocalDateTime lastEventTime
    );
}
