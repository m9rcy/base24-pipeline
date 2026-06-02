package com.commercial.cards.base24.dedupe;

import jakarta.persistence.Column;
import jakarta.persistence.EmbeddedId;
import jakarta.persistence.Entity;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.LocalDateTime;

@Entity
@Table(name = "event_deduplication")
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class EventDeduplicationEntity {

    @EmbeddedId
    private EventDeduplicationId id;

    @Column(name = "hash_version", nullable = false, length = 20)
    private String hashVersion;

    @Column(name = "last_hash", nullable = false, length = 128)
    private String lastHash;

    @Column(name = "last_event_time", columnDefinition = "TIMESTAMPTZ")
    private LocalDateTime lastEventTime;

    @Column(name = "updated_at", nullable = false, columnDefinition = "TIMESTAMPTZ")
    private LocalDateTime updatedAt;

    public static EventDeduplicationEntity of(
            EventDeduplicationId id,
            String hashVersion,
            String lastHash,
            LocalDateTime lastEventTime,
            LocalDateTime updatedAt
    ) {
        return EventDeduplicationEntity.builder()
                .id(id)
                .hashVersion(hashVersion)
                .lastHash(lastHash)
                .lastEventTime(lastEventTime)
                .updatedAt(updatedAt)
                .build();
    }
}
