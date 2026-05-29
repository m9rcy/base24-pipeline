package com.commercial.cards.base24.dedupe;

import jakarta.persistence.Column;
import jakarta.persistence.Embeddable;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Embeddable
@Getter
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
public class EventDeduplicationId implements Serializable {

    @Column(name = "domain", nullable = false, length = 100)
    private String domain;

    @Column(name = "dedupe_key", nullable = false, length = 255)
    private String dedupeKey;
}
