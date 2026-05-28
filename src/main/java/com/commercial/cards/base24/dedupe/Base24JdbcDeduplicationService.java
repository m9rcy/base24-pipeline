package com.commercial.cards.base24.dedupe;

import com.commercial.cards.base24.model.Base24Message;
import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(prefix = "base24.dedupe", name = "enabled", havingValue = "true")
public class Base24JdbcDeduplicationService extends JdbcDeduplicationService<Base24Message> {

    private final boolean initializeSchema;

    public Base24JdbcDeduplicationService(
            JdbcTemplate jdbcTemplate,
            EventFingerprint<Base24Message> eventFingerprint,
            FingerprintHasher fingerprintHasher,
            @Value("${base24.dedupe.initialize-schema:true}") boolean initializeSchema
    ) {
        super(jdbcTemplate, eventFingerprint, fingerprintHasher);
        this.initializeSchema = initializeSchema;
    }

    @PostConstruct
    void initialize() {
        if (initializeSchema) {
            initializeSchema();
        }
    }
}
