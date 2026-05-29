package com.commercial.cards.base24.dedupe;

import com.commercial.cards.base24.model.Base24Message;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

@Component("base24DeduplicationService")
@ConditionalOnProperty(prefix = "base24.dedupe", name = "enabled", havingValue = "true")
public class Base24JpaDeduplicationService extends JpaDeduplicationService<Base24Message> {

    public Base24JpaDeduplicationService(
            EventDeduplicationRepository repository,
            EventFingerprint<Base24Message> eventFingerprint,
            FingerprintHasher fingerprintHasher
    ) {
        super(repository, eventFingerprint, fingerprintHasher);
    }
}
