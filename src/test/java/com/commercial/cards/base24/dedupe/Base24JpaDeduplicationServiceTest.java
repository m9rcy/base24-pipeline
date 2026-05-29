package com.commercial.cards.base24.dedupe;

import com.commercial.cards.base24.model.Base24Message;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;

class Base24JpaDeduplicationServiceTest {

    private final EventDeduplicationRepository repository = mock(EventDeduplicationRepository.class);
    private final EventFingerprint<Base24Message> fingerprint = new Base24EventFingerprint();
    private final FingerprintHasher hasher = new FingerprintHasher(new ObjectMapper(), "");

    @Test
    void shouldBeInstantiable() {
        Base24JpaDeduplicationService service = new Base24JpaDeduplicationService(
                repository, fingerprint, hasher);
        assertNotNull(service);
    }
}
