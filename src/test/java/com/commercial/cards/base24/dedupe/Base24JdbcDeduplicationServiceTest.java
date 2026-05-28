package com.commercial.cards.base24.dedupe;

import com.commercial.cards.base24.model.Base24Message;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

class Base24JdbcDeduplicationServiceTest {

    private final JdbcTemplate jdbcTemplate = mock(JdbcTemplate.class);
    private final Base24EventFingerprint fingerprint = new Base24EventFingerprint();
    private final FingerprintHasher hasher = new FingerprintHasher(new ObjectMapper(), "");

    @Test
    void shouldInitializeSchemaWhenEnabled() {
        Base24JdbcDeduplicationService service = new Base24JdbcDeduplicationService(
                jdbcTemplate, fingerprint, hasher, true);
        service.initialize();
        verify(jdbcTemplate).execute(anyString());
    }

    @Test
    void shouldNotInitializeSchemaWhenDisabled() {
        Base24JdbcDeduplicationService service = new Base24JdbcDeduplicationService(
                jdbcTemplate, fingerprint, hasher, false);
        service.initialize();
        verifyNoInteractions(jdbcTemplate);
    }
}
