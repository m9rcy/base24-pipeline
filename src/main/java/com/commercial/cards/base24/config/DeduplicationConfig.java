package com.commercial.cards.base24.config;

import com.commercial.cards.base24.dedupe.DeduplicationService;
import com.commercial.cards.base24.dedupe.FingerprintHasher;
import com.commercial.cards.base24.dedupe.NoOpDeduplicationService;
import com.commercial.cards.base24.model.Base24Message;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class DeduplicationConfig {

    @Bean
    public FingerprintHasher fingerprintHasher(
            ObjectMapper objectMapper,
            @Value("${base24.dedupe.hmac-secret:}") String hmacSecret
    ) {
        return new FingerprintHasher(objectMapper, hmacSecret);
    }

    @Bean
    @ConditionalOnMissingBean(DeduplicationService.class)
    public DeduplicationService<Base24Message> noOpBase24DeduplicationService() {
        return new NoOpDeduplicationService<>();
    }
}
