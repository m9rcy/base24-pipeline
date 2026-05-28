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

    /**
     * Fallback no-op bean, registered only when no bean named {@code base24DeduplicationService}
     * exists. When {@code base24.dedupe.enabled=true}, {@link com.commercial.cards.base24.dedupe.Base24JdbcDeduplicationService}
     * is registered under that name and this bean is skipped.
     *
     * <p>Pattern for additional consumers: declare your own {@code @Bean("swiftDeduplicationService")}
     * and use {@code @Qualifier("swiftDeduplicationService")} in your consumer constructor.</p>
     */
    @Bean("base24DeduplicationService")
    @ConditionalOnMissingBean(name = "base24DeduplicationService")
    public DeduplicationService<Base24Message> noOpBase24DeduplicationService() {
        return new NoOpDeduplicationService<>();
    }
}
