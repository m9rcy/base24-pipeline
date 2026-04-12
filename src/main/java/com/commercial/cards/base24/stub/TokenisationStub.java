package com.commercial.cards.base24.stub;

import com.commercial.cards.base24.port.TokenisationPort;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

/**
 * Stub implementation of {@link TokenisationPort} for local development.
 *
 * <p>Active when {@code spring.profiles.active=stub}.
 * No real HTTP calls are made — returns a deterministic fake token.</p>
 *
 * <p>Switch to real adapter by removing the stub profile.</p>
 */
@Slf4j
@Component
@Profile("stub")
public class TokenisationStub implements TokenisationPort {

    @Override
    public String tokenise(String digitalPan) {
        String last4 = digitalPan != null && digitalPan.length() >= 4
                ? digitalPan.substring(digitalPan.length() - 4)
                : "0000";

        String tokenisedPan = "TOK-STUB-" + last4;

        log.info("[STUB] Tokenised PAN ending in {} → {}", last4, tokenisedPan);
        return tokenisedPan;
    }
}
