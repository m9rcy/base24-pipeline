package com.commercial.cards.base24.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.web.client.RestClient;

@Configuration
public class RestClientConfig {

    /**
     * Shared RestClient bean — adapters inject this.
     * Only needed when NOT on the stub profile (stubs make no HTTP calls).
     */
    @Bean
    @Profile("!stub")
    public RestClient restClient() {
        return RestClient.builder()
                .build();
    }
}
