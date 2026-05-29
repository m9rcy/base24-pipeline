package com.commercial.cards.base24.config;

import org.junit.jupiter.api.Test;

import javax.sql.DataSource;

import static org.junit.jupiter.api.Assertions.assertNotNull;

class DedupeDataSourceConfigTest {

    private final DedupeDataSourceConfig config = new DedupeDataSourceConfig();

    @Test
    void shouldCreateDataSourceWithGivenCredentials() {
        DataSource dataSource = config.dataSource(
                "jdbc:postgresql://localhost:5432/test", "sa", "");
        assertNotNull(dataSource);
    }
}
