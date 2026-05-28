package com.commercial.cards.base24.config;

import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;

class DedupeDataSourceConfigTest {

    private final DedupeDataSourceConfig config = new DedupeDataSourceConfig();

    @Test
    void shouldCreateDataSourceWithGivenCredentials() {
        DataSource dataSource = config.dataSource("jdbc:h2:mem:test", "sa", "");
        assertNotNull(dataSource);
    }

    @Test
    void shouldCreateJdbcTemplateFromDataSource() {
        DataSource dataSource = mock(DataSource.class);
        JdbcTemplate jdbcTemplate = config.jdbcTemplate(dataSource);
        assertNotNull(jdbcTemplate);
    }
}
