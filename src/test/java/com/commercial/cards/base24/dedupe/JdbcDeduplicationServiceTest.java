package com.commercial.cards.base24.dedupe;

import com.commercial.cards.base24.model.Base24Message;
import com.commercial.cards.base24.model.MessageType;
import com.commercial.cards.base24.model.RecordType;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.math.BigDecimal;
import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers(disabledWithoutDocker = true)
class JdbcDeduplicationServiceTest {

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine");

    private JdbcTemplate jdbcTemplate;
    private JdbcDeduplicationService<Base24Message> deduplicationService;

    @BeforeEach
    void setUp() {
        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setUrl(POSTGRES.getJdbcUrl());
        dataSource.setUsername(POSTGRES.getUsername());
        dataSource.setPassword(POSTGRES.getPassword());

        jdbcTemplate = new JdbcTemplate(dataSource);
        jdbcTemplate.execute("drop table if exists event_deduplication");

        deduplicationService = new JdbcDeduplicationService<>(
                jdbcTemplate,
                new Base24EventFingerprint(),
                new FingerprintHasher(new ObjectMapper(), "test-secret"));
        deduplicationService.initializeSchema();
    }

    @Test
    void shouldConsumeWhenNoDedupeRowExists() {
        assertTrue(deduplicationService.isConsumable(message("TXN-001", "123.45", time(10))));
    }

    @Test
    void shouldSkipSameHashAfterSuccessfulProcessing() {
        Base24Message message = message("TXN-001", "123.45", time(10));

        deduplicationService.markProcessed(message);

        assertFalse(deduplicationService.isConsumable(message));
    }

    @Test
    void shouldConsumeChangedInterestingFields() {
        deduplicationService.markProcessed(message("TXN-001", "123.45", time(10)));

        assertTrue(deduplicationService.isConsumable(message("TXN-001", "999.00", time(11))));
    }

    @Test
    void shouldRejectOlderEventTimeAsStale() {
        deduplicationService.markProcessed(message("TXN-001", "123.45", time(10)));

        assertFalse(deduplicationService.isConsumable(message("TXN-001", "999.00", time(9))));
    }

    @Test
    void shouldAdvanceLastEventTimeForNewerDuplicate() {
        deduplicationService.markProcessed(message("TXN-001", "123.45", time(10)));

        assertFalse(deduplicationService.isConsumable(message("TXN-001", "123.45", time(12))));
        assertEquals(time(12), lastEventTime("TXN-001"));
    }

    private LocalDateTime lastEventTime(String transactionId) {
        return jdbcTemplate.queryForObject("""
                        select last_event_time
                        from event_deduplication
                        where domain = ? and dedupe_key = ?
                        """,
                (rs, rowNum) -> rs.getTimestamp("last_event_time").toLocalDateTime(),
                Base24EventFingerprint.DOMAIN,
                transactionId);
    }

    private Base24Message message(String transactionId, String amount, LocalDateTime timestamp) {
        return Base24Message.builder()
                .messageType(MessageType.TVN)
                .recordType(RecordType.PTLFX)
                .transactionId(transactionId)
                .digitalPan("4111111111111111")
                .amount(new BigDecimal(amount))
                .currencyCode("NZD")
                .responseCode("00")
                .timestamp(timestamp)
                .build();
    }

    private LocalDateTime time(int hour) {
        return LocalDateTime.of(2026, 5, 28, hour, 0);
    }
}
