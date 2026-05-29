package com.commercial.cards.base24.dedupe;

import com.commercial.cards.base24.model.Base24Message;
import com.commercial.cards.base24.model.MessageType;
import com.commercial.cards.base24.model.RecordType;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.math.BigDecimal;
import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@DataJpaTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Testcontainers(disabledWithoutDocker = true)
class JpaDeduplicationServiceTest {

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine");

    @DynamicPropertySource
    static void configureDataSource(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("spring.flyway.enabled", () -> "true");
    }

    @Autowired
    private EventDeduplicationRepository repository;

    private JpaDeduplicationService<Base24Message> deduplicationService;

    @BeforeEach
    void setUp() {
        deduplicationService = new JpaDeduplicationService<>(
                repository,
                new Base24EventFingerprint(),
                new FingerprintHasher(new ObjectMapper(), "test-secret"));
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
        Base24Message newer = message("TXN-001", "123.45", time(12));
        deduplicationService.markProcessed(message("TXN-001", "123.45", time(10)));

        assertFalse(deduplicationService.isConsumable(newer));
        deduplicationService.afterRejected(newer);
        assertEquals(time(12), lastEventTime("TXN-001"));
    }

    @Test
    void shouldSkipDuplicateWhenIncomingHasNoTimestamp() {
        deduplicationService.markProcessed(message("TXN-001", "123.45", time(10)));

        assertFalse(deduplicationService.isConsumable(message("TXN-001", "123.45", null)));
    }

    @Test
    void shouldAdvanceEventTimeWhenExistingTimestampIsNull() {
        Base24Message newer = message("TXN-001", "123.45", time(12));
        deduplicationService.markProcessed(message("TXN-001", "123.45", null));

        assertFalse(deduplicationService.isConsumable(newer));
        deduplicationService.afterRejected(newer);
        assertEquals(time(12), lastEventTime("TXN-001"));
    }

    @Test
    void shouldReprocessWhenHashVersionMismatches() {
        EventDeduplicationEntity staleEntity = EventDeduplicationEntity.builder()
                .id(new EventDeduplicationId(Base24EventFingerprint.DOMAIN, "TXN-001"))
                .hashVersion("v0")
                .lastHash("old-hash")
                .lastEventTime(time(10))
                .updatedAt(LocalDateTime.now())
                .build();
        repository.save(staleEntity);

        assertTrue(deduplicationService.isConsumable(message("TXN-001", "123.45", time(11))));
    }

    @Test
    void shouldNotAdvanceEventTimeWhenAfterRejectedCalledWithNoTimestamp() {
        deduplicationService.markProcessed(message("TXN-001", "123.45", time(10)));

        Base24Message noTimestamp = message("TXN-001", "123.45", null);
        assertFalse(deduplicationService.isConsumable(noTimestamp));
        deduplicationService.afterRejected(noTimestamp);

        assertEquals(time(10), lastEventTime("TXN-001"));
    }

    private LocalDateTime lastEventTime(String transactionId) {
        return repository.findById(new EventDeduplicationId(Base24EventFingerprint.DOMAIN, transactionId))
                .map(EventDeduplicationEntity::getLastEventTime)
                .orElseThrow();
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
