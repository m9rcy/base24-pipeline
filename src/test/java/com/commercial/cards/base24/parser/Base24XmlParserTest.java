package com.commercial.cards.base24.parser;

import com.commercial.cards.base24.model.Base24Message;
import com.commercial.cards.base24.model.MessageType;
import com.commercial.cards.base24.model.RecordType;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class Base24XmlParserTest {

    // No Spring context needed — instantiate directly
    private final Base24XmlParser parser = new Base24XmlParser();

    @Test
    void shouldParseValidPtlfxTvnMessage() {
        String xml = """
                <Data>
                    <MessageType>TVN</MessageType>
                    <RecordType>PTLFX</RecordType>
                    <TransactionId>TXN-20240101-001</TransactionId>
                    <DigitalPan>4111111111111111</DigitalPan>
                    <Amount>123.45</Amount>
                    <CurrencyCode>NZD</CurrencyCode>
                    <ResponseCode>00</ResponseCode>
                    <Timestamp>2024-01-15T10:30:00</Timestamp>
                </Data>
                """;

        Optional<Base24Message> result = parser.parse(xml);

        assertTrue(result.isPresent());
        Base24Message msg = result.get();
        assertEquals(MessageType.TVN,             msg.getMessageType());
        assertEquals(RecordType.PTLFX,            msg.getRecordType());
        assertEquals("TXN-20240101-001",          msg.getTransactionId());
        assertEquals("4111111111111111",           msg.getDigitalPan());
        assertEquals(new BigDecimal("123.45"),     msg.getAmount());
        assertEquals("NZD",                        msg.getCurrencyCode());
        assertEquals("00",                         msg.getResponseCode());
        assertNotNull(msg.getTimestamp());
    }

    @Test
    void shouldParseAcnMessageType() {
        String xml = """
                <Data>
                    <MessageType>ACN</MessageType>
                    <RecordType>PTLFX</RecordType>
                    <TransactionId>TXN-ACN-001</TransactionId>
                    <DigitalPan>5500005555555559</DigitalPan>
                    <Amount>50.00</Amount>
                    <CurrencyCode>AUD</CurrencyCode>
                    <ResponseCode>00</ResponseCode>
                    <Timestamp>2024-03-10T08:00:00</Timestamp>
                </Data>
                """;

        Optional<Base24Message> result = parser.parse(xml);

        assertTrue(result.isPresent());
        assertEquals(MessageType.ACN, result.get().getMessageType());
    }

    @Test
    void shouldMapUnknownMessageTypeToUnknown() {
        String xml = """
                <Data>
                    <MessageType>XYZ</MessageType>
                    <RecordType>PTLFX</RecordType>
                    <TransactionId>TXN-999</TransactionId>
                    <DigitalPan>4111111111111111</DigitalPan>
                </Data>
                """;

        Optional<Base24Message> result = parser.parse(xml);

        assertTrue(result.isPresent());
        assertEquals(MessageType.UNKNOWN, result.get().getMessageType());
    }

    @Test
    void shouldMapNonPtlfxRecordType() {
        String xml = """
                <Data>
                    <MessageType>TVN</MessageType>
                    <RecordType>OTHER</RecordType>
                    <TransactionId>TXN-002</TransactionId>
                    <DigitalPan>4111111111111111</DigitalPan>
                </Data>
                """;

        Optional<Base24Message> result = parser.parse(xml);

        assertTrue(result.isPresent());
        assertEquals(RecordType.OTHER, result.get().getRecordType());
    }

    @Test
    void shouldReturnEmptyOnMalformedXml() {
        Optional<Base24Message> result = parser.parse("<not valid xml!!!");
        assertTrue(result.isEmpty());
    }

    @Test
    void shouldReturnEmptyOnNullInput() {
        Optional<Base24Message> result = parser.parse(null);
        assertTrue(result.isEmpty());
    }

    @Test
    void shouldReturnEmptyOnBlankInput() {
        Optional<Base24Message> result = parser.parse("   ");
        assertTrue(result.isEmpty());
    }

    @Test
    void shouldHandleMissingOptionalFieldsGracefully() {
        // Amount and Timestamp are optional — should not throw
        String xml = """
                <Data>
                    <MessageType>TCN</MessageType>
                    <RecordType>PTLFX</RecordType>
                    <TransactionId>TXN-003</TransactionId>
                    <DigitalPan>4111111111111111</DigitalPan>
                </Data>
                """;

        Optional<Base24Message> result = parser.parse(xml);

        assertTrue(result.isPresent());
        assertNull(result.get().getAmount());
        assertNull(result.get().getTimestamp());
    }
}
