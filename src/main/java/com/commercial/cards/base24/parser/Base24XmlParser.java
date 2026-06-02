package com.commercial.cards.base24.parser;

import com.commercial.cards.base24.model.Base24Message;
import com.commercial.cards.base24.model.RtfData;
import jakarta.xml.bind.JAXBContext;
import jakarta.xml.bind.JAXBException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.io.StringReader;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Optional;

@Slf4j
@Component
public class Base24XmlParser {

    private static final DateTimeFormatter TIMESTAMP_FORMAT =
            DateTimeFormatter.ISO_LOCAL_DATE_TIME;

    private final JAXBContext jaxbContext;

    public Base24XmlParser() {
        try {
            this.jaxbContext = JAXBContext.newInstance(RtfData.class);
        } catch (JAXBException e) {
            throw new IllegalStateException("Failed to initialise JAXB context", e);
        }
    }

    /**
     * Parse a raw XML fragment (the Data portion of an RTFDataRq message)
     * into a {@link Base24Message} domain object.
     *
     * @param xmlFragment the raw XML string from Kafka
     * @return populated message, or empty if parsing fails
     */
    public Optional<Base24Message> parse(String xmlFragment) {
        if (xmlFragment == null || xmlFragment.isBlank()) {
            log.warn("Received null or blank XML fragment");
            return Optional.empty();
        }

        try {
            RtfData rtfData = (RtfData) jaxbContext
                    .createUnmarshaller()
                    .unmarshal(new StringReader(xmlFragment));

            Base24Message message = Base24Message.from(
                    rtfData,
                    parseAmount(rtfData.getAmount()),
                    parseTimestamp(rtfData.getTimestamp()));

            return Optional.of(message);

        } catch (JAXBException e) {
            log.error("Failed to parse XML fragment: {}", e.getMessage());
            return Optional.empty();
        }
    }

    private BigDecimal parseAmount(String value) {
        if (value == null || value.isBlank()) return null;
        try {
            return new BigDecimal(value.trim());
        } catch (NumberFormatException e) {
            log.warn("Could not parse amount '{}', defaulting to null", value);
            return null;
        }
    }

    private LocalDateTime parseTimestamp(String value) {
        if (value == null || value.isBlank()) return null;
        try {
            return LocalDateTime.parse(value.trim(), TIMESTAMP_FORMAT);
        } catch (DateTimeParseException e) {
            log.warn("Could not parse timestamp '{}', defaulting to null", value);
            return null;
        }
    }
}
