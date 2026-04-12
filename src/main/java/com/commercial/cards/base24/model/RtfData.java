package com.commercial.cards.base24.model;

import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlElement;
import jakarta.xml.bind.annotation.XmlRootElement;
import lombok.Data;

/**
 * JAXB binding for the RTFDataRq Data fragment published to the Kafka topic.
 *
 * <pre>{@code
 * <Data>
 *     <MessageType>TVN</MessageType>
 *     <RecordType>PTLFX</RecordType>
 *     <TransactionId>TXN-20240101-001</TransactionId>
 *     <DigitalPan>4111111111111111</DigitalPan>
 *     <Amount>123.45</Amount>
 *     <CurrencyCode>NZD</CurrencyCode>
 *     <ResponseCode>00</ResponseCode>
 *     <Timestamp>2024-01-15T10:30:00</Timestamp>
 * </Data>
 * }</pre>
 *
 * Update element names here once the full xEE-SE451 spec is confirmed.
 */
@Data
@XmlRootElement(name = "Data")
@XmlAccessorType(XmlAccessType.FIELD)
public class RtfData {

    @XmlElement(name = "MessageType")
    private String messageType;

    @XmlElement(name = "RecordType")
    private String recordType;

    @XmlElement(name = "TransactionId")
    private String transactionId;

    @XmlElement(name = "DigitalPan")
    private String digitalPan;

    @XmlElement(name = "Amount")
    private String amount;

    @XmlElement(name = "CurrencyCode")
    private String currencyCode;

    @XmlElement(name = "ResponseCode")
    private String responseCode;

    @XmlElement(name = "Timestamp")
    private String timestamp;
}
