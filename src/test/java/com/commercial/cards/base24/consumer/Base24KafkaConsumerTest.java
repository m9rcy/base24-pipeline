package com.commercial.cards.base24.consumer;

import com.commercial.cards.base24.model.ProcessingResult;
import com.commercial.cards.base24.pipeline.Base24MessagePipeline;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.Acknowledgment;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class Base24KafkaConsumerTest {

    private Base24MessagePipeline pipeline;
    private Acknowledgment        ack;
    private Base24KafkaConsumer   consumer;

    @BeforeEach
    void setUp() {
        pipeline = mock(Base24MessagePipeline.class);
        ack      = mock(Acknowledgment.class);
        consumer = new Base24KafkaConsumer(pipeline);
    }

    @Test
    void shouldAckWhenPipelineReturnsSuccess() {
        when(pipeline.process(any())).thenReturn(ProcessingResult.SUCCESS);

        consumer.consume(aRecord("<xml/>"), ack);

        verify(ack).acknowledge();
    }

    @Test
    void shouldAckWhenPipelineReturnsSkipped() {
        when(pipeline.process(any())).thenReturn(ProcessingResult.SKIPPED);

        consumer.consume(aRecord("<xml/>"), ack);

        verify(ack).acknowledge();
    }

    @Test
    void shouldNotAckWhenPipelineReturnsRetry() {
        when(pipeline.process(any())).thenReturn(ProcessingResult.RETRY);

        consumer.consume(aRecord("<xml/>"), ack);

        verify(ack, never()).acknowledge();
    }

    @Test
    void shouldDelegateRawXmlToPipeline() {
        String rawXml = "<Data><MessageType>TVN</MessageType></Data>";
        when(pipeline.process(rawXml)).thenReturn(ProcessingResult.SUCCESS);

        consumer.consume(aRecord(rawXml), ack);

        verify(pipeline).process(rawXml);
    }

    private ConsumerRecord<String, String> aRecord(String value) {
        return new ConsumerRecord<>("base24-eps-realtime", 0, 100L, null, value);
    }
}
