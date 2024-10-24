package com.linkedin.venice.pubsub.api;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.message.KafkaKey;
import java.util.Arrays;
import java.util.List;
import org.testng.annotations.Test;


public class PubSubProducerAdapterDelegatorTest {
  @Test
  public void testDelegator() {
    PubSubProducerAdapter mockProducer1 = mock(PubSubProducerAdapter.class);
    PubSubProducerAdapter mockProducer2 = mock(PubSubProducerAdapter.class);
    List<PubSubProducerAdapter> producers = Arrays.asList(mockProducer1, mockProducer2);

    PubSubProducerAdapterDelegator delegator = new PubSubProducerAdapterDelegator(producers);
    String testTopicName = "topic_v1";
    delegator.getNumberOfPartitions(testTopicName);
    verify(mockProducer1).getNumberOfPartitions(testTopicName);

    delegator.sendMessage(
        testTopicName,
        1,
        mock(KafkaKey.class),
        mock(KafkaMessageEnvelope.class),
        mock(PubSubMessageHeaders.class),
        null);
    delegator.sendMessage(
        testTopicName,
        2,
        mock(KafkaKey.class),
        mock(KafkaMessageEnvelope.class),
        mock(PubSubMessageHeaders.class),
        null);
    delegator.sendMessage(
        testTopicName,
        3,
        mock(KafkaKey.class),
        mock(KafkaMessageEnvelope.class),
        mock(PubSubMessageHeaders.class),
        null);
    delegator.sendMessage(
        testTopicName,
        1,
        mock(KafkaKey.class),
        mock(KafkaMessageEnvelope.class),
        mock(PubSubMessageHeaders.class),
        null);
    delegator.sendMessage(
        testTopicName,
        2,
        mock(KafkaKey.class),
        mock(KafkaMessageEnvelope.class),
        mock(PubSubMessageHeaders.class),
        null);

    verify(mockProducer1, times(2)).sendMessage(eq(testTopicName), eq(1), any(), any(), any(), any());
    verify(mockProducer1, times(1)).sendMessage(eq(testTopicName), eq(3), any(), any(), any(), any());
    verify(mockProducer2, times(2)).sendMessage(eq(testTopicName), eq(2), any(), any(), any(), any());

    delegator.flush();
    verify(mockProducer1).flush();
    verify(mockProducer2).flush();

    delegator.close(1);
    verify(mockProducer1).close(1);
    verify(mockProducer2).close(1);

    assertTrue(delegator.getMeasurableProducerMetrics().isEmpty());

    delegator.getBrokerAddress();
    verify(mockProducer1).getBrokerAddress();
  }
}
