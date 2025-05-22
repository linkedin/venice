package com.linkedin.venice.pubsub.adapter;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.linkedin.venice.pubsub.api.PubSubPosition;
import org.testng.annotations.Test;


public class SimplePubSubProduceResultImplTest {
  private static final String TEST_TOPIC = "test-topic";
  private static final int TEST_PARTITION = 1;
  private static final PubSubPosition PUBSUB_POSITION = mock(PubSubPosition.class);
  private static final int TEST_SERIALIZED_SIZE = 256;

  @Test
  public void testConstructorWithPubSubPosition() {
    SimplePubSubProduceResultImpl result =
        new SimplePubSubProduceResultImpl(TEST_TOPIC, TEST_PARTITION, PUBSUB_POSITION, TEST_SERIALIZED_SIZE);

    assertEquals(result.getTopic(), TEST_TOPIC);
    assertEquals(result.getPartition(), TEST_PARTITION);
    assertEquals(result.getPubSubPosition(), PUBSUB_POSITION);
    assertEquals(result.getSerializedSize(), TEST_SERIALIZED_SIZE);
    assertEquals(result.getPubSubPosition(), PUBSUB_POSITION);
  }

  @Test
  public void testToString() {
    when(PUBSUB_POSITION.toString()).thenReturn("123");
    SimplePubSubProduceResultImpl result =
        new SimplePubSubProduceResultImpl(TEST_TOPIC, TEST_PARTITION, PUBSUB_POSITION, TEST_SERIALIZED_SIZE);

    String expectedString = "[TP: test-topic-1, pubSubPosition: 123, serializedSize: 256]";
    assertEquals(result.toString(), expectedString);
  }
}
