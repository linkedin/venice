package com.linkedin.venice.pubsub.adapter;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import com.linkedin.venice.pubsub.api.PubSubPosition;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class SimplePubSubProduceResultImplTest {
  private static final String TEST_TOPIC = "test-topic";
  private static final int TEST_PARTITION = 1;
  private static final long TEST_OFFSET = 100L;
  private static final int TEST_SERIALIZED_SIZE = 256;

  private PubSubPosition mockPubSubPosition;

  @BeforeMethod
  public void setUp() {
    mockPubSubPosition = mock(PubSubPosition.class);
  }

  @Test
  public void testConstructorWithoutPubSubPosition() {
    SimplePubSubProduceResultImpl result =
        new SimplePubSubProduceResultImpl(TEST_TOPIC, TEST_PARTITION, TEST_OFFSET, TEST_SERIALIZED_SIZE);

    assertEquals(result.getTopic(), TEST_TOPIC);
    assertEquals(result.getPartition(), TEST_PARTITION);
    assertEquals(result.getOffset(), TEST_OFFSET);
    assertEquals(result.getSerializedSize(), TEST_SERIALIZED_SIZE);
    assertNull(result.getPubSubPosition());
  }

  @Test
  public void testConstructorWithPubSubPosition() {
    SimplePubSubProduceResultImpl result = new SimplePubSubProduceResultImpl(
        TEST_TOPIC,
        TEST_PARTITION,
        TEST_OFFSET,
        mockPubSubPosition,
        TEST_SERIALIZED_SIZE);

    assertEquals(result.getTopic(), TEST_TOPIC);
    assertEquals(result.getPartition(), TEST_PARTITION);
    assertEquals(result.getOffset(), TEST_OFFSET);
    assertEquals(result.getSerializedSize(), TEST_SERIALIZED_SIZE);
    assertEquals(result.getPubSubPosition(), mockPubSubPosition);
  }

  @Test
  public void testToString() {
    SimplePubSubProduceResultImpl result =
        new SimplePubSubProduceResultImpl(TEST_TOPIC, TEST_PARTITION, TEST_OFFSET, TEST_SERIALIZED_SIZE);

    String expectedString = "[Topic: " + TEST_TOPIC + ", Partition: " + TEST_PARTITION + ", Offset: " + TEST_OFFSET
        + ", PubSubPosition: null, SerializedSize: " + TEST_SERIALIZED_SIZE + "]";
    assertEquals(result.toString(), expectedString);
  }
}
