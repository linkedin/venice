package com.linkedin.venice.spark.input.pubsub.raw;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.*;

import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class VeniceBasicPubsubInputPartitionTest {
  private static final String REGION = "test-region";
  private static final String TOPIC_NAME = "test-topic";
  private static final int PARTITION_NUMBER = 87;
  private static final long START_OFFSET = 93L;
  private static final long END_OFFSET = 1885L;

  private VeniceBasicPubsubInputPartition inputPartition;
  private PubSubTopicPartition topicPartition;
  private PubSubPosition beginningPosition;
  private PubSubPosition endPosition;

  @BeforeMethod
  public void setUp() {
    topicPartition = mock(PubSubTopicPartition.class);
    when(topicPartition.getTopicName()).thenReturn(TOPIC_NAME);
    when(topicPartition.getPartitionNumber()).thenReturn(PARTITION_NUMBER);

    beginningPosition = mock(PubSubPosition.class);
    when(beginningPosition.getNumericOffset()).thenReturn(START_OFFSET);

    endPosition = mock(PubSubPosition.class);
    when(endPosition.getNumericOffset()).thenReturn(END_OFFSET);

    inputPartition = new VeniceBasicPubsubInputPartition(REGION, topicPartition, beginningPosition, endPosition);
  }

  @Test
  public void testGetRegion() {
    assertEquals(inputPartition.getRegion(), REGION, "Region should match the value provided in constructor");
  }

  @Test
  public void testGetTopicName() {
    assertEquals(inputPartition.getTopicName(), TOPIC_NAME, "Topic name should match the value from topic partition");
  }

  @Test
  public void testGetPartitionNumber() {
    assertEquals(
        inputPartition.getPartitionNumber(),
        PARTITION_NUMBER,
        "Partition number should match the value from topic partition");
  }

  @Test
  public void testGetStartOffset() {
    assertEquals(
        inputPartition.getStartOffset(),
        START_OFFSET,
        "Start offset should match the value from beginning position");
  }

  @Test
  public void testGetEndOffset() {
    assertEquals(inputPartition.getEndOffset(), END_OFFSET, "End offset should match the value from end position");
  }

  @Test
  public void testSerializability() throws IOException, ClassNotFoundException {
    // Serialize the object
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    try (ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream)) {
      objectOutputStream.writeObject(inputPartition);
    }

    // Deserialize the object
    ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(byteArrayOutputStream.toByteArray());
    VeniceBasicPubsubInputPartition deserializedPartition;
    try (ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream)) {
      deserializedPartition = (VeniceBasicPubsubInputPartition) objectInputStream.readObject();
    }

    // Verify all properties are preserved
    assertEquals(deserializedPartition.getRegion(), REGION, "Region should be preserved after serialization");
    assertEquals(
        deserializedPartition.getTopicName(),
        TOPIC_NAME,
        "Topic name should be preserved after serialization");
    assertEquals(
        deserializedPartition.getPartitionNumber(),
        PARTITION_NUMBER,
        "Partition number should be preserved after serialization");
    assertEquals(
        deserializedPartition.getStartOffset(),
        START_OFFSET,
        "Start offset should be preserved after serialization");
    assertEquals(
        deserializedPartition.getEndOffset(),
        END_OFFSET,
        "End offset should be preserved after serialization");
  }
}
