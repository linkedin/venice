package com.linkedin.venice.spark.input.pubsub.table;

import static org.testng.Assert.*;

import org.apache.commons.lang3.SerializationUtils;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;


public class VenicePubsubInputPartitionTest {
  VenicePubsubInputPartition targetObject;

  @Test
  public void testSerializablity() {

    byte[] serialized = SerializationUtils.serialize(targetObject);
    VenicePubsubInputPartition deserializedObject = SerializationUtils.deserialize(serialized);

    assertEquals(deserializedObject.getTopicName(), targetObject.getTopicName());
    assertEquals(deserializedObject.getPartitionNumber(), targetObject.getPartitionNumber());
    assertEquals(deserializedObject.getSegmentStartOffset(), targetObject.getSegmentStartOffset());
    assertEquals(deserializedObject.getSegmentEndOffset(), targetObject.getSegmentEndOffset());

    // object should give back exactly what was put in.
    assertEquals(deserializedObject.getRegion(), "prod-lva2");
    assertEquals(deserializedObject.getTopicName(), "BigStrangePubSubTopic_V1_rt_r");
    assertEquals(deserializedObject.getSegmentEndOffset(), 100_000_000);
    assertEquals(deserializedObject.getSegmentStartOffset(), 49152);
    assertEquals(deserializedObject.getPartitionNumber(), 42);

  }

  @BeforeTest
  public void setUp() {
    targetObject =
        new VenicePubsubInputPartition("prod-lva2", "BigStrangePubSubTopic_V1_rt_r", 42, 49_152, 100_000_000);
  }
}
