package com.linkedin.venice.pubsub;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.pubsub.adapter.kafka.ApacheKafkaOffsetPosition;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import java.nio.ByteBuffer;
import org.testng.annotations.Test;


public class PubSubPositionUtilsTest {
  @Test
  public void testFromBytes() {
    ApacheKafkaOffsetPosition kafkaOffsetPosition = new ApacheKafkaOffsetPosition(98);
    PubSubPosition position = PubSubPositionUtils.fromBytes(kafkaOffsetPosition.toBytes());
    assertTrue(position instanceof ApacheKafkaOffsetPosition);
    assertEquals(position.toBytes(), kafkaOffsetPosition.toBytes());
  }

  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Unknown position type: 99")
  public void testFromBytesWithUnsupportedType() {
    byte[] bytes = ByteBuffer.allocate(4).putInt(99).array();
    PubSubPosition position = PubSubPositionUtils.fromBytes(bytes);
    assertFalse(position instanceof ApacheKafkaOffsetPosition);
    assertNotEquals(position.toBytes(), bytes);
  }

  @Test
  public void testToBytes() {
    ApacheKafkaOffsetPosition kafkaOffsetPosition = new ApacheKafkaOffsetPosition(98);
    byte[] bytes = PubSubPositionUtils.toBytes(kafkaOffsetPosition);
    assertEquals(bytes, kafkaOffsetPosition.toBytes());
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testToBytesNull() {
    PubSubPositionUtils.toBytes(null);
  }
}
