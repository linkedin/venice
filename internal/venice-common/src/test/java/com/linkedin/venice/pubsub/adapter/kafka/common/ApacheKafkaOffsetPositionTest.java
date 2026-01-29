package com.linkedin.venice.pubsub.adapter.kafka.common;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.pubsub.PubSubPositionTypeRegistry;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubPositionWireFormat;
import java.io.IOException;
import org.testng.annotations.Test;


/** Unit tests for ApacheKafkaOffsetPosition */
public class ApacheKafkaOffsetPositionTest {
  @Test
  public void testEquals() {
    ApacheKafkaOffsetPosition position1 = ApacheKafkaOffsetPosition.of(1);
    ApacheKafkaOffsetPosition position2 = ApacheKafkaOffsetPosition.of(2);
    ApacheKafkaOffsetPosition position3 = ApacheKafkaOffsetPosition.of(1);

    assertEquals(position1, position1);
    assertEquals(position1, position3);
    assertNotEquals(position1, position2);

    assertFalse(position1.equals(null));
    assertFalse(position1.equals(PubSubPosition.class));
  }

  @Test
  public void testHashCode() {
    ApacheKafkaOffsetPosition position1 = ApacheKafkaOffsetPosition.of(1);
    ApacheKafkaOffsetPosition position2 = ApacheKafkaOffsetPosition.of(2);
    ApacheKafkaOffsetPosition position3 = ApacheKafkaOffsetPosition.of(1);
    assertEquals(position1.hashCode(), position1.hashCode());
    assertEquals(position1.hashCode(), position3.hashCode());
    assertNotEquals(position1.hashCode(), position2.hashCode());
  }

  @Test
  public void testToString() {
    ApacheKafkaOffsetPosition position1 = ApacheKafkaOffsetPosition.of(231);
    assertTrue(
        position1.toString().contains("231"),
        "toString should contain the offset value 231 but was: " + position1);
  }

  @Test
  public void testGetInternalOffset() throws IOException {
    ApacheKafkaOffsetPosition position1 = ApacheKafkaOffsetPosition.of(1234);
    assertEquals(position1.getInternalOffset(), 1234);
    PubSubPositionWireFormat wireFormat = position1.getPositionWireFormat();
    ApacheKafkaOffsetPosition position2 = new ApacheKafkaOffsetPosition(wireFormat.rawBytes);
    assertEquals(position2.getInternalOffset(), position1.getInternalOffset());
    assertEquals(position1, position2);
  }

  @Test(expectedExceptions = NullPointerException.class)
  public void testGetInternalOffsetWithNull() throws IOException {
    new ApacheKafkaOffsetPosition(null);
  }

  @Test
  public void testGetPositionWireFormat() throws IOException {
    ApacheKafkaOffsetPosition kafkaPosition = ApacheKafkaOffsetPosition.of(Long.MAX_VALUE);
    PubSubPositionWireFormat wireFormat = kafkaPosition.getPositionWireFormat();
    assertEquals(wireFormat.type, PubSubPositionTypeRegistry.APACHE_KAFKA_OFFSET_POSITION_TYPE_ID);
    ApacheKafkaOffsetPosition kafkaPosition2 = new ApacheKafkaOffsetPosition(wireFormat.rawBytes);
    assertEquals(kafkaPosition2.getInternalOffset(), kafkaPosition.getInternalOffset());
    assertEquals(kafkaPosition2, kafkaPosition);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  void testIllegalPosition() {
    ApacheKafkaOffsetPosition.of(OffsetRecord.LOWEST_OFFSET - 1);
  }
}
