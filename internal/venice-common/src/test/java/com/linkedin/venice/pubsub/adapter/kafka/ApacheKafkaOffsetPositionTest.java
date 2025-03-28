package com.linkedin.venice.pubsub.adapter.kafka;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.pubsub.PubSubPositionType;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubPositionWireFormat;
import java.io.IOException;
import org.testng.annotations.Test;


/** Unit tests for ApacheKafkaOffsetPosition */
public class ApacheKafkaOffsetPositionTest {
  @Test
  public void testComparePosition() {
    ApacheKafkaOffsetPosition position1 = ApacheKafkaOffsetPosition.of(1);
    ApacheKafkaOffsetPosition position2 = ApacheKafkaOffsetPosition.of(2);
    ApacheKafkaOffsetPosition position3 = ApacheKafkaOffsetPosition.of(3);

    assertTrue(position1.comparePosition(position2) < 0);
    assertTrue(position2.comparePosition(position1) > 0);
    assertEquals(position1.comparePosition(position1), 0);
    assertTrue(position2.comparePosition(position3) < 0);
    assertTrue(position3.comparePosition(position2) > 0);
  }

  @Test
  public void testDiff() {
    ApacheKafkaOffsetPosition position1 = ApacheKafkaOffsetPosition.of(1);
    ApacheKafkaOffsetPosition position2 = ApacheKafkaOffsetPosition.of(2);
    ApacheKafkaOffsetPosition position3 = ApacheKafkaOffsetPosition.of(3);

    assertEquals(position1.diff(position2), -1);
    assertEquals(position2.diff(position1), 1);
    assertEquals(position1.diff(position1), 0);
    assertEquals(position2.diff(position3), -1);
    assertEquals(position3.diff(position2), 1);
  }

  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Cannot compare ApacheKafkaOffsetPosition with null")
  public void testComparePositionWithNull() {
    ApacheKafkaOffsetPosition position1 = ApacheKafkaOffsetPosition.of(1);
    position1.comparePosition(null);
  }

  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Cannot compare ApacheKafkaOffsetPosition with null")
  public void testDiffWithNull() {
    ApacheKafkaOffsetPosition position1 = ApacheKafkaOffsetPosition.of(1);
    position1.diff(null);
  }

  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Cannot compare ApacheKafkaOffsetPosition with .*")
  public void testComparePositionWithDifferentType() {
    ApacheKafkaOffsetPosition position1 = ApacheKafkaOffsetPosition.of(1);
    position1.comparePosition(mock(PubSubPosition.class));
  }

  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Cannot compare ApacheKafkaOffsetPosition with .*")
  public void testDiffWithDifferentType() {
    ApacheKafkaOffsetPosition position1 = ApacheKafkaOffsetPosition.of(1);
    position1.diff(mock(PubSubPosition.class));
  }

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
    ApacheKafkaOffsetPosition position1 = ApacheKafkaOffsetPosition.of(1);
    assertEquals(position1.toString(), "1");
  }

  @Test
  public void testGetOffset() throws IOException {
    ApacheKafkaOffsetPosition position1 = ApacheKafkaOffsetPosition.of(1234);
    assertEquals(position1.getOffset(), 1234);
    PubSubPositionWireFormat wireFormat = position1.getPositionWireFormat();
    ApacheKafkaOffsetPosition position2 = new ApacheKafkaOffsetPosition(wireFormat.rawBytes);
    assertEquals(position2.getOffset(), position1.getOffset());
    assertEquals(position1, position2);
  }

  @Test(expectedExceptions = NullPointerException.class)
  public void testGetOffsetWithNull() throws IOException {
    new ApacheKafkaOffsetPosition(null);
  }

  @Test
  public void testGetPositionWireFormat() throws IOException {
    ApacheKafkaOffsetPosition kafkaPosition = ApacheKafkaOffsetPosition.of(Long.MAX_VALUE);
    PubSubPositionWireFormat wireFormat = kafkaPosition.getPositionWireFormat();
    assertEquals(wireFormat.type, PubSubPositionType.APACHE_KAFKA_OFFSET);
    ApacheKafkaOffsetPosition kafkaPosition2 = new ApacheKafkaOffsetPosition(wireFormat.rawBytes);
    assertEquals(kafkaPosition2.getOffset(), kafkaPosition.getOffset());
    assertEquals(kafkaPosition2, kafkaPosition);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  void testIllegalPosition() {
    ApacheKafkaOffsetPosition.of(OffsetRecord.LOWEST_OFFSET - 1);
  }
}
