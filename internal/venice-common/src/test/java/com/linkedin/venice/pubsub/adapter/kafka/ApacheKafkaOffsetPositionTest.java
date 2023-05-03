package com.linkedin.venice.pubsub.adapter.kafka;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.pubsub.PubSubPositionType;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import java.nio.ByteBuffer;
import org.testng.annotations.Test;


/** Unit tests for ApacheKafkaOffsetPosition */
public class ApacheKafkaOffsetPositionTest {
  @Test
  public void testComparePosition() {
    ApacheKafkaOffsetPosition position1 = new ApacheKafkaOffsetPosition(1);
    ApacheKafkaOffsetPosition position2 = new ApacheKafkaOffsetPosition(2);
    ApacheKafkaOffsetPosition position3 = new ApacheKafkaOffsetPosition(3);

    assertTrue(position1.comparePosition(position2) < 0);
    assertTrue(position2.comparePosition(position1) > 0);
    assertEquals(position1.comparePosition(position1), 0);
    assertTrue(position2.comparePosition(position3) < 0);
    assertTrue(position3.comparePosition(position2) > 0);
  }

  @Test
  public void testDiff() {
    ApacheKafkaOffsetPosition position1 = new ApacheKafkaOffsetPosition(1);
    ApacheKafkaOffsetPosition position2 = new ApacheKafkaOffsetPosition(2);
    ApacheKafkaOffsetPosition position3 = new ApacheKafkaOffsetPosition(3);

    assertEquals(position1.diff(position2), -1);
    assertEquals(position2.diff(position1), 1);
    assertEquals(position1.diff(position1), 0);
    assertEquals(position2.diff(position3), -1);
    assertEquals(position3.diff(position2), 1);
  }

  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Cannot compare ApacheKafkaOffsetPosition with null")
  public void testComparePositionWithNull() {
    ApacheKafkaOffsetPosition position1 = new ApacheKafkaOffsetPosition(1);
    position1.comparePosition(null);
  }

  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Cannot compare ApacheKafkaOffsetPosition with null")
  public void testDiffWithNull() {
    ApacheKafkaOffsetPosition position1 = new ApacheKafkaOffsetPosition(1);
    position1.diff(null);
  }

  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Cannot compare ApacheKafkaOffsetPosition with .*")
  public void testComparePositionWithDifferentType() {
    ApacheKafkaOffsetPosition position1 = new ApacheKafkaOffsetPosition(1);
    position1.comparePosition(getDifferentTypePosition());
  }

  private PubSubPosition getDifferentTypePosition() {
    return new PubSubPosition() {
      @Override
      public int comparePosition(PubSubPosition other) {
        return 0;
      }

      @Override
      public long diff(PubSubPosition other) {
        return 0;
      }

      @Override
      public byte[] toBytes() {
        return new byte[0];
      }
    };
  }

  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Cannot compare ApacheKafkaOffsetPosition with .*")
  public void testDiffWithDifferentType() {
    ApacheKafkaOffsetPosition position1 = new ApacheKafkaOffsetPosition(1);
    position1.diff(getDifferentTypePosition());
  }

  @Test
  public void testToBytes() {
    ApacheKafkaOffsetPosition position1 = new ApacheKafkaOffsetPosition(99);
    byte[] bytes = position1.toBytes();
    assertEquals(bytes.length, 12); // 4 bytes for the type, 8 bytes for the offset
    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    assertEquals(buffer.getInt(), PubSubPositionType.APACHE_KAFKA_OFFSET);
    assertEquals(buffer.getLong(), 99);
  }

  @Test
  public void testEquals() {
    ApacheKafkaOffsetPosition position1 = new ApacheKafkaOffsetPosition(1);
    ApacheKafkaOffsetPosition position2 = new ApacheKafkaOffsetPosition(2);
    ApacheKafkaOffsetPosition position3 = new ApacheKafkaOffsetPosition(1);
    assertEquals(position1, position1);
    assertEquals(position1, position3);
    assertNotEquals(position1, position2);
    assertNotEquals(position1, null);
    assertNotEquals(position1, getDifferentTypePosition());
  }

  @Test
  public void testHashCode() {
    ApacheKafkaOffsetPosition position1 = new ApacheKafkaOffsetPosition(1);
    ApacheKafkaOffsetPosition position2 = new ApacheKafkaOffsetPosition(2);
    ApacheKafkaOffsetPosition position3 = new ApacheKafkaOffsetPosition(1);
    assertEquals(position1.hashCode(), position1.hashCode());
    assertEquals(position1.hashCode(), position3.hashCode());
    assertNotEquals(position1.hashCode(), position2.hashCode());
  }

  @Test
  public void testToString() {
    ApacheKafkaOffsetPosition position1 = new ApacheKafkaOffsetPosition(1);
    assertEquals(position1.toString(), "ApacheKafkaOffsetPosition{offset=1}");
  }

  @Test
  public void testGetOffset() {
    ApacheKafkaOffsetPosition position1 = new ApacheKafkaOffsetPosition(1);
    assertEquals(position1.getOffset(), 1);
  }
}
