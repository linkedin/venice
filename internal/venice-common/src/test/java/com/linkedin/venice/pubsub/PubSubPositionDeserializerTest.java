package com.linkedin.venice.pubsub;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pubsub.adapter.kafka.common.ApacheKafkaOffsetPosition;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubPositionWireFormat;
import com.linkedin.venice.pubsub.api.PubSubSymbolicPosition;
import java.nio.ByteBuffer;
import org.testng.annotations.Test;


/**
 * Unit tests for PubSubPositionFactory
 */
public class PubSubPositionDeserializerTest {
  @Test
  public void testToPositionForApacheKafkaPosition() {
    ApacheKafkaOffsetPosition position = ApacheKafkaOffsetPosition.of(123);
    PubSubPositionWireFormat wireFormat = position.getPositionWireFormat();

    PubSubPosition position1 = PubSubPositionDeserializer.getPositionFromWireFormat(wireFormat);
    assertTrue(position1 instanceof ApacheKafkaOffsetPosition);
    assertEquals(position1, position);

    byte[] wireFormatBytes = position1.toWireFormatBytes();
    PubSubPosition positionFromBytes = PubSubPositionDeserializer.getPositionFromWireFormat(wireFormatBytes);
    assertTrue(positionFromBytes instanceof ApacheKafkaOffsetPosition);
    assertEquals(positionFromBytes, position);

    ByteBuffer wireFormatBuffer = position1.toWireFormatBuffer();
    PubSubPosition positionFromBuffer = PubSubPositionDeserializer.getPositionFromWireFormat(wireFormatBuffer.array());
    assertTrue(positionFromBuffer instanceof ApacheKafkaOffsetPosition);
    assertEquals(positionFromBuffer, position);
  }

  @Test
  public void testToPositionForUnsupportedPosition() {
    PubSubPositionWireFormat wireFormat = new PubSubPositionWireFormat();
    wireFormat.type = Integer.MAX_VALUE;
    Exception e =
        expectThrows(VeniceException.class, () -> PubSubPositionDeserializer.getPositionFromWireFormat(wireFormat));
    assertTrue(e.getMessage().contains("PubSub position type ID not found:"), "Got: " + e.getMessage());
  }

  @Test
  public void testSerDerForSymbolicPositions() {
    byte[] startPositionWireFormatBytes = PubSubSymbolicPosition.EARLIEST.toWireFormatBytes();
    PubSubPosition positionFromBytes =
        PubSubPositionDeserializer.getPositionFromWireFormat(startPositionWireFormatBytes);
    assertTrue(positionFromBytes.isSymbolic());
    assertEquals(
        positionFromBytes,
        PubSubSymbolicPosition.EARLIEST,
        "Expected position to be EARLIEST, but got: " + positionFromBytes);
    assertSame(
        positionFromBytes,
        PubSubSymbolicPosition.EARLIEST,
        "Expected position to be EARLIEST, but got: " + positionFromBytes);

    byte[] endPositionWireFormatBytes = PubSubSymbolicPosition.LATEST.toWireFormatBytes();
    PubSubPosition positionFromEndBytes =
        PubSubPositionDeserializer.getPositionFromWireFormat(endPositionWireFormatBytes);
    assertTrue(positionFromEndBytes.isSymbolic());
    assertEquals(
        positionFromEndBytes,
        PubSubSymbolicPosition.LATEST,
        "Expected position to be LATEST, but got: " + positionFromEndBytes);
    assertSame(
        positionFromEndBytes,
        PubSubSymbolicPosition.LATEST,
        "Expected position to be LATEST, but got: " + positionFromEndBytes);
  }

  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Cannot deserialize null wire format position")
  public void testGetPositionFromWireFormatBytesThrowsExceptionWhenWireFormatBytesIsNull() {
    PubSubPositionDeserializer.getPositionFromWireFormat((byte[]) null);
  }

  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Cannot deserialize null wire format position")
  public void testGetPositionFromWireFormatBytesThrowsExceptionWhenWireFormatBytesIsNull1() {
    PubSubPositionDeserializer.getPositionFromWireFormat((PubSubPositionWireFormat) null);
  }
}
