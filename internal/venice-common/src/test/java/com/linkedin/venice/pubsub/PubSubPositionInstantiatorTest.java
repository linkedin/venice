package com.linkedin.venice.pubsub;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pubsub.adapter.kafka.common.ApacheKafkaOffsetPosition;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubPositionWireFormat;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
import org.testng.annotations.Test;


/**
 * Unit tests for PubSubPositionFactory
 */
public class PubSubPositionInstantiatorTest {
  @Test
  public void testConvertToPositionForApacheKafkaPosition() {
    ApacheKafkaOffsetPosition position = ApacheKafkaOffsetPosition.of(123);
    PubSubPositionWireFormat wireFormat = position.getPositionWireFormat();

    PubSubPosition position1 = PubSubPositionInstantiator.getPositionFromWireFormat(wireFormat);
    assertTrue(position1 instanceof ApacheKafkaOffsetPosition);
    assertEquals(position1, position);
  }

  @Test
  public void testConvertToPositionForUnsupportedPosition() {
    PubSubPositionWireFormat wireFormat = new PubSubPositionWireFormat();
    wireFormat.type = Integer.MAX_VALUE;
    Exception e =
        expectThrows(VeniceException.class, () -> PubSubPositionInstantiator.getPositionFromWireFormat(wireFormat));
    assertTrue(e.getMessage().contains("PubSub position type ID not found:"), "Got: " + e.getMessage());
  }

  @Test
  public void testConvertToPositionFromWireFormatPositionBytes() {
    ApacheKafkaOffsetPosition kafkaPosition = ApacheKafkaOffsetPosition.of(567);
    PubSubPositionWireFormat kafkaPositionWireFormat = kafkaPosition.getPositionWireFormat();
    InternalAvroSpecificSerializer<PubSubPositionWireFormat> wireFormatSerializer =
        AvroProtocolDefinition.PUBSUB_POSITION_WIRE_FORMAT.getSerializer();
    byte[] wireFormatBytes = wireFormatSerializer.serialize(kafkaPositionWireFormat).array();

    PubSubPosition position = PubSubPositionInstantiator.getPositionFromWireFormat(wireFormatBytes);
    assertTrue(position instanceof ApacheKafkaOffsetPosition);
    assertEquals(position, kafkaPosition);
  }

  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Cannot deserialize null wire format position")
  public void testGetPositionFromWireFormatBytesThrowsExceptionWhenWireFormatBytesIsNull() {
    PubSubPositionInstantiator.getPositionFromWireFormat((byte[]) null);
  }

  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Cannot deserialize null wire format position")
  public void testGetPositionFromWireFormatBytesThrowsExceptionWhenWireFormatBytesIsNull1() {
    PubSubPositionInstantiator.getPositionFromWireFormat((PubSubPositionWireFormat) null);
  }
}
