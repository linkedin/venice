package com.linkedin.venice.pubsub;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.pubsub.adapter.kafka.common.ApacheKafkaOffsetPosition;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubPositionWireFormat;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
import org.testng.annotations.Test;


/**
 * Unit tests for PubSubPositionFactory
 */
public class PubSubPositionFactoryTest {
  @Test
  public void testConvertToPositionForApacheKafkaPosition() {
    ApacheKafkaOffsetPosition position = ApacheKafkaOffsetPosition.of(123);
    PubSubPositionWireFormat wireFormat = position.getPositionWireFormat();

    PubSubPosition position1 = PubSubPositionFactory.getPositionFromWireFormat(wireFormat);
    assertTrue(position1 instanceof ApacheKafkaOffsetPosition);
    assertEquals(position1, position);
  }

  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Cannot convert to position. Unknown position type:.*")
  public void testConvertToPositionForUnsupportedPosition() {
    PubSubPositionWireFormat wireFormat = new PubSubPositionWireFormat();
    wireFormat.type = Integer.MAX_VALUE;
    PubSubPositionFactory.getPositionFromWireFormat(wireFormat);
  }

  @Test
  public void testConvertToPositionFromWireFormatPositionBytes() {
    ApacheKafkaOffsetPosition kafkaPosition = ApacheKafkaOffsetPosition.of(567);
    PubSubPositionWireFormat kafkaPositionWireFormat = kafkaPosition.getPositionWireFormat();
    InternalAvroSpecificSerializer<PubSubPositionWireFormat> wireFormatSerializer =
        AvroProtocolDefinition.PUBSUB_POSITION_WIRE_FORMAT.getSerializer();
    byte[] wireFormatBytes = wireFormatSerializer.serialize(kafkaPositionWireFormat).array();

    PubSubPosition position = PubSubPositionFactory.getPositionFromWireFormat(wireFormatBytes);
    assertTrue(position instanceof ApacheKafkaOffsetPosition);
    assertEquals(position, kafkaPosition);
  }

  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Cannot deserialize null wire format position")
  public void testGetPositionFromWireFormatBytesThrowsExceptionWhenWireFormatBytesIsNull() {
    PubSubPositionFactory.getPositionFromWireFormat((byte[]) null);
  }

  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Cannot deserialize null wire format position")
  public void testGetPositionFromWireFormatBytesThrowsExceptionWhenWireFormatBytesIsNull1() {
    PubSubPositionFactory.getPositionFromWireFormat((PubSubPositionWireFormat) null);
  }
}
