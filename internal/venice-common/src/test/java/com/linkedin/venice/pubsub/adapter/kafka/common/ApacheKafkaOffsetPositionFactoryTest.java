package com.linkedin.venice.pubsub.adapter.kafka.common;

import static com.linkedin.venice.pubsub.PubSubPositionTypeRegistry.APACHE_KAFKA_OFFSET_POSITION_TYPE_ID;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pubsub.PubSubPositionFactory;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubPositionWireFormat;
import java.nio.ByteBuffer;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class ApacheKafkaOffsetPositionFactoryTest {
  private PubSubPositionFactory factory;
  private static final int TYPE_ID = 9;

  @BeforeMethod(alwaysRun = true)
  public void setUp() {
    factory = new ApacheKafkaOffsetPositionFactory(TYPE_ID);
  }

  @Test
  public void testCreateFromPositionRawBytesSuccess() {
    ApacheKafkaOffsetPosition position = ApacheKafkaOffsetPosition.of(12345L);
    PubSubPositionWireFormat wireFormat = new PubSubPositionWireFormat();
    // Use a custom type ID for testing, but reuse the position's encoded bytes to avoid manually creating Avro-encoded
    // data
    wireFormat.setType(TYPE_ID);
    wireFormat.setRawBytes(position.getPositionWireFormat().getRawBytes());

    PubSubPosition deserialized = factory.fromWireFormat(wireFormat);
    assertTrue(deserialized instanceof ApacheKafkaOffsetPosition);
    assertEquals(((ApacheKafkaOffsetPosition) deserialized).getInternalOffset(), 12345L);
  }

  @Test
  public void testFromPositionRawBytesSuccess() {
    ApacheKafkaOffsetPosition position = ApacheKafkaOffsetPosition.of(67890L);
    ByteBuffer buffer = position.getPositionWireFormat().getRawBytes();

    PubSubPosition deserialized = factory.fromPositionRawBytes(buffer);
    assertTrue(deserialized instanceof ApacheKafkaOffsetPosition);
    assertEquals(((ApacheKafkaOffsetPosition) deserialized).getInternalOffset(), 67890L);
  }

  @Test
  public void testGetPubSubPositionClassName() {
    assertEquals(factory.getPubSubPositionClassName(), ApacheKafkaOffsetPosition.class.getName());
  }

  @Test
  public void testGetPositionTypeId() {
    assertEquals(factory.getPositionTypeId(), TYPE_ID);
  }

  @Test
  public void testCreateFromPositionRawBytesThrowsOnInvalidBuffer() {
    ByteBuffer badBuffer = ByteBuffer.allocate(0);
    PubSubPositionWireFormat wireFormat = new PubSubPositionWireFormat();
    wireFormat.setType(TYPE_ID);
    wireFormat.setRawBytes(badBuffer);

    VeniceException exception = expectThrows(VeniceException.class, () -> factory.fromWireFormat(wireFormat));
    assertTrue(exception.getMessage().contains("Failed to deserialize Apache Kafka offset position"));
  }

  @Test
  public void testFromPositionRawBytes() {
    ByteBuffer corruptedBuffer = ByteBuffer.wrap(new byte[0]); // too short to hold a long

    ApacheKafkaOffsetPositionFactory factory =
        new ApacheKafkaOffsetPositionFactory(APACHE_KAFKA_OFFSET_POSITION_TYPE_ID);

    VeniceException ex = expectThrows(VeniceException.class, () -> factory.fromPositionRawBytes(corruptedBuffer));
    assertTrue(
        ex.getMessage().contains("Failed to deserialize Apache Kafka offset position"),
        "Got: " + ex.getMessage());
  }

  @Test
  public void testCreateFromPositionRawBytesThrowsOnTypeIdMismatch() {
    ApacheKafkaOffsetPositionFactory factory = new ApacheKafkaOffsetPositionFactory(TYPE_ID);

    PubSubPositionWireFormat wireFormat = new PubSubPositionWireFormat();
    wireFormat.setType(Integer.MAX_VALUE);
    wireFormat.setRawBytes(ByteBuffer.wrap(new byte[] { 0 }));

    VeniceException exception = expectThrows(VeniceException.class, () -> factory.fromWireFormat(wireFormat));

    assertTrue(exception.getMessage().contains("Position type ID mismatch"), "Got: " + exception.getMessage());
  }
}
