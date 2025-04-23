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
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class ApacheKafkaOffsetPositionFactoryTest {
  private PubSubPositionFactory factory;
  private final int validTypeId = APACHE_KAFKA_OFFSET_POSITION_TYPE_ID;

  @BeforeMethod
  public void setUp() {
    factory = new ApacheKafkaOffsetPositionFactory(validTypeId);
  }

  @AfterMethod
  public void tearDown() {
    factory = null;
  }

  @Test
  public void testCreateFromWireFormatSuccess() {
    ApacheKafkaOffsetPosition position = ApacheKafkaOffsetPosition.of(12345L);
    PubSubPositionWireFormat wireFormat = position.getPositionWireFormat();

    PubSubPosition deserialized = factory.createFromWireFormat(wireFormat);
    assertTrue(deserialized instanceof ApacheKafkaOffsetPosition);
    assertEquals(((ApacheKafkaOffsetPosition) deserialized).getOffset(), 12345L);
  }

  @Test
  public void testCreateFromByteBufferSuccess() {
    ApacheKafkaOffsetPosition position = ApacheKafkaOffsetPosition.of(67890L);
    ByteBuffer buffer = position.getPositionWireFormat().getRawBytes();

    PubSubPosition deserialized = factory.createFromByteBuffer(buffer);
    assertTrue(deserialized instanceof ApacheKafkaOffsetPosition);
    assertEquals(((ApacheKafkaOffsetPosition) deserialized).getOffset(), 67890L);
  }

  @Test
  public void testGetPubSubPositionClassName() {
    assertEquals(factory.getPubSubPositionClassName(), ApacheKafkaOffsetPosition.class.getName());
  }

  @Test
  public void testGetPositionTypeId() {
    assertEquals(factory.getPositionTypeId(), validTypeId);
  }

  @Test
  public void testCreateFromWireFormatThrowsOnInvalidBuffer() {
    ByteBuffer badBuffer = ByteBuffer.allocate(0);
    PubSubPositionWireFormat wireFormat = new PubSubPositionWireFormat();
    wireFormat.setType(validTypeId);
    wireFormat.setRawBytes(badBuffer);

    VeniceException exception = expectThrows(VeniceException.class, () -> factory.createFromWireFormat(wireFormat));
    assertTrue(exception.getMessage().contains("Failed to deserialize Apache Kafka offset position"));
  }

  @Test
  public void testCreateFromByteBufferThrowsOnCorruptedBuffer() {
    ByteBuffer corruptedBuffer = ByteBuffer.wrap(new byte[0]); // too short to hold a long

    ApacheKafkaOffsetPositionFactory factory =
        new ApacheKafkaOffsetPositionFactory(APACHE_KAFKA_OFFSET_POSITION_TYPE_ID);

    VeniceException ex = expectThrows(VeniceException.class, () -> factory.createFromByteBuffer(corruptedBuffer));
    assertTrue(
        ex.getMessage().contains("Failed to deserialize Apache Kafka offset position"),
        "Got: " + ex.getMessage());
  }

  @Test
  public void testCreateFromWireFormatThrowsOnTypeIdMismatch() {
    ApacheKafkaOffsetPositionFactory factory = new ApacheKafkaOffsetPositionFactory(validTypeId);

    PubSubPositionWireFormat wireFormat = new PubSubPositionWireFormat();
    wireFormat.setType(Integer.MAX_VALUE);
    wireFormat.setRawBytes(ByteBuffer.wrap(new byte[] { 0 }));

    VeniceException exception = expectThrows(VeniceException.class, () -> factory.createFromWireFormat(wireFormat));

    assertTrue(exception.getMessage().contains("Position type ID mismatch"), "Got: " + exception.getMessage());
  }
}
