package com.linkedin.venice.pubsub;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.memory.ClassSizeEstimator;
import com.linkedin.venice.pubsub.adapter.kafka.common.ApacheKafkaOffsetPosition;
import com.linkedin.venice.pubsub.adapter.kafka.common.ApacheKafkaOffsetPositionFactory;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubPositionWireFormat;
import com.linkedin.venice.pubsub.api.PubSubSymbolicPosition;
import java.nio.ByteBuffer;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Comprehensive unit tests for PubSubPositionDeserializer
 *
 * Tests cover:
 * - Constructor and instance methods
 * - Static convenience methods
 * - Custom factory class name deserialization
 * - Wire format deserialization utilities
 * - Error handling and null input validation
 * - Different position type support
 */
public class PubSubPositionDeserializerTest {
  private PubSubPositionDeserializer customDeserializer;
  private PubSubPositionTypeRegistry customRegistry;

  @BeforeMethod
  public void setUp() {
    // Create custom registry for testing instance methods
    customRegistry = PubSubPositionTypeRegistry.RESERVED_POSITION_TYPE_REGISTRY;
    customDeserializer = new PubSubPositionDeserializer(customRegistry);
  }

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

    PubSubPosition positionFromBuffer =
        PubSubPositionDeserializer.getPositionFromWireFormat(position1.toWireFormatBuffer());
    assertTrue(positionFromBuffer instanceof ApacheKafkaOffsetPosition);
    assertEquals(positionFromBuffer, position);
  }

  @Test
  public void testToPositionForUnsupportedPosition() {
    PubSubPositionWireFormat wireFormat = new PubSubPositionWireFormat();
    wireFormat.type = Integer.MAX_VALUE;
    Exception e =
        expectThrows(VeniceException.class, () -> PubSubPositionDeserializer.getPositionFromWireFormat(wireFormat));
    assertTrue(e.getMessage().contains("PubSub position type ID not found: 2147483647"), "Got: " + e.getMessage());
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

  @Test
  public void testConstructorWithCustomRegistry() {
    // Test that constructor properly initializes with a custom registry
    assertNotNull(customDeserializer);

    // Test that it can deserialize using the custom registry
    ApacheKafkaOffsetPosition position = ApacheKafkaOffsetPosition.of(456L);
    PubSubPositionWireFormat wireFormat = position.getPositionWireFormat();

    PubSubPosition deserialized = customDeserializer.toPosition(wireFormat);
    assertEquals(deserialized, position);
  }

  @Test
  public void testInstanceToPositionWithWireFormat() {
    // Test instance method toPosition(PubSubPositionWireFormat)
    ApacheKafkaOffsetPosition position = ApacheKafkaOffsetPosition.of(789L);
    PubSubPositionWireFormat wireFormat = position.getPositionWireFormat();

    PubSubPosition result = customDeserializer.toPosition(wireFormat);
    assertTrue(result instanceof ApacheKafkaOffsetPosition);
    assertEquals(result, position);
  }

  @Test
  public void testInstanceToPositionWithByteArray() {
    // Test instance method toPosition(byte[])
    ApacheKafkaOffsetPosition position = ApacheKafkaOffsetPosition.of(321L);
    byte[] wireFormatBytes = position.toWireFormatBytes();

    PubSubPosition result = customDeserializer.toPosition(wireFormatBytes);
    assertTrue(result instanceof ApacheKafkaOffsetPosition);
    assertEquals(result, position);
  }

  @Test
  public void testInstanceToPositionWithByteBuffer() {
    // Test instance method toPosition(ByteBuffer)
    ApacheKafkaOffsetPosition position = ApacheKafkaOffsetPosition.of(654L);
    ByteBuffer wireFormatBuffer = position.toWireFormatBuffer();

    PubSubPosition result = customDeserializer.toPosition(wireFormatBuffer);
    assertTrue(result instanceof ApacheKafkaOffsetPosition);
    assertEquals(result, position);
  }

  @Test
  public void testInstanceMethodsWithNullInputs() {
    // Test null handling for instance methods
    assertThrows(IllegalArgumentException.class, () -> customDeserializer.toPosition((PubSubPositionWireFormat) null));
    assertThrows(IllegalArgumentException.class, () -> customDeserializer.toPosition((byte[]) null));
    assertThrows(IllegalArgumentException.class, () -> customDeserializer.toPosition((ByteBuffer) null));
  }

  @Test
  public void testInstanceMethodWithUnknownTypeId() {
    // Test instance method with unknown type ID
    PubSubPositionWireFormat wireFormat = new PubSubPositionWireFormat();
    wireFormat.type = 99999; // Unknown type ID
    wireFormat.rawBytes = ByteBuffer.allocate(8).putLong(123L);
    wireFormat.rawBytes.rewind();

    VeniceException exception = expectThrows(VeniceException.class, () -> customDeserializer.toPosition(wireFormat));
    assertTrue(
        exception.getMessage().contains("PubSub position type ID not found: 99999"),
        "Unexpected exception message: " + exception.getMessage());
  }

  @Test
  public void testDeserializePubSubPositionWithByteBufferAndFactoryClassName() {
    // Test deserializePubSubPosition(ByteBuffer, String)
    ApacheKafkaOffsetPosition position = ApacheKafkaOffsetPosition.of(888L);
    ByteBuffer wireBytes = ByteBuffer.wrap(position.toWireFormatBytes());
    String factoryClassName = ApacheKafkaOffsetPositionFactory.class.getName();

    PubSubPosition result = PubSubPositionDeserializer.deserializePubSubPosition(wireBytes, factoryClassName);
    assertTrue(result instanceof ApacheKafkaOffsetPosition);
    assertEquals(result, position);
  }

  @Test
  public void testDeserializePubSubPositionWithByteArrayAndFactoryClassName() {
    // Test deserializePubSubPosition(byte[], String)
    ApacheKafkaOffsetPosition position = ApacheKafkaOffsetPosition.of(777L);
    byte[] wireBytes = position.toWireFormatBytes();
    String factoryClassName = "com.linkedin.venice.pubsub.adapter.kafka.common.ApacheKafkaOffsetPositionFactory";

    PubSubPosition result = PubSubPositionDeserializer.deserializePubSubPosition(wireBytes, factoryClassName);
    assertTrue(result instanceof ApacheKafkaOffsetPosition);
    assertEquals(result, position);
  }

  @Test
  public void testDeserializePubSubPositionWithWireFormatAndFactoryClassName() {
    // Test deserializePubSubPosition(PubSubPositionWireFormat, String)
    ApacheKafkaOffsetPosition position = ApacheKafkaOffsetPosition.of(555L);
    PubSubPositionWireFormat wireFormat = position.getPositionWireFormat();
    String factoryClassName = "com.linkedin.venice.pubsub.adapter.kafka.common.ApacheKafkaOffsetPositionFactory";

    PubSubPosition result = PubSubPositionDeserializer.deserializePubSubPosition(wireFormat, factoryClassName);
    assertTrue(result instanceof ApacheKafkaOffsetPosition);
    assertEquals(result, position);
  }

  @Test
  public void testDeserializePubSubPositionWithCustomPosition() {
    // Test custom factory class name deserialization with a custom position
    CustomTestPosition position = new CustomTestPosition(999L);
    PubSubPositionWireFormat wireFormat = position.getPositionWireFormat();
    String factoryClassName = CustomTestPositionFactory.class.getName();

    PubSubPosition result = PubSubPositionDeserializer.deserializePubSubPosition(wireFormat, factoryClassName);
    assertTrue(result instanceof CustomTestPosition);
    assertEquals(result, position);
  }

  @Test
  public void testDeserializePubSubPositionWithInvalidFactoryClassName() {
    // Test with non-existent factory class name
    ApacheKafkaOffsetPosition position = ApacheKafkaOffsetPosition.of(123L);
    PubSubPositionWireFormat wireFormat = position.getPositionWireFormat();
    String invalidFactoryClassName = "com.nonexistent.InvalidFactory";

    VeniceException exception = expectThrows(
        VeniceException.class,
        () -> PubSubPositionDeserializer.deserializePubSubPosition(wireFormat, invalidFactoryClassName));
    assertTrue(exception.getMessage().contains("Failed to deserialize PubSubPosition using factory class:"));
  }

  @Test
  public void testDeserializePubSubPositionWithNonFactoryClassName() {
    // Test with a class that doesn't implement PubSubPositionFactory
    ApacheKafkaOffsetPosition position = ApacheKafkaOffsetPosition.of(123L);
    PubSubPositionWireFormat wireFormat = position.getPositionWireFormat();
    String nonFactoryClassName = String.class.getName(); // String is not a PubSubPositionFactory

    VeniceException exception = expectThrows(
        VeniceException.class,
        () -> PubSubPositionDeserializer.deserializePubSubPosition(wireFormat, nonFactoryClassName));
    assertTrue(exception.getMessage().contains("Failed to deserialize PubSubPosition using factory class:"));
  }

  @Test
  public void testKnownTypeIdWithWrongFactoryClassName() {
    // Test known type ID but incorrect factory class name
    ApacheKafkaOffsetPosition position = ApacheKafkaOffsetPosition.of(123L);
    PubSubPositionWireFormat wireFormat = position.getPositionWireFormat();
    String wrongFactoryClassName = "com.linkedin.venice.pubsub.adapter.kafka.common.WrongFactoryClass";

    VeniceException exception = expectThrows(
        VeniceException.class,
        () -> PubSubPositionDeserializer.deserializePubSubPosition(wireFormat, wrongFactoryClassName));
    assertTrue(
        exception.getMessage().contains("Failed to deserialize PubSubPosition using factory class:"),
        "Unexpected exception message: " + exception.getMessage());
    assertTrue(
        exception.getMessage().contains(wrongFactoryClassName),
        "Exception should mention the wrong factory class name: " + exception.getMessage());
  }

  @Test
  public void testDeserializeWireFormatFromByteArray() {
    // Test deserializeWireFormat(byte[])
    ApacheKafkaOffsetPosition position = ApacheKafkaOffsetPosition.of(111L);
    byte[] wireFormatBytes = position.toWireFormatBytes();

    PubSubPositionWireFormat wireFormat = PubSubPositionDeserializer.deserializeWireFormat(wireFormatBytes);
    assertNotNull(wireFormat);
    assertTrue(wireFormat.type >= 0); // Should have valid type ID
    assertNotNull(wireFormat.rawBytes);
  }

  @Test
  public void testDeserializeWireFormatFromByteBuffer() {
    // Test deserializeWireFormat(ByteBuffer)
    ApacheKafkaOffsetPosition position = ApacheKafkaOffsetPosition.of(222L);
    ByteBuffer wireFormatBuffer = position.toWireFormatBuffer();

    PubSubPositionWireFormat wireFormat = PubSubPositionDeserializer.deserializeWireFormat(wireFormatBuffer);
    assertNotNull(wireFormat);
    assertTrue(wireFormat.type >= 0); // Should have valid type ID
    assertNotNull(wireFormat.rawBytes);
  }

  @Test
  public void testDeserializeWireFormatRoundTrip() {
    // Test that deserializeWireFormat can reconstruct wire format that produces same position
    ApacheKafkaOffsetPosition originalPosition = ApacheKafkaOffsetPosition.of(333L);
    byte[] wireFormatBytes = originalPosition.toWireFormatBytes();

    // Deserialize wire format
    PubSubPositionWireFormat reconstructedWireFormat =
        PubSubPositionDeserializer.deserializeWireFormat(wireFormatBytes);

    // Convert back to position
    PubSubPosition reconstructedPosition =
        PubSubPositionDeserializer.getPositionFromWireFormat(reconstructedWireFormat);

    assertEquals(reconstructedPosition, originalPosition);
  }

  @Test
  public void testDeserializeWireFormatWithSymbolicPositions() {
    // Test wire format deserialization with symbolic positions
    byte[] earliestBytes = PubSubSymbolicPosition.EARLIEST.toWireFormatBytes();
    PubSubPositionWireFormat earliestWireFormat = PubSubPositionDeserializer.deserializeWireFormat(earliestBytes);
    assertNotNull(earliestWireFormat);

    byte[] latestBytes = PubSubSymbolicPosition.LATEST.toWireFormatBytes();
    PubSubPositionWireFormat latestWireFormat = PubSubPositionDeserializer.deserializeWireFormat(latestBytes);
    assertNotNull(latestWireFormat);
  }

  @Test
  public void testStaticMethodsWithNullByteBuffer() {
    // Test static methods with null ByteBuffer
    assertThrows(
        IllegalArgumentException.class,
        () -> PubSubPositionDeserializer.getPositionFromWireFormat((ByteBuffer) null));
  }

  @Test
  public void testDefaultDeserializerIsNotNull() {
    // Test that DEFAULT_DESERIALIZER is properly initialized
    assertNotNull(PubSubPositionDeserializer.DEFAULT_DESERIALIZER);

    // Test that it can deserialize a position
    ApacheKafkaOffsetPosition position = ApacheKafkaOffsetPosition.of(444L);
    PubSubPositionWireFormat wireFormat = position.getPositionWireFormat();

    PubSubPosition result = PubSubPositionDeserializer.DEFAULT_DESERIALIZER.toPosition(wireFormat);
    assertEquals(result, position);
  }

  @Test
  public void testDeserializationWithLargeOffsetValues() {
    // Test with edge case offset values
    ApacheKafkaOffsetPosition maxPosition = ApacheKafkaOffsetPosition.of(Long.MAX_VALUE - 1);
    byte[] wireFormatBytes = maxPosition.toWireFormatBytes();

    PubSubPosition result = PubSubPositionDeserializer.getPositionFromWireFormat(wireFormatBytes);
    assertEquals(result, maxPosition);
  }

  @Test
  public void testMultipleDeserializationsFromSameData() {
    // Test that multiple deserializations from same data produce equal results
    ApacheKafkaOffsetPosition position = ApacheKafkaOffsetPosition.of(555L);
    byte[] wireFormatBytes = position.toWireFormatBytes();

    PubSubPosition result1 = PubSubPositionDeserializer.getPositionFromWireFormat(wireFormatBytes);
    PubSubPosition result2 = PubSubPositionDeserializer.getPositionFromWireFormat(wireFormatBytes);
    PubSubPosition result3 = customDeserializer.toPosition(wireFormatBytes);

    assertEquals(result1, result2);
    assertEquals(result2, result3);
    assertEquals(result1, position);
  }

  /**
   * Custom PubSubPosition implementation for testing factory class name deserialization
   */
  public static class CustomTestPosition implements PubSubPosition {
    private final long offset;
    private static final int SHALLOW_CLASS_OVERHEAD = ClassSizeEstimator.getClassOverhead(CustomTestPosition.class);

    public CustomTestPosition(long offset) {
      this.offset = offset;
    }

    @Override
    public long getNumericOffset() {
      return offset;
    }

    @Override
    public int getHeapSize() {
      return SHALLOW_CLASS_OVERHEAD;
    }

    @Override
    public PubSubPositionWireFormat getPositionWireFormat() {
      PubSubPositionWireFormat wireFormat = new PubSubPositionWireFormat();
      wireFormat.type = 12345; // Custom type ID
      wireFormat.rawBytes = ByteBuffer.allocate(8).putLong(offset);
      wireFormat.rawBytes.rewind();
      return wireFormat;
    }

    @Override
    public Class<? extends PubSubPositionFactory> getFactoryClass() {
      return CustomTestPositionFactory.class;
    }

    @Override
    public String toString() {
      return "CustomTestPosition{offset=" + offset + "}";
    }

    @Override
    public boolean equals(Object obj) {
      return obj instanceof CustomTestPosition && ((CustomTestPosition) obj).offset == this.offset;
    }

    @Override
    public int hashCode() {
      return Long.hashCode(offset);
    }
  }

  /**
   * Custom PubSubPositionFactory for testing factory class name deserialization
   */
  public static class CustomTestPositionFactory extends PubSubPositionFactory {
    public CustomTestPositionFactory(int positionTypeId) {
      super(positionTypeId);
    }

    @Override
    public PubSubPosition fromPositionRawBytes(ByteBuffer positionRawBytes) {
      long offset = positionRawBytes.getLong();
      return new CustomTestPosition(offset);
    }

    @Override
    public String getPubSubPositionClassName() {
      return CustomTestPosition.class.getName();
    }
  }
}
