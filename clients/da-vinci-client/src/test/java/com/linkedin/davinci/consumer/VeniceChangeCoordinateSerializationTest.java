package com.linkedin.davinci.consumer;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.memory.ClassSizeEstimator;
import com.linkedin.venice.pubsub.PubSubPositionDeserializer;
import com.linkedin.venice.pubsub.PubSubPositionFactory;
import com.linkedin.venice.pubsub.PubSubUtil;
import com.linkedin.venice.pubsub.adapter.kafka.common.ApacheKafkaOffsetPosition;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubPositionWireFormat;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Comprehensive test suite for VeniceChangeCoordinate serialization and deserialization.
 */
public class VeniceChangeCoordinateSerializationTest {

  // Test constants
  private static final String TEST_STORE_NAME = "test_store";
  private static final String TEST_TOPIC = TEST_STORE_NAME + "_v1";
  private static final Integer TEST_PARTITION = 42;
  private static final long TEST_OFFSET = 12345L;
  private static final long TEST_CONSUMER_SEQUENCE_ID = 54321L;

  // Test fixtures
  private VeniceChangeCoordinate testCoordinate;
  private ApacheKafkaOffsetPosition testPosition;

  @BeforeMethod
  public void setUp() {
    testPosition = ApacheKafkaOffsetPosition.of(TEST_OFFSET);
    testCoordinate = new VeniceChangeCoordinate(TEST_TOPIC, testPosition, TEST_PARTITION, TEST_CONSUMER_SEQUENCE_ID);
  }

  @Test
  public void testV3FormatSerializationRoundTrip() throws IOException, ClassNotFoundException {
    // Test that V3 format can serialize and deserialize correctly
    byte[] serializedData = serializeToBytes(testCoordinate);
    VeniceChangeCoordinate deserializedCoordinate = deserializeFromBytes(serializedData);

    // Verify all fields are preserved
    assertCoordinatesEqual(testCoordinate, deserializedCoordinate);
  }

  @Test
  public void testV3FormatIncludesVersionAndFactory() throws IOException, ClassNotFoundException {
    // Test that V3 format includes version tag, factory class name and consumer sequence id
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    ObjectOutputStream objectOutput = new ObjectOutputStream(outputStream);

    testCoordinate.writeExternal(objectOutput);
    objectOutput.flush();

    byte[] data = outputStream.toByteArray();

    // Deserialize manually to verify structure
    ByteArrayInputStream inputStream = new ByteArrayInputStream(data);
    ObjectInputStream objectInput = new ObjectInputStream(inputStream);

    // Read core fields
    String topic = objectInput.readUTF();
    int partition = objectInput.readInt();
    PubSubPositionWireFormat wireFormat = (PubSubPositionWireFormat) objectInput.readObject();

    // Read version field
    int version = objectInput.readShort();
    assertEquals(version, 3);

    // Read factory class name
    String factoryClassName = objectInput.readUTF();
    assertTrue(factoryClassName.contains("ApacheKafkaOffsetPositionFactory"));

    // Read consumer sequence id
    long consumerSequenceId = objectInput.readLong();
    assertEquals(consumerSequenceId, TEST_CONSUMER_SEQUENCE_ID);

    assertEquals(topic, TEST_TOPIC);
    assertEquals(partition, TEST_PARTITION.intValue());
    assertNotNull(wireFormat);
  }

  @Test
  public void testBackwardCompatibilityWithV1Format() throws IOException, ClassNotFoundException {
    // Create V1 format data (without version and factory fields)
    byte[] v1Data = createV1FormatData(TEST_TOPIC, TEST_PARTITION, testPosition);

    // New code should be able to read V1 data
    VeniceChangeCoordinate deserializedCoordinate = deserializeFromBytes(v1Data);

    // Verify core fields are preserved
    assertEquals(deserializedCoordinate.getTopic(), TEST_TOPIC);
    assertEquals(deserializedCoordinate.getPartition(), TEST_PARTITION);
    assertEquals(PubSubUtil.comparePubSubPositions(deserializedCoordinate.getPosition(), testPosition), 0);
  }

  @Test
  public void testV1FormatFallbackBehavior() throws IOException, ClassNotFoundException {
    // Test that when version field reading fails, it falls back to V1 deserialization
    byte[] v1Data = createV1FormatData(TEST_TOPIC, TEST_PARTITION, testPosition);

    VeniceChangeCoordinate coordinate = new VeniceChangeCoordinate();
    coordinate.setPubSubPositionDeserializer(PubSubPositionDeserializer.DEFAULT_DESERIALIZER);

    ByteArrayInputStream inputStream = new ByteArrayInputStream(v1Data);
    ObjectInputStream objectInput = new ObjectInputStream(inputStream);

    coordinate.readExternal(objectInput);

    // Should successfully deserialize using fallback
    assertEquals(coordinate.getTopic(), TEST_TOPIC);
    assertEquals(coordinate.getPartition(), TEST_PARTITION);
  }

  @Test
  public void testCustomPubSubPositionSerialization() throws IOException, ClassNotFoundException {
    // Test serialization with a custom PubSubPosition implementation
    CustomTestPosition customPosition = new CustomTestPosition(777L);
    VeniceChangeCoordinate customCoordinate = new VeniceChangeCoordinate(TEST_TOPIC, customPosition, TEST_PARTITION);

    // Serialize and deserialize
    byte[] data = serializeToBytes(customCoordinate);
    VeniceChangeCoordinate deserializedCoordinate = deserializeFromBytes(data);

    // Verify custom position is preserved
    assertEquals(deserializedCoordinate.getTopic(), TEST_TOPIC);
    assertEquals(deserializedCoordinate.getPartition(), TEST_PARTITION);
    assertTrue(deserializedCoordinate.getPosition() instanceof CustomTestPosition);
    assertEquals(PubSubUtil.comparePubSubPositions(deserializedCoordinate.getPosition(), customPosition), 0);
  }

  @Test
  public void testInvalidFactoryClassNameFallback() throws IOException {
    // Test behavior when factory class name is invalid/non-existent

    // Create data with invalid factory class name by manually constructing it
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    ObjectOutputStream objectOutput = new ObjectOutputStream(outputStream);

    // Write core fields
    objectOutput.writeUTF(TEST_TOPIC);
    objectOutput.writeInt(TEST_PARTITION);
    objectOutput.writeObject(testPosition.getPositionWireFormat());

    // Write version
    objectOutput.writeUTF("2"); // Version 2

    // Write invalid factory class name
    objectOutput.writeUTF("com.nonexistent.InvalidFactory");
    objectOutput.flush();

    byte[] corruptedData = outputStream.toByteArray();

    // Should throw exception during deserialization
    assertThrows(VeniceException.class, () -> deserializeFromBytes(corruptedData));
  }

  @Test
  public void testCorruptedDataGracefulFallback() throws IOException, ClassNotFoundException {
    // Test that corrupted V2 data falls back to V1 parsing when possible

    // Create truncated V2 data (missing factory class name)
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    ObjectOutputStream objectOutput = new ObjectOutputStream(outputStream);

    // Write core fields
    objectOutput.writeUTF(TEST_TOPIC);
    objectOutput.writeInt(TEST_PARTITION);
    objectOutput.writeObject(testPosition.getPositionWireFormat());

    // Write version but no factory class name (truncated)
    objectOutput.writeShort(2); // Version 2
    objectOutput.flush();

    byte[] truncatedData = outputStream.toByteArray();

    // Should fall back to V1 behavior and use default deserializer
    VeniceChangeCoordinate coordinate = deserializeFromBytes(truncatedData);
    assertEquals(coordinate.getTopic(), TEST_TOPIC);
    assertEquals(coordinate.getPartition(), TEST_PARTITION);
  }

  @Test
  public void testUnsupportedVersionHandling() throws IOException {
    // Test handling of unsupported version strings

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    ObjectOutputStream objectOutput = new ObjectOutputStream(outputStream);

    // Write core fields
    objectOutput.writeUTF(TEST_TOPIC);
    objectOutput.writeInt(TEST_PARTITION);
    objectOutput.writeObject(testPosition.getPositionWireFormat());

    // Write unsupported version
    objectOutput.writeShort(125);
    objectOutput.writeUTF(testPosition.getFactoryClassName());
    objectOutput.flush();

    byte[] dataWithUnsupportedVersion = outputStream.toByteArray();

    // Should throw VeniceException for unsupported version
    Exception e = expectThrows(VeniceException.class, () -> deserializeFromBytes(dataWithUnsupportedVersion));
    assertTrue(
        e.getMessage().contains("Unsupported VeniceChangeCoordinate version: 125"),
        "Expected unsupported version exception, but got: " + e.getMessage());
  }

  @Test
  public void testBase64EncodingRoundtrip() throws IOException, ClassNotFoundException {
    // Test the Base64 string encoding and decoding methods
    String encodedString = VeniceChangeCoordinate.convertVeniceChangeCoordinateToStringAndEncode(testCoordinate);

    assertNotNull(encodedString);
    assertTrue(encodedString.length() > 0);

    // Decode and verify
    VeniceChangeCoordinate decodedCoordinate = VeniceChangeCoordinate
        .decodeStringAndConvertToVeniceChangeCoordinate(PubSubPositionDeserializer.DEFAULT_DESERIALIZER, encodedString);

    assertCoordinatesEqual(testCoordinate, decodedCoordinate);
  }

  @Test
  public void testBase64EncodingWithCustomPosition() throws IOException, ClassNotFoundException {
    // Test Base64 encoding with custom position types
    CustomTestPosition customPosition = new CustomTestPosition(999L);
    VeniceChangeCoordinate customCoordinate = new VeniceChangeCoordinate(TEST_TOPIC, customPosition, TEST_PARTITION);

    String encodedString = VeniceChangeCoordinate.convertVeniceChangeCoordinateToStringAndEncode(customCoordinate);
    VeniceChangeCoordinate decodedCoordinate = VeniceChangeCoordinate
        .decodeStringAndConvertToVeniceChangeCoordinate(PubSubPositionDeserializer.DEFAULT_DESERIALIZER, encodedString);

    assertCoordinatesEqual(customCoordinate, decodedCoordinate);
  }

  @Test
  public void testBase64EncodingV1BackwardCompatibility() throws IOException, ClassNotFoundException {
    // Test that Base64 encoding works with V1 data
    byte[] v1Data = createV1FormatData(TEST_TOPIC, TEST_PARTITION, testPosition);
    String encodedV1 = java.util.Base64.getEncoder().encodeToString(v1Data);

    VeniceChangeCoordinate decodedCoordinate = VeniceChangeCoordinate
        .decodeStringAndConvertToVeniceChangeCoordinate(PubSubPositionDeserializer.DEFAULT_DESERIALIZER, encodedV1);

    assertEquals(decodedCoordinate.getTopic(), TEST_TOPIC);
    assertEquals(decodedCoordinate.getPartition(), TEST_PARTITION);
  }

  @Test
  public void testNullFieldValidation() {
    // Test that serialization fails gracefully with null required fields

    // Null topic
    assertThrows(IllegalStateException.class, () -> {
      VeniceChangeCoordinate invalidCoordinate = new VeniceChangeCoordinate(null, testPosition, TEST_PARTITION);
      serializeToBytes(invalidCoordinate);
    });

    // Null position
    assertThrows(IllegalStateException.class, () -> {
      VeniceChangeCoordinate invalidCoordinate = new VeniceChangeCoordinate(TEST_TOPIC, null, TEST_PARTITION);
      serializeToBytes(invalidCoordinate);
    });

    // Null partition
    assertThrows(IllegalStateException.class, () -> {
      VeniceChangeCoordinate invalidCoordinate = new VeniceChangeCoordinate(TEST_TOPIC, testPosition, null);
      serializeToBytes(invalidCoordinate);
    });
  }

  @Test
  public void testEmptyStringHandling() throws IOException, ClassNotFoundException {
    // Test handling of empty strings in topic names
    VeniceChangeCoordinate emptyTopicCoordinate = new VeniceChangeCoordinate("", testPosition, TEST_PARTITION);

    byte[] data = serializeToBytes(emptyTopicCoordinate);
    VeniceChangeCoordinate deserializedCoordinate = deserializeFromBytes(data);

    assertEquals(deserializedCoordinate.getTopic(), "");
    assertEquals(deserializedCoordinate.getPartition(), TEST_PARTITION);
  }

  /**
   * Serializes a VeniceChangeCoordinate to byte array
   */
  private byte[] serializeToBytes(VeniceChangeCoordinate coordinate) throws IOException {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    ObjectOutputStream objectOutput = new ObjectOutputStream(outputStream);
    coordinate.writeExternal(objectOutput);
    objectOutput.flush();
    return outputStream.toByteArray();
  }

  /**
   * Deserializes a VeniceChangeCoordinate from byte array
   */
  private VeniceChangeCoordinate deserializeFromBytes(byte[] data) throws IOException, ClassNotFoundException {
    ByteArrayInputStream inputStream = new ByteArrayInputStream(data);
    ObjectInputStream objectInput = new ObjectInputStream(inputStream);
    VeniceChangeCoordinate coordinate = new VeniceChangeCoordinate();
    coordinate.readExternal(objectInput);
    return coordinate;
  }

  /**
   * Creates V1 format data (without version and factory fields)
   */
  private byte[] createV1FormatData(String topic, Integer partition, PubSubPosition position) throws IOException {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    ObjectOutputStream objectOutput = new ObjectOutputStream(outputStream);

    // V1 format: only core fields
    objectOutput.writeUTF(topic);
    objectOutput.writeInt(partition);
    objectOutput.writeObject(position.getPositionWireFormat());

    objectOutput.flush();
    return outputStream.toByteArray();
  }

  /**
   * Asserts that two VeniceChangeCoordinate instances are equal
   */
  private void assertCoordinatesEqual(VeniceChangeCoordinate expected, VeniceChangeCoordinate actual) {
    assertEquals(actual.getTopic(), expected.getTopic());
    assertEquals(actual.getPartition(), expected.getPartition());
    assertEquals(PubSubUtil.comparePubSubPositions(actual.getPosition(), expected.getPosition()), 0);
    assertEquals(actual.getPosition().getClass(), expected.getPosition().getClass());
  }

  /**
   * Custom PubSubPosition implementation for testing
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
      wireFormat.type = 999; // Custom type ID
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
   * Custom PubSubPositionFactory for testing
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
