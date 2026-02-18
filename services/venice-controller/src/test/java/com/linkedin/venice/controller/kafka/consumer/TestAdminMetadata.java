package com.linkedin.venice.controller.kafka.consumer;

import static com.linkedin.venice.controller.AdminTopicMetadataAccessor.UNDEFINED_VALUE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.controller.AdminTopicMetadataAccessor;
import com.linkedin.venice.pubsub.PubSubPositionDeserializer;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubSymbolicPosition;
import com.linkedin.venice.pubsub.mock.InMemoryPubSubPosition;
import com.linkedin.venice.pubsub.mock.InMemoryPubSubPositionFactory;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Unit tests for {@link AdminMetadataJSONSerializer} and {@link AdminMetadata}.
 * Includes tests for backward and forward compatibility with V1 offset fields.
 */
public class TestAdminMetadata {
  private AdminMetadataJSONSerializer serializer;
  private final PubSubPositionDeserializer pubSubPositionDeserializer =
      InMemoryPubSubPositionFactory.getPubSubPositionDeserializerWithInMemoryPosition();

  @BeforeMethod
  public void setUp() {
    serializer = new AdminMetadataJSONSerializer(pubSubPositionDeserializer);
  }

  @Test
  public void testSerializeAndDeserializeAdminMetadata() throws IOException {
    PubSubPosition originalPosition = InMemoryPubSubPosition.of(12345L);
    PubSubPosition originalUpstreamPosition = InMemoryPubSubPosition.of(67890L);

    AdminMetadata originalMetadata = new AdminMetadata();
    originalMetadata.setExecutionId(123L);
    originalMetadata.setAdminOperationProtocolVersion(88L);
    originalMetadata.setPubSubPosition(originalPosition);
    originalMetadata.setUpstreamPubSubPosition(originalUpstreamPosition);

    // Serialize
    byte[] jsonBytes = serializer.serialize(originalMetadata, "test-path");
    assertNotNull(jsonBytes);
    assertTrue(jsonBytes.length > 0);

    // Verify JSON is human-readable and contains only position fields (no offset fields)
    String jsonString = new String(jsonBytes);
    assertTrue(jsonString.contains("\"executionId\" : 123"));
    assertFalse(jsonString.contains("\"offset\""), "Should not include deprecated offset field");
    assertFalse(jsonString.contains("\"upstreamOffset\""), "Should not include deprecated upstreamOffset field");
    assertTrue(jsonString.contains("\"adminOperationProtocolVersion\" : 88"));
    assertTrue(jsonString.contains("\"pubSubPositionJsonWireFormat\""));
    assertTrue(jsonString.contains("\"pubSubUpstreamPositionJsonWireFormat\""));
    assertTrue(jsonString.contains("\"typeId\" : " + InMemoryPubSubPosition.INMEMORY_PUBSUB_POSITION_TYPE_ID));
    assertTrue(jsonString.contains("\"base64PositionBytes\""));

    // Deserialize
    AdminMetadata deserializedMetadata = serializer.deserialize(jsonBytes, "test-path");

    // Verify all fields
    assertNotNull(deserializedMetadata);
    assertEquals(deserializedMetadata.getExecutionId(), Long.valueOf(123L));
    assertEquals(deserializedMetadata.getAdminOperationProtocolVersion(), Long.valueOf(88L));

    // Verify position objects
    PubSubPosition deserializedPosition = deserializedMetadata.getPosition();
    PubSubPosition deserializedUpstreamPosition = deserializedMetadata.getUpstreamPosition();
    assertEquals(deserializedPosition, originalPosition);
    assertEquals(deserializedUpstreamPosition, originalUpstreamPosition);
  }

  @Test
  public void testSerializeWithSymbolicPosition() throws IOException {
    // Test serialization with symbolic positions (EARLIEST)
    AdminMetadata metadata = new AdminMetadata();
    metadata.setExecutionId(123L);
    metadata.setPubSubPosition(PubSubSymbolicPosition.EARLIEST);
    metadata.setUpstreamPubSubPosition(PubSubSymbolicPosition.EARLIEST);

    byte[] jsonBytes = serializer.serialize(metadata, "test-path");
    String jsonString = new String(jsonBytes);

    // Offset fields should never be present (removed from format)
    assertFalse(jsonString.contains("\"offset\""), "Should not include offset field");
    assertFalse(jsonString.contains("\"upstreamOffset\""), "Should not include upstreamOffset field");

    // Verify round-trip works
    AdminMetadata deserialized = serializer.deserialize(jsonBytes, "test-path");
    assertEquals(deserialized.getPosition(), PubSubSymbolicPosition.EARLIEST);
    assertEquals(deserialized.getUpstreamPosition(), PubSubSymbolicPosition.EARLIEST);
  }

  /**
   * Test backward compatibility: old data (with offset fields) read by new code.
   * New code should ignore the offset fields and use position as the source of truth.
   */
  @Test
  public void testBackwardCompatibilityOldDataNewCodeIgnoresOffsetFields() throws IOException {
    // Create metadata with known positions
    AdminMetadata originalMetadata = new AdminMetadata();
    originalMetadata.setExecutionId(100L);
    originalMetadata.setAdminOperationProtocolVersion(1L);
    originalMetadata.setPubSubPosition(InMemoryPubSubPosition.of(12340L));
    originalMetadata.setUpstreamPubSubPosition(com.linkedin.venice.pubsub.mock.InMemoryPubSubPosition.of(6770L));

    // Serialize to get valid JSON (new format without offset fields)
    byte[] validJson = serializer.serialize(originalMetadata, "test-path");
    String jsonString = new String(validJson);

    // Simulate old data by injecting offset fields with different values
    // This tests that new code ignores offset fields and uses position
    String oldFormatJson = jsonString
        .replace("\"executionId\" : 100", "\"executionId\" : 100,\n  \"offset\" : 9999,\n  \"upstreamOffset\" : 8888");

    AdminMetadata deserializedMetadata = serializer.deserialize(oldFormatJson.getBytes(), "test-path");

    // New code should use position as source of truth, NOT the offset fields
    assertEquals(deserializedMetadata.getExecutionId(), Long.valueOf(100L));
    assertEquals(deserializedMetadata.getAdminOperationProtocolVersion(), Long.valueOf(1L));

    // Position should come from pubSubPositionJsonWireFormat, not from offset field
    PubSubPosition position = deserializedMetadata.getPosition();
    assertTrue(position instanceof InMemoryPubSubPosition);
    assertEquals(((InMemoryPubSubPosition) position).getInternalOffset(), 12340L); // From position, not 9999

    PubSubPosition upstreamPosition = deserializedMetadata.getUpstreamPosition();
    assertTrue(upstreamPosition instanceof InMemoryPubSubPosition);
    assertEquals(((InMemoryPubSubPosition) upstreamPosition).getInternalOffset(), 6770L); // From position, not 8888
  }

  /**
   * Test that new serialized data does not contain offset fields.
   * Offset fields have been fully removed from the V2 format.
   */
  @Test
  public void testNewDataDoesNotContainOffsetFields() throws IOException {
    AdminMetadata originalMetadata = new AdminMetadata();
    originalMetadata.setExecutionId(200L);
    originalMetadata.setAdminOperationProtocolVersion(2L);
    originalMetadata.setPubSubPosition(InMemoryPubSubPosition.of(5000L));
    originalMetadata.setUpstreamPubSubPosition(InMemoryPubSubPosition.of(6000L));

    byte[] jsonBytes = serializer.serialize(originalMetadata, "test-path");
    String jsonString = new String(jsonBytes);

    // Verify offset fields are NOT written (they have been removed)
    assertFalse(jsonString.contains("\"offset\""), "Should not contain offset field");
    assertFalse(jsonString.contains("\"upstreamOffset\""), "Should not contain upstreamOffset field");

    // Verify position fields ARE written
    assertTrue(jsonString.contains("\"pubSubPositionJsonWireFormat\""));
    assertTrue(jsonString.contains("\"pubSubUpstreamPositionJsonWireFormat\""));

    // Deserialize and verify positions are correct
    AdminMetadata deserializedMetadata = serializer.deserialize(jsonBytes, "test-path");
    assertEquals(deserializedMetadata.getExecutionId(), Long.valueOf(200L));
    assertEquals(((InMemoryPubSubPosition) deserializedMetadata.getPosition()).getInternalOffset(), 5000L);
    assertEquals(((InMemoryPubSubPosition) deserializedMetadata.getUpstreamPosition()).getInternalOffset(), 6000L);
  }

  /**
   * Test that if position fields are missing (hypothetical corrupted/partial data),
   * deserialization handles it gracefully.
   */
  @Test(expectedExceptions = NullPointerException.class)
  public void testDeserializationWithMissingPositionFields() throws IOException {
    // JSON with only offset fields, no position fields - this is malformed for V2
    String malformedJson =
        "{\n" + "  \"executionId\" : 100,\n" + "  \"offset\" : 9999,\n" + "  \"upstreamOffset\" : 8888\n" + "}";

    // This should throw because position fields are required in V2 format
    serializer.deserialize(malformedJson.getBytes(), "test-path");
  }

  @Test
  public void testNumericTypeConversion() {
    // Test conversion of different numeric types in map constructor
    Map<String, Object> mixedMap = new HashMap<>();
    mixedMap.put(AdminTopicMetadataAccessor.EXECUTION_ID_KEY, 123); // Integer
    mixedMap.put(AdminTopicMetadataAccessor.POSITION_KEY, InMemoryPubSubPosition.of(456L));
    mixedMap.put(AdminTopicMetadataAccessor.UPSTREAM_POSITION_KEY, InMemoryPubSubPosition.of(789L));

    AdminMetadata adminMetadata = new AdminMetadata(mixedMap);

    assertEquals(adminMetadata.getExecutionId(), Long.valueOf(123L));
    assertEquals(((InMemoryPubSubPosition) adminMetadata.getPosition()).getInternalOffset(), 456L);
    assertEquals(((InMemoryPubSubPosition) adminMetadata.getUpstreamPosition()).getInternalOffset(), 789L);
  }

  @Test
  public void testEmptyAdminMetadata() throws IOException {
    // Test serialization/deserialization of empty AdminMetadata
    AdminMetadata emptyMetadata = new AdminMetadata();

    byte[] jsonBytes = serializer.serialize(emptyMetadata, "test-path");
    assertNotNull(jsonBytes);

    AdminMetadata deserializedMetadata = serializer.deserialize(jsonBytes, "test-path");
    assertNotNull(deserializedMetadata);

    assertEquals(deserializedMetadata.getExecutionId(), UNDEFINED_VALUE);
    assertEquals(deserializedMetadata.getAdminOperationProtocolVersion(), UNDEFINED_VALUE);
    assertEquals(deserializedMetadata.getPosition(), PubSubSymbolicPosition.EARLIEST);
    assertEquals(deserializedMetadata.getUpstreamPosition(), PubSubSymbolicPosition.EARLIEST);
  }

  @Test
  public void testToString() {
    AdminMetadata metadata = new AdminMetadata();
    metadata.setExecutionId(123L);
    metadata.setPubSubPosition(InMemoryPubSubPosition.of(456L));

    String toString = metadata.toString();
    assertNotNull(toString);
    assertTrue(toString.contains("executionId=123"));
    assertTrue(toString.contains("position="));
  }

  /**
   * Test deserializing real production data that contains legacy offset fields.
   * This verifies backward compatibility with existing ZK data.
   */
  @Test
  public void testDeserializeRealProductionDataWithOffsetFields() throws IOException {
    String oldJson = "{\n" + "  \"executionId\": 376594,\n" + "  \"offset\": 5697,\n"
        + "  \"upstreamOffset\": 585177,\n" + "  \"adminOperationProtocolVersion\": -1,\n"
        + "  \"pubSubPositionJsonWireFormat\": {\n" + "    \"typeId\": 0,\n" + "    \"base64PositionBytes\": \"glk=\"\n"
        + "  },\n" + "  \"pubSubUpstreamPositionJsonWireFormat\": {\n" + "    \"typeId\": 0,\n"
        + "    \"base64PositionBytes\": \"glk=\"\n" + "  }\n" + "}";

    AdminMetadata metadata = serializer.deserialize(oldJson.getBytes(), "test-path");

    // Verify deserialization succeeds and uses position fields (ignoring offset fields)
    assertNotNull(metadata);
    assertEquals(metadata.getExecutionId(), Long.valueOf(376594L));
    assertEquals(metadata.getAdminOperationProtocolVersion(), Long.valueOf(-1L));
    assertNotNull(metadata.getPosition());
    assertNotNull(metadata.getUpstreamPosition());
  }

  /**
   * Test forward compatibility (rollback case): new data (without offset fields) read by OLD code.
   * This verifies that if we roll back to older code after new data has been written,
   * the older code can still deserialize the data correctly.
   *
   * Note: Old code's setPubSubPosition() automatically derives numeric offset from position,
   * so positions are correctly populated even when offset fields are missing.
   */
  @Test
  public void testForwardCompatibilityNewDataWithoutOffsetFields() throws IOException {
    // First, serialize metadata using NEW serializer to get new-format JSON (without offset fields)
    PubSubPosition originalPosition = InMemoryPubSubPosition.of(5858L);
    PubSubPosition originalUpstreamPosition = InMemoryPubSubPosition.of(1458769L);

    AdminMetadata originalMetadata = new AdminMetadata();
    originalMetadata.setExecutionId(376594L);
    originalMetadata.setAdminOperationProtocolVersion(-1L);
    originalMetadata.setPubSubPosition(originalPosition);
    originalMetadata.setUpstreamPubSubPosition(originalUpstreamPosition);

    byte[] newFormatJson = serializer.serialize(originalMetadata, "test-path");
    String jsonString = new String(newFormatJson);

    // Verify the new format does not contain offset fields
    assertFalse(jsonString.contains("\"offset\""), "New format should not contain offset field");
    assertFalse(jsonString.contains("\"upstreamOffset\""), "New format should not contain upstreamOffset field");

    // Now deserialize using OLD serializer - this simulates old code reading new data
    AdminMetadataJSONSerializerOld oldSerializer = new AdminMetadataJSONSerializerOld(pubSubPositionDeserializer);
    AdminMetadataOld metadata = oldSerializer.deserialize(newFormatJson, "test-path");

    // Verify deserialization succeeds
    assertNotNull(metadata);
    assertEquals(metadata.getExecutionId(), Long.valueOf(376594L));
    assertEquals(metadata.getAdminOperationProtocolVersion(), Long.valueOf(-1L));

    // Old code derives offset from position via setPubSubPosition()
    // Since offset fields are missing, they should be derived from position
    assertEquals(metadata.getOffset(), Long.valueOf(5858L));
    assertEquals(metadata.getUpstreamOffset(), Long.valueOf(1458769L));

    // Verify positions are correctly populated from the JSON
    PubSubPosition position = metadata.getPosition();
    PubSubPosition upstreamPosition = metadata.getUpstreamPosition();
    assertNotNull(position);
    assertNotNull(upstreamPosition);
    assertTrue(position instanceof InMemoryPubSubPosition);
    assertTrue(upstreamPosition instanceof InMemoryPubSubPosition);

    // Verify the numeric offsets can be extracted from positions
    assertEquals(((InMemoryPubSubPosition) position).getInternalOffset(), 5858L);
    assertEquals(((InMemoryPubSubPosition) upstreamPosition).getInternalOffset(), 1458769L);
  }

  @Test
  public void testEqualsAndHashCode() {
    AdminMetadata metadata1 = new AdminMetadata();
    metadata1.setExecutionId(123L);
    metadata1.setPubSubPosition(InMemoryPubSubPosition.of(456L));
    metadata1.setUpstreamPubSubPosition(InMemoryPubSubPosition.of(789L));

    AdminMetadata metadata2 = new AdminMetadata();
    metadata2.setExecutionId(123L);
    metadata2.setPubSubPosition(InMemoryPubSubPosition.of(456L));
    metadata2.setUpstreamPubSubPosition(InMemoryPubSubPosition.of(789L));

    assertEquals(metadata1, metadata2);
    assertEquals(metadata1.hashCode(), metadata2.hashCode());
  }
}
