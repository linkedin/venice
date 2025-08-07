package com.linkedin.venice.controller.kafka.consumer;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.venice.controller.AdminTopicMetadataAccessor;
import com.linkedin.venice.exceptions.VeniceException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TestStringToObjectMapJSONSerializer {
  private StringToObjectMapJSONSerializer serializer;
  private ObjectMapper objectMapper;

  @BeforeMethod
  public void setUp() {
    serializer = new StringToObjectMapJSONSerializer();
    objectMapper = new ObjectMapper();
  }

  @Test
  public void testDeserializePositionObject() throws IOException {
    // Create test data with Position object
    AdminTopicMetadataAccessor.Position position = new AdminTopicMetadataAccessor.Position(1, "test".getBytes());
    Map<String, Object> originalMap = new HashMap<>();
    originalMap.put(AdminTopicMetadataAccessor.POSITION_KEY, position);
    originalMap.put("someOtherKey", 123L);

    // Serialize to JSON bytes
    byte[] jsonBytes = objectMapper.writeValueAsBytes(originalMap);

    // Deserialize using our serializer
    Map<String, Object> deserializedMap = serializer.deserialize(jsonBytes, "test-path");

    // Verify Position object was correctly deserialized
    assertNotNull(deserializedMap);
    assertEquals(deserializedMap.size(), 2);
    assertTrue(deserializedMap.containsKey(AdminTopicMetadataAccessor.POSITION_KEY));
    assertTrue(
        deserializedMap.get(AdminTopicMetadataAccessor.POSITION_KEY) instanceof AdminTopicMetadataAccessor.Position);

    AdminTopicMetadataAccessor.Position deserializedPosition =
        (AdminTopicMetadataAccessor.Position) deserializedMap.get(AdminTopicMetadataAccessor.POSITION_KEY);
    assertEquals(deserializedPosition.typeId, 1);
    assertEquals(new String(deserializedPosition.positionBytes), "test");

    // Verify Long value was correctly deserialized
    assertEquals(deserializedMap.get("someOtherKey"), 123L);
  }

  @Test
  public void testDeserializeUpstreamPositionObject() throws IOException {
    // Create test data with upstream Position object
    AdminTopicMetadataAccessor.Position upstreamPosition =
        new AdminTopicMetadataAccessor.Position(2, "upstream".getBytes());
    Map<String, Object> originalMap = new HashMap<>();
    originalMap.put(AdminTopicMetadataAccessor.UPSTREAM_POSITION_KEY, upstreamPosition);
    originalMap.put("offset", 456L);

    // Serialize to JSON bytes
    byte[] jsonBytes = objectMapper.writeValueAsBytes(originalMap);

    // Deserialize using our serializer
    Map<String, Object> deserializedMap = serializer.deserialize(jsonBytes, "test-path");

    // Verify upstream Position object was correctly deserialized
    assertNotNull(deserializedMap);
    assertEquals(deserializedMap.size(), 2);
    assertTrue(deserializedMap.containsKey(AdminTopicMetadataAccessor.UPSTREAM_POSITION_KEY));
    assertTrue(
        deserializedMap
            .get(AdminTopicMetadataAccessor.UPSTREAM_POSITION_KEY) instanceof AdminTopicMetadataAccessor.Position);

    AdminTopicMetadataAccessor.Position deserializedPosition =
        (AdminTopicMetadataAccessor.Position) deserializedMap.get(AdminTopicMetadataAccessor.UPSTREAM_POSITION_KEY);
    assertEquals(deserializedPosition.typeId, 2);
    assertEquals(new String(deserializedPosition.positionBytes), "upstream");

    // Verify Long value was correctly deserialized
    assertEquals(deserializedMap.get("offset"), 456L);
  }

  @Test
  public void testDeserializeLongValues() throws IOException {
    // Create test data with various Long values
    Map<String, Object> originalMap = new HashMap<>();
    originalMap.put("offset", 100L);
    originalMap.put("upstreamOffset", 200L);
    originalMap.put("executionId", 300L);
    originalMap.put("version", 1L);

    // Serialize to JSON bytes
    byte[] jsonBytes = objectMapper.writeValueAsBytes(originalMap);

    // Deserialize using our serializer
    Map<String, Object> deserializedMap = serializer.deserialize(jsonBytes, "test-path");

    // Verify all Long values were correctly deserialized
    assertNotNull(deserializedMap);
    assertEquals(deserializedMap.size(), 4);
    assertEquals(deserializedMap.get("offset"), 100L);
    assertEquals(deserializedMap.get("upstreamOffset"), 200L);
    assertEquals(deserializedMap.get("executionId"), 300L);
    assertEquals(deserializedMap.get("version"), 1L);
  }

  @Test
  public void testDeserializeMixedTypes() throws IOException {
    // Create test data with mixed types
    AdminTopicMetadataAccessor.Position position = new AdminTopicMetadataAccessor.Position(3, "mixed".getBytes());
    AdminTopicMetadataAccessor.Position upstreamPosition =
        new AdminTopicMetadataAccessor.Position(4, "upstream_mixed".getBytes());

    Map<String, Object> originalMap = new HashMap<>();
    originalMap.put(AdminTopicMetadataAccessor.POSITION_KEY, position);
    originalMap.put(AdminTopicMetadataAccessor.UPSTREAM_POSITION_KEY, upstreamPosition);
    originalMap.put("offset", 500L);
    originalMap.put("stringValue", "test string");
    originalMap.put("booleanValue", true);

    // Serialize to JSON bytes
    byte[] jsonBytes = objectMapper.writeValueAsBytes(originalMap);

    // Deserialize using our serializer
    try {
      serializer.deserialize(jsonBytes, "test-path");
      fail("Expected RuntimeException for invalid Position object");
    } catch (RuntimeException e) {
      // Expected exception due to invalid Position structure
      assertTrue(e.getMessage().contains("Unexpected type of field"));
    }
  }

  @Test
  public void testDeserializeEmptyMap() throws IOException {
    // Create empty map
    Map<String, Object> originalMap = new HashMap<>();

    // Serialize to JSON bytes
    byte[] jsonBytes = objectMapper.writeValueAsBytes(originalMap);

    // Deserialize using our serializer
    Map<String, Object> deserializedMap = serializer.deserialize(jsonBytes, "test-path");

    // Verify empty map was correctly handled
    assertNotNull(deserializedMap);
    assertEquals(deserializedMap.size(), 0);
  }

  @Test
  public void testDeserializeInvalidPositionObject() throws IOException {
    // Create test data with object that looks like Position but is missing required fields
    Map<String, Object> invalidPosition = new HashMap<>();
    invalidPosition.put("typeId", 1);
    // Missing positionBytes field

    Map<String, Object> originalMap = new HashMap<>();
    originalMap.put(AdminTopicMetadataAccessor.POSITION_KEY, invalidPosition);

    // Serialize to JSON bytes
    byte[] jsonBytes = objectMapper.writeValueAsBytes(originalMap);

    // Deserialize using our serializer - should handle gracefully or throw exception
    try {
      serializer.deserialize(jsonBytes, "test-path");
      fail("Expected VeniceException for invalid Position object");
    } catch (VeniceException e) {
      // Expected exception due to invalid Position structure
      assertTrue(e.getMessage().contains("Unexpected type of field"));
    }
  }

  @Test
  public void testDeserializeNonPositionObjectWithPositionKey() throws IOException {
    // Create test data where position key contains a non-Position object
    Map<String, Object> originalMap = new HashMap<>();
    originalMap.put(AdminTopicMetadataAccessor.POSITION_KEY, "not a position object");
    originalMap.put("normalKey", 123L);

    // Serialize to JSON bytes
    byte[] jsonBytes = objectMapper.writeValueAsBytes(originalMap);

    try {
      serializer.deserialize(jsonBytes, "test-path");
      fail("Expected VeniceException for invalid Position object");
    } catch (VeniceException e) {
      // Expected exception due to invalid Position structure
      assertTrue(e.getMessage().contains("Unexpected type of field"));
    }
  }

  @Test
  public void testDeserializeInvalidJson() {
    // Test with invalid JSON bytes
    byte[] invalidJsonBytes = "invalid json".getBytes();

    try {
      serializer.deserialize(invalidJsonBytes, "test-path");
      fail("Expected IOException for invalid JSON");
    } catch (IOException e) {
      // Expected exception for invalid JSON
      assertNotNull(e);
    }
  }

  @Test
  public void testDeserializeNumericValues() throws IOException {
    // Test various numeric types to ensure they're converted to Long
    Map<String, Object> originalMap = new HashMap<>();
    originalMap.put("intValue", 42);
    originalMap.put("longValue", 9223372036854775807L);
    originalMap.put("doubleValue", Math.PI);

    // Serialize to JSON bytes
    byte[] jsonBytes = objectMapper.writeValueAsBytes(originalMap);

    // Deserialize using our serializer
    Map<String, Object> deserializedMap = serializer.deserialize(jsonBytes, "test-path");

    // Verify numeric values are handled correctly
    assertNotNull(deserializedMap);
    assertEquals(deserializedMap.size(), 3);
    assertEquals(deserializedMap.get("intValue"), 42L);
    assertEquals(deserializedMap.get("longValue"), 9223372036854775807L);
    // Double should be converted to long (truncated)
    assertEquals(deserializedMap.get("doubleValue"), 3L);
  }

  @Test
  public void testConstructor() {
    // Test that constructor properly initializes the serializer
    StringToObjectMapJSONSerializer newSerializer = new StringToObjectMapJSONSerializer();
    assertNotNull(newSerializer);
  }
}
