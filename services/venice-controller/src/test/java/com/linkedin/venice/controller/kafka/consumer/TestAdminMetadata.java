package com.linkedin.venice.controller.kafka.consumer;

import static com.linkedin.venice.controller.AdminTopicMetadataAccessor.UNDEFINED_VALUE;
import static com.linkedin.venice.pubsub.PubSubPositionTypeRegistry.APACHE_KAFKA_OFFSET_POSITION_TYPE_ID;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.controller.AdminTopicMetadataAccessor;
import com.linkedin.venice.helix.VeniceJsonSerializer;
import com.linkedin.venice.pubsub.PubSubPositionDeserializer;
import com.linkedin.venice.pubsub.adapter.kafka.common.ApacheKafkaOffsetPosition;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubSymbolicPosition;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Unit tests for {@link VeniceJsonSerializer<AdminMetadata>} with the new AdminMetadata objects.
 */
public class TestAdminMetadata {
  private AdminMetadataJSONSerializer serializer;

  @BeforeMethod
  public void setUp() {
    serializer = new AdminMetadataJSONSerializer(PubSubPositionDeserializer.DEFAULT_DESERIALIZER);
  }

  @Test
  public void testSerializeAndDeserializeAdminMetadata() throws IOException {
    PubSubPosition originalPosition = PubSubSymbolicPosition.EARLIEST;
    PubSubPosition originalUpstreamPosition = ApacheKafkaOffsetPosition.of(67890L);

    AdminMetadata originalMetadata = new AdminMetadata();
    originalMetadata.setExecutionId(12345L);
    originalMetadata.setOffset(67890L);
    originalMetadata.setUpstreamOffset(11111L);
    originalMetadata.setAdminOperationProtocolVersion(88L);
    originalMetadata.setPubSubPosition(originalPosition);
    originalMetadata.setUpstreamPubSubPosition(originalUpstreamPosition);

    // Serialize
    byte[] jsonBytes = serializer.serialize(originalMetadata, "test-path");
    assertNotNull(jsonBytes);
    assertTrue(jsonBytes.length > 0);

    // Verify JSON is human-readable
    String jsonString = new String(jsonBytes);
    assertTrue(jsonString.contains("\"executionId\" : 12345"));
    assertTrue(jsonString.contains("\"offset\" : 67890"));
    assertTrue(jsonString.contains("\"upstreamOffset\" : 11111"));
    assertTrue(jsonString.contains("\"adminOperationProtocolVersion\" : 88"));
    assertTrue(jsonString.contains("\"pubSubPositionJsonWireFormat\""));
    assertTrue(jsonString.contains("\"pubSubUpstreamPositionJsonWireFormat\""));
    assertTrue(jsonString.contains("\"typeId\" : " + APACHE_KAFKA_OFFSET_POSITION_TYPE_ID));
    assertTrue(jsonString.contains("\"base64PositionBytes\""));

    // Deserialize
    AdminMetadata deserializedMetadata = serializer.deserialize(jsonBytes, "test-path");

    // Verify all fields
    assertNotNull(deserializedMetadata);
    assertEquals(deserializedMetadata.getExecutionId(), Long.valueOf(12345L));
    assertEquals(deserializedMetadata.getOffset(), Long.valueOf(67890L));
    assertEquals(deserializedMetadata.getUpstreamOffset(), Long.valueOf(11111L));
    assertEquals(deserializedMetadata.getAdminOperationProtocolVersion(), Long.valueOf(88L));

    // Verify position objects
    PubSubPosition deserializedPosition = deserializedMetadata.getPosition();
    PubSubPosition deserializedUpstreamPosition = deserializedMetadata.getUpstreamPosition();
    assertEquals(deserializedPosition, originalPosition);
    assertEquals(deserializedUpstreamPosition, originalUpstreamPosition);
  }

  @Test
  public void testSerializeAndDeserializePartialAdminMetadata() throws IOException {
    // Create AdminMetadata with only some fields populated
    AdminMetadata originalMetadata = new AdminMetadata();
    originalMetadata.setExecutionId(999L);
    originalMetadata.setOffset(1234L);
    // Leave other fields null

    // Serialize
    byte[] jsonBytes = serializer.serialize(originalMetadata, "test-path");
    assertNotNull(jsonBytes);

    // Deserialize
    AdminMetadata deserializedMetadata = serializer.deserialize(jsonBytes, "test-path");

    // Verify populated fields
    assertNotNull(deserializedMetadata);
    assertEquals(deserializedMetadata.getExecutionId(), Long.valueOf(999L));
    assertEquals(deserializedMetadata.getOffset(), Long.valueOf(1234L));

    // Verify null fields remain null
    assertNull(deserializedMetadata.getUpstreamOffset());
    assertEquals(deserializedMetadata.getAdminOperationProtocolVersion(), UNDEFINED_VALUE);

    // verify position objects derived from offset
    assertEquals(deserializedMetadata.getPosition(), ApacheKafkaOffsetPosition.of(1234L));
    assertEquals(deserializedMetadata.getUpstreamPosition(), PubSubSymbolicPosition.EARLIEST);
  }

  @Test
  public void testNumericTypeConversion() {
    // Test conversion of different numeric types
    Map<String, Object> mixedMap = new HashMap<>();
    mixedMap.put(AdminTopicMetadataAccessor.EXECUTION_ID_KEY, 123); // Integer
    mixedMap.put(AdminTopicMetadataAccessor.OFFSET_KEY, 456L); // Long
    mixedMap.put(AdminTopicMetadataAccessor.UPSTREAM_OFFSET_KEY, 789.0); // Double

    AdminMetadata adminMetadata = new AdminMetadata(mixedMap);

    // All should be converted to Long
    assertEquals(adminMetadata.getExecutionId(), Long.valueOf(123L));
    assertEquals(adminMetadata.getOffset(), Long.valueOf(456L));
    assertEquals(adminMetadata.getUpstreamOffset(), Long.valueOf(789L));
  }

  @Test
  public void testEmptyAdminMetadata() throws IOException {
    // Test serialization/deserialization of empty AdminMetadata
    AdminMetadata emptyMetadata = new AdminMetadata();

    byte[] jsonBytes = serializer.serialize(emptyMetadata, "test-path");
    assertNotNull(jsonBytes);

    AdminMetadata deserializedMetadata = serializer.deserialize(jsonBytes, "test-path");
    assertNotNull(deserializedMetadata);

    // All fields should be null
    assertNull(deserializedMetadata.getExecutionId());
    assertNull(deserializedMetadata.getOffset());
    assertNull(deserializedMetadata.getUpstreamOffset());
    assertEquals(deserializedMetadata.getAdminOperationProtocolVersion(), UNDEFINED_VALUE);
  }

  @Test
  public void testToString() {
    AdminMetadata metadata = new AdminMetadata();
    metadata.setExecutionId(123L);
    metadata.setOffset(456L);

    String toString = metadata.toString();
    assertNotNull(toString);
    assertTrue(toString.contains("executionId=123"));
    assertTrue(toString.contains("offset=456"));
  }
}
