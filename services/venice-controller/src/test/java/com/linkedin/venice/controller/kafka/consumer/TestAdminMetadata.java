package com.linkedin.venice.controller.kafka.consumer;

import static com.linkedin.venice.controller.AdminTopicMetadataAccessor.UNDEFINED_VALUE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.controller.AdminTopicMetadataAccessor;
import com.linkedin.venice.helix.VeniceJsonSerializer;
import com.linkedin.venice.pubsub.adapter.kafka.common.ApacheKafkaOffsetPosition;
import com.linkedin.venice.pubsub.api.PubSubPositionWireFormat;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Unit tests for {@link VeniceJsonSerializer<AdminMetadata>} with the new AdminMetadata objects.
 */
public class TestAdminMetadata {
  private VeniceJsonSerializer<AdminMetadata> serializer;

  @BeforeMethod
  public void setUp() {
    serializer = new VeniceJsonSerializer<>(AdminMetadata.class);
  }

  @Test
  public void testSerializeAndDeserializeAdminMetadata() throws IOException {
    // Create AdminMetadata with all fields populated
    AdminMetadata originalMetadata = new AdminMetadata();
    originalMetadata.setExecutionId(12345L);
    originalMetadata.setOffset(67890L);
    originalMetadata.setUpstreamOffset(11111L);
    originalMetadata.setAdminOperationProtocolVersion(88L);

    // Create position objects using ApacheKafkaOffsetPosition
    PubSubPositionWireFormat positionWf = ApacheKafkaOffsetPosition.of(12345L).getPositionWireFormat();
    PubSubPositionWireFormat upstreamPositionWf = ApacheKafkaOffsetPosition.of(67890L).getPositionWireFormat();

    // Convert to AdminMetadata format
    AdminMetadata.PubSubPositionJsonWireFormat positionJson =
        AdminMetadata.PubSubPositionJsonWireFormat.fromWireFormat(positionWf);
    AdminMetadata.PubSubPositionJsonWireFormat upstreamPositionJson =
        AdminMetadata.PubSubPositionJsonWireFormat.fromWireFormat(upstreamPositionWf);

    originalMetadata.setPosition(positionJson);
    originalMetadata.setUpstreamPosition(upstreamPositionJson);

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
    assertTrue(jsonString.contains("\"typeId\" : " + positionWf.getType()));
    assertTrue(
        jsonString.contains(
            "\"positionBytes\" : \"" + java.util.Base64.getEncoder().encodeToString(positionWf.getRawBytes().array())
                + "\""));

    // Deserialize
    AdminMetadata deserializedMetadata = serializer.deserialize(jsonBytes, "test-path");

    // Verify all fields
    assertNotNull(deserializedMetadata);
    assertEquals(deserializedMetadata.getExecutionId(), Long.valueOf(12345L));
    assertEquals(deserializedMetadata.getOffset(), Long.valueOf(67890L));
    assertEquals(deserializedMetadata.getUpstreamOffset(), Long.valueOf(11111L));
    assertEquals(deserializedMetadata.getAdminOperationProtocolVersion(), Long.valueOf(88L));

    // Verify position objects
    assertNotNull(deserializedMetadata.getPosition());
    assertEquals(deserializedMetadata.getPosition().getTypeId(), Integer.valueOf(positionWf.getType()));
    assertEquals(
        deserializedMetadata.getPosition().getPositionBytes(),
        java.util.Base64.getEncoder().encodeToString(positionWf.getRawBytes().array()));

    assertNotNull(deserializedMetadata.getUpstreamPosition());
    assertEquals(deserializedMetadata.getUpstreamPosition().getTypeId(), Integer.valueOf(upstreamPositionWf.getType()));
    assertEquals(
        deserializedMetadata.getUpstreamPosition().getPositionBytes(),
        java.util.Base64.getEncoder().encodeToString(upstreamPositionWf.getRawBytes().array()));
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
    assertNull(deserializedMetadata.getPosition());
    assertNull(deserializedMetadata.getUpstreamPosition());
  }

  @Test
  public void testPubSubPositionJsonWireFormatConversion() {
    // Test conversion from wire format using ApacheKafkaOffsetPosition
    ApacheKafkaOffsetPosition position = ApacheKafkaOffsetPosition.of(12345L);
    PubSubPositionWireFormat wireFormat = position.getPositionWireFormat();

    AdminMetadata.PubSubPositionJsonWireFormat jsonFormat =
        AdminMetadata.PubSubPositionJsonWireFormat.fromWireFormat(wireFormat);

    assertNotNull(jsonFormat);
    assertEquals(jsonFormat.getTypeId(), Integer.valueOf(wireFormat.getType()));
    assertEquals(
        jsonFormat.getPositionBytes(),
        java.util.Base64.getEncoder().encodeToString(wireFormat.getRawBytes().array()));

    // Test conversion back to wire format
    PubSubPositionWireFormat convertedWireFormat = jsonFormat.toWireFormat();

    assertNotNull(convertedWireFormat);
    assertEquals(convertedWireFormat.getType(), wireFormat.getType());
    assertEquals(convertedWireFormat.getRawBytes().array(), wireFormat.getRawBytes().array());
  }

  @Test
  public void testPubSubPositionJsonWireFormatNullHandling() {
    // Test null input
    AdminMetadata.PubSubPositionJsonWireFormat jsonFormat =
        AdminMetadata.PubSubPositionJsonWireFormat.fromWireFormat(null);
    assertNull(jsonFormat);

    // Test null fields
    AdminMetadata.PubSubPositionJsonWireFormat emptyFormat = new AdminMetadata.PubSubPositionJsonWireFormat();
    PubSubPositionWireFormat wireFormat = emptyFormat.toWireFormat();

    assertNotNull(wireFormat);
    // Type should be 0 (default) and rawBytes should be null
    assertEquals(wireFormat.getType(), 0);
    assertNull(wireFormat.getRawBytes());
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
    assertNull(deserializedMetadata.getPosition());
    assertNull(deserializedMetadata.getUpstreamPosition());
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

  @Test
  public void testPubSubPositionJsonWireFormatEquals() {
    AdminMetadata.PubSubPositionJsonWireFormat pos1 = new AdminMetadata.PubSubPositionJsonWireFormat(1, "test");
    AdminMetadata.PubSubPositionJsonWireFormat pos2 = new AdminMetadata.PubSubPositionJsonWireFormat(1, "test");
    AdminMetadata.PubSubPositionJsonWireFormat pos3 = new AdminMetadata.PubSubPositionJsonWireFormat(2, "test");

    assertEquals(pos1, pos2);
    assertEquals(pos1.hashCode(), pos2.hashCode());
    assertFalse(pos1.equals(pos3));
  }
}
