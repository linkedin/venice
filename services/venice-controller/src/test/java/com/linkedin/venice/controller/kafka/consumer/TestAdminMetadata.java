package com.linkedin.venice.controller.kafka.consumer;

import static com.linkedin.venice.controller.AdminTopicMetadataAccessor.UNDEFINED_VALUE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.controller.AdminTopicMetadataAccessor;
import com.linkedin.venice.helix.VeniceJsonSerializer;
import com.linkedin.venice.pubsub.adapter.kafka.common.ApacheKafkaOffsetPosition;
import com.linkedin.venice.pubsub.api.PubSubPosition;
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

    originalMetadata.setPubSubPositionJsonWireFormat(positionJson);
    originalMetadata.setPubSubUpstreamPositionJsonWireFormat(upstreamPositionJson);

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
            "\"base64PositionBytes\" : \""
                + java.util.Base64.getEncoder().encodeToString(positionWf.getRawBytes().array()) + "\""));

    // Deserialize
    AdminMetadata deserializedMetadata = serializer.deserialize(jsonBytes, "test-path");

    // Verify all fields
    assertNotNull(deserializedMetadata);
    assertEquals(deserializedMetadata.getExecutionId(), Long.valueOf(12345L));
    assertEquals(deserializedMetadata.getOffset(), Long.valueOf(67890L));
    assertEquals(deserializedMetadata.getUpstreamOffset(), Long.valueOf(11111L));
    assertEquals(deserializedMetadata.getAdminOperationProtocolVersion(), Long.valueOf(88L));

    // Verify position objects
    assertNotNull(deserializedMetadata.getPubSubPositionJsonWireFormat());
    assertEquals(deserializedMetadata.getPubSubPositionJsonWireFormat().getTypeId().intValue(), positionWf.getType());
    assertEquals(
        deserializedMetadata.getPubSubPositionJsonWireFormat().getBase64PositionBytes(),
        java.util.Base64.getEncoder().encodeToString(positionWf.getRawBytes().array()));

    assertNotNull(deserializedMetadata.getPubSubUpstreamPositionJsonWireFormat());
    assertEquals(
        deserializedMetadata.getPubSubUpstreamPositionJsonWireFormat().getTypeId().intValue(),
        upstreamPositionWf.getType());
    assertEquals(
        deserializedMetadata.getPubSubUpstreamPositionJsonWireFormat().getBase64PositionBytes(),
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
    assertNull(deserializedMetadata.getPubSubPositionJsonWireFormat());
    assertNull(deserializedMetadata.getPubSubUpstreamPositionJsonWireFormat());
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
        jsonFormat.getBase64PositionBytes(),
        java.util.Base64.getEncoder().encodeToString(wireFormat.getRawBytes().array()));

    // Test conversion back to wire format
    PubSubPositionWireFormat convertedWireFormat = jsonFormat.toWireFormat();

    assertNotNull(convertedWireFormat);
    assertEquals(convertedWireFormat.getType(), wireFormat.getType());
    assertEquals(convertedWireFormat.getRawBytes().array(), wireFormat.getRawBytes().array());
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
    assertNull(deserializedMetadata.getPubSubPositionJsonWireFormat());
    assertNull(deserializedMetadata.getPubSubUpstreamPositionJsonWireFormat());
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
    assertNotEquals(pos3, pos1);
  }

  @Test
  public void testPositionConversionMethods() throws IOException {
    // Create AdminMetadata with position objects
    AdminMetadata metadata = new AdminMetadata();

    // Create position objects using ApacheKafkaOffsetPosition
    long localOffset = 12345L;
    long upstreamOffset = 67890L;

    ApacheKafkaOffsetPosition localPosition = ApacheKafkaOffsetPosition.of(localOffset);
    ApacheKafkaOffsetPosition upstreamPosition = ApacheKafkaOffsetPosition.of(upstreamOffset);

    PubSubPositionWireFormat localPositionWf = localPosition.getPositionWireFormat();
    PubSubPositionWireFormat upstreamPositionWf = upstreamPosition.getPositionWireFormat();

    // Convert to AdminMetadata format
    AdminMetadata.PubSubPositionJsonWireFormat localPositionJson =
        AdminMetadata.PubSubPositionJsonWireFormat.fromWireFormat(localPositionWf);
    AdminMetadata.PubSubPositionJsonWireFormat upstreamPositionJson =
        AdminMetadata.PubSubPositionJsonWireFormat.fromWireFormat(upstreamPositionWf);

    metadata.setPubSubPositionJsonWireFormat(localPositionJson);
    metadata.setPubSubUpstreamPositionJsonWireFormat(upstreamPositionJson);

    // Test getPubSubPositionJsonWireFormat() methods
    AdminMetadata.PubSubPositionJsonWireFormat retrievedLocalJson = metadata.getPubSubPositionJsonWireFormat();
    AdminMetadata.PubSubPositionJsonWireFormat retrievedUpstreamJson =
        metadata.getPubSubUpstreamPositionJsonWireFormat();

    assertNotNull(retrievedLocalJson);
    assertNotNull(retrievedUpstreamJson);

    // Verify the JSON wire format objects are correct
    assertEquals(retrievedLocalJson.getTypeId().intValue(), localPositionWf.getType());
    assertEquals(
        retrievedLocalJson.getBase64PositionBytes(),
        java.util.Base64.getEncoder().encodeToString(localPositionWf.getRawBytes().array()));

    assertEquals(retrievedUpstreamJson.getTypeId().intValue(), upstreamPositionWf.getType());
    assertEquals(
        retrievedUpstreamJson.getBase64PositionBytes(),
        java.util.Base64.getEncoder().encodeToString(upstreamPositionWf.getRawBytes().array()));

    // Test getPosition() methods - these should convert back to PubSubPosition objects
    PubSubPosition retrievedLocalPosition = metadata.getPosition();
    PubSubPosition retrievedUpstreamPosition = metadata.getUpstreamPosition();

    assertNotNull(retrievedLocalPosition);
    assertNotNull(retrievedUpstreamPosition);

    // Verify the converted positions are equivalent to the original positions
    assertTrue(retrievedLocalPosition instanceof ApacheKafkaOffsetPosition);
    assertTrue(retrievedUpstreamPosition instanceof ApacheKafkaOffsetPosition);

    ApacheKafkaOffsetPosition convertedLocal = (ApacheKafkaOffsetPosition) retrievedLocalPosition;
    ApacheKafkaOffsetPosition convertedUpstream = (ApacheKafkaOffsetPosition) retrievedUpstreamPosition;

    // Verify the offset values are preserved through the conversion
    assertEquals(convertedLocal.getInternalOffset(), localOffset);
    assertEquals(convertedUpstream.getInternalOffset(), upstreamOffset);

    // Verify the wire formats are equivalent
    assertEquals(convertedLocal.getPositionWireFormat().getType(), localPositionWf.getType());
    assertEquals(convertedLocal.getPositionWireFormat().getRawBytes().array(), localPositionWf.getRawBytes().array());

    assertEquals(convertedUpstream.getPositionWireFormat().getType(), upstreamPositionWf.getType());
    assertEquals(
        convertedUpstream.getPositionWireFormat().getRawBytes().array(),
        upstreamPositionWf.getRawBytes().array());
  }

  @Test
  public void testPositionConversionWithNullValues() throws IOException {
    // Test behavior when position fields are null
    AdminMetadata metadata = new AdminMetadata();

    // Both position fields should be null initially
    assertNull(metadata.getPubSubPositionJsonWireFormat());
    assertNull(metadata.getPubSubUpstreamPositionJsonWireFormat());

    // Set only local position
    ApacheKafkaOffsetPosition localPosition = ApacheKafkaOffsetPosition.of(123L);
    AdminMetadata.PubSubPositionJsonWireFormat localPositionJson =
        AdminMetadata.PubSubPositionJsonWireFormat.fromWireFormat(localPosition.getPositionWireFormat());

    metadata.setPubSubPositionJsonWireFormat(localPositionJson);

    // Local position should be available, upstream should still be null
    assertNotNull(metadata.getPubSubPositionJsonWireFormat());
    assertNull(metadata.getPubSubUpstreamPositionJsonWireFormat());
    assertNotNull(metadata.getPosition());

    // Verify the local position is correct
    assertEquals(((ApacheKafkaOffsetPosition) metadata.getPosition()).getInternalOffset(), 123L);
  }

  @Test
  public void testPositionConversionRoundTrip() throws IOException {
    // Test complete round-trip: Position -> JSON -> Wire -> Position
    long originalOffset = 98765L;
    ApacheKafkaOffsetPosition originalPosition = ApacheKafkaOffsetPosition.of(originalOffset);

    // Convert to JSON format
    AdminMetadata.PubSubPositionJsonWireFormat jsonFormat =
        AdminMetadata.PubSubPositionJsonWireFormat.fromWireFormat(originalPosition.getPositionWireFormat());

    // Create AdminMetadata and set the position
    AdminMetadata metadata = new AdminMetadata();
    metadata.setPubSubPositionJsonWireFormat(jsonFormat);

    // Convert back to PubSubPosition
    PubSubPosition convertedPosition = metadata.getPosition();

    // Verify the round-trip preserved the original data
    assertNotNull(convertedPosition);
    assertTrue(convertedPosition instanceof ApacheKafkaOffsetPosition);
    assertEquals(((ApacheKafkaOffsetPosition) convertedPosition).getInternalOffset(), originalOffset);

    // Verify wire format equivalence
    PubSubPositionWireFormat originalWireFormat = originalPosition.getPositionWireFormat();
    PubSubPositionWireFormat convertedWireFormat = convertedPosition.getPositionWireFormat();

    assertEquals(convertedWireFormat.getType(), originalWireFormat.getType());
    assertEquals(convertedWireFormat.getRawBytes().array(), originalWireFormat.getRawBytes().array());
  }
}
