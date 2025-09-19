package com.linkedin.venice.spark.input.pubsub;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.pubsub.api.DefaultPubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.DataTypes;


public class VenicePubSubMessageToRowTest {
  @org.testng.annotations.Test
  public void testProcessMessage() {
    // Create test data
    byte[] keyBytes = "testKey".getBytes();
    byte[] valueBytes = "testValue".getBytes();
    byte[] replicationMetadataBytes = "replicationMetadata".getBytes();

    // Mock the PubSubPosition
    PubSubPosition mockPosition = mock(PubSubPosition.class);

    // Mock the KafkaKey
    KafkaKey mockKey = mock(KafkaKey.class);
    when(mockKey.getKey()).thenReturn(keyBytes);
    when(mockKey.getKeyLength()).thenReturn(keyBytes.length);

    // Create Put message payload
    Put putPayload = new Put();
    putPayload.schemaId = 11;
    putPayload.putValue = ByteBuffer.wrap(valueBytes);
    putPayload.replicationMetadataPayload = ByteBuffer.wrap(replicationMetadataBytes);
    putPayload.replicationMetadataVersionId = 37;

    // Mock the KafkaMessageEnvelope
    KafkaMessageEnvelope mockEnvelope = mock(KafkaMessageEnvelope.class);
    mockEnvelope.payloadUnion = putPayload;
    when(mockEnvelope.getMessageType()).thenReturn(MessageType.PUT.getValue());

    // Mock the PubSubMessage
    @SuppressWarnings("unchecked")
    DefaultPubSubMessage mockMessage = mock(DefaultPubSubMessage.class);
    when(mockMessage.getKey()).thenReturn(mockKey);
    when(mockMessage.getValue()).thenReturn(mockEnvelope);
    when(mockMessage.getPosition()).thenReturn(mockPosition);

    // Test parameters
    String region = "test-region";
    int partitionNumber = 5;

    // Call the method under test
    InternalRow result = VenicePubSubMessageToRow.convertPubSubMessageToRow(mockMessage, region, partitionNumber, 100L);

    // Verify the result
    assertEquals(result.get(0, DataTypes.StringType).toString(), region, "Region should match");
    assertEquals(result.getInt(1), partitionNumber, "Partition number should match");
    assertEquals(result.getInt(2), MessageType.PUT.getValue(), "Message type should be PUT");
    assertEquals(result.getLong(3), 100L, "Offset should match");
    assertEquals(result.getInt(4), 11, "Schema ID should match");
    assertTrue(Arrays.equals((byte[]) result.get(5, DataTypes.BinaryType), keyBytes), "Key bytes should match");
    assertTrue(Arrays.equals((byte[]) result.get(6, DataTypes.BinaryType), valueBytes), "Value bytes should match");
    assertTrue(
        Arrays.equals((byte[]) result.get(7, DataTypes.BinaryType), replicationMetadataBytes),
        "Replication metadata payload should match");
    assertEquals(result.getInt(8), 37, "Replication metadata version ID should match");
  }
}
