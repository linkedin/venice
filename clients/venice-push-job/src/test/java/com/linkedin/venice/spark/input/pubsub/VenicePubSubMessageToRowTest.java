package com.linkedin.venice.spark.input.pubsub;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.kafka.protocol.Delete;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.pubsub.ImmutablePubSubMessage;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.adapter.kafka.common.ApacheKafkaOffsetPosition;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.apache.spark.sql.catalyst.InternalRow;
import org.testng.annotations.Test;


/**
 * Test class to verify that VenicePubSubMessageToRow correctly converts Kafka messages
 * to Spark InternalRows with the correct field ordering matching RAW_PUBSUB_INPUT_TABLE_SCHEMA.
 */
public class VenicePubSubMessageToRowTest {
  /**
   * Test that PUT messages are converted correctly with all fields in the right order.
   */
  @Test
  public void testConvertPutMessage() {
    // Setup
    VenicePubSubMessageToRow converter = new VenicePubSubMessageToRow();

    byte[] keyBytes = "test-key".getBytes();
    byte[] valueBytes = "test-value".getBytes();
    byte[] rmdPayload = "rmd-data".getBytes();
    int schemaId = 42;
    int rmdVersionId = 5;

    KafkaKey kafkaKey = new KafkaKey(MessageType.PUT, keyBytes);
    Put put = new Put();
    put.putValue = ByteBuffer.wrap(valueBytes);
    put.schemaId = schemaId;
    put.replicationMetadataPayload = ByteBuffer.wrap(rmdPayload);
    put.replicationMetadataVersionId = rmdVersionId;

    KafkaMessageEnvelope envelope = new KafkaMessageEnvelope();
    envelope.messageType = MessageType.PUT.getValue();
    envelope.payloadUnion = put;

    String region = "us-west";
    int partitionNumber = 3;
    long offset = 12345L;

    PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();
    PubSubTopicPartition pubSubTopicPartition =
        new PubSubTopicPartitionImpl(pubSubTopicRepository.getTopic("test_store_v1"), partitionNumber);
    ImmutablePubSubMessage message = new ImmutablePubSubMessage(
        kafkaKey,
        envelope,
        pubSubTopicPartition,
        ApacheKafkaOffsetPosition.of(offset),
        -1,
        -1);

    // Execute
    InternalRow row = converter.convert(message, region, partitionNumber, offset);

    assertEquals(row.numFields(), 9, "Row should have 9 fields");

    // Verify field 0: region
    assertEquals(row.getString(0), region, "Region should match");

    // Verify field 1: partition
    assertEquals(row.getInt(1), partitionNumber, "Partition should match");

    // Verify field 2: offset
    assertEquals(row.getLong(2), offset, "Offset should match");

    // Verify field 3: message_type
    assertEquals(row.getInt(3), MessageType.PUT.getValue(), "Message type should be PUT");

    // Verify field 4: schema_id
    assertEquals(row.getInt(4), schemaId, "Schema ID should match");

    // Verify field 5: key
    assertTrue(Arrays.equals(row.getBinary(5), keyBytes), "Key bytes should match");

    // Verify field 6: value
    assertTrue(Arrays.equals(row.getBinary(6), valueBytes), "Value bytes should match");

    // Verify field 7: rmd_version_id
    assertEquals(row.getInt(7), rmdVersionId, "RMD version ID should match");

    // Verify field 8: rmd_payload
    assertTrue(Arrays.equals(row.getBinary(8), rmdPayload), "RMD payload should match");
  }

  /**
   * Test that DELETE messages are converted correctly with all fields in the right order.
   */
  @Test
  public void testConvertDeleteMessage() {
    // Setup
    VenicePubSubMessageToRow converter = new VenicePubSubMessageToRow();

    byte[] keyBytes = "deleted-key".getBytes();
    byte[] rmdPayload = "delete-rmd".getBytes();
    int schemaId = 10;
    int rmdVersionId = 3;

    KafkaKey kafkaKey = new KafkaKey(MessageType.DELETE, keyBytes);
    Delete delete = new Delete();
    delete.schemaId = schemaId;
    delete.replicationMetadataPayload = ByteBuffer.wrap(rmdPayload);
    delete.replicationMetadataVersionId = rmdVersionId;

    KafkaMessageEnvelope envelope = new KafkaMessageEnvelope();
    envelope.messageType = MessageType.DELETE.getValue();
    envelope.payloadUnion = delete;

    String region = "eu-central";
    int partitionNumber = 7;
    long offset = 67890L;

    PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();
    PubSubTopicPartition pubSubTopicPartition =
        new PubSubTopicPartitionImpl(pubSubTopicRepository.getTopic("test_store_v1"), partitionNumber);
    ImmutablePubSubMessage message = new ImmutablePubSubMessage(
        kafkaKey,
        envelope,
        pubSubTopicPartition,
        ApacheKafkaOffsetPosition.of(offset),
        -1,
        -1);

    // Execute
    InternalRow row = converter.convert(message, region, partitionNumber, offset);

    // Verify
    assertEquals(row.numFields(), 9, "Row should have 9 fields");

    // Verify field order matches schema
    assertEquals(row.getString(0), region, "Region should match");
    assertEquals(row.getInt(1), partitionNumber, "Partition should match");
    assertEquals(row.getLong(2), offset, "Offset should match");
    assertEquals(row.getInt(3), MessageType.DELETE.getValue(), "Message type should be DELETE");
    assertEquals(row.getInt(4), schemaId, "Schema ID should match");
    assertTrue(Arrays.equals(row.getBinary(5), keyBytes), "Key bytes should match");
    assertTrue(Arrays.equals(row.getBinary(6), new byte[0]), "Value should be empty for DELETE");
    assertEquals(row.getInt(7), rmdVersionId, "RMD version ID should match");
    assertTrue(Arrays.equals(row.getBinary(8), rmdPayload), "RMD payload should match");
  }

  /**
   * Test that PUT messages without RMD (batch-only) are handled correctly.
   */
  @Test
  public void testConvertPutMessageWithoutRmd() {
    // Setup
    VenicePubSubMessageToRow converter = new VenicePubSubMessageToRow();

    byte[] keyBytes = "batch-key".getBytes();
    byte[] valueBytes = "batch-value".getBytes();
    int schemaId = 100;

    KafkaKey kafkaKey = new KafkaKey(MessageType.PUT, keyBytes);
    Put put = new Put();
    put.putValue = ByteBuffer.wrap(valueBytes);
    put.schemaId = schemaId;
    put.replicationMetadataPayload = ByteBuffer.wrap(new byte[0]); // Empty RMD
    put.replicationMetadataVersionId = -1; // No RMD version

    KafkaMessageEnvelope envelope = new KafkaMessageEnvelope();
    envelope.messageType = MessageType.PUT.getValue();
    envelope.payloadUnion = put;

    String region = "ap-south";
    int partitionNumber = 1;
    long offset = 999L;

    PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();
    PubSubTopicPartition pubSubTopicPartition =
        new PubSubTopicPartitionImpl(pubSubTopicRepository.getTopic("test_store_v1"), partitionNumber);
    ImmutablePubSubMessage message = new ImmutablePubSubMessage(
        kafkaKey,
        envelope,
        pubSubTopicPartition,
        ApacheKafkaOffsetPosition.of(offset),
        -1,
        -1);

    // Execute
    InternalRow row = converter.convert(message, region, partitionNumber, offset);

    // Verify
    assertEquals(row.numFields(), 9, "Row should have 9 fields");

    // Verify key fields
    assertEquals(row.getString(0), region);
    assertEquals(row.getInt(1), partitionNumber);
    assertEquals(row.getLong(2), offset);
    assertEquals(row.getInt(3), MessageType.PUT.getValue());
    assertEquals(row.getInt(4), schemaId);
    assertTrue(Arrays.equals(row.getBinary(5), keyBytes));
    assertTrue(Arrays.equals(row.getBinary(6), valueBytes));

    // Verify RMD fields are empty/default
    assertEquals(row.getInt(7), -1, "RMD version ID should be -1 for batch-only");
    assertTrue(Arrays.equals(row.getBinary(8), new byte[0]), "RMD payload should be empty for batch-only");
  }

  /**
   * Test that chunked PUT messages (negative schema ID) are handled correctly.
   */
  @Test
  public void testConvertChunkedPutMessage() {
    // Setup
    VenicePubSubMessageToRow converter = new VenicePubSubMessageToRow();

    byte[] keyBytes = "chunked-key".getBytes();
    byte[] manifestBytes = "manifest-data".getBytes();
    int chunkedSchemaId = -10; // Negative schema ID indicates chunking

    KafkaKey kafkaKey = new KafkaKey(MessageType.PUT, keyBytes);
    Put put = new Put();
    put.putValue = ByteBuffer.wrap(manifestBytes);
    put.schemaId = chunkedSchemaId;
    put.replicationMetadataPayload = ByteBuffer.wrap(new byte[0]);
    put.replicationMetadataVersionId = -1;

    KafkaMessageEnvelope envelope = new KafkaMessageEnvelope();
    envelope.messageType = MessageType.PUT.getValue();
    envelope.payloadUnion = put;

    String region = "us-east";
    int partitionNumber = 5;
    long offset = 5555L;

    PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();
    PubSubTopicPartition pubSubTopicPartition =
        new PubSubTopicPartitionImpl(pubSubTopicRepository.getTopic("test_store_v1"), partitionNumber);
    ImmutablePubSubMessage message = new ImmutablePubSubMessage(
        kafkaKey,
        envelope,
        pubSubTopicPartition,
        ApacheKafkaOffsetPosition.of(offset),
        -1,
        -1);

    // Execute
    InternalRow row = converter.convert(message, region, partitionNumber, offset);

    // Verify
    assertEquals(row.numFields(), 9, "Row should have 9 fields");
    assertEquals(row.getString(0), region);
    assertEquals(row.getInt(1), partitionNumber);
    assertEquals(row.getLong(2), offset);
    assertEquals(row.getInt(3), MessageType.PUT.getValue());
    assertEquals(row.getInt(4), chunkedSchemaId, "Schema ID should be negative for chunked messages");
    assertTrue(Arrays.equals(row.getBinary(5), keyBytes));
    assertTrue(Arrays.equals(row.getBinary(6), manifestBytes));
  }

  /**
   * Test the static factory method for backward compatibility.
   */
  @Test
  public void testStaticFactoryMethod() {
    // Setup
    byte[] keyBytes = "factory-key".getBytes();
    byte[] valueBytes = "factory-value".getBytes();
    int schemaId = 7;

    KafkaKey kafkaKey = new KafkaKey(MessageType.PUT, keyBytes);
    Put put = new Put();
    put.putValue = ByteBuffer.wrap(valueBytes);
    put.schemaId = schemaId;
    put.replicationMetadataPayload = ByteBuffer.wrap(new byte[0]);
    put.replicationMetadataVersionId = -1;

    KafkaMessageEnvelope envelope = new KafkaMessageEnvelope();
    envelope.messageType = MessageType.PUT.getValue();
    envelope.payloadUnion = put;

    String region = "test-region";
    int partitionNumber = 0;
    long offset = 1L;

    PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();
    PubSubTopicPartition pubSubTopicPartition =
        new PubSubTopicPartitionImpl(pubSubTopicRepository.getTopic("test_store_v1"), partitionNumber);
    ImmutablePubSubMessage message = new ImmutablePubSubMessage(
        kafkaKey,
        envelope,
        pubSubTopicPartition,
        ApacheKafkaOffsetPosition.of(offset),
        -1,
        -1);

    // Execute using static method
    InternalRow row = VenicePubSubMessageToRow.convertPubSubMessageToRow(message, region, partitionNumber, offset);

    // Verify
    assertEquals(row.numFields(), 9, "Row should have 9 fields");
    assertEquals(row.getString(0), region);
    assertEquals(row.getInt(1), partitionNumber);
    assertEquals(row.getLong(2), offset);
    assertEquals(row.getInt(3), MessageType.PUT.getValue());
    assertEquals(row.getInt(4), schemaId);
  }

}
