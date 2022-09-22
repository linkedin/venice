package com.linkedin.venice.serialization;

import static com.linkedin.venice.writer.VeniceWriter.VENICE_DEFAULT_TIMESTAMP_METADATA_VERSION_ID;
import static com.linkedin.venice.writer.VeniceWriter.VENICE_DEFAULT_VALUE_SCHEMA_ID;

import com.linkedin.venice.kafka.protocol.Delete;
import com.linkedin.venice.kafka.protocol.GUID;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.ProducerMetadata;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.serialization.avro.KafkaValueSerializer;
import com.linkedin.venice.writer.VeniceWriter;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Tests for the Kafka Key and Value Serialization classes
 * 1. Verify magic byte, schema version, operation type and payload are serialized/de-serialized correctly.
 * 2. Repeat for a PUT message and a DELETE message
 */
public class TestKafkaSerializer {
  private static final String TEST_TOPIC = "TEST_TOPIC";

  @Test
  public void testKafkaKeySerializer() {
    KafkaKeySerializer serializer = new KafkaKeySerializer();

    byte[] expectedKeyContent = "p1".getBytes();

    /* TEST 1: PUT */
    KafkaKey expectedKafkaKey = new KafkaKey(MessageType.PUT, expectedKeyContent);
    byte[] byteArray = serializer.serialize(TEST_TOPIC, expectedKafkaKey);
    KafkaKey actualKafkaKey = serializer.deserialize(TEST_TOPIC, byteArray);

    Assert.assertEquals(
        actualKafkaKey.getKeyHeaderByte(),
        expectedKafkaKey.getKeyHeaderByte(),
        "The KafkaKey for PUT does not have the same header byte before and after serialization");

    Assert.assertTrue(
        Arrays.equals(expectedKeyContent, actualKafkaKey.getKey()),
        "The KafkaKey for PUT does not have the same payload before and after serialization");

    /* TEST 2: DELETE */
    byte[] key2 = "d1".getBytes();
    expectedKafkaKey = new KafkaKey(MessageType.DELETE, key2);
    byteArray = serializer.serialize(TEST_TOPIC, expectedKafkaKey);
    actualKafkaKey = serializer.deserialize(TEST_TOPIC, byteArray);

    Assert.assertEquals(
        actualKafkaKey.getKeyHeaderByte(),
        expectedKafkaKey.getKeyHeaderByte(),
        "The KafkaKey for DELETE does not have the same header byte before and after serialization");

    Assert.assertTrue(
        Arrays.equals(key2, actualKafkaKey.getKey()),
        "The KafkaKey for DELETE does not have the same payload before and after serialization");

    /* TEST 3: CONTROL_MESSAGE */
    expectedKafkaKey = new KafkaKey(MessageType.CONTROL_MESSAGE, expectedKeyContent);
    byteArray = serializer.serialize(TEST_TOPIC, expectedKafkaKey);
    actualKafkaKey = serializer.deserialize(TEST_TOPIC, byteArray);

    Assert.assertEquals(
        actualKafkaKey.getKeyHeaderByte(),
        expectedKafkaKey.getKeyHeaderByte(),
        "The KafkaKey for a CONTROL_MESSAGE does not have the same header byte before and after serialization");

    Assert.assertTrue(
        Arrays.equals(expectedKeyContent, actualKafkaKey.getKey()),
        "The KafkaKey for a CONTROL_MESSAGE does not have the same payload before and after serialization");
  }

  @Test
  public void testValueSerializer() {
    KafkaValueSerializer serializer = new KafkaValueSerializer();

    /* TEST 1 */
    byte[] val1 = "p1".getBytes();

    KafkaMessageEnvelope expectedKafkaValue1 = new KafkaMessageEnvelope();
    expectedKafkaValue1.messageType = MessageType.PUT.getValue();

    Put put = new Put();
    put.schemaId = -1; // TODO: Use valid schema ID.
    put.putValue = ByteBuffer.wrap(val1);
    put.replicationMetadataVersionId = VeniceWriter.VENICE_DEFAULT_TIMESTAMP_METADATA_VERSION_ID;
    put.replicationMetadataPayload = ByteBuffer.wrap(new byte[0]);
    expectedKafkaValue1.payloadUnion = put;

    // TODO: Populate producer metadata properly
    ProducerMetadata producerMetadata = new ProducerMetadata();
    producerMetadata.producerGUID = new GUID();
    producerMetadata.producerGUID.bytes(new byte[16]);
    producerMetadata.messageSequenceNumber = -1;
    producerMetadata.segmentNumber = -1;
    producerMetadata.messageTimestamp = -1;
    expectedKafkaValue1.producerMetadata = producerMetadata;

    byte[] byteArray = serializer.serialize(TEST_TOPIC, expectedKafkaValue1);
    KafkaMessageEnvelope actualKafkaValue1 = serializer.deserialize(TEST_TOPIC, byteArray);

    Assert.assertEquals(
        actualKafkaValue1,
        expectedKafkaValue1,
        "KafkaMessageEnvelope for PUT should be equal() before and after serialization");

    Assert.assertEquals(
        actualKafkaValue1.producerMetadata,
        producerMetadata,
        "ProducerMetadata for PUT should be equal() before and after serialization");

    Assert.assertTrue(
        Arrays.equals(val1, ((Put) actualKafkaValue1.payloadUnion).putValue.array()),
        "PUT value should be the same before and after serialization");

    Assert.assertEquals(
        ((Put) actualKafkaValue1.payloadUnion).schemaId,
        put.schemaId,
        "PUT schemaId should be equal() before and after serialization");

    /* TEST 2 */
    KafkaMessageEnvelope expectedKafkaValue2 = new KafkaMessageEnvelope();
    expectedKafkaValue2.messageType = MessageType.DELETE.getValue();
    Delete delete = new Delete();
    delete.schemaId = VENICE_DEFAULT_VALUE_SCHEMA_ID;
    delete.replicationMetadataVersionId = VENICE_DEFAULT_TIMESTAMP_METADATA_VERSION_ID;
    delete.replicationMetadataPayload = ByteBuffer.wrap(new byte[0]);
    expectedKafkaValue2.payloadUnion = delete;

    // TODO: Populate producer metadata properly
    ProducerMetadata producerMetadata2 = new ProducerMetadata();
    producerMetadata2.producerGUID = new GUID();
    producerMetadata2.producerGUID.bytes(new byte[16]);
    producerMetadata2.messageSequenceNumber = -2;
    producerMetadata2.segmentNumber = -2;
    producerMetadata2.messageTimestamp = -2;
    expectedKafkaValue2.producerMetadata = producerMetadata2;

    byteArray = serializer.serialize(TEST_TOPIC, expectedKafkaValue2);
    KafkaMessageEnvelope actualKafkaValue2 = serializer.deserialize(TEST_TOPIC, byteArray);

    Assert.assertEquals(
        actualKafkaValue2,
        expectedKafkaValue2,
        "KafkaMessageEnvelope for DELETE should be equal() before and after serialization");

    Assert.assertEquals(
        actualKafkaValue2.producerMetadata,
        producerMetadata2,
        "KafkaMessageEnvelope's ProducerMetadata for DELETE should be equal() before and after serialization");

  }

}
