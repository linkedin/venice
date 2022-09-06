package com.linkedin.venice.serialization;

import com.linkedin.venice.controller.kafka.protocol.serializer.AdminOperationSerializer;
import com.linkedin.venice.kafka.protocol.ControlMessage;
import com.linkedin.venice.kafka.protocol.EndOfSegment;
import com.linkedin.venice.kafka.protocol.GUID;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.ProducerMetadata;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.protocol.enums.ControlMessageType;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.serialization.avro.OptimizedKafkaValueSerializer;
import com.linkedin.venice.writer.VeniceWriter;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestOptimizedKafkaValueSerializer {
  @Test
  public static void testControlMessageDeserialization() {
    KafkaMessageEnvelope record = new KafkaMessageEnvelope();
    record.messageType = MessageType.CONTROL_MESSAGE.getValue();
    record.producerMetadata = new ProducerMetadata();
    record.producerMetadata.messageSequenceNumber = 1;
    record.producerMetadata.messageTimestamp = -1;
    record.producerMetadata.producerGUID = new GUID();
    record.producerMetadata.segmentNumber = 1;

    EndOfSegment endOfSegment = new EndOfSegment();
    byte[] checksumBytes = "checksum".getBytes();
    endOfSegment.checksumValue = ByteBuffer.wrap(checksumBytes);
    endOfSegment.computedAggregates = new ArrayList<>();
    endOfSegment.finalSegment = true;

    ControlMessage controlMessage = new ControlMessage();
    controlMessage.controlMessageType = ControlMessageType.END_OF_SEGMENT.getValue();
    controlMessage.debugInfo = new HashMap<>();
    controlMessage.controlMessageUnion = endOfSegment;

    record.payloadUnion = controlMessage;

    OptimizedKafkaValueSerializer valueSerializer = new OptimizedKafkaValueSerializer();
    String topic = "test_topic";
    byte[] serializedRecord = valueSerializer.serialize(topic, record);

    KafkaMessageEnvelope deserializedRecord = valueSerializer.deserialize(topic, serializedRecord);
    EndOfSegment deserializedEndOfSegment =
        (EndOfSegment) ((ControlMessage) deserializedRecord.payloadUnion).controlMessageUnion;
    ByteBuffer deserializedChecksumValue = deserializedEndOfSegment.checksumValue;
    Assert.assertTrue(
        deserializedChecksumValue.position() > 0,
        "Deserialized checksum should be backed by the original byte array");
    Assert.assertEquals(deserializedChecksumValue, ByteBuffer.wrap(checksumBytes));
  }

  @Test
  public static void testPutMessageDeserialization() {
    KafkaMessageEnvelope record = new KafkaMessageEnvelope();
    record.messageType = MessageType.PUT.getValue();
    record.producerMetadata = new ProducerMetadata();
    record.producerMetadata.messageSequenceNumber = 1;
    record.producerMetadata.messageTimestamp = -1;
    record.producerMetadata.producerGUID = new GUID();
    record.producerMetadata.segmentNumber = 1;
    Put put = new Put();
    put.schemaId = AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION;
    byte[] putValueBytes = "put_value".getBytes();
    put.putValue = ByteBuffer.wrap(putValueBytes);
    put.replicationMetadataVersionId = VeniceWriter.VENICE_DEFAULT_TIMESTAMP_METADATA_VERSION_ID;
    put.replicationMetadataPayload = ByteBuffer.wrap(new byte[0]);
    record.payloadUnion = put;

    OptimizedKafkaValueSerializer valueSerializer = new OptimizedKafkaValueSerializer();
    String topic = "test_topic";
    byte[] serializedRecord = valueSerializer.serialize(topic, record);

    KafkaMessageEnvelope deserializedRecord = valueSerializer.deserialize(topic, serializedRecord);
    Put deserializedPut = (Put) deserializedRecord.payloadUnion;
    int expectedPutValueLen = putValueBytes.length;
    Assert.assertEquals(deserializedPut.putValue.remaining(), expectedPutValueLen);
    Assert.assertTrue(deserializedPut.putValue.position() > 0, "There must be some head room at the beginning");
    byte[] actualPutValueBytes = new byte[deserializedPut.putValue.remaining()];
    deserializedPut.putValue.get(actualPutValueBytes);
    Assert.assertEquals(actualPutValueBytes, putValueBytes);
  }
}
