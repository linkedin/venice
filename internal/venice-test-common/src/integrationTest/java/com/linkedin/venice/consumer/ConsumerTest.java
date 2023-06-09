package com.linkedin.venice.consumer;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.linkedin.venice.exceptions.VeniceMessageException;
import com.linkedin.venice.guid.GuidUtils;
import com.linkedin.venice.kafka.protocol.ProducerMetadata;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.schema.SchemaReader;
import com.linkedin.venice.serialization.avro.KafkaValueSerializer;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.writer.VeniceWriter;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.testng.Assert;
import org.testng.annotations.Test;


@Test
public class ConsumerTest {
  private static final String PRODUCER_METADATA_FIELD = "producerMetadata";
  private static final String MESSAGE_TYPE_FIELD = "messageType";
  private static final String TARGET_VERSION_FIELD = "targetVersion";
  private static final String PAYLOAD_UNION_FIELD = "payloadUnion";
  private static final String NEW_FIELD = "newField";
  private static final KafkaValueSerializer REGULAR_KAFKA_VALUE_SERIALIZER = new KafkaValueSerializer();
  private static final Schema NEW_PROTOCOL_SCHEMA = ConsumerIntegrationTest.NEW_PROTOCOL_SCHEMA;
  private static final int NEW_PROTOCOL_VERSION = ConsumerIntegrationTest.NEW_PROTOCOL_VERSION;

  @Test
  void testForwardCompatibility() {
    // Serialization of a value encoded with the new protocol version
    GenericRecord messageFromNewProtocol = getMessageFromNewProtocol();
    byte[] serializedMessage = ConsumerIntegrationTest.serializeNewProtocol(messageFromNewProtocol);

    // Sanity check to make sure that a serializer without forward compat enabled cannot deserialize the new protocol
    try {
      REGULAR_KAFKA_VALUE_SERIALIZER.deserialize("", serializedMessage);
      Assert.fail(
          "The regular serializer should have failed to decode the message encoded with the new version. The test may have regressed.");
    } catch (VeniceMessageException e) {
      // Expected
    }

    // Preparation of a serializer with forward compat enabled
    SchemaReader mockSchemaReader = mock(SchemaReader.class);
    doReturn(NEW_PROTOCOL_SCHEMA).when(mockSchemaReader).getValueSchema(NEW_PROTOCOL_VERSION);
    KafkaValueSerializer obliviousDeserializer = new KafkaValueSerializer();
    obliviousDeserializer.setSchemaReader(mockSchemaReader);

    // Sanity checks to make sure the new protocol has not somehow crept into the fresh (not warmed up) serializer
    Assert.assertEquals(
        obliviousDeserializer.knownProtocols(),
        REGULAR_KAFKA_VALUE_SERIALIZER.knownProtocols(),
        "The obliviousDeserializer should not know the same as the REGULAR_KAFKA_VALUE_SERIALIZER before reading the new version.");
    Assert.assertFalse(
        obliviousDeserializer.knownProtocols().contains(NEW_PROTOCOL_VERSION),
        "The obliviousDeserializer should not know about the new protocol ahead of time.");

    // CODE UNDER TEST
    GenericRecord messageFromObliviousDeserializer = obliviousDeserializer.deserialize("", serializedMessage);

    // Check that the new protocol which was previously absent has now been fetched
    Assert.assertTrue(
        obliviousDeserializer.knownProtocols().contains(NEW_PROTOCOL_VERSION),
        "The obliviousDeserializer should know about the new protocol after encountering it.");

    // Data integrity checks for the fields the reader is interested in
    Arrays.asList(PRODUCER_METADATA_FIELD, MESSAGE_TYPE_FIELD, PAYLOAD_UNION_FIELD)
        .stream()
        .forEach(
            f -> Assert.assertEquals(
                messageFromObliviousDeserializer.get(f),
                messageFromNewProtocol.get(f),
                "Field '" + f + "' is not equal pre- and post-serialization."));

    // The new field should be absent in order to be fully compliant with the reader's compiled schema
    try {
      messageFromObliviousDeserializer.get(NEW_FIELD);
      Assert.fail("The new field name should not be available because the reader does not want it.");
    } catch (Exception e) {
      // Expected
      Assert.assertEquals(e.getClass(), NullPointerException.class);
    }
    Assert.assertNotEquals(
        messageFromObliviousDeserializer,
        messageFromNewProtocol,
        "The two records should not be completely equal pre- and post-serialization since the new field should be ignored.");
  }

  public static GenericRecord getMessageFromNewProtocol() {
    // Serialization of a value encoded with the new protocol version
    GenericRecord messageFromNewProtocol = new GenericData.Record(NEW_PROTOCOL_SCHEMA);
    ProducerMetadata producerMetadata = new ProducerMetadata();
    producerMetadata.producerGUID = GuidUtils.getGUID(VeniceProperties.empty());
    producerMetadata.messageTimestamp = System.currentTimeMillis();
    producerMetadata.segmentNumber = 0;
    producerMetadata.messageSequenceNumber = 0;
    messageFromNewProtocol.put(PRODUCER_METADATA_FIELD, producerMetadata);
    messageFromNewProtocol.put(MESSAGE_TYPE_FIELD, MessageType.PUT.getValue());
    Put put = new Put();
    put.schemaId = 1;
    put.putValue = ByteBuffer.allocate(1);
    put.replicationMetadataVersionId = VeniceWriter.VENICE_DEFAULT_TIMESTAMP_METADATA_VERSION_ID;
    put.replicationMetadataPayload = ByteBuffer.wrap(new byte[0]);
    messageFromNewProtocol.put(PAYLOAD_UNION_FIELD, put);
    messageFromNewProtocol.put(NEW_FIELD, 1);

    return messageFromNewProtocol;
  }
}
