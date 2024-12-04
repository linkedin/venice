package com.linkedin.venice;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.MultiSchemaResponse;
import com.linkedin.venice.controllerapi.SchemaResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.kafka.protocol.ControlMessage;
import com.linkedin.venice.kafka.protocol.GUID;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.ProducerMetadata;
import com.linkedin.venice.kafka.protocol.StartOfSegment;
import com.linkedin.venice.kafka.protocol.TopicSwitch;
import com.linkedin.venice.kafka.protocol.enums.ControlMessageType;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.ImmutablePubSubMessage;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.adapter.kafka.consumer.ApacheKafkaConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.pubsub.api.exceptions.PubSubClientException;
import com.linkedin.venice.schema.rmd.RmdSchemaGenerator;
import com.linkedin.venice.schema.writecompute.WriteComputeSchemaConverter;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.serializer.SerializerDeserializerFactory;
import com.linkedin.venice.utils.ChunkingTestUtils;
import com.linkedin.venice.utils.TestWriteUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.writer.update.UpdateBuilderImpl;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.testng.annotations.Test;


public class TestKafkaTopicDumper {
  private static final PubSubTopicRepository TOPIC_REPOSITORY = new PubSubTopicRepository();

  @Test
  public void testAdminToolConsumptionForChunkedData() throws IOException {
    String schemaStr = "\"string\"";
    String storeName = "test_store";
    int versionNumber = 1;
    String topic = Version.composeKafkaTopic(storeName, versionNumber);
    ControllerClient controllerClient = mock(ControllerClient.class);
    SchemaResponse schemaResponse = mock(SchemaResponse.class);
    when(schemaResponse.getSchemaStr()).thenReturn(schemaStr);
    when(controllerClient.getKeySchema(storeName)).thenReturn(schemaResponse);
    StoreResponse storeResponse = mock(StoreResponse.class);
    StoreInfo storeInfo = mock(StoreInfo.class);

    Version version = mock(Version.class);
    when(version.getCompressionStrategy()).thenReturn(CompressionStrategy.NO_OP);
    when(version.isChunkingEnabled()).thenReturn(true);

    when(storeInfo.getPartitionCount()).thenReturn(2);
    when(storeInfo.getVersion(versionNumber)).thenReturn(Optional.of(version));
    when(controllerClient.getStore(storeName)).thenReturn(storeResponse);
    when(storeResponse.getStore()).thenReturn(storeInfo);

    int assignedPartition = 0;
    long startOffset = 0;
    long endOffset = 4;
    String keyString = "test";
    byte[] serializedKey = TopicMessageFinder.serializeKey(keyString, schemaStr);
    PubSubTopicPartition pubSubTopicPartition =
        new PubSubTopicPartitionImpl(TOPIC_REPOSITORY.getTopic(topic), assignedPartition);

    ApacheKafkaConsumerAdapter apacheKafkaConsumer = mock(ApacheKafkaConsumerAdapter.class);
    long startTimestamp = 10;
    long endTimestamp = 20;
    when(apacheKafkaConsumer.offsetForTime(pubSubTopicPartition, startTimestamp)).thenReturn(startOffset);
    when(apacheKafkaConsumer.offsetForTime(pubSubTopicPartition, endTimestamp)).thenReturn(endOffset);
    when(apacheKafkaConsumer.endOffset(pubSubTopicPartition)).thenReturn(endOffset);

    KafkaTopicDumper kafkaTopicDumper = new KafkaTopicDumper(
        controllerClient,
        apacheKafkaConsumer,
        topic,
        assignedPartition,
        0,
        -1,
        2,
        "",
        3,
        true,
        false,
        false,
        false);

    int numChunks = 3;
    String metadataFormat = " ChunkMd=(type:%s, FirstChunkMd=(guid:00000000000000000000000000000000,seg:1,seq:1))";
    PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> chunkMessage = null;
    for (int i = 0; i < numChunks; i++) {
      chunkMessage = ChunkingTestUtils.createChunkedRecord(serializedKey, 1, 1, i, 0, pubSubTopicPartition);
      String metadataLog = kafkaTopicDumper.getChunkMetadataLog(chunkMessage);
      assertEquals(metadataLog, String.format(metadataFormat, "WITH_VALUE_CHUNK, ChunkIndex: " + i));
    }

    PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> manifestMessage =
        ChunkingTestUtils.createChunkValueManifestRecord(serializedKey, chunkMessage, numChunks, pubSubTopicPartition);
    String manifestChunkMetadataLog = kafkaTopicDumper.getChunkMetadataLog(manifestMessage);
    assertEquals(manifestChunkMetadataLog, String.format(metadataFormat, "WITH_CHUNK_MANIFEST"));

    PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> deleteMessage =
        ChunkingTestUtils.createDeleteRecord(serializedKey, null, pubSubTopicPartition);
    String deleteChunkMetadataLog = kafkaTopicDumper.getChunkMetadataLog(deleteMessage);
    assertEquals(deleteChunkMetadataLog, " ChunkMd=(type:WITH_FULL_VALUE)");
  }

  @Test
  public void testDumpDataRecord() throws IOException {
    Schema keySchema = TestWriteUtils.STRING_SCHEMA;
    Schema valueSchema = TestWriteUtils.NAME_RECORD_V1_SCHEMA;
    Schema updateSchema = WriteComputeSchemaConverter.getInstance().convertFromValueRecordSchema(valueSchema);
    Schema rmdSchema = RmdSchemaGenerator.generateMetadataSchema(valueSchema);
    RecordSerializer keySerializer = SerializerDeserializerFactory.getAvroGenericSerializer(keySchema);
    RecordSerializer valueSerializer = SerializerDeserializerFactory.getAvroGenericSerializer(valueSchema);
    RecordSerializer updateSerializer = SerializerDeserializerFactory.getAvroGenericSerializer(updateSchema);
    RecordSerializer rmdSerializer = SerializerDeserializerFactory.getAvroGenericSerializer(rmdSchema);

    String storeName = "test_store";
    int versionNumber = 1;
    String topic = Version.composeKafkaTopic(storeName, versionNumber);
    ControllerClient controllerClient = mock(ControllerClient.class);

    SchemaResponse keySchemaResponse = mock(SchemaResponse.class);
    when(keySchemaResponse.getSchemaStr()).thenReturn(keySchema.toString());
    when(controllerClient.getKeySchema(storeName)).thenReturn(keySchemaResponse);

    MultiSchemaResponse valueSchemaResponse = mock(MultiSchemaResponse.class);
    MultiSchemaResponse.Schema[] valueSchemas = new MultiSchemaResponse.Schema[1];
    valueSchemas[0] = new MultiSchemaResponse.Schema();
    valueSchemas[0].setId(1);
    valueSchemas[0].setSchemaStr(valueSchema.toString());
    when(valueSchemaResponse.getSchemas()).thenReturn(valueSchemas);
    when(controllerClient.getAllValueSchema(storeName)).thenReturn(valueSchemaResponse);

    MultiSchemaResponse rmdSchemaResponse = mock(MultiSchemaResponse.class);
    MultiSchemaResponse.Schema[] rmdSchemas = new MultiSchemaResponse.Schema[1];
    rmdSchemas[0] = new MultiSchemaResponse.Schema();
    rmdSchemas[0].setSchemaStr(rmdSchema.toString());
    rmdSchemas[0].setId(1);
    rmdSchemas[0].setRmdValueSchemaId(1);
    when(rmdSchemaResponse.getSchemas()).thenReturn(rmdSchemas);
    when(controllerClient.getAllReplicationMetadataSchemas(storeName)).thenReturn(rmdSchemaResponse);

    MultiSchemaResponse valueAndDerivedSchemaResponse = mock(MultiSchemaResponse.class);
    MultiSchemaResponse.Schema[] valueAndDerivedSchema = new MultiSchemaResponse.Schema[2];
    valueAndDerivedSchema[0] = valueSchemas[0];
    valueAndDerivedSchema[1] = new MultiSchemaResponse.Schema();
    valueAndDerivedSchema[1].setDerivedSchemaId(1);
    valueAndDerivedSchema[1].setId(1);
    valueAndDerivedSchema[1].setSchemaStr(updateSchema.toString());
    when(valueAndDerivedSchemaResponse.getSchemas()).thenReturn(valueAndDerivedSchema);
    when(controllerClient.getAllValueAndDerivedSchema(storeName)).thenReturn(valueAndDerivedSchemaResponse);

    StoreResponse storeResponse = mock(StoreResponse.class);
    StoreInfo storeInfo = mock(StoreInfo.class);
    Version version = mock(Version.class);
    when(version.getCompressionStrategy()).thenReturn(CompressionStrategy.NO_OP);
    when(version.isChunkingEnabled()).thenReturn(false);
    when(storeInfo.getPartitionCount()).thenReturn(1);
    when(storeInfo.isActiveActiveReplicationEnabled()).thenReturn(true);
    when(storeInfo.isWriteComputationEnabled()).thenReturn(true);
    when(storeInfo.getVersion(versionNumber)).thenReturn(Optional.of(version));
    when(controllerClient.getStore(storeName)).thenReturn(storeResponse);
    when(storeResponse.getStore()).thenReturn(storeInfo);

    int assignedPartition = 0;
    long startOffset = 0;
    long endOffset = 4;
    String keyString = "test";
    byte[] serializedKey = keySerializer.serialize(keyString);
    PubSubTopicPartition pubSubTopicPartition =
        new PubSubTopicPartitionImpl(TOPIC_REPOSITORY.getTopic(topic), assignedPartition);

    ApacheKafkaConsumerAdapter apacheKafkaConsumer = mock(ApacheKafkaConsumerAdapter.class);
    long startTimestamp = 10;
    long endTimestamp = 20;
    when(apacheKafkaConsumer.offsetForTime(pubSubTopicPartition, startTimestamp)).thenReturn(startOffset);
    when(apacheKafkaConsumer.offsetForTime(pubSubTopicPartition, endTimestamp)).thenReturn(endOffset);
    when(apacheKafkaConsumer.endOffset(pubSubTopicPartition)).thenReturn(endOffset);

    KafkaTopicDumper kafkaTopicDumper = new KafkaTopicDumper(
        controllerClient,
        apacheKafkaConsumer,
        topic,
        assignedPartition,
        0,
        -1,
        2,
        "",
        3,
        true,
        true,
        false,
        false);

    // Test different message type.
    GenericRecord valueRecord = new GenericData.Record(valueSchema);
    valueRecord.put("firstName", "f1");
    valueRecord.put("lastName", "l1");
    GenericRecord rmdRecord = new GenericData.Record(rmdSchema);
    rmdRecord.put("timestamp", 1L);
    rmdRecord.put("replication_checkpoint_vector", Collections.singletonList(1L));
    GenericRecord updateRecord = new UpdateBuilderImpl(updateSchema).setNewFieldValue("firstName", "f2").build();

    // Test PUT with and without RMD
    byte[] serializedValue = valueSerializer.serialize(valueRecord);
    byte[] serializedRmd = rmdSerializer.serialize(rmdRecord);
    byte[] serializedUpdate = updateSerializer.serialize(updateRecord);
    PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> putMessage =
        ChunkingTestUtils.createPutRecord(serializedKey, serializedValue, serializedRmd, pubSubTopicPartition);
    String returnedLog = kafkaTopicDumper.buildDataRecordLog(putMessage, false);
    String expectedLog = String.format("Key: %s; Value: %s; Schema: %d", keyString, valueRecord, 1);
    assertEquals(returnedLog, expectedLog);
    returnedLog = kafkaTopicDumper.buildDataRecordLog(putMessage, true);
    expectedLog = String.format("Key: %s; Value: %s; Schema: %d; RMD: %s", keyString, valueRecord, 1, rmdRecord);
    assertEquals(returnedLog, expectedLog);

    // Test UPDATE
    PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> updateMessage =
        ChunkingTestUtils.createUpdateRecord(serializedKey, serializedUpdate, pubSubTopicPartition);
    returnedLog = kafkaTopicDumper.buildDataRecordLog(updateMessage, false);
    expectedLog = String.format("Key: %s; Value: %s; Schema: %d-%d", keyString, updateRecord, 1, 1);
    assertEquals(returnedLog, expectedLog);
    returnedLog = kafkaTopicDumper.buildDataRecordLog(updateMessage, true);
    expectedLog = String.format("Key: %s; Value: %s; Schema: %d-%d; RMD: null", keyString, updateRecord, 1, 1);
    assertEquals(returnedLog, expectedLog);

    // Test DELETE with and without RMD
    PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> deleteMessage =
        ChunkingTestUtils.createDeleteRecord(serializedKey, serializedRmd, pubSubTopicPartition);
    returnedLog = kafkaTopicDumper.buildDataRecordLog(deleteMessage, false);
    expectedLog = String.format("Key: %s; Value: %s; Schema: %d", keyString, null, 1);
    assertEquals(returnedLog, expectedLog);
    returnedLog = kafkaTopicDumper.buildDataRecordLog(deleteMessage, true);
    expectedLog = String.format("Key: %s; Value: %s; Schema: %d; RMD: %s", keyString, null, 1, rmdRecord);
    assertEquals(returnedLog, expectedLog);
  }

  @Test
  public void testTopicSwitchMessageLogging() {
    // Case 1: TopicSwitch message with non-null sourceKafkaServers
    List<CharSequence> sourceKafkaServers = Arrays.asList("source1", "source2");
    ControlMessage controlMessage = new ControlMessage();
    controlMessage.controlMessageType = ControlMessageType.TOPIC_SWITCH.getValue();
    TopicSwitch topicSwitch = new TopicSwitch();
    topicSwitch.sourceKafkaServers = sourceKafkaServers;
    topicSwitch.sourceTopicName = "test_topic_rt";
    topicSwitch.rewindStartTimestamp = 123456789L;
    controlMessage.controlMessageUnion = topicSwitch;
    KafkaKey kafkaKey = new KafkaKey(MessageType.CONTROL_MESSAGE, Utils.getUniqueString("key-").getBytes());
    KafkaMessageEnvelope messageEnvelope = new KafkaMessageEnvelope();
    messageEnvelope.producerMetadata = new ProducerMetadata();
    messageEnvelope.producerMetadata.messageTimestamp = 0;
    messageEnvelope.producerMetadata.messageSequenceNumber = 0;
    messageEnvelope.producerMetadata.segmentNumber = 0;
    messageEnvelope.producerMetadata.producerGUID = new GUID();
    messageEnvelope.payloadUnion = controlMessage;

    PubSubTopicPartition pubSubTopicPartition =
        new PubSubTopicPartitionImpl(TOPIC_REPOSITORY.getTopic("test_topic_rt"), 0);

    PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> message =
        new ImmutablePubSubMessage<>(kafkaKey, messageEnvelope, pubSubTopicPartition, 120, 0, 0, null);

    String actualLog = KafkaTopicDumper.constructTopicSwitchLog(message);
    assertNotNull(actualLog);
    assertTrue(actualLog.contains("[source1, source2]"));
    assertTrue(actualLog.contains("test_topic_rt"));
    assertTrue(actualLog.contains("123456789"));

    // Case 2: Non TS Control message
    controlMessage = new ControlMessage();
    controlMessage.controlMessageType = ControlMessageType.START_OF_SEGMENT.getValue();
    controlMessage.controlMessageUnion = new StartOfSegment();
    messageEnvelope.payloadUnion = controlMessage;

    PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> nonTsCtrlMsg =
        new ImmutablePubSubMessage<>(kafkaKey, messageEnvelope, pubSubTopicPartition, 120, 0, 0, null);
    KafkaTopicDumper.logIfTopicSwitchMessage(nonTsCtrlMsg); // Should not throw any exception

    // Case 3: Non-control message
    KafkaKey regularMsgKey = new KafkaKey(MessageType.PUT, Utils.getUniqueString("key-").getBytes());
    PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> regularMessage =
        new ImmutablePubSubMessage<>(regularMsgKey, null, pubSubTopicPartition, 120, 0, 0, null);
    KafkaTopicDumper.logIfTopicSwitchMessage(regularMessage); // Should not throw any exception
  }

  @Test
  public void testGetOffsetToConsumerFrom() {
    PubSubConsumerAdapter consumerAdapter = mock(PubSubConsumerAdapter.class);
    PubSubTopicPartition topicPartition = new PubSubTopicPartitionImpl(TOPIC_REPOSITORY.getTopic("test_topic_rt"), 0);
    // Case 1: When start timestamp is non-negative and start offset is non-negative; offsetForTime is
    // non-null then it should be used as the start offset.
    long startOffset = -1;
    long startTimestamp = 123456789L;
    long offsetForTime = 1234L;
    long beginningOffset = 0;
    when(consumerAdapter.offsetForTime(topicPartition, startTimestamp)).thenReturn(offsetForTime);
    when(consumerAdapter.beginningOffset(eq(topicPartition), any())).thenReturn(beginningOffset);
    long actualStartOffset =
        KafkaTopicDumper.getOffsetToConsumerFrom(consumerAdapter, topicPartition, startOffset, startTimestamp);
    assertEquals(actualStartOffset, offsetForTime);

    // Case 2: When start timestamp is non-negative and start offset is non-negative; but offsetForTime is null,
    // beginning offset should be used as the start offset.
    when(consumerAdapter.offsetForTime(topicPartition, startTimestamp)).thenReturn(null);
    long finalStartOffset = -1;
    long finalStartTimestamp = 123456789L;
    PubSubClientException e = expectThrows(
        PubSubClientException.class,
        () -> KafkaTopicDumper
            .getOffsetToConsumerFrom(consumerAdapter, topicPartition, finalStartOffset, finalStartTimestamp));
    assertTrue(e.getMessage().contains("No offset found"));

    // Case 3: When start timestamp is non-negative and start offset is non-negative; but beginning offset is higher
    // than offsetForTime, beginning offset should be used as the start offset.
    beginningOffset = 12356L;
    when(consumerAdapter.offsetForTime(topicPartition, startTimestamp)).thenReturn(startOffset);
    when(consumerAdapter.beginningOffset(eq(topicPartition), any())).thenReturn(beginningOffset);
    actualStartOffset =
        KafkaTopicDumper.getOffsetToConsumerFrom(consumerAdapter, topicPartition, startOffset, startTimestamp);
    assertEquals(actualStartOffset, beginningOffset);

    // Case 4: When start timestamp is negative and start offset > beginning offset, start offset should be used.
    startOffset = 1234L;
    startTimestamp = -1;
    when(consumerAdapter.offsetForTime(topicPartition, startTimestamp)).thenReturn(null);
    when(consumerAdapter.beginningOffset(eq(topicPartition), any())).thenReturn(0L);
    actualStartOffset =
        KafkaTopicDumper.getOffsetToConsumerFrom(consumerAdapter, topicPartition, startOffset, startTimestamp);
    assertEquals(actualStartOffset, startOffset);
  }
}
