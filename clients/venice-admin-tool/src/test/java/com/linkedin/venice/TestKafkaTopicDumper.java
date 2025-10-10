package com.linkedin.venice;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
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
import com.linkedin.venice.pubsub.PubSubPositionDeserializer;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.PubSubUtil;
import com.linkedin.venice.pubsub.adapter.kafka.common.ApacheKafkaOffsetPosition;
import com.linkedin.venice.pubsub.adapter.kafka.consumer.ApacheKafkaConsumerAdapter;
import com.linkedin.venice.pubsub.api.DefaultPubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubSymbolicPosition;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
    ApacheKafkaOffsetPosition startPosition = ApacheKafkaOffsetPosition.of(0L);
    ApacheKafkaOffsetPosition endPosition = ApacheKafkaOffsetPosition.of(4L);
    String keyString = "test";
    byte[] serializedKey = TopicMessageFinder.serializeKey(keyString, schemaStr);
    PubSubTopicPartition pubSubTopicPartition =
        new PubSubTopicPartitionImpl(TOPIC_REPOSITORY.getTopic(topic), assignedPartition);

    ApacheKafkaConsumerAdapter apacheKafkaConsumer = mock(ApacheKafkaConsumerAdapter.class);
    long startTimestamp = 10;
    long endTimestamp = 20;
    when(apacheKafkaConsumer.getPositionByTimestamp(pubSubTopicPartition, startTimestamp)).thenReturn(startPosition);
    when(apacheKafkaConsumer.getPositionByTimestamp(pubSubTopicPartition, endTimestamp)).thenReturn(endPosition);
    when(apacheKafkaConsumer.endPosition(pubSubTopicPartition)).thenReturn(endPosition);

    KafkaTopicDumper kafkaTopicDumper = new KafkaTopicDumper(
        controllerClient,
        apacheKafkaConsumer,
        pubSubTopicPartition,
        "",
        3,
        true,
        false,
        false,
        false,
        PubSubPositionDeserializer.DEFAULT_DESERIALIZER);

    int numChunks = 3;
    String metadataFormat = " ChunkMd=(type:%s, FirstChunkMd=(guid:00000000000000000000000000000000,seg:1,seq:1))";
    DefaultPubSubMessage chunkMessage = null;
    for (int i = 0; i < numChunks; i++) {
      chunkMessage = ChunkingTestUtils.createChunkedRecord(serializedKey, 1, 1, i, 0, pubSubTopicPartition);
      String metadataLog = kafkaTopicDumper.getChunkMetadataLog(chunkMessage);
      assertEquals(metadataLog, String.format(metadataFormat, "WITH_VALUE_CHUNK, ChunkIndex: " + i));
    }

    DefaultPubSubMessage manifestMessage =
        ChunkingTestUtils.createChunkValueManifestRecord(serializedKey, chunkMessage, numChunks, pubSubTopicPartition);
    String manifestChunkMetadataLog = kafkaTopicDumper.getChunkMetadataLog(manifestMessage);
    assertEquals(manifestChunkMetadataLog, String.format(metadataFormat, "WITH_CHUNK_MANIFEST"));

    DefaultPubSubMessage deleteMessage =
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
    ApacheKafkaOffsetPosition startPosition = ApacheKafkaOffsetPosition.of(0L);
    ApacheKafkaOffsetPosition endPosition = ApacheKafkaOffsetPosition.of(4L);
    String keyString = "test";
    byte[] serializedKey = keySerializer.serialize(keyString);
    PubSubTopicPartition pubSubTopicPartition =
        new PubSubTopicPartitionImpl(TOPIC_REPOSITORY.getTopic(topic), assignedPartition);

    ApacheKafkaConsumerAdapter apacheKafkaConsumer = mock(ApacheKafkaConsumerAdapter.class);
    long startTimestamp = 10;
    long endTimestamp = 20;
    when(apacheKafkaConsumer.getPositionByTimestamp(pubSubTopicPartition, startTimestamp)).thenReturn(startPosition);
    when(apacheKafkaConsumer.getPositionByTimestamp(pubSubTopicPartition, endTimestamp)).thenReturn(endPosition);
    when(apacheKafkaConsumer.endPosition(pubSubTopicPartition)).thenReturn(endPosition);

    KafkaTopicDumper kafkaTopicDumper = new KafkaTopicDumper(
        controllerClient,
        apacheKafkaConsumer,
        pubSubTopicPartition,
        "",
        3,
        true,
        true,
        false,
        false,
        PubSubPositionDeserializer.DEFAULT_DESERIALIZER);

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
    DefaultPubSubMessage putMessage =
        ChunkingTestUtils.createPutRecord(serializedKey, serializedValue, serializedRmd, pubSubTopicPartition);
    String returnedLog = kafkaTopicDumper.buildDataRecordLog(putMessage, false);
    String expectedLog = String.format("Key: %s; Value: %s; Schema: %d", keyString, valueRecord, 1);
    assertEquals(returnedLog, expectedLog);
    returnedLog = kafkaTopicDumper.buildDataRecordLog(putMessage, true);
    expectedLog = String.format("Key: %s; Value: %s; Schema: %d; RMD: %s", keyString, valueRecord, 1, rmdRecord);
    assertEquals(returnedLog, expectedLog);

    // Test UPDATE
    DefaultPubSubMessage updateMessage =
        ChunkingTestUtils.createUpdateRecord(serializedKey, serializedUpdate, pubSubTopicPartition);
    returnedLog = kafkaTopicDumper.buildDataRecordLog(updateMessage, false);
    expectedLog = String.format("Key: %s; Value: %s; Schema: %d-%d", keyString, updateRecord, 1, 1);
    assertEquals(returnedLog, expectedLog);
    returnedLog = kafkaTopicDumper.buildDataRecordLog(updateMessage, true);
    expectedLog = String.format("Key: %s; Value: %s; Schema: %d-%d; RMD: null", keyString, updateRecord, 1, 1);
    assertEquals(returnedLog, expectedLog);

    // Test DELETE with and without RMD
    DefaultPubSubMessage deleteMessage =
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

    DefaultPubSubMessage message = new ImmutablePubSubMessage(
        kafkaKey,
        messageEnvelope,
        pubSubTopicPartition,
        ApacheKafkaOffsetPosition.of(120),
        0,
        0,
        null);

    String actualLog =
        KafkaTopicDumper.constructTopicSwitchLog(message, PubSubPositionDeserializer.DEFAULT_DESERIALIZER);
    assertNotNull(actualLog);
    assertTrue(actualLog.contains("[source1, source2]"));
    assertTrue(actualLog.contains("test_topic_rt"));
    assertTrue(actualLog.contains("123456789"));

    // Case 2: Non TS Control message
    controlMessage = new ControlMessage();
    controlMessage.controlMessageType = ControlMessageType.START_OF_SEGMENT.getValue();
    controlMessage.controlMessageUnion = new StartOfSegment();
    messageEnvelope.payloadUnion = controlMessage;

    DefaultPubSubMessage nonTsCtrlMsg = new ImmutablePubSubMessage(
        kafkaKey,
        messageEnvelope,
        pubSubTopicPartition,
        ApacheKafkaOffsetPosition.of(120),
        0,
        0,
        null);
    // Should not throw any exception
    KafkaTopicDumper.logIfTopicSwitchMessage(nonTsCtrlMsg, PubSubPositionDeserializer.DEFAULT_DESERIALIZER);

    // Case 3: Non-control message
    KafkaKey regularMsgKey = new KafkaKey(MessageType.PUT, Utils.getUniqueString("key-").getBytes());
    DefaultPubSubMessage regularMessage = new ImmutablePubSubMessage(
        regularMsgKey,
        null,
        pubSubTopicPartition,
        ApacheKafkaOffsetPosition.of(120),
        0,
        0,
        null);
    // Should not throw any exception
    KafkaTopicDumper.logIfTopicSwitchMessage(regularMessage, PubSubPositionDeserializer.DEFAULT_DESERIALIZER);
  }

  @Test
  public void testCalculateStartingPosition() {
    PubSubConsumerAdapter consumerAdapter = mock(PubSubConsumerAdapter.class);
    PubSubTopicPartition topicPartition = new PubSubTopicPartitionImpl(TOPIC_REPOSITORY.getTopic("test_topic_rt"), 0);
    // Case 1: When start timestamp is non-negative and start position is non-negative; positionForTime is
    // non-null then it should be used as the start position.
    long startTimestamp = 123456789L;
    ApacheKafkaOffsetPosition positionForTime = ApacheKafkaOffsetPosition.of(1234L);
    ApacheKafkaOffsetPosition startPosition = ApacheKafkaOffsetPosition.of(0L);
    when(consumerAdapter.getPositionByTimestamp(topicPartition, startTimestamp)).thenReturn(positionForTime);
    when(consumerAdapter.beginningPosition(eq(topicPartition))).thenReturn(startPosition);
    doAnswer(invocation -> {
      PubSubPosition p1 = invocation.getArgument(1);
      PubSubPosition p2 = invocation.getArgument(2);
      return PubSubUtil.computeOffsetDelta(topicPartition, p1, p2, consumerAdapter);
    }).when(consumerAdapter)
        .positionDifference(any(PubSubTopicPartition.class), any(PubSubPosition.class), any(PubSubPosition.class));

    PubSubPosition actualStartPosition = KafkaTopicDumper
        .calculateStartingPosition(consumerAdapter, topicPartition, PubSubSymbolicPosition.EARLIEST, startTimestamp);
    assertEquals(actualStartPosition, positionForTime);

    // Case 2: When start timestamp is non-negative and start position is non-negative; but positionForTime is null,
    // beginning position should be used as the start position.
    when(consumerAdapter.getPositionByTimestamp(topicPartition, startTimestamp)).thenReturn(null);
    long finalStartTimestamp = 123456789L;
    PubSubClientException e = expectThrows(
        PubSubClientException.class,
        () -> KafkaTopicDumper.calculateStartingPosition(
            consumerAdapter,
            topicPartition,
            PubSubSymbolicPosition.EARLIEST,
            finalStartTimestamp));
    assertTrue(e.getMessage().contains("Failed to find an position"), "Actual error message: " + e.getMessage());

    // Case 3: When start timestamp is non-negative and start position is non-negative; but beginning position is higher
    // than positionForTime, beginning position should be used as the start position.
    startPosition = ApacheKafkaOffsetPosition.of(12356L);
    when(consumerAdapter.getPositionByTimestamp(topicPartition, startTimestamp))
        .thenReturn(PubSubSymbolicPosition.EARLIEST);
    when(consumerAdapter.beginningPosition(eq(topicPartition))).thenReturn(startPosition);
    actualStartPosition =
        KafkaTopicDumper.calculateStartingPosition(consumerAdapter, topicPartition, startPosition, startTimestamp);
    assertEquals(actualStartPosition, startPosition);

    // Case 4: When start timestamp is negative and start position > beginning position, start position should be used.
    startPosition = ApacheKafkaOffsetPosition.of(1234L);
    startTimestamp = -1;
    when(consumerAdapter.getPositionByTimestamp(topicPartition, startTimestamp)).thenReturn(null);
    when(consumerAdapter.beginningPosition(eq(topicPartition))).thenReturn(ApacheKafkaOffsetPosition.of(0L));
    actualStartPosition =
        KafkaTopicDumper.calculateStartingPosition(consumerAdapter, topicPartition, startPosition, startTimestamp);
    assertEquals(actualStartPosition, startPosition);
  }

  @Test
  public void testCalculateEndingPosition() {
    PubSubTopicPartition partition = new PubSubTopicPartitionImpl(TOPIC_REPOSITORY.getTopic("test-topic"), 0);

    // Test Case 1: endTimestamp is -1, should return endPosition
    PubSubConsumerAdapter mockConsumer = mock(PubSubConsumerAdapter.class);
    ApacheKafkaOffsetPosition endPos = ApacheKafkaOffsetPosition.of(100L);
    when(mockConsumer.endPosition(partition)).thenReturn(endPos);
    PubSubPosition endPosition = KafkaTopicDumper.calculateEndingPosition(mockConsumer, partition, -1);
    assertEquals(endPosition, endPos, "Should return the endPosition when endTimestamp is -1");
    verify(mockConsumer).endPosition(partition);
    verify(mockConsumer, never()).getPositionByTimestamp(partition, -1L);

    // Test Case 2: Valid endTimestamp with positionForTime returning a value
    mockConsumer = mock(PubSubConsumerAdapter.class);
    ApacheKafkaOffsetPosition p80 = ApacheKafkaOffsetPosition.of(80L);
    when(mockConsumer.getPositionByTimestamp(partition, 200L)).thenReturn(p80);
    endPosition = KafkaTopicDumper.calculateEndingPosition(mockConsumer, partition, 200L);
    assertEquals(endPosition, p80, "Should return the position for the specified timestamp");
    verify(mockConsumer).getPositionByTimestamp(partition, 200L);
    verify(mockConsumer).endPosition(partition);

    // Test Case 3: Valid endTimestamp but positionForTime returns null
    mockConsumer = mock(PubSubConsumerAdapter.class);
    ApacheKafkaOffsetPosition endPos3 = ApacheKafkaOffsetPosition.of(100L);
    when(mockConsumer.endPosition(partition)).thenReturn(endPos3);
    when(mockConsumer.getPositionByTimestamp(partition, 300L)).thenReturn(null);
    endPosition = KafkaTopicDumper.calculateEndingPosition(mockConsumer, partition, 300L);
    assertEquals(endPosition, endPos3, "Should return the endPosition when no position is found for the timestamp");
    verify(mockConsumer).getPositionByTimestamp(partition, 300L);
    verify(mockConsumer).endPosition(partition);
  }

  @Test
  public void testFetchAndProcess() {
    PubSubConsumerAdapter mockConsumer = mock(PubSubConsumerAdapter.class);
    PubSubTopicPartition topicPartition = new PubSubTopicPartitionImpl(TOPIC_REPOSITORY.getTopic("test"), 0);
    KafkaTopicDumper dumper = new KafkaTopicDumper(mockConsumer, topicPartition, 3);

    KafkaTopicDumper spyDumper = spy(dumper);
    ApacheKafkaOffsetPosition p0 = ApacheKafkaOffsetPosition.of(0L);
    ApacheKafkaOffsetPosition p10 = ApacheKafkaOffsetPosition.of(10L);

    // Mock positionDifference for the dumper's consumer
    doAnswer(invocation -> {
      PubSubPosition pos1 = invocation.getArgument(1);
      PubSubPosition pos2 = invocation.getArgument(2);
      return PubSubUtil.computeOffsetDelta(topicPartition, pos1, pos2, mockConsumer);
    }).when(mockConsumer)
        .positionDifference(any(PubSubTopicPartition.class), any(PubSubPosition.class), any(PubSubPosition.class));

    // Case 1: Invalid message count
    Exception e = expectThrows(IllegalArgumentException.class, () -> spyDumper.fetchAndProcess(p0, p10, -1));
    assertTrue(e.getMessage().contains("Invalid message count"), "Actual error message: " + e.getMessage());

    // Case 2: Invalid position range
    e = expectThrows(IllegalArgumentException.class, () -> spyDumper.fetchAndProcess(p10, p0, 5));
    assertTrue(
        e.getMessage().contains("Start position")
            && e.getMessage().contains("is greater than or equal to end position"),
        "Actual error message: " + e.getMessage());

    // Case 3: Valid range with 5 records to process
    List<DefaultPubSubMessage> mockMessages = createMockMessages(15, 0);
    Map<PubSubTopicPartition, List<DefaultPubSubMessage>> mockPollResult = new HashMap<>();
    mockPollResult.put(topicPartition, mockMessages);
    when(mockConsumer.poll(5000L)).thenReturn(mockPollResult);
    doNothing().when(spyDumper).processRecord(any());
    int processedCount = spyDumper.fetchAndProcess(p0, p10, 5);
    assertEquals(processedCount, 5, "Should process all 5 messages in range");

    // Case 4: Poll returns no records
    when(mockConsumer.poll(5000L)).thenReturn(Collections.emptyMap());
    processedCount = spyDumper.fetchAndProcess(p0, p10, 5);
    assertEquals(processedCount, 0, "Should process no messages when poll returns empty");

    // Case 5: endPosition is reached before messageCount is reached
    mockMessages = createMockMessages(4, 0);
    mockPollResult.put(topicPartition, mockMessages);
    when(mockConsumer.poll(5000L)).thenReturn(mockPollResult);
    ApacheKafkaOffsetPosition p3 = ApacheKafkaOffsetPosition.of(3L);
    processedCount = spyDumper.fetchAndProcess(p0, p3, 5);
    assertEquals(processedCount, 3, "Should process all messages up to endPosition (positions 0,1,2,3)");

    // Verify unsubscription
    verify(mockConsumer, atLeastOnce()).unSubscribe(topicPartition);
  }

  private List<DefaultPubSubMessage> createMockMessages(int count, long startPosition) {
    List<DefaultPubSubMessage> messages = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      DefaultPubSubMessage message = mock(DefaultPubSubMessage.class);
      PubSubPosition pubSubPosition = ApacheKafkaOffsetPosition.of(startPosition + i);
      when(message.getPosition()).thenReturn(pubSubPosition);
      messages.add(message);
    }
    return messages;
  }
}
