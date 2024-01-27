package com.linkedin.venice;

import static com.linkedin.venice.kafka.protocol.enums.MessageType.DELETE;
import static com.linkedin.venice.kafka.protocol.enums.MessageType.PUT;
import static com.linkedin.venice.kafka.protocol.enums.MessageType.UPDATE;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.linkedin.venice.chunking.TestChunkingUtils;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.MultiSchemaResponse;
import com.linkedin.venice.controllerapi.SchemaResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.kafka.protocol.Delete;
import com.linkedin.venice.kafka.protocol.GUID;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.ProducerMetadata;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.protocol.Update;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.ImmutablePubSubMessage;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.adapter.kafka.consumer.ApacheKafkaConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.schema.rmd.RmdSchemaGenerator;
import com.linkedin.venice.schema.writecompute.WriteComputeSchemaConverter;
import com.linkedin.venice.serialization.KeyWithChunkingSuffixSerializer;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.ChunkedValueManifestSerializer;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.serializer.SerializerDeserializerFactory;
import com.linkedin.venice.storage.protocol.ChunkedKeySuffix;
import com.linkedin.venice.storage.protocol.ChunkedValueManifest;
import com.linkedin.venice.utils.ByteUtils;
import com.linkedin.venice.utils.TestWriteUtils;
import com.linkedin.venice.writer.update.UpdateBuilderImpl;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Optional;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.logging.log4j.LogManager;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestKafkaTopicDumper {
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
    when(version.isChunkingEnabled()).thenReturn(true);

    when(storeInfo.getPartitionCount()).thenReturn(2);
    when(storeInfo.getVersion(versionNumber)).thenReturn(Optional.of(version));
    when(controllerClient.getStore(storeName)).thenReturn(storeResponse);
    when(storeResponse.getStore()).thenReturn(storeInfo);

    PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();
    int assignedPartition = 0;
    long startOffset = 0;
    long endOffset = 4;
    String keyString = "test";
    byte[] serializedKey = TopicMessageFinder.serializeKey(keyString, schemaStr);
    PubSubTopicPartition pubSubTopicPartition =
        new PubSubTopicPartitionImpl(pubSubTopicRepository.getTopic(topic), assignedPartition);

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
        2,
        "",
        3,
        true,
        false,
        false);

    int firstChunkSegmentNumber = 1;
    int firstChunkSequenceNumber = 1;
    PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> pubSubMessage1 =
        getChunkedRecord(serializedKey, firstChunkSegmentNumber, firstChunkSequenceNumber, 0, 0, pubSubTopicPartition);
    String firstChunkMetadataLog = kafkaTopicDumper.getChunkMetadataLog(pubSubMessage1);
    Assert.assertEquals(
        firstChunkMetadataLog,
        " ChunkMd=(type:WITH_VALUE_CHUNK, ChunkIndex: 0, FirstChunkMd=(guid:00000000000000000000000000000000,seg:1,seq:1))");

    PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> pubSubMessage2 =
        getChunkedRecord(serializedKey, firstChunkSegmentNumber, firstChunkSequenceNumber, 1, 0, pubSubTopicPartition);
    String secondChunkMetadataLog = kafkaTopicDumper.getChunkMetadataLog(pubSubMessage2);
    Assert.assertEquals(
        secondChunkMetadataLog,
        " ChunkMd=(type:WITH_VALUE_CHUNK, ChunkIndex: 1, FirstChunkMd=(guid:00000000000000000000000000000000,seg:1,seq:1))");

    PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> pubSubMessage3 =
        getChunkedRecord(serializedKey, firstChunkSegmentNumber, firstChunkSequenceNumber, 2, 0, pubSubTopicPartition);
    String thirdChunkMetadataLog = kafkaTopicDumper.getChunkMetadataLog(pubSubMessage3);
    Assert.assertEquals(
        thirdChunkMetadataLog,
        " ChunkMd=(type:WITH_VALUE_CHUNK, ChunkIndex: 2, FirstChunkMd=(guid:00000000000000000000000000000000,seg:1,seq:1))");

    PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> pubSubMessage4 =
        getChunkValueManifestRecord(serializedKey, pubSubMessage1, firstChunkSequenceNumber, pubSubTopicPartition);
    String manifestChunkMetadataLog = kafkaTopicDumper.getChunkMetadataLog(pubSubMessage4);
    Assert.assertEquals(
        manifestChunkMetadataLog,
        " ChunkMd=(type:WITH_CHUNK_MANIFEST, FirstChunkMd=(guid:00000000000000000000000000000000,seg:1,seq:1))");

    PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> pubSubMessage5 =
        getDeleteRecord(serializedKey, null, pubSubTopicPartition);
    String deleteChunkMetadataLog = kafkaTopicDumper.getChunkMetadataLog(pubSubMessage5);
    Assert.assertEquals(deleteChunkMetadataLog, " ChunkMd=(type:WITH_FULL_VALUE)");
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
    when(version.isChunkingEnabled()).thenReturn(false);
    when(storeInfo.getPartitionCount()).thenReturn(1);
    when(storeInfo.isActiveActiveReplicationEnabled()).thenReturn(true);
    when(storeInfo.isWriteComputationEnabled()).thenReturn(true);
    when(storeInfo.getVersion(versionNumber)).thenReturn(Optional.of(version));
    when(controllerClient.getStore(storeName)).thenReturn(storeResponse);
    when(storeResponse.getStore()).thenReturn(storeInfo);

    PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();
    int assignedPartition = 0;
    long startOffset = 0;
    long endOffset = 4;
    String keyString = "test";
    byte[] serializedKey = keySerializer.serialize(keyString);
    PubSubTopicPartition pubSubTopicPartition =
        new PubSubTopicPartitionImpl(pubSubTopicRepository.getTopic(topic), assignedPartition);

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
        2,
        "",
        3,
        true,
        true,
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
    LogManager.getLogger(TestKafkaTopicDumper.class).info("DEBUGGING: {} {}", rmdRecord, rmdSchema);
    byte[] serializedValue = valueSerializer.serialize(valueRecord);
    byte[] serializedRmd = rmdSerializer.serialize(rmdRecord);
    byte[] serializedUpdate = updateSerializer.serialize(updateRecord);
    PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> putMessage =
        getPutRecord(serializedKey, serializedValue, serializedRmd, pubSubTopicPartition);
    String returnedLog = kafkaTopicDumper.buildDataRecordLog(putMessage, false);
    String expectedLog = String.format("Key: %s; Value: %s; Schema: %d", keyString, valueRecord, 1);
    Assert.assertEquals(returnedLog, expectedLog);
    returnedLog = kafkaTopicDumper.buildDataRecordLog(putMessage, true);
    expectedLog = String.format("Key: %s; Value: %s; Schema: %d; RMD: %s", keyString, valueRecord, 1, rmdRecord);
    Assert.assertEquals(returnedLog, expectedLog);

    // Test UPDATE
    PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> updateMessage =
        getUpdateRecord(serializedKey, serializedUpdate, pubSubTopicPartition);
    returnedLog = kafkaTopicDumper.buildDataRecordLog(updateMessage, false);
    expectedLog = String.format("Key: %s; Value: %s; Schema: %d-%d", keyString, updateRecord, 1, 1);
    Assert.assertEquals(returnedLog, expectedLog);
    returnedLog = kafkaTopicDumper.buildDataRecordLog(updateMessage, true);
    expectedLog = String.format("Key: %s; Value: %s; Schema: %d-%d; RMD: null", keyString, updateRecord, 1, 1);
    Assert.assertEquals(returnedLog, expectedLog);

    // Test DELETE with and without RMD
    PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> deleteMessage =
        getDeleteRecord(serializedKey, serializedRmd, pubSubTopicPartition);
    returnedLog = kafkaTopicDumper.buildDataRecordLog(deleteMessage, false);
    expectedLog = String.format("Key: %s; Value: %s; Schema: %d", keyString, null, 1);
    Assert.assertEquals(returnedLog, expectedLog);
    returnedLog = kafkaTopicDumper.buildDataRecordLog(deleteMessage, true);
    expectedLog = String.format("Key: %s; Value: %s; Schema: %d; RMD: %s", keyString, null, 1, rmdRecord);
    Assert.assertEquals(returnedLog, expectedLog);
  }

  private PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> getChunkedRecord(
      byte[] serializedKey,
      int firstChunkSegmentNumber,
      int firstChunkSequenceNumber,
      int chunkIndex,
      int firstMessageOffset,
      PubSubTopicPartition pubSubTopicPartition) {
    int chunkLength = 10;
    ChunkedKeySuffix chunkKeySuffix1 =
        TestChunkingUtils.createChunkedKeySuffix(firstChunkSegmentNumber, firstChunkSequenceNumber, chunkIndex);
    KeyWithChunkingSuffixSerializer keyWithChunkingSuffixSerializer = new KeyWithChunkingSuffixSerializer();
    ByteBuffer chunkKeyWithSuffix1 =
        keyWithChunkingSuffixSerializer.serializeChunkedKey(serializedKey, chunkKeySuffix1);
    KafkaKey kafkaKey = new KafkaKey(PUT, ByteUtils.extractByteArray(chunkKeyWithSuffix1));
    KafkaMessageEnvelope messageEnvelope = new KafkaMessageEnvelope();
    messageEnvelope.messageType = 0; // PUT
    messageEnvelope.producerMetadata = new ProducerMetadata();
    messageEnvelope.producerMetadata.messageTimestamp = 0;
    messageEnvelope.producerMetadata.segmentNumber = firstChunkSegmentNumber;
    messageEnvelope.producerMetadata.messageSequenceNumber = firstChunkSequenceNumber + chunkIndex;
    messageEnvelope.producerMetadata.producerGUID = new GUID();
    Put put = new Put();
    put.schemaId = AvroProtocolDefinition.CHUNK.getCurrentProtocolVersion();
    put.putValue = ByteBuffer.wrap(TestChunkingUtils.createChunkBytes(chunkIndex * chunkLength, chunkLength));
    messageEnvelope.payloadUnion = put;
    return new ImmutablePubSubMessage<>(
        kafkaKey,
        messageEnvelope,
        pubSubTopicPartition,
        firstMessageOffset + chunkIndex,
        0,
        20);
  }

  private PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> getChunkValueManifestRecord(
      byte[] serializedKey,
      PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> firstChunkMessage,
      int numberOfChunks,
      PubSubTopicPartition pubSubTopicPartition) {
    int chunkLength = 10;
    KeyWithChunkingSuffixSerializer keyWithChunkingSuffixSerializer = new KeyWithChunkingSuffixSerializer();

    byte[] chunkKeyWithSuffix = keyWithChunkingSuffixSerializer.serializeNonChunkedKey(serializedKey);
    KafkaKey kafkaKey = new KafkaKey(PUT, chunkKeyWithSuffix);
    KafkaMessageEnvelope messageEnvelope = new KafkaMessageEnvelope();
    messageEnvelope.messageType = 0; // PUT
    messageEnvelope.producerMetadata = new ProducerMetadata();
    messageEnvelope.producerMetadata.messageTimestamp = 0;
    messageEnvelope.producerMetadata.segmentNumber = firstChunkMessage.getValue().getProducerMetadata().segmentNumber;
    messageEnvelope.producerMetadata.messageSequenceNumber =
        firstChunkMessage.getValue().getProducerMetadata().messageSequenceNumber + numberOfChunks;
    messageEnvelope.producerMetadata.producerGUID = new GUID();

    ChunkedValueManifestSerializer chunkedValueManifestSerializer = new ChunkedValueManifestSerializer(true);
    ChunkedValueManifest manifest = new ChunkedValueManifest();
    manifest.keysWithChunkIdSuffix = new ArrayList<>(numberOfChunks);
    manifest.schemaId = 1;
    manifest.size = chunkLength * numberOfChunks;

    manifest.keysWithChunkIdSuffix.add(ByteBuffer.wrap(firstChunkMessage.getKey().getKey()));

    Put put = new Put();
    put.schemaId = AvroProtocolDefinition.CHUNKED_VALUE_MANIFEST.getCurrentProtocolVersion();
    put.putValue = chunkedValueManifestSerializer.serialize(manifest);
    messageEnvelope.payloadUnion = put;
    return new ImmutablePubSubMessage<>(
        kafkaKey,
        messageEnvelope,
        pubSubTopicPartition,
        firstChunkMessage.getOffset() + numberOfChunks,
        0,
        20);
  }

  private PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> getDeleteRecord(
      byte[] serializedKey,
      byte[] serializedRmd,
      PubSubTopicPartition pubSubTopicPartition) {
    KeyWithChunkingSuffixSerializer keyWithChunkingSuffixSerializer = new KeyWithChunkingSuffixSerializer();
    byte[] chunkKeyWithSuffix = keyWithChunkingSuffixSerializer.serializeNonChunkedKey(serializedKey);
    KafkaKey kafkaKey = new KafkaKey(DELETE, chunkKeyWithSuffix);
    KafkaMessageEnvelope messageEnvelope = new KafkaMessageEnvelope();
    messageEnvelope.messageType = 1;
    messageEnvelope.producerMetadata = new ProducerMetadata();
    messageEnvelope.producerMetadata.messageTimestamp = 0;
    messageEnvelope.producerMetadata.segmentNumber = 0;
    messageEnvelope.producerMetadata.messageSequenceNumber = 0;
    messageEnvelope.producerMetadata.producerGUID = new GUID();

    Delete delete = new Delete();
    delete.schemaId = 1;
    if (serializedRmd != null) {
      delete.replicationMetadataPayload = ByteBuffer.wrap(serializedRmd);
      delete.replicationMetadataVersionId = 1;
    }
    messageEnvelope.payloadUnion = delete;
    return new ImmutablePubSubMessage<>(kafkaKey, messageEnvelope, pubSubTopicPartition, 1, 0, 20);
  }

  private PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> getPutRecord(
      byte[] serializedKey,
      byte[] serializedValue,
      byte[] serializedRmd,
      PubSubTopicPartition pubSubTopicPartition) {
    KeyWithChunkingSuffixSerializer keyWithChunkingSuffixSerializer = new KeyWithChunkingSuffixSerializer();
    byte[] chunkKeyWithSuffix = keyWithChunkingSuffixSerializer.serializeNonChunkedKey(serializedKey);
    KafkaKey kafkaKey = new KafkaKey(PUT, chunkKeyWithSuffix);
    KafkaMessageEnvelope messageEnvelope = new KafkaMessageEnvelope();
    messageEnvelope.messageType = PUT.getValue();
    messageEnvelope.producerMetadata = new ProducerMetadata();
    messageEnvelope.producerMetadata.messageTimestamp = 0;
    messageEnvelope.producerMetadata.segmentNumber = 0;
    messageEnvelope.producerMetadata.messageSequenceNumber = 0;
    messageEnvelope.producerMetadata.producerGUID = new GUID();
    Put put = new Put();
    put.schemaId = 1;
    put.putValue = ByteBuffer.wrap(serializedValue);
    if (serializedRmd != null) {
      put.replicationMetadataPayload = ByteBuffer.wrap(serializedRmd);
      put.replicationMetadataVersionId = 1;
    }
    messageEnvelope.payloadUnion = put;
    return new ImmutablePubSubMessage<>(kafkaKey, messageEnvelope, pubSubTopicPartition, 1, 0, serializedValue.length);
  }

  private PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> getUpdateRecord(
      byte[] serializedKey,
      byte[] serializedValue,
      PubSubTopicPartition pubSubTopicPartition) {
    KeyWithChunkingSuffixSerializer keyWithChunkingSuffixSerializer = new KeyWithChunkingSuffixSerializer();
    byte[] chunkKeyWithSuffix = keyWithChunkingSuffixSerializer.serializeNonChunkedKey(serializedKey);
    KafkaKey kafkaKey = new KafkaKey(UPDATE, chunkKeyWithSuffix);
    KafkaMessageEnvelope messageEnvelope = new KafkaMessageEnvelope();
    messageEnvelope.messageType = UPDATE.getValue();
    messageEnvelope.producerMetadata = new ProducerMetadata();
    messageEnvelope.producerMetadata.messageTimestamp = 0;
    messageEnvelope.producerMetadata.segmentNumber = 0;
    messageEnvelope.producerMetadata.messageSequenceNumber = 0;
    messageEnvelope.producerMetadata.producerGUID = new GUID();
    Update update = new Update();
    update.schemaId = 1;
    update.updateValue = ByteBuffer.wrap(serializedValue);
    update.updateSchemaId = 1;

    messageEnvelope.payloadUnion = update;
    return new ImmutablePubSubMessage<>(kafkaKey, messageEnvelope, pubSubTopicPartition, 1, 0, serializedValue.length);
  }
}
