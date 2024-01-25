package com.linkedin.venice;

import static com.linkedin.venice.kafka.protocol.enums.MessageType.DELETE;
import static com.linkedin.venice.kafka.protocol.enums.MessageType.PUT;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.linkedin.venice.chunking.TestChunkingUtils;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.SchemaResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.kafka.protocol.Delete;
import com.linkedin.venice.kafka.protocol.GUID;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.ProducerMetadata;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.ImmutablePubSubMessage;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.adapter.kafka.consumer.ApacheKafkaConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.serialization.KeyWithChunkingSuffixSerializer;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.ChunkedValueManifestSerializer;
import com.linkedin.venice.storage.protocol.ChunkedKeySuffix;
import com.linkedin.venice.storage.protocol.ChunkedValueManifest;
import com.linkedin.venice.utils.ByteUtils;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Optional;
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
        getDeleteRecord(serializedKey, 4, pubSubTopicPartition);
    String deleteChunkMetadataLog = kafkaTopicDumper.getChunkMetadataLog(pubSubMessage5);
    Assert.assertEquals(deleteChunkMetadataLog, " ChunkMd=(type:WITH_FULL_VALUE)");
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
      int pubSubMessageOffset,
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
    messageEnvelope.payloadUnion = delete;
    return new ImmutablePubSubMessage<>(kafkaKey, messageEnvelope, pubSubTopicPartition, pubSubMessageOffset, 0, 20);
  }
}
