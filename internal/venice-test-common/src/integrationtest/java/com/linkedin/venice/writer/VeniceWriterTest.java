package com.linkedin.venice.writer;

import static com.linkedin.venice.kafka.TopicManager.DEFAULT_KAFKA_OPERATION_TIMEOUT_MS;
import static com.linkedin.venice.writer.VeniceWriter.APP_DEFAULT_LOGICAL_TS;
import static com.linkedin.venice.writer.VeniceWriter.DEFAULT_MAX_SIZE_FOR_USER_PAYLOAD_PER_MESSAGE_IN_BYTES;
import static com.linkedin.venice.writer.VeniceWriter.VENICE_DEFAULT_LOGICAL_TS;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.integration.utils.PubSubBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.kafka.TopicManager;
import com.linkedin.venice.kafka.protocol.Delete;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.ProducerMetadata;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.partitioner.DefaultVenicePartitioner;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapterFactory;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubProducerAdapter;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.pubsub.kafka.KafkaPubSubMessageDeserializer;
import com.linkedin.venice.serialization.KeyWithChunkingSuffixSerializer;
import com.linkedin.venice.serialization.VeniceKafkaSerializer;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.ChunkedValueManifestSerializer;
import com.linkedin.venice.serialization.avro.KafkaValueSerializer;
import com.linkedin.venice.serialization.avro.OptimizedKafkaValueSerializer;
import com.linkedin.venice.serialization.avro.VeniceAvroKafkaSerializer;
import com.linkedin.venice.storage.protocol.ChunkId;
import com.linkedin.venice.storage.protocol.ChunkedKeySuffix;
import com.linkedin.venice.storage.protocol.ChunkedValueManifest;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.SystemTime;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.pools.LandFillObjectPool;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import org.mockito.ArgumentCaptor;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


@Test
public class VeniceWriterTest {
  private PubSubBrokerWrapper pubSubBrokerWrapper;
  private TopicManager topicManager;
  private PubSubConsumerAdapterFactory pubSubConsumerAdapterFactory;

  private final PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();

  @BeforeClass
  public void setUp() {
    pubSubBrokerWrapper = ServiceFactory.getPubSubBroker();
    pubSubConsumerAdapterFactory = IntegrationTestPushUtils.getVeniceConsumerFactory();
    topicManager =
        IntegrationTestPushUtils
            .getTopicManagerRepo(
                DEFAULT_KAFKA_OPERATION_TIMEOUT_MS,
                100L,
                0L,
                pubSubBrokerWrapper.getAddress(),
                pubSubTopicRepository)
            .getTopicManager();
  }

  @AfterClass
  public void cleanUp() throws IOException {
    Utils.closeQuietlyWithErrorLogged(topicManager, pubSubBrokerWrapper);
  }

  private void testThreadSafety(
      int numberOfThreads,
      java.util.function.Consumer<VeniceWriter<KafkaKey, byte[], byte[]>> veniceWriterTask)
      throws ExecutionException, InterruptedException {
    String topicName = TestUtils.getUniqueTopicString("topic-for-vw-thread-safety");
    int partitionCount = 1;
    PubSubTopic pubSubTopic = pubSubTopicRepository.getTopic(topicName);
    topicManager.createTopic(pubSubTopic, partitionCount, 1, true);
    Properties properties = new Properties();
    properties.put(ConfigKeys.KAFKA_BOOTSTRAP_SERVERS, pubSubBrokerWrapper.getAddress());
    properties.put(ConfigKeys.PARTITIONER_CLASS, DefaultVenicePartitioner.class.getName());

    ExecutorService executorService = null;
    try (VeniceWriter<KafkaKey, byte[], byte[]> veniceWriter = TestUtils.getVeniceWriterFactory(properties)
        .createVeniceWriter(
            new VeniceWriterOptions.Builder(topicName).setUseKafkaKeySerializer(true)
                .setPartitionCount(partitionCount)
                .build())) {
      executorService = Executors.newFixedThreadPool(numberOfThreads);
      Future[] vwFutures = new Future[numberOfThreads];
      for (int i = 0; i < numberOfThreads; i++) {
        vwFutures[i] = executorService.submit(() -> veniceWriterTask.accept(veniceWriter));
      }
      for (int i = 0; i < numberOfThreads; i++) {
        vwFutures[i].get();
      }
    } finally {
      TestUtils.shutdownExecutor(executorService);
    }
    KafkaValueSerializer kafkaValueSerializer = new OptimizedKafkaValueSerializer();
    KafkaPubSubMessageDeserializer pubSubDeserializer = new KafkaPubSubMessageDeserializer(
        kafkaValueSerializer,
        new LandFillObjectPool<>(KafkaMessageEnvelope::new),
        new LandFillObjectPool<>(KafkaMessageEnvelope::new));
    try (PubSubConsumerAdapter consumer = pubSubConsumerAdapterFactory
        .create(new VeniceProperties(properties), false, pubSubDeserializer, pubSubBrokerWrapper.getAddress())) {
      PubSubTopicPartition pubSubTopicPartition = new PubSubTopicPartitionImpl(pubSubTopic, 0);
      consumer.subscribe(pubSubTopicPartition, -1);
      int lastSeenSequenceNumber = -1;
      int lastSeenSegmentNumber = -1;
      Map<PubSubTopicPartition, List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>>> messages;
      do {
        messages = consumer.poll(10 * Time.MS_PER_SECOND);
        if (messages.containsKey(pubSubTopicPartition)) {
          for (final PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> message: messages.get(pubSubTopicPartition)) {
            ProducerMetadata producerMetadata = message.getValue().producerMetadata;
            int currentSegmentNumber = producerMetadata.segmentNumber;
            int currentSequenceNumber = producerMetadata.messageSequenceNumber;

            if (currentSegmentNumber == lastSeenSegmentNumber && currentSequenceNumber == lastSeenSequenceNumber + 1) {
              lastSeenSequenceNumber = currentSequenceNumber;
            } else if (currentSegmentNumber == lastSeenSegmentNumber + 1 && currentSequenceNumber == 0) {
              lastSeenSegmentNumber = currentSegmentNumber;
              lastSeenSequenceNumber = currentSequenceNumber;
            } else {
              Assert.fail(
                  "DIV Error caught.\n" + "Last segment Number: " + lastSeenSegmentNumber + ". Current segment number: "
                      + currentSegmentNumber + ".\n" + "Last sequence Number: " + lastSeenSequenceNumber
                      + ". Current sequence number: " + currentSequenceNumber + ".");
            }
          }
        }
      } while (!messages.isEmpty());
    }
  }

  @Test(invocationCount = 3)
  public void testThreadSafetyForPutMessages() throws ExecutionException, InterruptedException {
    testThreadSafety(
        100,
        veniceWriter -> veniceWriter.put(new KafkaKey(MessageType.PUT, "blah".getBytes()), "blah".getBytes(), 1, null));
  }

  @Test
  public void testCloseSegmentBasedOnElapsedTime() throws InterruptedException, ExecutionException, TimeoutException {
    PubSubProducerAdapter mockedProducer = mock(PubSubProducerAdapter.class);
    Future mockedFuture = mock(Future.class);
    when(mockedProducer.getNumberOfPartitions(any())).thenReturn(1);
    when(mockedProducer.getNumberOfPartitions(any(), anyInt(), any())).thenReturn(1);
    when(mockedProducer.sendMessage(any(), any(), any(), any(), any(), any())).thenReturn(mockedFuture);
    Properties writerProperties = new Properties();
    writerProperties.put(VeniceWriter.MAX_ELAPSED_TIME_FOR_SEGMENT_IN_MS, 0);
    String stringSchema = "\"string\"";
    VeniceKafkaSerializer serializer = new VeniceAvroKafkaSerializer(stringSchema);
    String testTopic = "test";
    VeniceWriterOptions veniceWriterOptions = new VeniceWriterOptions.Builder(testTopic).setKeySerializer(serializer)
        .setValueSerializer(serializer)
        .setWriteComputeSerializer(serializer)
        .setPartitioner(new DefaultVenicePartitioner())
        .setTime(SystemTime.INSTANCE)
        .build();
    VeniceWriter<Object, Object, Object> writer =
        new VeniceWriter(veniceWriterOptions, new VeniceProperties(writerProperties), mockedProducer);
    for (int i = 0; i < 1000; i++) {
      writer.put(Integer.toString(i), Integer.toString(i), 1, null);
    }
    ArgumentCaptor<KafkaMessageEnvelope> kmeArgumentCaptor = ArgumentCaptor.forClass(KafkaMessageEnvelope.class);
    verify(mockedProducer, atLeast(1000)).sendMessage(any(), any(), any(), kmeArgumentCaptor.capture(), any(), any());
    int segmentNumber = -1;
    for (KafkaMessageEnvelope envelope: kmeArgumentCaptor.getAllValues()) {
      if (segmentNumber == -1) {
        segmentNumber = envelope.producerMetadata.segmentNumber;
      } else {
        // Segment number should not change since we disabled closing segment based on elapsed time.
        Assert.assertEquals(envelope.producerMetadata.segmentNumber, segmentNumber);
      }
    }
  }

  @Test
  public void testReplicationMetadataWrittenCorrectly()
      throws InterruptedException, ExecutionException, TimeoutException {
    PubSubProducerAdapter mockedProducer = mock(PubSubProducerAdapter.class);
    Future mockedFuture = mock(Future.class);
    when(mockedProducer.getNumberOfPartitions(any())).thenReturn(1);
    when(mockedProducer.getNumberOfPartitions(any(), anyInt(), any())).thenReturn(1);
    when(mockedProducer.sendMessage(any(), any(), any(), any(), any(), any())).thenReturn(mockedFuture);
    Properties writerProperties = new Properties();
    String stringSchema = "\"string\"";
    VeniceKafkaSerializer serializer = new VeniceAvroKafkaSerializer(stringSchema);
    String testTopic = "test";
    VeniceWriterOptions veniceWriterOptions = new VeniceWriterOptions.Builder(testTopic).setKeySerializer(serializer)
        .setValueSerializer(serializer)
        .setWriteComputeSerializer(serializer)
        .setPartitioner(new DefaultVenicePartitioner())
        .setTime(SystemTime.INSTANCE)
        .build();
    VeniceWriter<Object, Object, Object> writer =
        new VeniceWriter(veniceWriterOptions, new VeniceProperties(writerProperties), mockedProducer);

    // verify the new veniceWriter API's are able to encode the A/A metadat info correctly.
    long ctime = System.currentTimeMillis();
    ByteBuffer replicationMetadata = ByteBuffer.wrap(new byte[] { 0xa, 0xb });
    PutMetadata putMetadata = new PutMetadata(1, replicationMetadata);
    DeleteMetadata deleteMetadata = new DeleteMetadata(1, 1, replicationMetadata);

    writer.put(
        Integer.toString(1),
        Integer.toString(1),
        1,
        null,
        VeniceWriter.DEFAULT_LEADER_METADATA_WRAPPER,
        ctime,
        null);
    writer.put(
        Integer.toString(2),
        Integer.toString(2),
        1,
        null,
        VeniceWriter.DEFAULT_LEADER_METADATA_WRAPPER,
        APP_DEFAULT_LOGICAL_TS,
        putMetadata);
    writer.update(Integer.toString(3), Integer.toString(2), 1, 1, null, ctime);
    writer.delete(Integer.toString(4), null, VeniceWriter.DEFAULT_LEADER_METADATA_WRAPPER, ctime);
    writer.delete(Integer.toString(5), null, VeniceWriter.DEFAULT_LEADER_METADATA_WRAPPER, deleteMetadata);
    writer.put(Integer.toString(6), Integer.toString(1), 1, null, VeniceWriter.DEFAULT_LEADER_METADATA_WRAPPER);

    ArgumentCaptor<KafkaMessageEnvelope> kmeArgumentCaptor = ArgumentCaptor.forClass(KafkaMessageEnvelope.class);
    verify(mockedProducer, atLeast(2)).sendMessage(any(), any(), any(), kmeArgumentCaptor.capture(), any(), any());

    // first one will be control message SOS, there should not be any aa metadata.
    KafkaMessageEnvelope value0 = kmeArgumentCaptor.getAllValues().get(0);
    Assert.assertEquals(value0.producerMetadata.logicalTimestamp, VENICE_DEFAULT_LOGICAL_TS);

    // verify timestamp is encoded correctly.
    KafkaMessageEnvelope value1 = kmeArgumentCaptor.getAllValues().get(1);
    KafkaMessageEnvelope value3 = kmeArgumentCaptor.getAllValues().get(3);
    KafkaMessageEnvelope value4 = kmeArgumentCaptor.getAllValues().get(4);
    for (KafkaMessageEnvelope kme: Arrays.asList(value1, value3, value4)) {
      Assert.assertEquals(kme.producerMetadata.logicalTimestamp, ctime);
    }

    // verify default values for replicationMetadata are written correctly
    Put put = (Put) value1.payloadUnion;
    Assert.assertEquals(put.schemaId, 1);
    Assert.assertEquals(put.replicationMetadataVersionId, VeniceWriter.VENICE_DEFAULT_TIMESTAMP_METADATA_VERSION_ID);
    Assert.assertEquals(put.replicationMetadataPayload, ByteBuffer.wrap(new byte[0]));

    Delete delete = (Delete) value4.payloadUnion;
    Assert.assertEquals(delete.schemaId, VeniceWriter.VENICE_DEFAULT_VALUE_SCHEMA_ID);
    Assert.assertEquals(delete.replicationMetadataVersionId, VeniceWriter.VENICE_DEFAULT_TIMESTAMP_METADATA_VERSION_ID);
    Assert.assertEquals(delete.replicationMetadataPayload, ByteBuffer.wrap(new byte[0]));

    // verify replicationMetadata is encoded correctly for Put.
    KafkaMessageEnvelope value2 = kmeArgumentCaptor.getAllValues().get(2);
    Assert.assertEquals(value2.messageType, MessageType.PUT.getValue());
    put = (Put) value2.payloadUnion;
    Assert.assertEquals(put.schemaId, 1);
    Assert.assertEquals(put.replicationMetadataVersionId, 1);
    Assert.assertEquals(put.replicationMetadataPayload, ByteBuffer.wrap(new byte[] { 0xa, 0xb }));
    Assert.assertEquals(value2.producerMetadata.logicalTimestamp, APP_DEFAULT_LOGICAL_TS);

    // verify replicationMetadata is encoded correctly for Delete.
    KafkaMessageEnvelope value5 = kmeArgumentCaptor.getAllValues().get(5);
    Assert.assertEquals(value5.messageType, MessageType.DELETE.getValue());
    delete = (Delete) value5.payloadUnion;
    Assert.assertEquals(delete.schemaId, 1);
    Assert.assertEquals(delete.replicationMetadataVersionId, 1);
    Assert.assertEquals(delete.replicationMetadataPayload, ByteBuffer.wrap(new byte[] { 0xa, 0xb }));
    Assert.assertEquals(value5.producerMetadata.logicalTimestamp, APP_DEFAULT_LOGICAL_TS);

    // verify default logical_ts is encoded correctly
    KafkaMessageEnvelope value6 = kmeArgumentCaptor.getAllValues().get(6);
    Assert.assertEquals(value6.messageType, MessageType.PUT.getValue());
    Assert.assertEquals(value6.producerMetadata.logicalTimestamp, APP_DEFAULT_LOGICAL_TS);
  }

  @Test(timeOut = 10000)
  public void testReplicationMetadataChunking() throws ExecutionException, InterruptedException, TimeoutException {
    PubSubProducerAdapter mockedProducer = mock(PubSubProducerAdapter.class);
    Future mockedFuture = mock(Future.class);
    when(mockedProducer.getNumberOfPartitions(any())).thenReturn(1);
    when(mockedProducer.getNumberOfPartitions(any(), anyInt(), any())).thenReturn(1);
    when(mockedProducer.sendMessage(any(), any(), any(), any(), any(), any())).thenReturn(mockedFuture);
    String stringSchema = "\"string\"";
    VeniceKafkaSerializer serializer = new VeniceAvroKafkaSerializer(stringSchema);
    String testTopic = "test";
    VeniceWriterOptions veniceWriterOptions = new VeniceWriterOptions.Builder(testTopic).setKeySerializer(serializer)
        .setValueSerializer(serializer)
        .setWriteComputeSerializer(serializer)
        .setPartitioner(new DefaultVenicePartitioner())
        .setTime(SystemTime.INSTANCE)
        .setChunkingEnabled(true)
        .setRmdChunkingEnabled(true)
        .build();
    VeniceWriter<Object, Object, Object> writer =
        new VeniceWriter(veniceWriterOptions, VeniceProperties.empty(), mockedProducer);

    ByteBuffer replicationMetadata = ByteBuffer.wrap(new byte[] { 0xa, 0xb });
    PutMetadata putMetadata = new PutMetadata(1, replicationMetadata);

    StringBuilder stringBuilder = new StringBuilder();
    for (int i = 0; i < 50000; i++) {
      stringBuilder.append("abcdefghabcdefghabcdefghabcdefgh");
    }
    String valueString = stringBuilder.toString();

    writer.put(
        Integer.toString(1),
        valueString,
        1,
        null,
        VeniceWriter.DEFAULT_LEADER_METADATA_WRAPPER,
        APP_DEFAULT_LOGICAL_TS,
        putMetadata);
    ArgumentCaptor<KafkaKey> keyArgumentCaptor = ArgumentCaptor.forClass(KafkaKey.class);
    ArgumentCaptor<KafkaMessageEnvelope> kmeArgumentCaptor = ArgumentCaptor.forClass(KafkaMessageEnvelope.class);
    verify(mockedProducer, atLeast(2))
        .sendMessage(any(), any(), keyArgumentCaptor.capture(), kmeArgumentCaptor.capture(), any(), any());
    KeyWithChunkingSuffixSerializer keyWithChunkingSuffixSerializer = new KeyWithChunkingSuffixSerializer();
    byte[] serializedKey = serializer.serialize(testTopic, Integer.toString(1));
    byte[] serializedValue = serializer.serialize(testTopic, valueString);
    byte[] serializedRmd = replicationMetadata.array();
    int availableMessageSize = DEFAULT_MAX_SIZE_FOR_USER_PAYLOAD_PER_MESSAGE_IN_BYTES - serializedKey.length;

    // The order should be SOS, valueChunk1, valueChunk2, replicationMetadataChunk1, manifest for value and RMD.
    Assert.assertEquals(kmeArgumentCaptor.getAllValues().size(), 5);

    // Verify value of the 1st chunk.
    KafkaMessageEnvelope actualValue1 = kmeArgumentCaptor.getAllValues().get(1);
    Assert.assertEquals(actualValue1.messageType, MessageType.PUT.getValue());
    Assert.assertEquals(((Put) actualValue1.payloadUnion).schemaId, -10);
    Assert.assertEquals(((Put) actualValue1.payloadUnion).replicationMetadataVersionId, -1);
    Assert.assertEquals(((Put) actualValue1.payloadUnion).replicationMetadataPayload, ByteBuffer.allocate(0));
    Assert.assertEquals(((Put) actualValue1.payloadUnion).putValue.array().length, availableMessageSize + 4);
    Assert.assertEquals(actualValue1.producerMetadata.logicalTimestamp, VENICE_DEFAULT_LOGICAL_TS);

    // Verify value of the 2nd chunk.
    KafkaMessageEnvelope actualValue2 = kmeArgumentCaptor.getAllValues().get(2);
    Assert.assertEquals(actualValue2.messageType, MessageType.PUT.getValue());
    Assert.assertEquals(((Put) actualValue2.payloadUnion).schemaId, -10);
    Assert.assertEquals(((Put) actualValue2.payloadUnion).replicationMetadataVersionId, -1);
    Assert.assertEquals(((Put) actualValue2.payloadUnion).replicationMetadataPayload, ByteBuffer.allocate(0));
    Assert.assertEquals(
        ((Put) actualValue2.payloadUnion).putValue.array().length,
        (serializedValue.length - availableMessageSize) + 4);
    Assert.assertEquals(actualValue2.producerMetadata.logicalTimestamp, VENICE_DEFAULT_LOGICAL_TS);

    ChunkedValueManifestSerializer chunkedValueManifestSerializer = new ChunkedValueManifestSerializer(true);

    final ChunkedValueManifest chunkedValueManifest = new ChunkedValueManifest();
    chunkedValueManifest.schemaId = 1;
    chunkedValueManifest.keysWithChunkIdSuffix = new ArrayList<>(2);
    chunkedValueManifest.size = serializedValue.length;

    // Verify key of the 1st value chunk.
    ChunkedKeySuffix chunkedKeySuffix = new ChunkedKeySuffix();
    chunkedKeySuffix.isChunk = true;
    chunkedKeySuffix.chunkId = new ChunkId();
    chunkedKeySuffix.chunkId.chunkIndex = 0;
    ProducerMetadata producerMetadata = actualValue1.producerMetadata;
    chunkedKeySuffix.chunkId.producerGUID = producerMetadata.producerGUID;
    chunkedKeySuffix.chunkId.segmentNumber = producerMetadata.segmentNumber;
    chunkedKeySuffix.chunkId.messageSequenceNumber = producerMetadata.messageSequenceNumber;

    ByteBuffer keyWithSuffix = keyWithChunkingSuffixSerializer.serializeChunkedKey(serializedKey, chunkedKeySuffix);
    chunkedValueManifest.keysWithChunkIdSuffix.add(keyWithSuffix);
    KafkaKey expectedKey1 = new KafkaKey(MessageType.PUT, keyWithSuffix.array());
    KafkaKey actualKey1 = keyArgumentCaptor.getAllValues().get(1);
    Assert.assertEquals(actualKey1.getKey(), expectedKey1.getKey());

    // Verify key of the 2nd value chunk.
    chunkedKeySuffix.chunkId.chunkIndex = 1;
    keyWithSuffix = keyWithChunkingSuffixSerializer.serializeChunkedKey(serializedKey, chunkedKeySuffix);
    chunkedValueManifest.keysWithChunkIdSuffix.add(keyWithSuffix);
    KafkaKey expectedKey2 = new KafkaKey(MessageType.PUT, keyWithSuffix.array());
    KafkaKey actualKey2 = keyArgumentCaptor.getAllValues().get(2);
    Assert.assertEquals(actualKey2.getKey(), expectedKey2.getKey());

    // Check value of the 1st RMD chunk.
    KafkaMessageEnvelope actualValue3 = kmeArgumentCaptor.getAllValues().get(3);
    Assert.assertEquals(actualValue3.messageType, MessageType.PUT.getValue());
    Assert.assertEquals(((Put) actualValue3.payloadUnion).schemaId, -10);
    Assert.assertEquals(((Put) actualValue3.payloadUnion).replicationMetadataVersionId, -1);
    Assert.assertEquals(((Put) actualValue3.payloadUnion).putValue, ByteBuffer.allocate(0));
    Assert.assertEquals(
        ((Put) actualValue3.payloadUnion).replicationMetadataPayload.array().length,
        serializedRmd.length + 4);
    Assert.assertEquals(actualValue3.producerMetadata.logicalTimestamp, VENICE_DEFAULT_LOGICAL_TS);

    // Check key of the 1st RMD chunk.
    ChunkedValueManifest chunkedRmdManifest = new ChunkedValueManifest();
    chunkedRmdManifest.schemaId = 1;
    chunkedRmdManifest.keysWithChunkIdSuffix = new ArrayList<>(1);
    chunkedRmdManifest.size = serializedRmd.length;
    chunkedKeySuffix = new ChunkedKeySuffix();
    chunkedKeySuffix.isChunk = true;
    chunkedKeySuffix.chunkId = new ChunkId();
    producerMetadata = actualValue3.producerMetadata;
    chunkedKeySuffix.chunkId.producerGUID = producerMetadata.producerGUID;
    chunkedKeySuffix.chunkId.segmentNumber = producerMetadata.segmentNumber;
    chunkedKeySuffix.chunkId.messageSequenceNumber = producerMetadata.messageSequenceNumber;
    // The chunkIndex of the first RMD should be the number of value chunks so that key space of value chunk and RMD
    // chunk will not collide.
    chunkedKeySuffix.chunkId.chunkIndex = 2;
    keyWithSuffix = keyWithChunkingSuffixSerializer.serializeChunkedKey(serializedKey, chunkedKeySuffix);
    chunkedRmdManifest.keysWithChunkIdSuffix.add(keyWithSuffix);
    KafkaKey expectedKey3 = new KafkaKey(MessageType.PUT, keyWithSuffix.array());
    KafkaKey actualKey3 = keyArgumentCaptor.getAllValues().get(3);
    Assert.assertEquals(actualKey3.getKey(), expectedKey3.getKey());

    // Check key of the manifest.
    byte[] topLevelKey = keyWithChunkingSuffixSerializer.serializeNonChunkedKey(serializedKey);
    KafkaKey expectedKey4 = new KafkaKey(MessageType.PUT, topLevelKey);
    KafkaKey actualKey4 = keyArgumentCaptor.getAllValues().get(4);
    Assert.assertEquals(actualKey4.getKey(), expectedKey4.getKey());

    // Check manifest for both value and rmd.
    KafkaMessageEnvelope actualValue4 = kmeArgumentCaptor.getAllValues().get(4);
    Assert.assertEquals(actualValue4.messageType, MessageType.PUT.getValue());
    Assert.assertEquals(
        ((Put) actualValue4.payloadUnion).schemaId,
        AvroProtocolDefinition.CHUNKED_VALUE_MANIFEST.getCurrentProtocolVersion());
    Assert.assertEquals(((Put) actualValue4.payloadUnion).replicationMetadataVersionId, putMetadata.getRmdVersionId());
    Assert.assertEquals(
        ((Put) actualValue4.payloadUnion).replicationMetadataPayload,
        ByteBuffer.wrap(chunkedValueManifestSerializer.serialize(testTopic, chunkedRmdManifest)));
    Assert.assertEquals(
        ((Put) actualValue4.payloadUnion).putValue,
        ByteBuffer.wrap(chunkedValueManifestSerializer.serialize(testTopic, chunkedValueManifest)));
    Assert.assertEquals(actualValue4.producerMetadata.logicalTimestamp, APP_DEFAULT_LOGICAL_TS);

  }
}
