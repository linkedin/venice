package com.linkedin.venice.writer;

import static com.linkedin.venice.writer.VeniceWriter.APP_DEFAULT_LOGICAL_TS;
import static com.linkedin.venice.writer.VeniceWriter.DEFAULT_MAX_SIZE_FOR_USER_PAYLOAD_PER_MESSAGE_IN_BYTES;
import static com.linkedin.venice.writer.VeniceWriter.ENABLE_CHUNKING;
import static com.linkedin.venice.writer.VeniceWriter.ENABLE_RMD_CHUNKING;
import static com.linkedin.venice.writer.VeniceWriter.VENICE_DEFAULT_LOGICAL_TS;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.integration.utils.KafkaBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.kafka.KafkaClientFactory;
import com.linkedin.venice.kafka.TopicManager;
import com.linkedin.venice.kafka.protocol.Delete;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.ProducerMetadata;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.partitioner.DefaultVenicePartitioner;
import com.linkedin.venice.serialization.KeyWithChunkingSuffixSerializer;
import com.linkedin.venice.serialization.VeniceKafkaSerializer;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.ChunkedValueManifestSerializer;
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
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.mockito.ArgumentCaptor;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


@Test
public class VeniceWriterTest {
  private KafkaBrokerWrapper kafka;
  private TopicManager topicManager;
  private KafkaClientFactory kafkaClientFactory;
  private ZkServerWrapper zkServer;

  @BeforeClass
  public void setUp() {
    zkServer = ServiceFactory.getZkServer();
    kafka = ServiceFactory.getKafkaBroker(zkServer);
    kafkaClientFactory = IntegrationTestPushUtils.getVeniceConsumerFactory(kafka);
    topicManager = new TopicManager(kafkaClientFactory);
  }

  @AfterClass
  public void cleanUp() throws IOException {
    Utils.closeQuietlyWithErrorLogged(topicManager, kafka, zkServer);
  }

  private void testThreadSafety(
      int numberOfThreads,
      java.util.function.Consumer<VeniceWriter<KafkaKey, byte[], byte[]>> veniceWriterTask)
      throws ExecutionException, InterruptedException {
    String topicName = Utils.getUniqueString("topic-for-vw-thread-safety");
    int partitionCount = 1;
    topicManager.createTopic(topicName, partitionCount, 1, true);
    Properties properties = new Properties();
    properties.put(ConfigKeys.KAFKA_BOOTSTRAP_SERVERS, kafka.getAddress());
    properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, kafka.getAddress());
    properties.put(ConfigKeys.PARTITIONER_CLASS, DefaultVenicePartitioner.class.getName());

    ExecutorService executorService = null;
    try (VeniceWriter<KafkaKey, byte[], byte[]> veniceWriter =
        TestUtils.getVeniceWriterFactory(properties).createVeniceWriter(topicName, partitionCount)) {
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

    try (Consumer<KafkaKey, KafkaMessageEnvelope> consumer = kafkaClientFactory.getRecordKafkaConsumer()) {
      List<TopicPartition> partitions = Collections.singletonList(new TopicPartition(topicName, 0));
      consumer.assign(partitions);
      consumer.seekToBeginning(partitions);
      int lastSeenSequenceNumber = -1;
      int lastSeenSegmentNumber = -1;
      ConsumerRecords<KafkaKey, KafkaMessageEnvelope> records;
      do {
        records = consumer.poll(10 * Time.MS_PER_SECOND);
        for (final ConsumerRecord<KafkaKey, KafkaMessageEnvelope> record: records) {
          ProducerMetadata producerMetadata = record.value().producerMetadata;
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
      } while (!records.isEmpty());
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
    KafkaProducerWrapper mockedProducer = mock(KafkaProducerWrapper.class);
    Future mockedFuture = mock(Future.class);
    when(mockedProducer.getNumberOfPartitions(any())).thenReturn(1);
    when(mockedProducer.getNumberOfPartitions(any(), anyInt(), any())).thenReturn(1);
    when(mockedProducer.sendMessage(any(), any())).thenReturn(mockedFuture);
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
        new VeniceWriter(veniceWriterOptions, new VeniceProperties(writerProperties), () -> mockedProducer);
    for (int i = 0; i < 1000; i++) {
      writer.put(Integer.toString(i), Integer.toString(i), 1, null);
    }
    ArgumentCaptor<ProducerRecord<KafkaKey, KafkaMessageEnvelope>> producerRecordArgumentCaptor =
        ArgumentCaptor.forClass(ProducerRecord.class);
    verify(mockedProducer, atLeast(1000)).sendMessage(producerRecordArgumentCaptor.capture(), any());
    int segmentNumber = -1;
    for (ProducerRecord<KafkaKey, KafkaMessageEnvelope> producerRecord: producerRecordArgumentCaptor.getAllValues()) {
      KafkaMessageEnvelope envelope = producerRecord.value();
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
    KafkaProducerWrapper mockedProducer = mock(KafkaProducerWrapper.class);
    Future mockedFuture = mock(Future.class);
    when(mockedProducer.getNumberOfPartitions(any())).thenReturn(1);
    when(mockedProducer.getNumberOfPartitions(any(), anyInt(), any())).thenReturn(1);
    when(mockedProducer.sendMessage(any(), any())).thenReturn(mockedFuture);
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
        new VeniceWriter(veniceWriterOptions, new VeniceProperties(writerProperties), () -> mockedProducer);

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

    ArgumentCaptor<ProducerRecord<KafkaKey, KafkaMessageEnvelope>> producerRecordArgumentCaptor =
        ArgumentCaptor.forClass(ProducerRecord.class);
    verify(mockedProducer, atLeast(2)).sendMessage(producerRecordArgumentCaptor.capture(), any());

    // first one will be control message SOS, there should not be any aa metadata.
    KafkaMessageEnvelope value0 = producerRecordArgumentCaptor.getAllValues().get(0).value();
    Assert.assertEquals(value0.producerMetadata.logicalTimestamp, VENICE_DEFAULT_LOGICAL_TS);

    // verify timestamp is encoded correctly.
    KafkaMessageEnvelope value1 = producerRecordArgumentCaptor.getAllValues().get(1).value();
    KafkaMessageEnvelope value3 = producerRecordArgumentCaptor.getAllValues().get(3).value();
    KafkaMessageEnvelope value4 = producerRecordArgumentCaptor.getAllValues().get(4).value();
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
    KafkaMessageEnvelope value2 = producerRecordArgumentCaptor.getAllValues().get(2).value();
    Assert.assertEquals(value2.messageType, MessageType.PUT.getValue());
    put = (Put) value2.payloadUnion;
    Assert.assertEquals(put.schemaId, 1);
    Assert.assertEquals(put.replicationMetadataVersionId, 1);
    Assert.assertEquals(put.replicationMetadataPayload, ByteBuffer.wrap(new byte[] { 0xa, 0xb }));
    Assert.assertEquals(value2.producerMetadata.logicalTimestamp, APP_DEFAULT_LOGICAL_TS);

    // verify replicationMetadata is encoded correctly for Delete.
    KafkaMessageEnvelope value5 = producerRecordArgumentCaptor.getAllValues().get(5).value();
    Assert.assertEquals(value5.messageType, MessageType.DELETE.getValue());
    delete = (Delete) value5.payloadUnion;
    Assert.assertEquals(delete.schemaId, 1);
    Assert.assertEquals(delete.replicationMetadataVersionId, 1);
    Assert.assertEquals(delete.replicationMetadataPayload, ByteBuffer.wrap(new byte[] { 0xa, 0xb }));
    Assert.assertEquals(value5.producerMetadata.logicalTimestamp, APP_DEFAULT_LOGICAL_TS);

    // verify default logical_ts is encoded correctly
    KafkaMessageEnvelope value6 = producerRecordArgumentCaptor.getAllValues().get(6).value();
    Assert.assertEquals(value6.messageType, MessageType.PUT.getValue());
    Assert.assertEquals(value6.producerMetadata.logicalTimestamp, APP_DEFAULT_LOGICAL_TS);
  }

  @Test(timeOut = 10000)
  public void testReplicationMetadataChunking() throws ExecutionException, InterruptedException, TimeoutException {
    KafkaProducerWrapper mockedProducer = mock(KafkaProducerWrapper.class);
    Future mockedFuture = mock(Future.class);
    when(mockedProducer.getNumberOfPartitions(any())).thenReturn(1);
    when(mockedProducer.getNumberOfPartitions(any(), anyInt(), any())).thenReturn(1);
    when(mockedProducer.sendMessage(any(), any())).thenReturn(mockedFuture);
    Properties writerProperties = new Properties();
    writerProperties.put(ENABLE_CHUNKING, true);
    writerProperties.put(ENABLE_RMD_CHUNKING, true);

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
        new VeniceWriter(veniceWriterOptions, new VeniceProperties(writerProperties), () -> mockedProducer);

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
    ArgumentCaptor<ProducerRecord<KafkaKey, KafkaMessageEnvelope>> producerRecordArgumentCaptor =
        ArgumentCaptor.forClass(ProducerRecord.class);
    verify(mockedProducer, atLeast(2)).sendMessage(producerRecordArgumentCaptor.capture(), any());
    KeyWithChunkingSuffixSerializer keyWithChunkingSuffixSerializer = new KeyWithChunkingSuffixSerializer();
    byte[] serializedKey = serializer.serialize(testTopic, Integer.toString(1));
    byte[] serializedValue = serializer.serialize(testTopic, valueString);
    byte[] serializedRmd = replicationMetadata.array();
    int availableMessageSize = DEFAULT_MAX_SIZE_FOR_USER_PAYLOAD_PER_MESSAGE_IN_BYTES - serializedKey.length;

    // The order should be SOS, valueChunk1, valueChunk2, replicationMetadataChunk1, manifest for value and RMD.
    Assert.assertEquals(producerRecordArgumentCaptor.getAllValues().size(), 5);

    // Verify value of the 1st chunk.
    KafkaMessageEnvelope actualValue1 = producerRecordArgumentCaptor.getAllValues().get(1).value();
    Assert.assertEquals(actualValue1.messageType, MessageType.PUT.getValue());
    Assert.assertEquals(((Put) actualValue1.payloadUnion).schemaId, -10);
    Assert.assertEquals(((Put) actualValue1.payloadUnion).replicationMetadataVersionId, -1);
    Assert.assertEquals(((Put) actualValue1.payloadUnion).replicationMetadataPayload, ByteBuffer.allocate(0));
    Assert.assertEquals(((Put) actualValue1.payloadUnion).putValue.array().length, availableMessageSize + 4);
    Assert.assertEquals(actualValue1.producerMetadata.logicalTimestamp, VENICE_DEFAULT_LOGICAL_TS);

    // Verify value of the 2nd chunk.
    KafkaMessageEnvelope actualValue2 = producerRecordArgumentCaptor.getAllValues().get(2).value();
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
    ProducerMetadata producerMetadata = actualValue1.producerMetadata;
    chunkedKeySuffix.chunkId.producerGUID = producerMetadata.producerGUID;
    chunkedKeySuffix.chunkId.segmentNumber = producerMetadata.segmentNumber;
    chunkedKeySuffix.chunkId.messageSequenceNumber = producerMetadata.messageSequenceNumber;

    ByteBuffer keyWithSuffix = keyWithChunkingSuffixSerializer.serializeChunkedKey(serializedKey, chunkedKeySuffix);
    chunkedValueManifest.keysWithChunkIdSuffix.add(keyWithSuffix);
    KafkaKey expectedKey1 = new KafkaKey(MessageType.PUT, keyWithSuffix.array());
    KafkaKey actualKey1 = producerRecordArgumentCaptor.getAllValues().get(1).key();
    Assert.assertEquals(actualKey1.getKey(), expectedKey1.getKey());

    // Verify key of the 2nd value chunk.
    chunkedKeySuffix.chunkId.chunkIndex = 1;
    keyWithSuffix = keyWithChunkingSuffixSerializer.serializeChunkedKey(serializedKey, chunkedKeySuffix);
    chunkedValueManifest.keysWithChunkIdSuffix.add(keyWithSuffix);
    KafkaKey expectedKey2 = new KafkaKey(MessageType.PUT, keyWithSuffix.array());
    KafkaKey actualKey2 = producerRecordArgumentCaptor.getAllValues().get(2).key();
    Assert.assertEquals(actualKey2.getKey(), expectedKey2.getKey());

    // Check value of the 1st RMD chunk.
    KafkaMessageEnvelope actualValue3 = producerRecordArgumentCaptor.getAllValues().get(3).value();
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
    keyWithSuffix = keyWithChunkingSuffixSerializer.serializeChunkedKey(serializedKey, chunkedKeySuffix);
    chunkedRmdManifest.keysWithChunkIdSuffix.add(keyWithSuffix);
    KafkaKey expectedKey3 = new KafkaKey(MessageType.PUT, keyWithSuffix.array());
    KafkaKey actualKey3 = producerRecordArgumentCaptor.getAllValues().get(3).key();
    Assert.assertEquals(actualKey3.getKey(), expectedKey3.getKey());

    // Check key of the manifest.
    byte[] topLevelKey = keyWithChunkingSuffixSerializer.serializeNonChunkedKey(serializedKey);
    KafkaKey expectedKey4 = new KafkaKey(MessageType.PUT, topLevelKey);
    KafkaKey actualKey4 = producerRecordArgumentCaptor.getAllValues().get(4).key();
    Assert.assertEquals(actualKey4.getKey(), expectedKey4.getKey());

    // Check manifest for both value and rmd.
    KafkaMessageEnvelope actualValue4 = producerRecordArgumentCaptor.getAllValues().get(4).value();
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

  @Test(timeOut = 30000)
  public void testProducerClose() {
    String topicName = Utils.getUniqueString("topic-for-vw-thread-safety");
    int partitionCount = 1;
    topicManager.createTopic(topicName, partitionCount, 1, true);
    Properties properties = new Properties();
    properties.put(ConfigKeys.KAFKA_BOOTSTRAP_SERVERS, kafka.getAddress());
    properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, kafka.getAddress());
    properties.put(ConfigKeys.PARTITIONER_CLASS, DefaultVenicePartitioner.class.getName());

    VeniceWriter<KafkaKey, byte[], byte[]> veniceWriter =
        TestUtils.getVeniceWriterFactory(properties).createVeniceWriter(topicName, partitionCount);
    KafkaProducerWrapper producer = veniceWriter.getProducer();
    ExecutorService executor = Executors.newSingleThreadExecutor();
    CountDownLatch countDownLatch = new CountDownLatch(1);

    try {
      Future future = executor.submit(() -> {
        countDownLatch.countDown();
        // send to non-existent topic
        producer.sendMessage(new ProducerRecord("topic", "key", "value"), null);
        fail("Should be blocking send");
      });

      try {
        countDownLatch.await();
        // Still wait for some time to make sure blocking sendMessage is inside kafka before closing it.
        Utils.sleep(50);
        producer.close(5000, true);
      } catch (Exception e) {
        fail("Close should be able to close.", e);
      }
      try {
        future.get();
      } catch (ExecutionException exception) {
        assertEquals(
            exception.getCause().getMessage(),
            "Got an error while trying to produce message into Kafka. Topic: 'topic', partition: null");
      } catch (Exception e) {
        fail(" Should not throw other types of exception", e);
      }
    } finally {
      executor.shutdownNow();
    }
  }
}
