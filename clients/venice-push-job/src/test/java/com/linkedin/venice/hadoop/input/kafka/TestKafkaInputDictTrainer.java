package com.linkedin.venice.hadoop.input.kafka;

import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.ConfigKeys.PUBSUB_BROKER_ADDRESS;
import static com.linkedin.venice.ConfigKeys.PUBSUB_CONSUMER_ADAPTER_FACTORY_CLASS;
import static com.linkedin.venice.ConfigKeys.PUBSUB_SECURITY_PROTOCOL;
import static com.linkedin.venice.ConfigKeys.PUBSUB_TYPE_ID_TO_POSITION_CLASS_NAME_MAP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.COMPRESSION_DICTIONARY_MAX_MESSAGES;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_TOPIC;
import static com.linkedin.venice.vpj.VenicePushJobConstants.PUBSUB_INPUT_SPLIT_STRATEGY;
import static com.linkedin.venice.vpj.VenicePushJobConstants.PUB_SUB_ENCRYPTION_ENABLED;
import static com.linkedin.venice.vpj.VenicePushJobConstants.PUB_SUB_ENCRYPTION_KEY_URN;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.github.luben.zstd.ZstdDictTrainer;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.compression.NoopCompressor;
import com.linkedin.venice.compression.VeniceCompressor;
import com.linkedin.venice.compression.ZstdWithDictCompressor;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.input.kafka.avro.KafkaInputMapperKey;
import com.linkedin.venice.hadoop.input.kafka.avro.KafkaInputMapperValue;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.adapter.kafka.common.ApacheKafkaOffsetPosition;
import com.linkedin.venice.pubsub.api.DefaultPubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.vpj.PubSubEncryptionUtilsTest;
import com.linkedin.venice.vpj.pubsub.input.PartitionSplitStrategy;
import com.linkedin.venice.vpj.pubsub.input.PubSubPartitionSplit;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class TestKafkaInputDictTrainer {
  private final static PubSubTopicRepository PUB_SUB_TOPIC_REPOSITORY = new PubSubTopicRepository();

  private KafkaInputDictTrainer.CompressorBuilder getCompressorBuilder(VeniceCompressor mockCompressor) {
    return (compressorFactory, compressionStrategy, kafkaUrl, topic, props) -> mockCompressor;
  }

  private KafkaInputDictTrainer.Param getParam(int sampleSize, CompressionStrategy sourceVersionCompressionStrategy) {
    return getParam(sampleSize, sourceVersionCompressionStrategy, new Properties());
  }

  private KafkaInputDictTrainer.Param getParam(
      int sampleSize,
      CompressionStrategy sourceVersionCompressionStrategy,
      Properties consumerProperties) {
    Map<Integer, Schema> allSchemas = Utils.getAllSchemasFromResources(AvroProtocolDefinition.KAFKA_MESSAGE_ENVELOPE);
    Map<Integer, String> allSchemaStr =
        allSchemas.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().toString()));
    return new KafkaInputDictTrainer.ParamBuilder().setKafkaInputBroker("test_url")
        .setTopicName("test_topic")
        .setKeySchema("\"string\"")
        .setCompressionDictSize(900 * 1024)
        .setDictSampleSize(sampleSize)
        .setConsumerProperties(consumerProperties)
        .setSourceVersionCompressionStrategy(sourceVersionCompressionStrategy)
        .setNewKMESchemasFromController(allSchemaStr)
        .build();
  }

  @Test(dataProvider = "encryptionConfigurations", dataProviderClass = PubSubEncryptionUtilsTest.class)
  public void testConsumerPropertiesAndOwnedLookupForEmptyTopic(Boolean enabled, String keyUrn, String expectedUrn)
      throws IOException {
    KafkaInputFormat mockFormat = mock(KafkaInputFormat.class);
    doReturn(new KafkaInputSplit[] { split(0, 0, 0) }).when(mockFormat).getSplits(any(VeniceProperties.class));
    doReturn(mockReader(Collections.emptyList(), false)).when(mockFormat).getRecordReader(any(), any(), any(), any());

    Properties consumerProperties = new Properties();
    consumerProperties.setProperty(
        PUBSUB_CONSUMER_ADAPTER_FACTORY_CLASS,
        "com.linkedin.venice.pubsub.adapter.xinfra.consumer.XcConsumerAdapterFactory");
    consumerProperties.setProperty(
        PUBSUB_TYPE_ID_TO_POSITION_CLASS_NAME_MAP,
        "1:com.linkedin.venice.pubsub.adapter.xinfra.XinfraPosition");
    consumerProperties.setProperty("xc.pubsub.broker.url.to.region.name.map", "northguard:ei4");
    consumerProperties.setProperty(PUBSUB_BROKER_ADDRESS, "test_url");
    consumerProperties.setProperty(KAFKA_BOOTSTRAP_SERVERS, "test_url");
    consumerProperties.setProperty(PUBSUB_SECURITY_PROTOCOL, "SSL");
    consumerProperties.setProperty("ssl.keystore.location", "credential-keystore");
    consumerProperties.setProperty(COMPRESSION_DICTIONARY_MAX_MESSAGES, "12345");
    consumerProperties.putAll(PubSubEncryptionUtilsTest.encryptionProperties(enabled, keyUrn));

    KafkaInputDictTrainer trainer = new KafkaInputDictTrainer(
        mockFormat,
        Optional.empty(),
        getParam(100, CompressionStrategy.NO_OP, consumerProperties),
        getCompressorBuilder(new NoopCompressor()));
    PubSubConsumerAdapter suppliedConsumer = mock(PubSubConsumerAdapter.class);
    assertTrue(
        expectThrows(VeniceException.class, () -> trainer.trainDict(Optional.of(suppliedConsumer))).getMessage()
            .startsWith("No record"));

    ArgumentCaptor<VeniceProperties> consumerPropertiesCaptor = ArgumentCaptor.forClass(VeniceProperties.class);
    verify(mockFormat).getSplits(consumerPropertiesCaptor.capture());
    VeniceProperties actualConsumerProperties = consumerPropertiesCaptor.getValue();
    for (String property: Arrays.asList(
        PUBSUB_CONSUMER_ADAPTER_FACTORY_CLASS,
        PUBSUB_TYPE_ID_TO_POSITION_CLASS_NAME_MAP,
        "xc.pubsub.broker.url.to.region.name.map",
        PUBSUB_BROKER_ADDRESS,
        KAFKA_BOOTSTRAP_SERVERS,
        PUBSUB_SECURITY_PROTOCOL,
        "ssl.keystore.location")) {
      assertEquals(actualConsumerProperties.getString(property), consumerProperties.getProperty(property), property);
    }
    assertEquals(actualConsumerProperties.getLong(COMPRESSION_DICTIONARY_MAX_MESSAGES), 12345L);
    assertEquals(
        actualConsumerProperties.getString(PUBSUB_INPUT_SPLIT_STRATEGY),
        PartitionSplitStrategy.CAPPED_SINGLE_SPLIT_PER_PARTITION.name());
    assertEquals(actualConsumerProperties.getBoolean(PUB_SUB_ENCRYPTION_ENABLED), Boolean.TRUE.equals(enabled));
    assertEquals(
        actualConsumerProperties.getString(PUB_SUB_ENCRYPTION_KEY_URN, (String) null),
        Boolean.TRUE.equals(enabled) ? keyUrn : null);
    verify(suppliedConsumer, never()).close();
    assertFalse(
        consumerProperties.containsKey(KAFKA_INPUT_TOPIC),
        "Building trainer properties must not mutate the supplied consumer properties");
    KafkaInputUtilsTest.RecordingPubSubConsumerAdapterFactory.reset();
    clearInvocations(mockFormat);
    KafkaInputDictTrainer ownedTrainer = new KafkaInputDictTrainer(
        mockFormat,
        Optional.empty(),
        getParam(100, CompressionStrategy.NO_OP, KafkaInputUtilsTest.consumerProperties(enabled, keyUrn)),
        getCompressorBuilder(new NoopCompressor()));
    boolean invalid = Boolean.TRUE.equals(enabled) && expectedUrn == null;
    VeniceException error = expectThrows(VeniceException.class, ownedTrainer::trainDict);
    assertTrue(invalid ? error.getMessage().contains("missing or blank") : error.getMessage().startsWith("No record"));
    if (invalid) {
      verify(mockFormat, never()).getSplits(any(VeniceProperties.class));
      assertNull(KafkaInputUtilsTest.RecordingPubSubConsumerAdapterFactory.getObservedContext());
      return;
    }

    Function<String, String> observedLookup =
        KafkaInputUtilsTest.RecordingPubSubConsumerAdapterFactory.getObservedContext()
            .getPubSubEncryptionKeyUrnLookup();
    assertEquals(observedLookup != null, expectedUrn != null);
    assertEquals(observedLookup == null ? null : observedLookup.apply("test_topic"), expectedUrn);
  }

  private KafkaInputSplit split(int partition, long start, long count) {
    PubSubTopicPartition topicPartition =
        new PubSubTopicPartitionImpl(PUB_SUB_TOPIC_REPOSITORY.getTopic("test_topic"), partition);
    return new KafkaInputSplit(
        new PubSubPartitionSplit(
            PUB_SUB_TOPIC_REPOSITORY,
            topicPartition,
            ApacheKafkaOffsetPosition.of(start),
            ApacheKafkaOffsetPosition.of(start + count),
            count,
            0,
            0));
  }

  private KafkaInputRecordReader mockReader(List<byte[]> values, boolean firstValueIsChunk) throws IOException {
    KafkaInputRecordReader reader = mock(KafkaInputRecordReader.class);
    AtomicInteger cursor = new AtomicInteger();
    doReturn(new KafkaInputMapperKey()).when(reader).createKey();
    doReturn(new KafkaInputMapperValue()).when(reader).createValue();
    doAnswer(invocation -> (long) cursor.get() - 1).when(reader).getPos();
    doAnswer(invocation -> {
      if (cursor.get() == values.size()) {
        return false;
      }
      int index = cursor.getAndIncrement();
      KafkaInputMapperValue value = invocation.getArgument(1);
      value.value = ByteBuffer.wrap(values.get(index));
      value.schemaId = firstValueIsChunk && index == 0 ? -1 : 1;
      return true;
    }).when(reader).next(any(), any());
    return reader;
  }

  private ZstdWithDictCompressor sourceCompressor(byte[] dictionary) throws IOException {
    ZstdWithDictCompressor compressor = mock(ZstdWithDictCompressor.class);
    doReturn(dictionary).when(compressor).getDictionary();
    doReturn(CompressionStrategy.ZSTD_WITH_DICT).when(compressor).getCompressionStrategy();
    doAnswer(invocation -> invocation.getArgument(0)).when(compressor).decompress(any(ByteBuffer.class));
    return compressor;
  }

  @DataProvider
  public Object[][] samplingConfigurations() {
    return new Object[][] { { CompressionStrategy.NO_OP, false, 1000, 3 }, { CompressionStrategy.NO_OP, false, 20, 1 },
        { CompressionStrategy.GZIP, false, 1000, 3 }, { CompressionStrategy.GZIP, true, 1000, 3 } };
  }

  @Test(dataProvider = "samplingConfigurations")
  public void testSamplingAcrossPartitions(
      CompressionStrategy strategy,
      boolean firstValueIsChunk,
      int sampleBudget,
      int recordsPerPartition) throws IOException {
    KafkaInputFormat format = mock(KafkaInputFormat.class);
    KafkaInputSplit[] splits = { split(0, 0, 3), split(1, 0, 3) };
    doReturn(splits).when(format).getSplits(any(VeniceProperties.class));
    List<List<byte[]>> values = Arrays.asList(
        Arrays.asList("p0_value0".getBytes(), "p0_value1".getBytes(), "p0_value2".getBytes()),
        Arrays.asList("p1_value0".getBytes(), "p1_value1".getBytes(), "p1_value2".getBytes()));
    for (int partition = 0; partition < splits.length; partition++) {
      doReturn(mockReader(values.get(partition), firstValueIsChunk && partition == 0)).when(format)
          .getRecordReader(eq(splits[partition]), any(), any(), any());
    }
    VeniceCompressor compressor = new NoopCompressor();
    if (strategy != CompressionStrategy.NO_OP) {
      compressor = mock(VeniceCompressor.class);
      doReturn(strategy).when(compressor).getCompressionStrategy();
      doAnswer(invocation -> invocation.getArgument(0)).when(compressor).decompress(any(ByteBuffer.class));
    }
    ZstdDictTrainer samples = mock(ZstdDictTrainer.class);
    doReturn(true).when(samples).addSample(any(byte[].class));
    KafkaInputDictTrainer trainer = new KafkaInputDictTrainer(
        format,
        Optional.of(samples),
        getParam(sampleBudget, strategy),
        getCompressorBuilder(compressor));
    PubSubConsumerAdapter consumer = mock(PubSubConsumerAdapter.class);
    assertTrue(
        expectThrows(VeniceException.class, () -> trainer.trainDict(Optional.of(consumer))).getMessage()
            .startsWith("Insufficient samples"));
    for (int partition = 0; partition < splits.length; partition++) {
      for (int record = 0; record < values.get(partition).size(); record++) {
        byte[] value = values.get(partition).get(record);
        boolean chunk = firstValueIsChunk && partition == 0 && record == 0;
        verify(samples, record < recordsPerPartition && !chunk ? times(1) : never()).addSample(eq(value));
        if (strategy != CompressionStrategy.NO_OP) {
          verify(compressor, chunk ? never() : times(1)).decompress(eq(ByteBuffer.wrap(value)));
        }
      }
    }
    verify(samples, never()).trainSamples();
    verify(consumer, never()).batchUnsubscribe(any());
    verify(consumer, never()).close();
  }

  @DataProvider
  public Object[][] sampleCounts() {
    return new Object[][] { { 0, true }, { 19, true }, { 20, true }, { 20, false } };
  }

  @Test(dataProvider = "sampleCounts")
  public void testInsufficientSamplesReuseCachedSourceDictionary(int sampleCount, boolean acceptsSamples)
      throws IOException {
    KafkaInputFormat format = mock(KafkaInputFormat.class);
    doReturn(new KafkaInputSplit[] { split(0, 0, sampleCount) }).when(format).getSplits(any(VeniceProperties.class));
    doReturn(mockReader(Collections.nCopies(sampleCount, new byte[] { 1, 2, 3 }), false)).when(format)
        .getRecordReader(any(), any(), any(), any());
    byte[] sourceDictionary = new byte[] { 1, 2 };
    ZstdWithDictCompressor compressor = sourceCompressor(sourceDictionary);
    ZstdDictTrainer zstdTrainer = mock(ZstdDictTrainer.class);
    doReturn(acceptsSamples).when(zstdTrainer).addSample(any(byte[].class));
    byte[] trainedDictionary = new byte[] { 3, 4 };
    doReturn(trainedDictionary).when(zstdTrainer).trainSamples();
    KafkaInputDictTrainer trainer = new KafkaInputDictTrainer(
        format,
        Optional.of(zstdTrainer),
        getParam(1000, CompressionStrategy.ZSTD_WITH_DICT),
        getCompressorBuilder(compressor));
    PubSubConsumerAdapter consumer = mock(PubSubConsumerAdapter.class);

    boolean trainNewDictionary = sampleCount >= 20 && acceptsSamples;
    byte[] expected = trainNewDictionary ? trainedDictionary : sourceDictionary;
    assertEquals(trainer.trainDict(Optional.of(consumer)), expected);
    assertEquals(trainer.trainDict(Optional.of(consumer)), expected);
    verify(format).getSplits(any(VeniceProperties.class));
    verify(compressor).getDictionary();
    verify(zstdTrainer, trainNewDictionary ? times(1) : never()).trainSamples();
    verify(consumer, never()).close();
  }

  @Test
  public void testSourceDictionarySnapshotDoesNotExposeCompressorDictionary() throws IOException {
    byte[] dictionary = new byte[] { 1, 2, 3 };
    try (ZstdWithDictCompressor compressor = new ZstdWithDictCompressor(dictionary, 3)) {
      byte[] snapshot = compressor.getDictionary();
      snapshot[0] = 4;
      assertEquals(compressor.getDictionary(), dictionary);
    }
  }

  @Test
  public void testHeartbeatOnlyPrefixStopsBeforeDataAndReusesSourceDictionary() throws IOException {
    KafkaInputSplit split = split(0, 10, 2);
    KafkaInputFormat format = mock(KafkaInputFormat.class);
    doReturn(new KafkaInputSplit[] { split }).when(format).getSplits(any(VeniceProperties.class));
    doCallRealMethod().when(format).getRecordReader(any(), any(), any(), any());
    PubSubConsumerAdapter consumer = mock(PubSubConsumerAdapter.class);
    doReturn(Collections.emptySet()).when(consumer).getAssignment();
    doReturn(10L).when(consumer).positionDifference(any(), any(), any());
    List<DefaultPubSubMessage> messages = new ArrayList<>();
    for (int offset = 10; offset < 12; offset++) {
      DefaultPubSubMessage heartbeat = mock(DefaultPubSubMessage.class);
      KafkaKey key = mock(KafkaKey.class);
      doReturn(true).when(key).isControlMessage();
      doReturn(key).when(heartbeat).getKey();
      doReturn(ApacheKafkaOffsetPosition.of(offset)).when(heartbeat).getPosition();
      messages.add(heartbeat);
    }
    DefaultPubSubMessage beyondCap = mock(DefaultPubSubMessage.class);
    messages.add(beyondCap);
    doReturn(Collections.singletonMap(split.getTopicPartition(), messages)).when(consumer).poll(anyLong());
    byte[] dictionary = new byte[] { 1, 2 };
    ZstdWithDictCompressor compressor = sourceCompressor(dictionary);
    ZstdDictTrainer zstdTrainer = mock(ZstdDictTrainer.class);
    KafkaInputDictTrainer trainer = new KafkaInputDictTrainer(
        format,
        Optional.of(zstdTrainer),
        getParam(1000, CompressionStrategy.ZSTD_WITH_DICT),
        getCompressorBuilder(compressor));

    assertEquals(trainer.trainDict(Optional.of(consumer)), dictionary);
    verify(consumer).poll(anyLong());
    verify(beyondCap, never()).getKey();
    verify(zstdTrainer, never()).trainSamples();
    verify(consumer, never()).close();
  }
}
