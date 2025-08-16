package com.linkedin.venice.hadoop.input.kafka;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import com.github.luben.zstd.ZstdDictTrainer;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.compression.NoopCompressor;
import com.linkedin.venice.compression.VeniceCompressor;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.input.kafka.avro.KafkaInputMapperKey;
import com.linkedin.venice.hadoop.input.kafka.avro.KafkaInputMapperValue;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.adapter.kafka.common.ApacheKafkaOffsetPosition;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.vpj.pubsub.input.PubSubPartitionSplit;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.testng.annotations.Test;


public class TestKafkaInputDictTrainer {
  private final static PubSubTopicRepository PUB_SUB_TOPIC_REPOSITORY = new PubSubTopicRepository();

  private KafkaInputDictTrainer.CompressorBuilder getCompressorBuilder(VeniceCompressor mockCompressor) {
    return (compressorFactory, compressionStrategy, kafkaUrl, topic, props) -> mockCompressor;
  }

  private KafkaInputDictTrainer.Param getParam(int sampleSize) {
    return getParam(sampleSize, CompressionStrategy.NO_OP);
  }

  private KafkaInputDictTrainer.Param getParam(int sampleSize, CompressionStrategy sourceVersionCompressionStrategy) {
    return new KafkaInputDictTrainer.ParamBuilder().setKafkaInputBroker("test_url")
        .setTopicName("test_topic")
        .setKeySchema("\"string\"")
        .setCompressionDictSize(900 * 1024)
        .setDictSampleSize(sampleSize)
        .setSslProperties(new Properties())
        .setSourceVersionCompressionStrategy(sourceVersionCompressionStrategy)
        .build();
  }

  @Test(expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = "No record.*")
  public void testEmptyTopic() throws IOException {
    KafkaInputFormat mockFormat = mock(KafkaInputFormat.class);
    PubSubTopicPartition topicPartition =
        new PubSubTopicPartitionImpl(PUB_SUB_TOPIC_REPOSITORY.getTopic("test_topic"), 0);
    PubSubPosition position0 = ApacheKafkaOffsetPosition.of(0);
    InputSplit[] splits = new KafkaInputSplit[] { new KafkaInputSplit(
        new PubSubPartitionSplit(PUB_SUB_TOPIC_REPOSITORY, topicPartition, position0, position0, 0L, 0, 0L)) };
    doReturn(splits).when(mockFormat).getSplits(any(VeniceProperties.class));
    RecordReader<KafkaInputMapperKey, KafkaInputMapperValue> mockRecordReader = mock(RecordReader.class);
    doReturn(false).when(mockRecordReader).next(any(), any());
    doReturn(mockRecordReader).when(mockFormat).getRecordReader(any(), any(), any(), any());

    KafkaInputDictTrainer trainer = new KafkaInputDictTrainer(
        mockFormat,
        Optional.empty(),
        getParam(100),
        getCompressorBuilder(new NoopCompressor()));
    trainer.trainDict(Optional.of(mock(PubSubConsumerAdapter.class)));
  }

  interface ResettableRecordReader<K, V> extends RecordReader<K, V> {
    void reset();
  }

  private static class ValueSchemaPair {
    int schemaId;
    byte[] value;

    public ValueSchemaPair(int schemaId, byte[] value) {
      this.schemaId = schemaId;
      this.value = value;
    }
  }

  private ResettableRecordReader<KafkaInputMapperKey, KafkaInputMapperValue> mockReaderForValueSchemaPairs(
      List<ValueSchemaPair> values) {
    ResettableRecordReader<KafkaInputMapperKey, KafkaInputMapperValue> mockRecordReader =
        new ResettableRecordReader<KafkaInputMapperKey, KafkaInputMapperValue>() {
          @Override
          public void reset() {
            cur = 0;
          }

          int cur = 0;

          @Override
          public boolean next(KafkaInputMapperKey key, KafkaInputMapperValue value) throws IOException {
            if (values.size() <= cur) {
              return false;
            }
            key.offset = cur;
            key.key = ByteBuffer.wrap(("test_key" + cur).getBytes());
            value.offset = cur;
            value.value = ByteBuffer.wrap(values.get(cur).value);
            value.schemaId = values.get(cur).schemaId;
            ++cur;
            return true;
          }

          @Override
          public KafkaInputMapperKey createKey() {
            return new KafkaInputMapperKey();
          }

          @Override
          public KafkaInputMapperValue createValue() {
            return new KafkaInputMapperValue();
          }

          @Override
          public long getPos() throws IOException {
            return 0;
          }

          @Override
          public void close() throws IOException {

          }

          @Override
          public float getProgress() throws IOException {
            return 0;
          }
        };

    return mockRecordReader;
  }

  private ResettableRecordReader<KafkaInputMapperKey, KafkaInputMapperValue> mockReader(List<byte[]> values) {
    List<ValueSchemaPair> valueSchemaPairs = new ArrayList<>();
    values.forEach(v -> valueSchemaPairs.add(new ValueSchemaPair(1, v)));
    return mockReaderForValueSchemaPairs(valueSchemaPairs);
  }

  @Test
  public void testSamplingFromMultiplePartitions() throws IOException {
    KafkaInputFormat mockFormat = mock(KafkaInputFormat.class);
    PubSubTopicPartition topicPartition =
        new PubSubTopicPartitionImpl(PUB_SUB_TOPIC_REPOSITORY.getTopic("test_topic"), 0);
    PubSubPosition startPosition = ApacheKafkaOffsetPosition.of(0);
    PubSubPosition endPosition = ApacheKafkaOffsetPosition.of(2);
    InputSplit[] splits = new KafkaInputSplit[] {
        new KafkaInputSplit(
            new PubSubPartitionSplit(PUB_SUB_TOPIC_REPOSITORY, topicPartition, startPosition, endPosition, 2L, 0, 0L)),
        new KafkaInputSplit(
            new PubSubPartitionSplit(
                PUB_SUB_TOPIC_REPOSITORY,
                topicPartition,
                startPosition,
                endPosition,
                2L,
                1,
                2L)) };
    doReturn(splits).when(mockFormat).getSplits(any(VeniceProperties.class));

    // Return 3 records
    ResettableRecordReader<KafkaInputMapperKey, KafkaInputMapperValue> readerForP0 =
        mockReader(Arrays.asList("p0_value0".getBytes(), "p0_value1".getBytes(), "p0_value2".getBytes()));

    ResettableRecordReader<KafkaInputMapperKey, KafkaInputMapperValue> readerForP1 =
        mockReader(Arrays.asList("p1_value0".getBytes(), "p1_value1".getBytes(), "p1_value2".getBytes()));

    doReturn(readerForP0).when(mockFormat).getRecordReader(eq(splits[0]), any(), any(), any());
    doReturn(readerForP1).when(mockFormat).getRecordReader(eq(splits[1]), any(), any(), any());

    // Big sampling will collect every record.
    ZstdDictTrainer mockTrainer1 = mock(ZstdDictTrainer.class);
    KafkaInputDictTrainer trainer1 = new KafkaInputDictTrainer(
        mockFormat,
        Optional.of(mockTrainer1),
        getParam(1000),
        getCompressorBuilder(new NoopCompressor()));
    trainer1.trainDict(Optional.of(mock(PubSubConsumerAdapter.class)));

    verify(mockTrainer1).addSample(eq("p0_value0".getBytes()));
    verify(mockTrainer1).addSample(eq("p0_value1".getBytes()));
    verify(mockTrainer1).addSample(eq("p0_value2".getBytes()));
    verify(mockTrainer1).addSample(eq("p1_value0".getBytes()));
    verify(mockTrainer1).addSample(eq("p1_value1".getBytes()));
    verify(mockTrainer1).addSample(eq("p1_value2".getBytes()));

    // Small sampling will collect the first several records
    readerForP0.reset();
    readerForP1.reset();
    ZstdDictTrainer mockTrainer2 = mock(ZstdDictTrainer.class);
    KafkaInputDictTrainer trainer2 = new KafkaInputDictTrainer(
        mockFormat,
        Optional.of(mockTrainer2),
        getParam(20),
        getCompressorBuilder(new NoopCompressor()));
    trainer2.trainDict(Optional.of(mock(PubSubConsumerAdapter.class)));
    verify(mockTrainer2).addSample(eq("p0_value0".getBytes()));
    verify(mockTrainer2, never()).addSample(eq("p0_value1".getBytes()));
    verify(mockTrainer2, never()).addSample(eq("p0_value2".getBytes()));
    verify(mockTrainer2).addSample(eq("p1_value0".getBytes()));
    verify(mockTrainer2, never()).addSample(eq("p1_value1".getBytes()));
    verify(mockTrainer2, never()).addSample(eq("p1_value2".getBytes()));
  }

  @Test
  public void testSamplingFromMultiplePartitionsWithSourceVersionCompressionEnabled() throws IOException {
    KafkaInputFormat mockFormat = mock(KafkaInputFormat.class);
    PubSubTopicPartition topicPartition =
        new PubSubTopicPartitionImpl(PUB_SUB_TOPIC_REPOSITORY.getTopic("test_topic"), 0);
    PubSubPosition startPosition = ApacheKafkaOffsetPosition.of(0);
    PubSubPosition endPosition = ApacheKafkaOffsetPosition.of(2);
    InputSplit[] splits = new KafkaInputSplit[] {
        new KafkaInputSplit(
            new PubSubPartitionSplit(PUB_SUB_TOPIC_REPOSITORY, topicPartition, startPosition, endPosition, 2L, 0, 0L)),
        new KafkaInputSplit(
            new PubSubPartitionSplit(
                PUB_SUB_TOPIC_REPOSITORY,
                topicPartition,
                startPosition,
                endPosition,
                2L,
                1,
                2L)) };
    doReturn(splits).when(mockFormat).getSplits(any(VeniceProperties.class));

    // Return 3 records
    ResettableRecordReader<KafkaInputMapperKey, KafkaInputMapperValue> readerForP0 =
        mockReader(Arrays.asList("p0_value0".getBytes(), "p0_value1".getBytes(), "p0_value2".getBytes()));

    ResettableRecordReader<KafkaInputMapperKey, KafkaInputMapperValue> readerForP1 =
        mockReader(Arrays.asList("p1_value0".getBytes(), "p1_value1".getBytes(), "p1_value2".getBytes()));

    doReturn(readerForP0).when(mockFormat).getRecordReader(eq(splits[0]), any(), any(), any());
    doReturn(readerForP1).when(mockFormat).getRecordReader(eq(splits[1]), any(), any(), any());

    VeniceCompressor mockedCompressor = mock(VeniceCompressor.class);
    doReturn(CompressionStrategy.GZIP).when(mockedCompressor).getCompressionStrategy();
    // Just return whatever it receives
    doAnswer(invocation -> invocation.getArgument(0)).when(mockedCompressor).decompress(any(ByteBuffer.class));

    // Big sampling will collect every record.
    ZstdDictTrainer mockTrainer1 = mock(ZstdDictTrainer.class);
    KafkaInputDictTrainer trainer1 = new KafkaInputDictTrainer(
        mockFormat,
        Optional.of(mockTrainer1),
        getParam(1000, CompressionStrategy.GZIP),
        getCompressorBuilder(mockedCompressor));
    trainer1.trainDict(Optional.of(mock(PubSubConsumerAdapter.class)));

    List<byte[]> allValues = new ArrayList<>(
        Arrays.asList(
            "p0_value0".getBytes(),
            "p0_value1".getBytes(),
            "p0_value2".getBytes(),
            "p1_value0".getBytes(),
            "p1_value1".getBytes(),
            "p1_value2".getBytes()));

    allValues.forEach(v -> {
      try {
        verify(mockedCompressor).decompress(eq(ByteBuffer.wrap(v)));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      verify(mockTrainer1).addSample(eq(v));
    });
  }

  @Test
  public void testSamplingFromMultiplePartitionsWithSourceVersionCompressionEnabledWithChunking() throws IOException {
    KafkaInputFormat mockFormat = mock(KafkaInputFormat.class);
    PubSubTopicPartition topicPartition =
        new PubSubTopicPartitionImpl(PUB_SUB_TOPIC_REPOSITORY.getTopic("test_topic"), 0);
    PubSubPosition startPosition = ApacheKafkaOffsetPosition.of(0);
    PubSubPosition endPosition = ApacheKafkaOffsetPosition.of(2);
    InputSplit[] splits = new KafkaInputSplit[] {
        new KafkaInputSplit(
            new PubSubPartitionSplit(PUB_SUB_TOPIC_REPOSITORY, topicPartition, startPosition, endPosition, 2L, 0, 0L)),
        new KafkaInputSplit(
            new PubSubPartitionSplit(
                PUB_SUB_TOPIC_REPOSITORY,
                topicPartition,
                startPosition,
                endPosition,
                2L,
                1,
                2L)) };
    doReturn(splits).when(mockFormat).getSplits(any(VeniceProperties.class));

    // Return 3 records
    ResettableRecordReader<KafkaInputMapperKey, KafkaInputMapperValue> readerForP0 = mockReaderForValueSchemaPairs(
        Arrays.asList(
            new ValueSchemaPair(-1, "p0_value0".getBytes()),
            new ValueSchemaPair(1, "p0_value1".getBytes()),
            new ValueSchemaPair(1, "p0_value2".getBytes())));

    ResettableRecordReader<KafkaInputMapperKey, KafkaInputMapperValue> readerForP1 =
        mockReader(Arrays.asList("p1_value0".getBytes(), "p1_value1".getBytes(), "p1_value2".getBytes()));

    doReturn(readerForP0).when(mockFormat).getRecordReader(eq(splits[0]), any(), any(), any());
    doReturn(readerForP1).when(mockFormat).getRecordReader(eq(splits[1]), any(), any(), any());

    VeniceCompressor mockedCompressor = mock(VeniceCompressor.class);
    doReturn(CompressionStrategy.GZIP).when(mockedCompressor).getCompressionStrategy();
    // Just return whatever it receives
    doAnswer(invocation -> invocation.getArgument(0)).when(mockedCompressor).decompress(any(ByteBuffer.class));

    // Big sampling will collect every record.
    ZstdDictTrainer mockTrainer1 = mock(ZstdDictTrainer.class);
    KafkaInputDictTrainer trainer1 = new KafkaInputDictTrainer(
        mockFormat,
        Optional.of(mockTrainer1),
        getParam(1000, CompressionStrategy.GZIP),
        getCompressorBuilder(mockedCompressor));
    trainer1.trainDict(Optional.of(mock(PubSubConsumerAdapter.class)));

    List<byte[]> allValues = new ArrayList<>(
        Arrays.asList(
            "p0_value1".getBytes(),
            "p0_value2".getBytes(),
            "p1_value0".getBytes(),
            "p1_value1".getBytes(),
            "p1_value2".getBytes()));

    allValues.forEach(v -> {
      try {
        verify(mockedCompressor).decompress(eq(ByteBuffer.wrap(v)));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      verify(mockTrainer1).addSample(eq(v));
    });
    verify(mockedCompressor, never()).decompress(eq(ByteBuffer.wrap("p0_value0".getBytes())));
    verify(mockTrainer1, never()).addSample(eq("p0_value0".getBytes()));
  }
}
