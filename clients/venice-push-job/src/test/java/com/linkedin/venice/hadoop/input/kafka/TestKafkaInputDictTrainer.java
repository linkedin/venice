package com.linkedin.venice.hadoop.input.kafka;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import com.github.luben.zstd.ZstdDictTrainer;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.input.kafka.avro.KafkaInputMapperKey;
import com.linkedin.venice.hadoop.input.kafka.avro.KafkaInputMapperValue;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.testng.annotations.Test;


public class TestKafkaInputDictTrainer {
  private KafkaInputDictTrainer.Param getParam(int sampleSize) {
    return new KafkaInputDictTrainer.ParamBuilder().setKafkaInputBroker("test_url")
        .setTopicName("test_topic")
        .setKeySchema("\"string\"")
        .setCompressionDictSize(900 * 1024)
        .setDictSampleSize(sampleSize)
        .setSslProperties(new Properties())
        .build();
  }

  @Test(expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = "No record.*")
  public void testEmptyTopic() throws IOException {
    KafkaInputFormat mockFormat = mock(KafkaInputFormat.class);
    InputSplit[] splits = new KafkaInputSplit[] { new KafkaInputSplit("test_topic", 0, 0, 0) };
    doReturn(splits).when(mockFormat).getSplitsByRecordsPerSplit(any(), anyLong());
    RecordReader<KafkaInputMapperKey, KafkaInputMapperValue> mockRecordReader = mock(RecordReader.class);
    doReturn(false).when(mockRecordReader).next(any(), any());
    doReturn(mockRecordReader).when(mockFormat).getRecordReader(any(), any(), any());

    KafkaInputDictTrainer trainer = new KafkaInputDictTrainer(mockFormat, Optional.empty(), getParam(100));
    trainer.trainDict();
  }

  interface ResettableRecordReader<K, V> extends RecordReader<K, V> {
    void reset();
  }

  private ResettableRecordReader<KafkaInputMapperKey, KafkaInputMapperValue> mockReader(List<byte[]> values) {
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
            value.value = ByteBuffer.wrap(values.get(cur));
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

  @Test
  public void testSamplingFromMultiplePartitions() throws IOException {
    KafkaInputFormat mockFormat = mock(KafkaInputFormat.class);
    InputSplit[] splits = new KafkaInputSplit[] { new KafkaInputSplit("test_topic", 0, 0, 2),
        new KafkaInputSplit("test_topic", 0, 0, 2) };
    doReturn(splits).when(mockFormat).getSplitsByRecordsPerSplit(any(), anyLong());

    // Return 3 records
    ResettableRecordReader<KafkaInputMapperKey, KafkaInputMapperValue> readerForP0 =
        mockReader(Arrays.asList("p0_value0".getBytes(), "p0_value1".getBytes(), "p0_value2".getBytes()));

    ResettableRecordReader<KafkaInputMapperKey, KafkaInputMapperValue> readerForP1 =
        mockReader(Arrays.asList("p1_value0".getBytes(), "p1_value1".getBytes(), "p1_value2".getBytes()));

    doReturn(readerForP0).when(mockFormat).getRecordReader(eq(splits[0]), any(), any());
    doReturn(readerForP1).when(mockFormat).getRecordReader(eq(splits[1]), any(), any());

    // Big sampling will collect every record.
    ZstdDictTrainer mockTrainer1 = mock(ZstdDictTrainer.class);
    KafkaInputDictTrainer trainer1 = new KafkaInputDictTrainer(mockFormat, Optional.of(mockTrainer1), getParam(1000));
    trainer1.trainDict();

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
    KafkaInputDictTrainer trainer2 = new KafkaInputDictTrainer(mockFormat, Optional.of(mockTrainer2), getParam(20));
    trainer2.trainDict();
    verify(mockTrainer2).addSample(eq("p0_value0".getBytes()));
    verify(mockTrainer2, never()).addSample(eq("p0_value1".getBytes()));
    verify(mockTrainer2, never()).addSample(eq("p0_value2".getBytes()));
    verify(mockTrainer2).addSample(eq("p1_value0".getBytes()));
    verify(mockTrainer2, never()).addSample(eq("p1_value1".getBytes()));
    verify(mockTrainer2, never()).addSample(eq("p1_value2".getBytes()));
  }
}
