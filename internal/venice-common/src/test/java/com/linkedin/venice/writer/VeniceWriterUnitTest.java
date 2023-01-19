package com.linkedin.venice.writer;

import static com.linkedin.venice.writer.VeniceWriter.ENABLE_CHUNKING;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.venice.partitioner.DefaultVenicePartitioner;
import com.linkedin.venice.serialization.VeniceKafkaSerializer;
import com.linkedin.venice.serialization.avro.VeniceAvroKafkaSerializer;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.Future;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.mockito.ArgumentCaptor;
import org.testng.Assert;
import org.testng.annotations.Test;


public class VeniceWriterUnitTest {
  @Test(dataProvider = "Chunking-And-Partition-Counts", dataProviderClass = DataProviderUtils.class)
  public void testTargetPartitionIsSameForAllOperationsWithTheSameKey(boolean isChunkingEnabled, int partitionCount) {
    KafkaProducerWrapper mockedProducer = mock(KafkaProducerWrapper.class);
    Future mockedFuture = mock(Future.class);
    when(mockedProducer.sendMessage(any(), any())).thenReturn(mockedFuture);
    Properties writerProperties = new Properties();
    writerProperties.put(ENABLE_CHUNKING, isChunkingEnabled);

    String stringSchema = "\"string\"";
    VeniceKafkaSerializer serializer = new VeniceAvroKafkaSerializer(stringSchema);
    String testTopic = "test";
    VeniceWriterOptions veniceWriterOptions = new VeniceWriterOptions.Builder(testTopic).setKeySerializer(serializer)
        .setValueSerializer(serializer)
        .setWriteComputeSerializer(serializer)
        .setPartitioner(new DefaultVenicePartitioner())
        .setPartitionCount(Optional.of(partitionCount))
        .build();
    VeniceWriter<Object, Object, Object> writer =
        new VeniceWriter(veniceWriterOptions, new VeniceProperties(writerProperties), () -> mockedProducer);

    String valueString = "value-string";
    String key = "test-key";

    ArgumentCaptor<ProducerRecord> putProducerRecordArgumentCaptor = ArgumentCaptor.forClass(ProducerRecord.class);
    writer.put(key, valueString, 1, null);
    verify(mockedProducer, atLeast(2)).sendMessage(putProducerRecordArgumentCaptor.capture(), any());

    ArgumentCaptor<ProducerRecord> deleteProducerRecordArgumentCaptor = ArgumentCaptor.forClass(ProducerRecord.class);
    writer.delete(key, null);
    verify(mockedProducer, atLeast(2)).sendMessage(deleteProducerRecordArgumentCaptor.capture(), any());

    Assert.assertEquals(
        putProducerRecordArgumentCaptor.getValue().partition(),
        deleteProducerRecordArgumentCaptor.getValue().partition());

    ArgumentCaptor<ProducerRecord> updateProducerRecordArgumentCaptor = ArgumentCaptor.forClass(ProducerRecord.class);
    writer.delete(key, null);
    verify(mockedProducer, atLeast(2)).sendMessage(updateProducerRecordArgumentCaptor.capture(), any());

    Assert.assertEquals(
        putProducerRecordArgumentCaptor.getValue().partition(),
        updateProducerRecordArgumentCaptor.getValue().partition());
  }
}
