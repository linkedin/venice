package com.linkedin.venice.writer;

import static com.linkedin.venice.writer.VeniceWriter.ENABLE_CHUNKING;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
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
import org.mockito.ArgumentCaptor;
import org.testng.Assert;
import org.testng.annotations.Test;


public class VeniceWriterUnitTest {
  @Test(dataProvider = "Chunking-And-Partition-Counts", dataProviderClass = DataProviderUtils.class)
  public void testTargetPartitionIsSameForAllOperationsWithTheSameKey(boolean isChunkingEnabled, int partitionCount) {
    KafkaProducerWrapper mockedProducer = mock(KafkaProducerWrapper.class);
    Future mockedFuture = mock(Future.class);
    when(mockedProducer.sendMessage(anyString(), any(), any(), anyInt(), any())).thenReturn(mockedFuture);
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

    ArgumentCaptor<Integer> putOpTargetPartitionCaptor = ArgumentCaptor.forClass(Integer.class);
    writer.put(key, valueString, 1, null);
    verify(mockedProducer, atLeast(2))
        .sendMessage(eq(testTopic), any(), any(), putOpTargetPartitionCaptor.capture(), any());

    ArgumentCaptor<Integer> deleteOpTargetPartitionCaptor = ArgumentCaptor.forClass(Integer.class);
    writer.delete(key, null);
    verify(mockedProducer, atLeast(2))
        .sendMessage(eq(testTopic), any(), any(), deleteOpTargetPartitionCaptor.capture(), any());

    Assert.assertEquals(putOpTargetPartitionCaptor.getValue(), deleteOpTargetPartitionCaptor.getValue());

    ArgumentCaptor<Integer> updateOpTargetPartitionCaptor = ArgumentCaptor.forClass(Integer.class);
    writer.delete(key, null);
    verify(mockedProducer, atLeast(2))
        .sendMessage(eq(testTopic), any(), any(), updateOpTargetPartitionCaptor.capture(), any());

    Assert.assertEquals(putOpTargetPartitionCaptor.getValue(), updateOpTargetPartitionCaptor.getValue());
  }
}
