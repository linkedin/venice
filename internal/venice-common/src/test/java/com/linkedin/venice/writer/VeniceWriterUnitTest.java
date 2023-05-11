package com.linkedin.venice.writer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.venice.partitioner.DefaultVenicePartitioner;
import com.linkedin.venice.pubsub.api.PubSubProducerAdapter;
import com.linkedin.venice.serialization.VeniceKafkaSerializer;
import com.linkedin.venice.serialization.avro.VeniceAvroKafkaSerializer;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.concurrent.Future;
import org.mockito.ArgumentCaptor;
import org.testng.Assert;
import org.testng.annotations.Test;


public class VeniceWriterUnitTest {
  @Test(dataProvider = "Chunking-And-Partition-Counts", dataProviderClass = DataProviderUtils.class)
  public void testTargetPartitionIsSameForAllOperationsWithTheSameKey(boolean isChunkingEnabled, int partitionCount) {
    PubSubProducerAdapter mockedProducer = mock(PubSubProducerAdapter.class);
    Future mockedFuture = mock(Future.class);
    when(mockedProducer.sendMessage(any(), any(), any(), any(), any(), any())).thenReturn(mockedFuture);

    String stringSchema = "\"string\"";
    VeniceKafkaSerializer serializer = new VeniceAvroKafkaSerializer(stringSchema);
    String testTopic = "test";
    VeniceWriterOptions veniceWriterOptions = new VeniceWriterOptions.Builder(testTopic).setKeySerializer(serializer)
        .setValueSerializer(serializer)
        .setWriteComputeSerializer(serializer)
        .setPartitioner(new DefaultVenicePartitioner())
        .setPartitionCount(partitionCount)
        .setChunkingEnabled(isChunkingEnabled)
        .build();
    VeniceWriter<Object, Object, Object> writer =
        new VeniceWriter(veniceWriterOptions, VeniceProperties.empty(), mockedProducer);

    String valueString = "value-string";
    String key = "test-key";

    ArgumentCaptor<Integer> putPartitionArgumentCaptor = ArgumentCaptor.forClass(Integer.class);
    writer.put(key, valueString, 1, null);
    verify(mockedProducer, atLeast(2))
        .sendMessage(anyString(), putPartitionArgumentCaptor.capture(), any(), any(), any(), any());

    ArgumentCaptor<Integer> deletePartitionArgumentCaptor = ArgumentCaptor.forClass(Integer.class);
    writer.delete(key, null);
    verify(mockedProducer, atLeast(2))
        .sendMessage(anyString(), deletePartitionArgumentCaptor.capture(), any(), any(), any(), any());

    ArgumentCaptor<Integer> updatePartitionArgumentCaptor = ArgumentCaptor.forClass(Integer.class);
    writer.delete(key, null);
    verify(mockedProducer, atLeast(2))
        .sendMessage(anyString(), updatePartitionArgumentCaptor.capture(), any(), any(), any(), any());

    Assert.assertEquals(putPartitionArgumentCaptor.getValue(), deletePartitionArgumentCaptor.getValue());
    Assert.assertEquals(putPartitionArgumentCaptor.getValue(), updatePartitionArgumentCaptor.getValue());
  }
}
