package com.linkedin.venice.writer;

import static org.mockito.Mockito.mock;

import com.linkedin.venice.partitioner.DefaultVenicePartitioner;
import com.linkedin.venice.partitioner.VenicePartitioner;
import com.linkedin.venice.pubsub.api.PubSubProducerAdapter;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.Properties;
import org.testng.Assert;
import org.testng.annotations.Test;


public class ComplexVeniceWriterTest {
  @Test
  public void testUnsupportedPublicAPIs() {
    PubSubProducerAdapter mockProducerAdapter = mock(PubSubProducerAdapter.class);
    VeniceProperties veniceProperties = new VeniceProperties(new Properties());
    ComplexVeniceWriter<byte[], byte[], byte[]> complexVeniceWriter = new ComplexVeniceWriter<>(
        getVeniceWriterOptions("ignored-topic", new DefaultVenicePartitioner(), 1),
        veniceProperties,
        mockProducerAdapter);
    Assert.assertThrows(
        UnsupportedOperationException.class,
        () -> complexVeniceWriter.put(new byte[0], new byte[0], 1, null));
    Assert.assertThrows(
        UnsupportedOperationException.class,
        () -> complexVeniceWriter.update(new byte[0], new byte[0], 1, 1, null));
    Assert.assertThrows(UnsupportedOperationException.class, () -> complexVeniceWriter.delete(new byte[0], null));
  }

  private VeniceWriterOptions getVeniceWriterOptions(
      String topicName,
      VenicePartitioner partitioner,
      int partitionCount) {
    VeniceWriterOptions.Builder configBuilder = new VeniceWriterOptions.Builder(topicName);
    configBuilder.setPartitionCount(partitionCount).setPartitioner(partitioner);
    return configBuilder.build();
  }
}
