package com.linkedin.davinci.kafka.consumer;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.linkedin.davinci.config.VeniceServerConfig;
import io.tehuti.metrics.MetricsRepository;
import org.testng.Assert;
import org.testng.annotations.Test;


public class SeparatedStoreBufferServiceTest {
  private final MetricsRepository metricsRepo = new MetricsRepository();

  @Test
  public void testSeparatedStoreBufferHaveTwoSetsOfMetrics() {
    VeniceServerConfig serverConfig = mock(VeniceServerConfig.class);
    doReturn(8).when(serverConfig).getDrainerPoolSizeSortedInput();
    doReturn(8).when(serverConfig).getDrainerPoolSizeUnsortedInput();
    doReturn(1000l).when(serverConfig).getStoreWriterBufferNotifyDelta();
    doReturn(1000l).when(serverConfig).getStoreWriterBufferMemoryCapacity();
    SeparatedStoreBufferService separatedStoreBufferService =
        new SeparatedStoreBufferService(serverConfig, metricsRepo);
    // Verify that metricsRepo has two sets of metrics, example metric:
    // StoreBufferServiceSorted--max_memory_usage_per_writer and StoreBufferServiceUnsorted--max_memory_usage_per_writer
    Assert.assertNotNull(
        metricsRepo.getSensor(".StoreBufferServiceSorted--max_memory_usage_per_writer"),
        "Missing metric from sorted drainers");
    Assert.assertNotNull(
        metricsRepo.getSensor(".StoreBufferServiceUnsorted--max_memory_usage_per_writer"),
        "Missing metric from unsorted drainers");
    separatedStoreBufferService.close();
  }
}
