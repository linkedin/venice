package com.linkedin.davinci.kafka.consumer;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;

import com.linkedin.davinci.config.VeniceServerConfig;
import io.tehuti.metrics.MetricsRepository;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import org.mockito.MockedConstruction;
import org.testng.Assert;
import org.testng.annotations.Test;


public class SeparatedStoreBufferServiceTest {
  private final MetricsRepository metricsRepo = new MetricsRepository();

  @Test
  public void testDiagnosticsSettingIsPassedToBothPools() {
    AtomicBoolean enabled = new AtomicBoolean();
    BooleanSupplier liveSetting = enabled::get;
    VeniceServerConfig serverConfig = mock(VeniceServerConfig.class);
    try (MockedConstruction<StoreBufferService> services =
        mockConstruction(StoreBufferService.class, (service, context) -> {
          Assert.assertEquals(context.arguments().size(), 11);
          Assert.assertSame(context.arguments().get(10), liveSetting);
        });
        SeparatedStoreBufferService ignored =
            new SeparatedStoreBufferService(serverConfig, metricsRepo, "test-cluster", liveSetting)) {
      Assert.assertEquals(services.constructed().size(), 2);
      Assert.assertFalse(liveSetting.getAsBoolean());
      enabled.set(true);
      Assert.assertTrue(liveSetting.getAsBoolean());
    }
  }

  @Test
  public void testSeparatedStoreBufferHaveTwoSetsOfMetrics() {
    VeniceServerConfig serverConfig = mock(VeniceServerConfig.class);
    doReturn(8).when(serverConfig).getDrainerPoolSizeSortedInput();
    doReturn(8).when(serverConfig).getDrainerPoolSizeUnsortedInput();
    doReturn(1000l).when(serverConfig).getStoreWriterBufferNotifyDelta();
    doReturn(1000l).when(serverConfig).getStoreWriterBufferMemoryCapacity();
    doReturn(VeniceServerConfig.DEFAULT_WRITE_PATH_THREAD_PRIORITY).when(serverConfig).getWritePathThreadPriority();
    SeparatedStoreBufferService separatedStoreBufferService =
        new SeparatedStoreBufferService(serverConfig, metricsRepo, "test-cluster");
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
