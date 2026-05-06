package com.linkedin.davinci.stats;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.kafka.consumer.StoreIngestionTask;
import io.tehuti.metrics.MetricConfig;
import io.tehuti.metrics.stats.AsyncGauge;
import it.unimi.dsi.fastutil.ints.Int2ObjectArrayMap;
import java.io.IOException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class IngestionStatsTest {
  // Dedicated AsyncGauge executor: the default static singleton can be shut down by other tests
  // calling MetricsRepository.close() in the same JVM, which makes AsyncGauge.measure() return 0.0
  // permanently.
  private AsyncGauge.AsyncGaugeExecutor asyncGaugeExecutor;

  @BeforeMethod
  public void setUp() {
    asyncGaugeExecutor = new AsyncGauge.AsyncGaugeExecutor.Builder().build();
  }

  @AfterMethod
  public void tearDown() throws IOException {
    if (asyncGaugeExecutor != null) {
      asyncGaugeExecutor.close();
    }
  }

  @Test
  public void testIngestionStatsGauge() {
    AbstractVeniceStatsReporter mockReporter = mock(AbstractVeniceStatsReporter.class);
    doReturn(mock(IngestionStats.class)).when(mockReporter).getStats();
    IngestionStatsReporter.IngestionStatsGauge gauge =
        new IngestionStatsReporter.IngestionStatsGauge(mockReporter, () -> 1.0, "testIngestionStatsGauge");
    assertEquals(gauge.measure(new MetricConfig(asyncGaugeExecutor), System.currentTimeMillis()), 1.0);
  }

  @Test
  public void testGetUniqueKeyCount() {
    VeniceServerConfig serverConfig = mock(VeniceServerConfig.class);
    when(serverConfig.getKafkaClusterIdToAliasMap()).thenReturn(new Int2ObjectArrayMap<>());
    IngestionStats stats = new IngestionStats(serverConfig);

    // No task set — should return 0
    assertEquals(stats.getUniqueKeyCount(), 0L);

    // Set a mock task that returns a known count
    StoreIngestionTask mockTask = mock(StoreIngestionTask.class);
    when(mockTask.isRunning()).thenReturn(true);
    when(mockTask.getEstimatedUniqueIngestedKeyCount()).thenReturn(42_000L);
    stats.setIngestionTask(mockTask);
    assertEquals(stats.getUniqueKeyCount(), 42_000L);
  }
}
