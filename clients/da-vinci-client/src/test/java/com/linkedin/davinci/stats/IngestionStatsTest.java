package com.linkedin.davinci.stats;

import static com.linkedin.venice.ConfigKeys.CLUSTER_NAME;
import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.ConfigKeys.ZOOKEEPER_ADDRESS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.kafka.consumer.StoreIngestionTask;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.VeniceProperties;
import io.tehuti.metrics.MetricConfig;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


public class IngestionStatsTest {
  @Test
  public void testGetLeaderStalledHybridIngestion() {
    PropertyBuilder builder = new PropertyBuilder();
    builder.put(CLUSTER_NAME, "testCluster");
    builder.put(ZOOKEEPER_ADDRESS, "fake");
    builder.put(KAFKA_BOOTSTRAP_SERVERS, "faker");
    VeniceProperties veniceProperties = new VeniceProperties(builder.build().toProperties());
    VeniceServerConfig serverConfig = new VeniceServerConfig(veniceProperties);
    IngestionStats ingestionStats = new IngestionStats(serverConfig);

    StoreIngestionTask caughtUpMockIngestionTask = mock(StoreIngestionTask.class);
    Mockito.when(caughtUpMockIngestionTask.getHybridLeaderOffsetLag()).thenReturn(0L);
    Mockito.when(caughtUpMockIngestionTask.isRunning()).thenReturn(true);
    ingestionStats.setIngestionTask(caughtUpMockIngestionTask);
    Assert.assertEquals(ingestionStats.getLeaderStalledHybridIngestion(), 0.0);

    StoreIngestionTask stuckIngestionTask = mock(StoreIngestionTask.class);
    Mockito.when(stuckIngestionTask.getHybridLeaderOffsetLag()).thenReturn(1L);
    Mockito.when(stuckIngestionTask.isRunning()).thenReturn(true);
    ingestionStats.setIngestionTask(stuckIngestionTask);
    Assert.assertEquals(ingestionStats.getLeaderStalledHybridIngestion(), 1.0);
  }

  @Test
  public void testIngestionStatsGauge() {
    AbstractVeniceStatsReporter mockReporter = mock(AbstractVeniceStatsReporter.class);
    doReturn(mock(IngestionStats.class)).when(mockReporter).getStats();
    IngestionStatsReporter.IngestionStatsGauge gauge =
        new IngestionStatsReporter.IngestionStatsGauge(mockReporter, () -> 1.0, "testIngestionStatsGauge");
    Assert.assertEquals(gauge.measure(new MetricConfig(), System.currentTimeMillis()), 1.0);
  }
}
