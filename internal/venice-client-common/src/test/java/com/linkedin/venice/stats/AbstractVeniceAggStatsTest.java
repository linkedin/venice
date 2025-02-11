package com.linkedin.venice.stats;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

import com.linkedin.venice.client.stats.ClientStats;
import io.tehuti.metrics.MetricsRepository;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class AbstractVeniceAggStatsTest {
  @DataProvider(name = "ClusterName-And-Boolean")
  public Object[][] fcRequestTypes() {
    return new Object[][] { { null, false }, { null, true }, { "test-cluster", false }, { "test-cluster", true } };
  }

  @Test(dataProvider = "ClusterName-And-Boolean")
  public void abstractVeniceAggStatsWithNoClusterName(String clusterName, boolean perClusterAggregate) {
    MetricsRepository metricsRepository = mock(MetricsRepository.class);
    StatsSupplier<ClientStats> statsSupplier = mock(StatsSupplier.class);
    try {
      new AbstractVeniceAggStats<ClientStats>(clusterName, metricsRepository, statsSupplier, perClusterAggregate) {
      };
      if (clusterName == null && perClusterAggregate) {
        fail("Expected IllegalArgumentException");
      }
    } catch (IllegalArgumentException e) {
      if (clusterName == null && perClusterAggregate) {
        assertEquals(e.getMessage(), "perClusterAggregate cannot be true when clusterName is null");
      } else {
        fail("IllegalArgumentException not expected");
      }
    }
  }
}
