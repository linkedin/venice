package com.linkedin.venice.stats;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.linkedin.venice.client.stats.ClientStats;
import io.tehuti.metrics.MetricsRepository;
import java.util.concurrent.atomic.AtomicBoolean;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class AbstractVeniceAggStatsTest {
  static class ClosableStats extends AbstractVeniceStats {
    private final AtomicBoolean closed = new AtomicBoolean(false);

    ClosableStats(MetricsRepository metricsRepository, String name) {
      super(metricsRepository, name);
    }

    @Override
    public void close() {
      super.close();
      closed.set(true);
    }
  }

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

  @Test
  public void testRemoveStoreClosesAndRemovesOnlyRequestedStore() {
    MetricsRepository metricsRepository = mock(MetricsRepository.class);
    AbstractVeniceAggStats<ClosableStats> aggStats = new AbstractVeniceAggStats<ClosableStats>(
        "cluster",
        metricsRepository,
        (repository, storeName, clusterName) -> new ClosableStats(repository, storeName),
        false) {
    };

    ClosableStats storeA = aggStats.getStoreStats("storeA");
    ClosableStats storeB = aggStats.getStoreStats("storeB");
    assertSame(aggStats.getNullableStoreStats("storeA"), storeA);
    assertSame(aggStats.getNullableStoreStats("storeB"), storeB);

    aggStats.removeStore("storeA");

    assertTrue(storeA.closed.get());
    assertNull(aggStats.getNullableStoreStats("storeA"));
    assertFalse(storeB.closed.get());
    assertSame(aggStats.getNullableStoreStats("storeB"), storeB);

    aggStats.removeStore("unknown");
    assertFalse(storeB.closed.get());
    assertSame(aggStats.getNullableStoreStats("storeB"), storeB);
  }
}
