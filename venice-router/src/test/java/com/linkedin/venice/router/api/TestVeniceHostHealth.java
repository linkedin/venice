package com.linkedin.venice.router.api;

import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.LiveInstanceMonitor;
import com.linkedin.venice.router.stats.RouteHttpRequestStats;
import io.tehuti.metrics.MetricsRepository;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;


public class TestVeniceHostHealth {

  @Test
  public void checkHostHealthByLiveInstance() {
    Instance deadInstance = Instance.fromNodeId("deadhost_123");
    Instance liveInstance = Instance.fromNodeId("liveHost_123");
    LiveInstanceMonitor mockLiveInstanceMonitor = mock(LiveInstanceMonitor.class);
    doReturn(false).when(mockLiveInstanceMonitor).isInstanceAlive(deadInstance);
    doReturn(true).when(mockLiveInstanceMonitor).isInstanceAlive(liveInstance);

    String fakePartition = "fake_partition";
    RouteHttpRequestStats routeHttpRequestStats = mock(RouteHttpRequestStats.class);
    VeniceHostHealth hostHealth = new VeniceHostHealth(mockLiveInstanceMonitor, routeHttpRequestStats,
        false, 5);
    Assert.assertFalse(hostHealth.isHostHealthy(deadInstance, fakePartition), "Host should be unhealthy when it is dead.");
    Assert.assertTrue(hostHealth.isHostHealthy(liveInstance, fakePartition), "Host should be healthy when it is alive");
  }

  @Test
  public void checkHostHealthByPendingConnection() {
    Instance deadInstance = Instance.fromNodeId("deadhost_123");
    Instance liveInstance = Instance.fromNodeId("liveHost_123");
    Instance slowInstance = Instance.fromNodeId("slowHost_123");

    RouteHttpRequestStats routeHttpRequestStats = mock(RouteHttpRequestStats.class);

    LiveInstanceMonitor mockLiveInstanceMonitor = mock(LiveInstanceMonitor.class);
    doReturn(false).when(mockLiveInstanceMonitor).isInstanceAlive(deadInstance);
    doReturn(true).when(mockLiveInstanceMonitor).isInstanceAlive(liveInstance);
    doReturn(10L).when(routeHttpRequestStats).getPendingRequestCount("slowHost_123");

    String fakePartition = "fake_partition";
    VeniceHostHealth hostHealth = new VeniceHostHealth(mockLiveInstanceMonitor, routeHttpRequestStats,
        true, 4);
    Assert.assertFalse(hostHealth.isHostHealthy(deadInstance, fakePartition), "Host should be unhealthy when it is dead.");
    Assert.assertTrue(hostHealth.isHostHealthy(liveInstance, fakePartition), "Host should be healthy when it is alive");
    Assert.assertFalse(hostHealth.isHostHealthy(slowInstance, fakePartition), "Host should be unhealthy when it has lots of pending connection.");
  }
}
