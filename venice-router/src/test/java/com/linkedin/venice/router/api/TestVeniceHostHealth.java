package com.linkedin.venice.router.api;

import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.LiveInstanceMonitor;
import com.linkedin.venice.router.stats.AggHostHealthStats;
import com.linkedin.venice.router.stats.RouteHttpRequestStats;
import java.util.concurrent.TimeUnit;
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

    AggHostHealthStats mockAggHostHealthStats = mock(AggHostHealthStats.class);

    String fakePartition = "fake_partition";
    RouteHttpRequestStats routeHttpRequestStats = mock(RouteHttpRequestStats.class);
    VeniceHostHealth hostHealth = new VeniceHostHealth(mockLiveInstanceMonitor, routeHttpRequestStats,
        false, 5, 2, 10, mockAggHostHealthStats);
    Assert.assertFalse(hostHealth.isHostHealthy(deadInstance, fakePartition), "Host should be unhealthy when it is dead.");
    Assert.assertTrue(hostHealth.isHostHealthy(liveInstance, fakePartition), "Host should be healthy when it is alive");
    verify(mockAggHostHealthStats, times(1)).recordUnhealthyHostOfflineInstance(deadInstance.getNodeId());
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
    doReturn(true).when(mockLiveInstanceMonitor).isInstanceAlive(slowInstance);
    doReturn(10L).when(routeHttpRequestStats).getPendingRequestCount("slowHost_123");

    AggHostHealthStats mockAggHostHealthStats = mock(AggHostHealthStats.class);

    String fakePartition = "fake_partition";
    VeniceHostHealth hostHealth = new VeniceHostHealth(mockLiveInstanceMonitor, routeHttpRequestStats,
        true, 4, 2, 10, mockAggHostHealthStats);
    Assert.assertFalse(hostHealth.isHostHealthy(deadInstance, fakePartition), "Host should be unhealthy when it is dead.");
    Assert.assertTrue(hostHealth.isHostHealthy(liveInstance, fakePartition), "Host should be healthy when it is alive");
    Assert.assertFalse(hostHealth.isHostHealthy(slowInstance, fakePartition), "Host should be unhealthy when it has lots of pending connection.");
    verify(mockAggHostHealthStats, times(1)).recordUnhealthyHostOfflineInstance(deadInstance.getNodeId());
    verify(mockAggHostHealthStats, times(1)).recordUnhealthyHostTooManyPendingRequest(slowInstance.getNodeId());
    verify(mockAggHostHealthStats, times(1)).recordPendingRequestCount(slowInstance.getNodeId(), 10);
  }

  @Test
  public void testOORDurationWhenPendingConnectionCountIsHigh() {
    Instance slowInstance = Instance.fromNodeId("slowHost_123");

    RouteHttpRequestStats routeHttpRequestStats = mock(RouteHttpRequestStats.class);

    LiveInstanceMonitor mockLiveInstanceMonitor = mock(LiveInstanceMonitor.class);
    doReturn(true).when(mockLiveInstanceMonitor).isInstanceAlive(slowInstance);
    doReturn(10L).when(routeHttpRequestStats).getPendingRequestCount("slowHost_123");
    when(routeHttpRequestStats.getPendingRequestCount("slowHost_123")).thenReturn(10L, 3L, 1L, 10L, 3L, 1L);

    AggHostHealthStats mockAggHostHealthStats = mock(AggHostHealthStats.class);

    String fakePartition = "fake_partition";
    VeniceHostHealth hostHealth = new VeniceHostHealth(mockLiveInstanceMonitor, routeHttpRequestStats,
        true, 4, 2, 0, mockAggHostHealthStats);
    Assert.assertFalse(hostHealth.isHostHealthy(slowInstance, fakePartition), "Host should be unhealthy when it has lots of pending connection.");
    Assert.assertFalse(hostHealth.isHostHealthy(slowInstance, fakePartition), "Host should be unhealthy when the current pending request count is above resume threshold.");
    Assert.assertTrue(hostHealth.isHostHealthy(slowInstance, fakePartition), "Host should be healthy when the current pending request count is below resume threshold.");

    hostHealth = new VeniceHostHealth(mockLiveInstanceMonitor, routeHttpRequestStats,
        true, 4, 2, TimeUnit.MINUTES.toMillis(10), mockAggHostHealthStats);
    Assert.assertFalse(hostHealth.isHostHealthy(slowInstance, fakePartition), "Host should be unhealthy when it has lots of pending connection.");
    Assert.assertFalse(hostHealth.isHostHealthy(slowInstance, fakePartition), "Host should be unhealthy when the current pending request count is above resume threshold.");
    Assert.assertFalse(hostHealth.isHostHealthy(slowInstance, fakePartition), "Host should be unhealthy when the OOR period is not over yet.");
  }
}
