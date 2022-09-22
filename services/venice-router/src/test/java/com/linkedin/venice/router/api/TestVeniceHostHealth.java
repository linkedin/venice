package com.linkedin.venice.router.api;

import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.LiveInstanceMonitor;
import com.linkedin.venice.router.VeniceRouterConfig;
import com.linkedin.venice.router.httpclient.StorageNodeClient;
import com.linkedin.venice.router.stats.AggHostHealthStats;
import com.linkedin.venice.router.stats.RouteHttpRequestStats;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestVeniceHostHealth {
  private StorageNodeClient mockStorageNodeClient(boolean ret) {
    StorageNodeClient client = mock(StorageNodeClient.class);
    doReturn(ret).when(client).isInstanceReadyToServe(anyString());
    return client;
  }

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
    VeniceRouterConfig config = mock(VeniceRouterConfig.class);
    doReturn(5).when(config).getRouterUnhealthyPendingConnThresholdPerRoute();
    doReturn(2).when(config).getRouterPendingConnResumeThresholdPerRoute();
    doReturn(10l).when(config).getFullPendingQueueServerOORMs();

    VeniceHostHealth hostHealth = new VeniceHostHealth(
        mockLiveInstanceMonitor,
        mockStorageNodeClient(true),
        config,
        routeHttpRequestStats,
        mockAggHostHealthStats);
    Assert.assertFalse(
        hostHealth.isHostHealthy(deadInstance, fakePartition),
        "Host should be unhealthy when it is dead.");
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
    VeniceRouterConfig config = mock(VeniceRouterConfig.class);
    doReturn(true).when(config).isStatefulRouterHealthCheckEnabled();
    doReturn(4).when(config).getRouterUnhealthyPendingConnThresholdPerRoute();
    doReturn(2).when(config).getRouterPendingConnResumeThresholdPerRoute();
    doReturn(10l).when(config).getFullPendingQueueServerOORMs();
    VeniceHostHealth hostHealth = new VeniceHostHealth(
        mockLiveInstanceMonitor,
        mockStorageNodeClient(true),
        config,
        routeHttpRequestStats,
        mockAggHostHealthStats);
    Assert.assertFalse(
        hostHealth.isHostHealthy(deadInstance, fakePartition),
        "Host should be unhealthy when it is dead.");
    Assert.assertTrue(hostHealth.isHostHealthy(liveInstance, fakePartition), "Host should be healthy when it is alive");
    Assert.assertFalse(
        hostHealth.isHostHealthy(slowInstance, fakePartition),
        "Host should be unhealthy when it has lots of pending connection.");
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
    VeniceRouterConfig config = mock(VeniceRouterConfig.class);
    doReturn(true).when(config).isStatefulRouterHealthCheckEnabled();
    doReturn(4).when(config).getRouterUnhealthyPendingConnThresholdPerRoute();
    doReturn(2).when(config).getRouterPendingConnResumeThresholdPerRoute();

    VeniceHostHealth hostHealth = new VeniceHostHealth(
        mockLiveInstanceMonitor,
        mockStorageNodeClient(true),
        config,
        routeHttpRequestStats,
        mockAggHostHealthStats);
    Assert.assertFalse(
        hostHealth.isHostHealthy(slowInstance, fakePartition),
        "Host should be unhealthy when it has lots of pending connection.");
    Assert.assertFalse(
        hostHealth.isHostHealthy(slowInstance, fakePartition),
        "Host should be unhealthy when the current pending request count is above resume threshold.");
    Assert.assertTrue(
        hostHealth.isHostHealthy(slowInstance, fakePartition),
        "Host should be healthy when the current pending request count is below resume threshold.");
    doReturn(true).when(config).isStatefulRouterHealthCheckEnabled();
    doReturn(4).when(config).getRouterUnhealthyPendingConnThresholdPerRoute();
    doReturn(2).when(config).getRouterPendingConnResumeThresholdPerRoute();
    doReturn(100l).when(config).getFullPendingQueueServerOORMs();
    hostHealth = new VeniceHostHealth(
        mockLiveInstanceMonitor,
        mockStorageNodeClient(true),
        config,
        routeHttpRequestStats,
        mockAggHostHealthStats);
    Assert.assertFalse(
        hostHealth.isHostHealthy(slowInstance, fakePartition),
        "Host should be unhealthy when it has lots of pending connection.");
    Assert.assertFalse(
        hostHealth.isHostHealthy(slowInstance, fakePartition),
        "Host should be unhealthy when the current pending request count is above resume threshold.");
    Assert.assertFalse(
        hostHealth.isHostHealthy(slowInstance, fakePartition),
        "Host should be unhealthy when the OOR period is not over yet.");
  }
}
