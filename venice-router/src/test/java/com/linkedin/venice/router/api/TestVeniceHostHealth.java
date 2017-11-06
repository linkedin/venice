package com.linkedin.venice.router.api;

import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.LiveInstanceMonitor;
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
    VeniceHostHealth hostHealth = new VeniceHostHealth(mockLiveInstanceMonitor);
    Assert.assertFalse(hostHealth.isHostHealthy(deadInstance, fakePartition), "Host should be unhealthy when it is dead.");
    Assert.assertTrue(hostHealth.isHostHealthy(liveInstance, fakePartition), "Host should be healthy when it is alive");
  }
}
