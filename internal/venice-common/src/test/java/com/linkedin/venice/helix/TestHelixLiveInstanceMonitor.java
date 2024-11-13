package com.linkedin.venice.helix;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TestHelixLiveInstanceMonitor {
  private ZkClient zkClient;

  @BeforeMethod
  void setUp() {
    zkClient = Mockito.mock(ZkClient.class);
  }

  @Test
  public void testRefresh() {
    HelixLiveInstanceMonitor liveInstanceMonitor = new HelixLiveInstanceMonitor(zkClient, "cluster");
    doReturn(true).when(zkClient).exists(anyString());
    liveInstanceMonitor.refresh();
    verify(zkClient, times(1)).subscribeChildChanges("/cluster/LIVEINSTANCES", liveInstanceMonitor);
  }
}
