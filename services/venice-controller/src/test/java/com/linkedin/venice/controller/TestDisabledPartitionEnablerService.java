package com.linkedin.venice.controller;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

import com.linkedin.venice.utils.TestMockTime;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.Test;


public class TestDisabledPartitionEnablerService {
  @Test
  public void testCleanupBackupVersion() throws Exception {
    VeniceHelixAdmin admin = mock(VeniceHelixAdmin.class);
    String clusterName = "test_cluster";
    VeniceControllerMultiClusterConfig config = mock(VeniceControllerMultiClusterConfig.class);
    long defaultRetentionMs = TimeUnit.DAYS.toMillis(7);
    doReturn(defaultRetentionMs).when(config).getBackupVersionDefaultRetentionMs();
    VeniceControllerClusterConfig controllerConfig = mock(VeniceControllerClusterConfig.class);
    doReturn(controllerConfig).when(config).getControllerConfig(anyString());
    Set<String> clusters = new HashSet<>();
    doReturn(true).when(admin).isLeaderControllerFor(any());
    clusters.add(clusterName);
    doReturn(true).when(controllerConfig).isEnableDisabledReplicaEnabled();
    doReturn(clusters).when(config).getClusters();
    TestMockTime time = new TestMockTime();
    DisabledPartitionEnablerService service = new DisabledPartitionEnablerService(admin, config, time);

    service.startInner();
    verify(admin, timeout(1000).atLeastOnce()).enableDisabledPartition(clusterName, "", true);
  }

}
