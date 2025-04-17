package com.linkedin.venice.controller.systemstore;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.linkedin.venice.controller.VeniceControllerClusterConfig;
import com.linkedin.venice.controller.VeniceControllerMultiClusterConfig;
import com.linkedin.venice.controller.VeniceParentHelixAdmin;
import io.tehuti.metrics.MetricsRepository;
import java.util.HashSet;
import java.util.Set;
import org.testng.Assert;
import org.testng.annotations.Test;


public class SystemStoreRepairServiceTest {
  @Test
  public void testInitialization() {
    VeniceControllerClusterConfig commonConfig = mock(VeniceControllerClusterConfig.class);
    VeniceControllerMultiClusterConfig multiClusterConfigs = mock(VeniceControllerMultiClusterConfig.class);
    doReturn(commonConfig).when(multiClusterConfigs).getCommonConfig();
    Set<String> clusterSet = new HashSet<>();
    clusterSet.add("venice-1");
    clusterSet.add("venice-2");
    doReturn(clusterSet).when(multiClusterConfigs).getClusters();

    VeniceControllerClusterConfig v1Config = mock(VeniceControllerClusterConfig.class);
    VeniceControllerClusterConfig v2Config = mock(VeniceControllerClusterConfig.class);
    doReturn(true).when(v2Config).isParentSystemStoreRepairServiceEnabled();
    doReturn(v1Config).when(multiClusterConfigs).getControllerConfig("venice-1");
    doReturn(v2Config).when(multiClusterConfigs).getControllerConfig("venice-2");

    SystemStoreRepairService systemStoreRepairService =
        new SystemStoreRepairService(mock(VeniceParentHelixAdmin.class), multiClusterConfigs, new MetricsRepository());
    Assert.assertFalse(systemStoreRepairService.getClusterToSystemStoreHealthCheckStatsMap().containsKey("venice-1"));
    Assert.assertTrue(systemStoreRepairService.getClusterToSystemStoreHealthCheckStatsMap().containsKey("venice-2"));
  }
}
