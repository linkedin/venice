package com.linkedin.venice.controller.util;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.ConfigConstants;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.VeniceControllerConfig;
import com.linkedin.venice.controller.VeniceControllerMultiClusterConfig;
import com.linkedin.venice.controller.kafka.protocol.admin.HybridStoreConfigRecord;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.BufferReplayPolicy;
import com.linkedin.venice.meta.DataReplicationPolicy;
import com.linkedin.venice.meta.HybridStoreConfig;
import com.linkedin.venice.meta.HybridStoreConfigImpl;
import com.linkedin.venice.meta.Store;
import org.testng.Assert;
import org.testng.annotations.Test;


public class AdminUtilsTest {
  @Test
  public void testIsHybrid() {
    Assert.assertFalse(AdminUtils.isHybrid((HybridStoreConfig) null));
    Assert.assertFalse(AdminUtils.isHybrid((HybridStoreConfigRecord) null));

    HybridStoreConfig hybridStoreConfig;
    hybridStoreConfig = new HybridStoreConfigImpl(-1, -1, -1, null, null);
    Assert.assertFalse(AdminUtils.isHybrid(hybridStoreConfig));

    hybridStoreConfig = new HybridStoreConfigImpl(100, -1, -1, null, null);
    Assert.assertFalse(AdminUtils.isHybrid(hybridStoreConfig));

    hybridStoreConfig = new HybridStoreConfigImpl(100, 100, -1, null, null);
    assertTrue(AdminUtils.isHybrid(hybridStoreConfig));

    hybridStoreConfig = new HybridStoreConfigImpl(100, 100, 100, null, null);
    assertTrue(AdminUtils.isHybrid(hybridStoreConfig));

    hybridStoreConfig = new HybridStoreConfigImpl(100, -1, 100, null, null);
    assertTrue(AdminUtils.isHybrid(hybridStoreConfig));

    hybridStoreConfig = new HybridStoreConfigImpl(-1, -1, 100, null, null);
    Assert.assertFalse(AdminUtils.isHybrid(hybridStoreConfig));

    HybridStoreConfigRecord hybridStoreConfigRecord = new HybridStoreConfigRecord();
    hybridStoreConfigRecord.rewindTimeInSeconds = 100;
    hybridStoreConfigRecord.offsetLagThresholdToGoOnline = 100;
    hybridStoreConfigRecord.producerTimestampLagThresholdToGoOnlineInSeconds = -1;
    hybridStoreConfigRecord.dataReplicationPolicy = DataReplicationPolicy.ACTIVE_ACTIVE.getValue();
    hybridStoreConfigRecord.bufferReplayPolicy = BufferReplayPolicy.REWIND_FROM_SOP.getValue();
    assertTrue(AdminUtils.isHybrid(hybridStoreConfigRecord));
  }

  @Test
  public void testGetRmdVersionID() {
    String storeName = "storeName";
    String clusterName = "clusterName";

    Admin mockAdmin = mock(Admin.class);
    VeniceControllerMultiClusterConfig multiClusterConfig = mock(VeniceControllerMultiClusterConfig.class);
    VeniceControllerConfig controllerConfig = mock(VeniceControllerConfig.class);
    Store mockStore = mock(Store.class);

    // Store null + cluster config not set
    doReturn(null).when(mockAdmin).getStore(clusterName, storeName);
    doReturn(multiClusterConfig).when(mockAdmin).getMultiClusterConfigs();
    doReturn(null).when(multiClusterConfig).getControllerConfig(clusterName);
    VeniceException e1 =
        Assert.expectThrows(VeniceException.class, () -> AdminUtils.getRmdVersionID(mockAdmin, storeName, clusterName));
    assertTrue(e1.getMessage().contains("No controller cluster config found for cluster clusterName"));

    reset(mockAdmin);
    reset(multiClusterConfig);
    reset(controllerConfig);
    reset(mockStore);

    // Store null + cluster config set
    doReturn(null).when(mockAdmin).getStore(clusterName, storeName);
    doReturn(multiClusterConfig).when(mockAdmin).getMultiClusterConfigs();
    doReturn(controllerConfig).when(multiClusterConfig).getControllerConfig(clusterName);
    doReturn(10).when(controllerConfig).getReplicationMetadataVersion();
    assertEquals(AdminUtils.getRmdVersionID(mockAdmin, storeName, clusterName), 10);

    reset(mockAdmin);
    reset(multiClusterConfig);
    reset(controllerConfig);
    reset(mockStore);

    // Store-level RMD version ID not found + cluster config not set
    doReturn(mockStore).when(mockAdmin).getStore(clusterName, storeName);
    doReturn(ConfigConstants.UNSPECIFIED_REPLICATION_METADATA_VERSION).when(mockStore).getRmdVersion();
    doReturn(multiClusterConfig).when(mockAdmin).getMultiClusterConfigs();
    doReturn(null).when(multiClusterConfig).getControllerConfig(clusterName);
    VeniceException e2 =
        Assert.expectThrows(VeniceException.class, () -> AdminUtils.getRmdVersionID(mockAdmin, storeName, clusterName));
    assertTrue(e2.getMessage().contains("No controller cluster config found for cluster clusterName"));

    reset(mockAdmin);
    reset(multiClusterConfig);
    reset(controllerConfig);
    reset(mockStore);

    // Store-level RMD version ID not found + cluster config set
    doReturn(mockStore).when(mockAdmin).getStore(clusterName, storeName);
    doReturn(ConfigConstants.UNSPECIFIED_REPLICATION_METADATA_VERSION).when(mockStore).getRmdVersion();
    doReturn(multiClusterConfig).when(mockAdmin).getMultiClusterConfigs();
    doReturn(controllerConfig).when(multiClusterConfig).getControllerConfig(clusterName);
    doReturn(10).when(controllerConfig).getReplicationMetadataVersion();
    assertEquals(AdminUtils.getRmdVersionID(mockAdmin, storeName, clusterName), 10);

    reset(mockAdmin);
    reset(multiClusterConfig);
    reset(controllerConfig);
    reset(mockStore);

    // Store-level RMD version ID found
    doReturn(mockStore).when(mockAdmin).getStore(clusterName, storeName);
    doReturn(5).when(mockStore).getRmdVersion();
    doReturn(multiClusterConfig).when(mockAdmin).getMultiClusterConfigs();
    doReturn(controllerConfig).when(multiClusterConfig).getControllerConfig(clusterName);
    doReturn(10).when(controllerConfig).getReplicationMetadataVersion();
    assertEquals(AdminUtils.getRmdVersionID(mockAdmin, storeName, clusterName), 5);
    verify(mockAdmin, never()).getMultiClusterConfigs();
    verify(multiClusterConfig, never()).getControllerConfig(any());
    verify(controllerConfig, never()).getReplicationMetadataVersion();
  }
}
