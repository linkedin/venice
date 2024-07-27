package com.linkedin.venice.controller.util;

import static com.linkedin.venice.meta.HybridStoreConfigImpl.DEFAULT_REAL_TIME_TOPIC_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.ConfigConstants;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.VeniceControllerClusterConfig;
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
    hybridStoreConfigRecord.realTimeTopicName = DEFAULT_REAL_TIME_TOPIC_NAME;
    assertTrue(AdminUtils.isHybrid(hybridStoreConfigRecord));
  }

  @Test
  public void testGetRmdVersionID() {
    String storeName = "storeName";
    String clusterName = "clusterName";

    Admin mockAdmin = mock(Admin.class);
    VeniceControllerMultiClusterConfig multiClusterConfig = mock(VeniceControllerMultiClusterConfig.class);
    VeniceControllerClusterConfig controllerConfig = mock(VeniceControllerClusterConfig.class);
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

  @Test
  public void testIsIncrementalPushSupported() {
    HybridStoreConfig nonHybridConfig =
        new HybridStoreConfigImpl(-1, -1, -1, DataReplicationPolicy.AGGREGATE, BufferReplayPolicy.REWIND_FROM_EOP);
    HybridStoreConfig hybridConfigWithNonAggregateDRP = new HybridStoreConfigImpl(
        100,
        1000,
        -1,
        DataReplicationPolicy.NON_AGGREGATE,
        BufferReplayPolicy.REWIND_FROM_EOP);
    HybridStoreConfig hybridConfigWithAggregateDRP =
        new HybridStoreConfigImpl(100, 1000, -1, DataReplicationPolicy.AGGREGATE, BufferReplayPolicy.REWIND_FROM_EOP);
    HybridStoreConfig hybridConfigWithNoneDRP =
        new HybridStoreConfigImpl(100, 1000, -1, DataReplicationPolicy.NONE, BufferReplayPolicy.REWIND_FROM_EOP);

    // In single-region mode, any hybrid store supports incremental push.
    assertFalse(AdminUtils.isIncrementalPushSupported(false, false, null));
    assertFalse(AdminUtils.isIncrementalPushSupported(false, false, nonHybridConfig));
    assertTrue(AdminUtils.isIncrementalPushSupported(false, false, hybridConfigWithNonAggregateDRP));
    assertTrue(AdminUtils.isIncrementalPushSupported(false, false, hybridConfigWithAggregateDRP));
    assertTrue(AdminUtils.isIncrementalPushSupported(false, false, hybridConfigWithNoneDRP));

    // In multi-region mode, hybrid stores with NON_AGGREGATE DataReplicationPolicy do not support incremental push
    // enabled.
    assertFalse(AdminUtils.isIncrementalPushSupported(true, false, null));
    assertFalse(AdminUtils.isIncrementalPushSupported(true, false, nonHybridConfig));
    assertFalse(AdminUtils.isIncrementalPushSupported(true, false, hybridConfigWithNonAggregateDRP));
    assertTrue(AdminUtils.isIncrementalPushSupported(true, false, hybridConfigWithAggregateDRP));
    assertTrue(AdminUtils.isIncrementalPushSupported(true, false, hybridConfigWithNoneDRP));
    assertTrue(AdminUtils.isIncrementalPushSupported(true, true, hybridConfigWithNonAggregateDRP));
  }
}
