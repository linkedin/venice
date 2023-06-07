package com.linkedin.venice.controller.datarecovery;

import static com.linkedin.venice.client.store.ClientConfig.DEFAULT_CLUSTER_DISCOVERY_D2_SERVICE_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.venice.controller.VeniceHelixAdmin;
import com.linkedin.venice.meta.PartitionerConfig;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import java.util.Optional;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.Test;


public class TestDataRecoveryManager {
  private final VeniceHelixAdmin veniceAdmin = mock(VeniceHelixAdmin.class);
  private final D2Client d2Client = mock(D2Client.class);
  private final DataRecoveryManager dataRecoveryManager = new DataRecoveryManager(
      veniceAdmin,
      d2Client,
      DEFAULT_CLUSTER_DISCOVERY_D2_SERVICE_NAME,
      Optional.empty(),
      new PubSubTopicRepository());
  private static final String clusterName = "testCluster";
  private static final String storeName = "testStore";
  private static final String sourceFabric = "dc-0";
  private static final String destFabric = "dc-1";
  private static final String topic = "testStore_v1";
  private static final int version = 1;
  private static final int amplificationFactor = 1;

  @Test
  public void testPrepareDataRecovery() {
    Store store = mock(Store.class);
    doReturn(true).when(store).isNativeReplicationEnabled();
    PartitionerConfig partitionerConfig = mock(PartitionerConfig.class);
    doReturn(1).when(partitionerConfig).getAmplificationFactor();
    doReturn(partitionerConfig).when(store).getPartitionerConfig();
    doReturn(store).when(veniceAdmin).getStore(clusterName, storeName);
    dataRecoveryManager
        .prepareStoreVersionForDataRecovery(clusterName, storeName, destFabric, version, amplificationFactor);
    verify(veniceAdmin, times(1)).deleteOneStoreVersion(clusterName, storeName, version);
    verify(veniceAdmin, times(1)).deleteParticipantStoreKillMessage(clusterName, topic);
    doReturn(1).when(store).getCurrentVersion();
    doReturn(true).when(veniceAdmin).isClusterWipeAllowed(clusterName);
    dataRecoveryManager
        .prepareStoreVersionForDataRecovery(clusterName, storeName, destFabric, version, amplificationFactor);
    verify(veniceAdmin, times(1)).wipeCluster(clusterName, destFabric, Optional.of(storeName), Optional.of(version));
  }

  @Test
  public void testInitiateDataRecovery() {
    Store store = mock(Store.class);
    doReturn(store).when(veniceAdmin).getStore(clusterName, storeName);
    Version sourceFabricVersion = new VersionImpl(storeName, version, "pushJob1");
    doReturn(true).when(veniceAdmin).addSpecificVersion(any(), any(), any());
    dataRecoveryManager.initiateDataRecovery(clusterName, storeName, 2, sourceFabric, true, sourceFabricVersion);
    ArgumentCaptor<Version> captor = ArgumentCaptor.forClass(Version.class);
    verify(veniceAdmin).addSpecificVersion(eq(clusterName), eq(storeName), captor.capture());
    assertEquals(captor.getValue().getDataRecoveryVersionConfig().getDataRecoverySourceVersionNumber(), version);
    assertEquals(captor.getValue().getNumber(), 2);
    assertEquals(captor.getValue().getPushJobId(), "data_recovery_pushJob1");
  }
}
