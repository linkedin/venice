package com.linkedin.venice.controller.datarecovery;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.venice.client.store.AvroSpecificStoreClient;
import com.linkedin.venice.controller.Admin.OfflinePushStatusInfo;
import com.linkedin.venice.controller.ParticipantStoreClientsManager;
import com.linkedin.venice.controller.VeniceHelixAdmin;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.PartitionerConfig;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionImpl;
import com.linkedin.venice.participant.protocol.ParticipantMessageKey;
import com.linkedin.venice.participant.protocol.ParticipantMessageValue;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.manager.TopicManager;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.service.ICProvider;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TestDataRecoveryManager {
  private VeniceHelixAdmin veniceAdmin;
  private ParticipantStoreClientsManager participantStoreClientsManager;
  private PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();
  private DataRecoveryManager dataRecoveryManager;
  private static final String clusterName = "testCluster";
  private static final String storeName = "testStore";
  private static final String sourceFabric = "dc-0";
  private static final String destFabric = "dc-1";
  private static final String topic = "testStore_v1";
  private static final int version = 1;

  @BeforeMethod
  public void setUp() {
    veniceAdmin = mock(VeniceHelixAdmin.class);
    participantStoreClientsManager = mock(ParticipantStoreClientsManager.class);
    pubSubTopicRepository = new PubSubTopicRepository();
    dataRecoveryManager =
        new DataRecoveryManager(veniceAdmin, Optional.empty(), pubSubTopicRepository, participantStoreClientsManager);
  }

  @Test
  public void testPrepareDataRecovery() {
    Store store = mock(Store.class);
    doReturn(true).when(store).isNativeReplicationEnabled();
    PartitionerConfig partitionerConfig = mock(PartitionerConfig.class);
    doReturn(1).when(partitionerConfig).getAmplificationFactor();
    doReturn(partitionerConfig).when(store).getPartitionerConfig();
    doReturn(store).when(veniceAdmin).getStore(clusterName, storeName);
    dataRecoveryManager.prepareStoreVersionForDataRecovery(clusterName, storeName, destFabric, version);
    verify(veniceAdmin, times(1)).deleteOneStoreVersion(clusterName, storeName, version, false, false);
    verify(veniceAdmin, times(1)).deleteParticipantStoreKillMessage(clusterName, topic);
    doReturn(1).when(store).getCurrentVersion();
    doReturn(true).when(veniceAdmin).isClusterWipeAllowed(clusterName);
    dataRecoveryManager.prepareStoreVersionForDataRecovery(clusterName, storeName, destFabric, version);
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
    assertTrue(captor.getValue().getPushJobId().startsWith("data-recovery"));
  }

  @Test
  public void testIsStoreVersionKillRecordNull() throws Exception {
    String versionTopic = Version.composeKafkaTopic(storeName, version);
    Store store = mock(Store.class);
    when(store.getCurrentVersion()).thenReturn(1);
    when(veniceAdmin.getStore(clusterName, storeName)).thenReturn(store);
    when(store.isMigrating()).thenReturn(false);
    when(store.isNativeReplicationEnabled()).thenReturn(true);

    TopicManager topicManager = mock(TopicManager.class);
    when(veniceAdmin.getTopicManager()).thenReturn(topicManager);
    when(topicManager.containsTopic(pubSubTopicRepository.getTopic(versionTopic))).thenReturn(false);
    OfflinePushStatusInfo offlinePushStatusInfo = mock(OfflinePushStatusInfo.class);
    when(offlinePushStatusInfo.getExecutionStatus()).thenReturn(ExecutionStatus.NOT_CREATED);
    when(veniceAdmin.getOffLinePushStatus(clusterName, versionTopic)).thenReturn(offlinePushStatusInfo);

    AvroSpecificStoreClient<ParticipantMessageKey, ParticipantMessageValue> readClient =
        mock(AvroSpecificStoreClient.class);
    when(participantStoreClientsManager.getReader(clusterName)).thenReturn(readClient);
    when(readClient.get(any())).thenReturn(CompletableFuture.completedFuture(null));

    // test without icProvider
    dataRecoveryManager =
        new DataRecoveryManager(veniceAdmin, Optional.empty(), pubSubTopicRepository, participantStoreClientsManager);
    dataRecoveryManager.verifyStoreVersionIsReadyForDataRecovery(clusterName, storeName, version);

    // verify getReader is called
    verify(readClient).get(any());

    // test with icProvider
    ICProvider icProvider = mock(ICProvider.class);
    dataRecoveryManager = new DataRecoveryManager(
        veniceAdmin,
        Optional.of(icProvider),
        pubSubTopicRepository,
        participantStoreClientsManager);
    ParticipantMessageValue value = new ParticipantMessageValue();
    CompletableFuture<ParticipantMessageValue> future = new CompletableFuture<>();
    future.complete(value);
    when(readClient.get(any())).thenReturn(future);
    when(icProvider.call(any(), any())).thenReturn(future);
    Exception exception = expectThrows(
        VeniceException.class,
        () -> dataRecoveryManager.verifyStoreVersionIsReadyForDataRecovery(clusterName, storeName, version));
    assertTrue(exception.getMessage().contains("Previous kill record for " + versionTopic + " still exists"));
  }
}
