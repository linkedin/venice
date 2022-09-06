package com.linkedin.venice.pushmonitor;

import com.linkedin.venice.helix.HelixState;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.OfflinePushStrategy;
import com.linkedin.venice.meta.Partition;
import com.linkedin.venice.meta.PartitionAssignment;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.ReadStrategy;
import com.linkedin.venice.meta.RoutingDataRepository;
import com.linkedin.venice.meta.RoutingStrategy;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionImpl;
import com.linkedin.venice.meta.ZKStore;
import com.linkedin.venice.routerapi.ReplicaState;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestPartitionStatusOnlineInstanceFinder {
  private OfflinePushAccessor offlinePushAccessor = Mockito.mock(OfflinePushAccessor.class);
  private RoutingDataRepository routingDataRepo = Mockito.mock(RoutingDataRepository.class);
  private ReadOnlyStoreRepository metaDataRepo = Mockito.mock(ReadOnlyStoreRepository.class);

  private String testTopic = "testTopic_v1";
  private int testPartition = 0;
  private int partitionCount = 1;

  @Test
  public void testCanGetReadyToServeInstances() {
    PartitionStatusOnlineInstanceFinder finder = initFinder();
    List<Instance> onlineInstanceList = finder.getReadyToServeInstances(testTopic, testPartition);

    Assert.assertEquals(onlineInstanceList.size(), 1);
    Assert.assertEquals(onlineInstanceList.get(0).getNodeId(), "host1_1");
  }

  @Test
  public void testGetReplicaStates() {
    PartitionStatusOnlineInstanceFinder finder = initFinder();
    List<ReplicaState> replicaStates = finder.getReplicaStates(testTopic, testPartition);
    Assert.assertEquals(replicaStates.size(), 2, "Unexpected replication factor");
    List<String> veniceStatuses =
        Stream.of(ExecutionStatus.values()).map(ExecutionStatus::toString).collect(Collectors.toList());
    for (ReplicaState replicaState: replicaStates) {
      Assert.assertEquals(replicaState.getPartition(), 0, "Unexpected partition number");
      Assert.assertTrue(
          replicaState.getParticipantId().equals("host0_1") || replicaState.getParticipantId().equals("host1_1"));
      Assert.assertTrue(
          replicaState.getExternalViewStatus().equals(HelixState.LEADER_STATE)
              || replicaState.getExternalViewStatus().equals(HelixState.STANDBY_STATE));
      Assert.assertTrue(veniceStatuses.contains(replicaState.getVenicePushStatus()));
      Assert.assertEquals(
          replicaState.isReadyToServe(),
          replicaState.getVenicePushStatus().equals(ExecutionStatus.COMPLETED.toString()));
    }
  }

  @Test
  public void testCanUpdateAccordingPartitionChanges() {
    PartitionStatusOnlineInstanceFinder finder = initFinder();

    PartitionStatus partitionStatus = new PartitionStatus(0);
    ReplicaStatus host0 = new ReplicaStatus("host0_1");
    ReplicaStatus host1 = new ReplicaStatus("host1_1");
    ReplicaStatus host2 = new ReplicaStatus("host2_1");
    host0.updateStatus(ExecutionStatus.COMPLETED);
    host1.updateStatus(ExecutionStatus.STARTED);
    host2.updateStatus(ExecutionStatus.COMPLETED);
    partitionStatus.setReplicaStatuses(Arrays.asList(host0, host1, host2));

    finder.onPartitionStatusChange(testTopic, ReadOnlyPartitionStatus.fromPartitionStatus(partitionStatus));
    List<Instance> onlineInstanceList = finder.getReadyToServeInstances(testTopic, testPartition);

    // since host2 is not listed in RoutingDataRepo, it's supposed to be excluded from online instance list
    Assert.assertEquals(onlineInstanceList.size(), 1);
    Assert.assertEquals(onlineInstanceList.get(0).getNodeId(), "host0_1");

    host1.updateStatus(ExecutionStatus.COMPLETED);
    onlineInstanceList = finder.getReadyToServeInstances(testTopic, testPartition);
    Assert.assertEquals(onlineInstanceList.size(), 2);
  }

  @Test
  public void testSubscribePartitionStatusChange() {
    PartitionStatusOnlineInstanceFinder finder = initFinder();

    String storeName = "testDelayedStoreMetadataUpdate";
    int versionNumber = 1;
    String topic = Version.composeKafkaTopic(storeName, versionNumber);
    OfflinePushStatus offlinePushStatus = getMockPushStatus(topic).get(0);
    Store store = new ZKStore(
        storeName,
        "owner",
        System.currentTimeMillis(),
        PersistenceType.IN_MEMORY,
        RoutingStrategy.CONSISTENT_HASH,
        ReadStrategy.ANY_OF_ONLINE,
        OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION,
        1);
    store.setLeaderFollowerModelEnabled(true);
    Version version = new VersionImpl(storeName, versionNumber, "pushJobId");
    version.setLeaderFollowerModelEnabled(true);
    Store refreshedStore = store.cloneStore();
    refreshedStore.addVersion(version);

    Mockito.doReturn(offlinePushStatus).when(offlinePushAccessor).getOfflinePushStatusAndItsPartitionStatuses(topic);
    Mockito.doReturn(store).when(metaDataRepo).getStore(storeName);
    Mockito.doReturn(refreshedStore).when(metaDataRepo).refreshOneStore(storeName);

    finder.handleChildChange("/cluster/OfflinePushes", Arrays.asList(topic));
    Mockito.verify(metaDataRepo, Mockito.times(1)).refreshOneStore(storeName);
    Mockito.verify(offlinePushAccessor, Mockito.times(1)).subscribePartitionStatusChange(offlinePushStatus, finder);

    storeName = "testDeletedStoreVersion";
    topic = Version.composeKafkaTopic(storeName, versionNumber);
    offlinePushStatus = getMockPushStatus(topic).get(0);
    store = new ZKStore(
        storeName,
        "owner",
        System.currentTimeMillis(),
        PersistenceType.IN_MEMORY,
        RoutingStrategy.CONSISTENT_HASH,
        ReadStrategy.ANY_OF_ONLINE,
        OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION,
        1);
    store.setLeaderFollowerModelEnabled(true);
    version = new VersionImpl(storeName, versionNumber, "pushJobId");
    version.setLeaderFollowerModelEnabled(true);
    store.addVersion(version);
    store.deleteVersion(versionNumber);

    Mockito.doReturn(offlinePushStatus).when(offlinePushAccessor).getOfflinePushStatusAndItsPartitionStatuses(topic);
    Mockito.doReturn(store).when(metaDataRepo).getStore(storeName);
    Mockito.doReturn(store).when(metaDataRepo).refreshOneStore(storeName);

    finder.handleChildChange("/cluster/OfflinePushes", Arrays.asList(topic));
    Mockito.verify(metaDataRepo, Mockito.times(1)).refreshOneStore(storeName);
    Mockito.verify(offlinePushAccessor, Mockito.never()).subscribePartitionStatusChange(offlinePushStatus, finder);
  }

  @Test
  public void testGetLatestPartitionStatus() {
    // The snapshots have different times from earlier to later
    StatusSnapshot snapshot1 = new StatusSnapshot(ExecutionStatus.ERROR, LocalDateTime.now().toString());
    StatusSnapshot snapshot2 = new StatusSnapshot(ExecutionStatus.STARTED, LocalDateTime.now().toString());
    StatusSnapshot snapshot3 = new StatusSnapshot(ExecutionStatus.COMPLETED, LocalDateTime.now().toString());
    List<StatusSnapshot> historicStatusList = new ArrayList<>();
    historicStatusList.add(snapshot1);
    // The latest in time partition status should be returned
    Assert.assertEquals(ExecutionStatus.ERROR, PushStatusDecider.getReplicaCurrentStatus(historicStatusList));
    historicStatusList.add(snapshot2);
    Assert.assertEquals(ExecutionStatus.STARTED, PushStatusDecider.getReplicaCurrentStatus(historicStatusList));
    historicStatusList.add(snapshot3);
    Assert.assertEquals(ExecutionStatus.COMPLETED, PushStatusDecider.getReplicaCurrentStatus(historicStatusList));
  }

  private PartitionStatusOnlineInstanceFinder initFinder() {
    PartitionStatusOnlineInstanceFinder finder =
        new PartitionStatusOnlineInstanceFinder(metaDataRepo, offlinePushAccessor, routingDataRepo);

    Map<String, List<Instance>> instances = getMockInstances();
    PartitionAssignment partitionAssignment = new PartitionAssignment(testTopic, partitionCount);
    partitionAssignment.addPartition(new Partition(0, instances));
    Mockito.doReturn(partitionAssignment).when(routingDataRepo).getPartitionAssignments(testTopic);
    Mockito.doReturn(instances).when(routingDataRepo).getAllInstances(testTopic, 0);
    Mockito.doReturn(getMockPushStatus(testTopic))
        .when(offlinePushAccessor)
        .loadOfflinePushStatusesAndPartitionStatuses();
    Mockito.doReturn(partitionCount).when(routingDataRepo).getNumberOfPartitions(testTopic);

    finder.refresh();

    return finder;
  }

  private Map<String, List<Instance>> getMockInstances() {
    Map<String, List<Instance>> instanceMap = new HashMap<>();
    instanceMap.put(HelixState.LEADER_STATE, Arrays.asList(new Instance("host0_1", "host0", 1)));
    instanceMap.put(HelixState.STANDBY_STATE, Arrays.asList(new Instance("host1_1", "host1", 1)));

    return instanceMap;
  }

  private List<OfflinePushStatus> getMockPushStatus(String topic) {
    OfflinePushStatus offlinePushStatus =
        new OfflinePushStatus(topic, partitionCount, 2, OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION);
    PartitionStatus partitionStatus = new PartitionStatus(0);
    ReplicaStatus host0 = new ReplicaStatus("host0_1");
    ReplicaStatus host1 = new ReplicaStatus("host1_1");
    host1.updateStatus(ExecutionStatus.COMPLETED);
    partitionStatus.setReplicaStatuses(Arrays.asList(host0, host1));
    offlinePushStatus.setPartitionStatus(partitionStatus);

    return Arrays.asList(offlinePushStatus);
  }
}
