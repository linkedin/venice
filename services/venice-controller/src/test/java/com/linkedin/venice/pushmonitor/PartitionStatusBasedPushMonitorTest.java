package com.linkedin.venice.pushmonitor;

import static com.linkedin.venice.pushmonitor.ExecutionStatus.*;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.venice.controller.HelixAdminClient;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.CachedReadOnlyStoreRepository;
import com.linkedin.venice.helix.HelixState;
import com.linkedin.venice.ingestion.control.RealTimeTopicSwitcher;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.OfflinePushStrategy;
import com.linkedin.venice.meta.Partition;
import com.linkedin.venice.meta.PartitionAssignment;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreCleaner;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionImpl;
import com.linkedin.venice.utils.HelixUtils;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


public class PartitionStatusBasedPushMonitorTest extends AbstractPushMonitorTest {
  HelixAdminClient helixAdminClient = mock(HelixAdminClient.class);

  @Override
  protected AbstractPushMonitor getPushMonitor(StoreCleaner storeCleaner) {
    return new PartitionStatusBasedPushMonitor(
        getClusterName(),
        getMockAccessor(),
        storeCleaner,
        getMockStoreRepo(),
        getMockRoutingDataRepo(),
        getMockPushHealthStats(),
        mock(RealTimeTopicSwitcher.class),
        getClusterLockManager(),
        getAggregateRealTimeSourceKafkaUrl(),
        Collections.emptyList(),
        helixAdminClient,
        true);
  }

  @Override
  protected AbstractPushMonitor getPushMonitor(RealTimeTopicSwitcher mockRealTimeTopicSwitcher) {
    return new PartitionStatusBasedPushMonitor(
        getClusterName(),
        getMockAccessor(),
        getMockStoreCleaner(),
        getMockStoreRepo(),
        getMockRoutingDataRepo(),
        getMockPushHealthStats(),
        mockRealTimeTopicSwitcher,
        getClusterLockManager(),
        getAggregateRealTimeSourceKafkaUrl(),
        Collections.emptyList(),
        mock(HelixAdminClient.class),
        true);
  }

  @Test
  public void testLoadRunningPushWhichIsNotUpdateToDate() {
    String topic = getTopic();
    Store store = prepareMockStore(topic);
    List<OfflinePushStatus> statusList = new ArrayList<>();
    OfflinePushStatus pushStatus = new OfflinePushStatus(
        topic,
        getNumberOfPartition(),
        getReplicationFactor(),
        OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION);
    statusList.add(pushStatus);
    doReturn(statusList).when(getMockAccessor()).loadOfflinePushStatusesAndPartitionStatuses();
    PartitionAssignment partitionAssignment = new PartitionAssignment(topic, getNumberOfPartition());
    doReturn(true).when(getMockRoutingDataRepo()).containsKafkaTopic(eq(topic));
    doReturn(partitionAssignment).when(getMockRoutingDataRepo()).getPartitionAssignments(topic);
    PushStatusDecider decider = mock(PushStatusDecider.class);
    Pair<ExecutionStatus, Optional<String>> statusAndDetails = new Pair<>(ExecutionStatus.COMPLETED, Optional.empty());
    doReturn(statusAndDetails).when(decider)
        .checkPushStatusAndDetailsByPartitionsStatus(pushStatus, partitionAssignment, null);
    PushStatusDecider.updateDecider(OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION, decider);
    when(getMockAccessor().getOfflinePushStatusAndItsPartitionStatuses(Mockito.anyString())).thenAnswer(invocation -> {
      String kafkaTopic = invocation.getArgument(0);
      for (OfflinePushStatus status: statusList) {
        if (status.getKafkaTopic().equals(kafkaTopic)) {
          return status;
        }
      }
      return null;
    });
    getMonitor().loadAllPushes();
    verify(getMockStoreRepo(), atLeastOnce()).updateStore(store);
    verify(getMockStoreCleaner(), atLeastOnce()).retireOldStoreVersions(anyString(), anyString(), eq(false), anyInt());
    Assert.assertEquals(getMonitor().getOfflinePushOrThrow(topic).getCurrentStatus(), ExecutionStatus.COMPLETED);
    // After offline push completed, bump up the current version of this store.
    Assert.assertEquals(store.getCurrentVersion(), 1);

    // set the push status decider back
    PushStatusDecider.updateDecider(
        OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION,
        new WaitNMinusOnePushStatusDecider());
    Mockito.reset(getMockAccessor());
  }

  @Test
  public void testLoadRunningPushWhichIsNotUpdateToDateAndDeletionError() {
    String topic = getTopic();
    Store store = prepareMockStore(topic);
    List<OfflinePushStatus> statusList = new ArrayList<>();
    OfflinePushStatus pushStatus = new OfflinePushStatus(
        topic,
        getNumberOfPartition(),
        getReplicationFactor(),
        OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION);
    statusList.add(pushStatus);
    doReturn(statusList).when(getMockAccessor()).loadOfflinePushStatusesAndPartitionStatuses();
    PartitionAssignment partitionAssignment = new PartitionAssignment(topic, getNumberOfPartition());
    doReturn(true).when(getMockRoutingDataRepo()).containsKafkaTopic(eq(topic));
    doReturn(partitionAssignment).when(getMockRoutingDataRepo()).getPartitionAssignments(topic);
    PushStatusDecider decider = mock(PushStatusDecider.class);
    Pair<ExecutionStatus, Optional<String>> statusAndDetails = new Pair<>(ExecutionStatus.ERROR, Optional.empty());
    doReturn(statusAndDetails).when(decider)
        .checkPushStatusAndDetailsByPartitionsStatus(pushStatus, partitionAssignment, null);
    PushStatusDecider.updateDecider(OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION, decider);
    doThrow(new VeniceException("Could not delete.")).when(getMockStoreCleaner())
        .deleteOneStoreVersion(anyString(), anyString(), anyInt());
    when(getMockAccessor().getOfflinePushStatusAndItsPartitionStatuses(Mockito.anyString())).thenAnswer(invocation -> {
      String kafkaTopic = invocation.getArgument(0);
      for (OfflinePushStatus status: statusList) {
        if (status.getKafkaTopic().equals(kafkaTopic)) {
          return status;
        }
      }
      return null;
    });
    getMonitor().loadAllPushes();
    verify(getMockStoreRepo(), atLeastOnce()).updateStore(store);
    verify(getMockStoreCleaner(), atLeastOnce()).deleteOneStoreVersion(anyString(), anyString(), anyInt());
    Assert.assertEquals(getMonitor().getOfflinePushOrThrow(topic).getCurrentStatus(), ExecutionStatus.ERROR);

    // set the push status decider back
    PushStatusDecider.updateDecider(
        OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION,
        new WaitNMinusOnePushStatusDecider());
    Mockito.reset(getMockAccessor());
  }

  @Test(timeOut = 30 * Time.MS_PER_SECOND)
  public void testOnExternalViewChangeDisablePartition() {
    Instance[] instances = { new Instance("a", "a", 1), new Instance("b", "b", 2), new Instance("c", "c", 3),
        new Instance("d", "d", 4), new Instance("e", "e", 5) };
    // Setup a store where two of its partitions has exactly one error replica.
    Store store = getStoreWithCurrentVersion();
    String resourceName = store.getVersion(store.getCurrentVersion()).get().kafkaTopicName();
    Map<String, List<Instance>> errorStateInstanceMap = new HashMap<>();
    Map<String, List<Instance>> healthyStateInstanceMap = new HashMap<>();
    errorStateInstanceMap.put(HelixState.ERROR_STATE, Collections.singletonList(instances[0]));
    // if a replica is error, then the left should be 1 leader and 1 standby.
    errorStateInstanceMap.put(HelixState.LEADER_STATE, Collections.singletonList(instances[1]));
    errorStateInstanceMap.put(HelixState.OFFLINE_STATE, Collections.singletonList(instances[2]));
    healthyStateInstanceMap.put(HelixState.LEADER_STATE, Collections.singletonList(instances[0]));
    healthyStateInstanceMap.put(HelixState.STANDBY_STATE, Arrays.asList(instances[1], instances[2]));

    Partition errorPartition0 = new Partition(0, errorStateInstanceMap);
    Partition errorPartition1 = new Partition(1, errorStateInstanceMap);
    Partition healthyPartition2 = new Partition(2, healthyStateInstanceMap);
    PartitionAssignment partitionAssignment1 = new PartitionAssignment(resourceName, 3);
    partitionAssignment1.addPartition(errorPartition0);
    partitionAssignment1.addPartition(errorPartition1);
    partitionAssignment1.addPartition(healthyPartition2);
    // Mock a post reset assignment where 2 of the partition remains in error state
    OfflinePushStatus offlinePushStatus =
        new OfflinePushStatus(resourceName, 3, 3, OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION);
    PartitionStatus partitionStatus = new PartitionStatus(0);
    List<ReplicaStatus> replicaStatuses = new ArrayList<>(3);
    replicaStatuses.add(new ReplicaStatus("a"));
    replicaStatuses.add(new ReplicaStatus("c"));
    replicaStatuses.add(new ReplicaStatus("b"));

    replicaStatuses.get(2).updateStatus(ERROR);
    partitionStatus.setReplicaStatuses(replicaStatuses);
    offlinePushStatus.setPartitionStatus(partitionStatus);
    partitionStatus = new PartitionStatus(1);
    List<ReplicaStatus> replicaStatuses1 = new ArrayList<>(3);
    replicaStatuses1.add(new ReplicaStatus("a"));
    replicaStatuses1.add(new ReplicaStatus("c"));
    replicaStatuses1.add(new ReplicaStatus("b"));
    replicaStatuses1.get(2).updateStatus(ERROR);
    partitionStatus.setReplicaStatuses(replicaStatuses1);
    offlinePushStatus.setPartitionStatus(partitionStatus);
    CachedReadOnlyStoreRepository readOnlyStoreRepository = mock(CachedReadOnlyStoreRepository.class);
    doReturn(Arrays.asList(store)).when(readOnlyStoreRepository).getAllStores();
    AbstractPushMonitor pushMonitor = getPushMonitor(new MockStoreCleaner(clusterLockManager));
    Map<String, List<String>> map = new HashMap<>();
    String kafkaTopic = Version.composeKafkaTopic(store.getName(), 1);
    map.put(kafkaTopic, Arrays.asList(HelixUtils.getPartitionName(kafkaTopic, 0)));
    doReturn(map).when(helixAdminClient).getDisabledPartitionsMap(anyString(), anyString());
    doReturn(true).when(mockRoutingDataRepo).containsKafkaTopic(anyString());
    doReturn(partitionAssignment1).when(mockRoutingDataRepo).getPartitionAssignments(anyString());
    pushMonitor.startMonitorOfflinePush(resourceName, 3, 3, OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION);

    pushMonitor.updatePushStatus(offlinePushStatus, STARTED, Optional.empty());
    pushMonitor.onExternalViewChange(partitionAssignment1);

    Pair<ExecutionStatus, Optional<String>> statusOptionalPair =
        PushStatusDecider.getDecider(offlinePushStatus.getStrategy())
            .checkPushStatusAndDetailsByPartitionsStatus(offlinePushStatus, partitionAssignment1, null);
    Assert.assertEquals(statusOptionalPair.getFirst(), STARTED);

    // Should be reset 2 times on 2 error replicas
    verify(helixAdminClient, times(1)).enablePartition(
        eq(false),
        anyString(),
        anyString(),
        anyString(),
        eq(Collections.singletonList(HelixUtils.getPartitionName(offlinePushStatus.getKafkaTopic(), 0))));
    /*    verify(helixAdminClient, times(1)).enablePartition(
        eq(false),
        anyString(),
        anyString(),
        anyString(),
        eq(Collections.singletonList(HelixUtils.getPartitionName(offlinePushStatus.getKafkaTopic(), 1)))); */

  }

  private Store getStoreWithCurrentVersion() {
    Store store = TestUtils.getRandomStore();
    store.addVersion(new VersionImpl(store.getName(), 1, "", 3));
    store.setCurrentVersion(1);
    return store;
  }
}
