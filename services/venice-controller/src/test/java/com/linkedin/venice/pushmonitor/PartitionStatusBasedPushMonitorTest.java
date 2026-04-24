package com.linkedin.venice.pushmonitor;

import static com.linkedin.venice.LogMessages.KILLED_JOB_MESSAGE;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.COMPLETED;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.ERROR;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.STARTED;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.venice.controller.HelixAdminClient;
import com.linkedin.venice.controller.stats.DisabledPartitionStats;
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
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.HelixUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
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
        getMockControllerConfig(),
        null,
        mock(DisabledPartitionStats.class),
        getMockVeniceWriterFactory(),
        getCurrentVersionChangeNotifier());
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
        getMockControllerConfig(),
        null,
        mock(DisabledPartitionStats.class),
        getMockVeniceWriterFactory(),
        getCurrentVersionChangeNotifier());
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
    for (int i = 0; i < getNumberOfPartition(); i++) {
      Partition partition = mock(Partition.class);
      Map<Instance, HelixState> instanceToStateMap = new HashMap<>();
      instanceToStateMap.put(new Instance("instance0", "host0", 1), HelixState.STANDBY);
      instanceToStateMap.put(new Instance("instance1", "host1", 1), HelixState.STANDBY);
      instanceToStateMap.put(new Instance("instance2", "host2", 1), HelixState.LEADER);
      when(partition.getInstanceToHelixStateMap()).thenReturn(instanceToStateMap);
      when(partition.getId()).thenReturn(i);
      partitionAssignment.addPartition(partition);
      PartitionStatus partitionStatus = mock(ReadOnlyPartitionStatus.class);
      when(partitionStatus.getPartitionId()).thenReturn(i);
      when(partitionStatus.getReplicaHistoricStatusList(anyString()))
          .thenReturn(Collections.singletonList(new StatusSnapshot(COMPLETED, "")));
      pushStatus.setPartitionStatus(partitionStatus);
    }
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
    Mockito.reset(getMockAccessor());
  }

  @Test
  public void testVersionUpdateWithTargetRegionPush() {
    String topic = getTopic();
    Store store = prepareMockStore(topic, VersionStatus.STARTED, Collections.emptyMap(), null, "testRegion");
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
    for (int i = 0; i < getNumberOfPartition(); i++) {
      Partition partition = mock(Partition.class);
      Map<Instance, HelixState> instanceToStateMap = new HashMap<>();
      instanceToStateMap.put(new Instance("instance0", "host0", 1), HelixState.STANDBY);
      instanceToStateMap.put(new Instance("instance1", "host1", 1), HelixState.STANDBY);
      instanceToStateMap.put(new Instance("instance2", "host2", 1), HelixState.LEADER);
      when(partition.getInstanceToHelixStateMap()).thenReturn(instanceToStateMap);
      when(partition.getId()).thenReturn(i);
      partitionAssignment.addPartition(partition);
      PartitionStatus partitionStatus = mock(ReadOnlyPartitionStatus.class);
      when(partitionStatus.getPartitionId()).thenReturn(i);
      when(partitionStatus.getReplicaHistoricStatusList(anyString()))
          .thenReturn(Collections.singletonList(new StatusSnapshot(COMPLETED, "")));
      pushStatus.setPartitionStatus(partitionStatus);
    }
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

    // Check that version was not swapped and that its status is PUSHED
    Assert.assertEquals(getMonitor().getOfflinePushOrThrow(topic).getCurrentStatus(), ExecutionStatus.COMPLETED);
    Assert.assertEquals(store.getCurrentVersion(), 0);
    Assert.assertEquals(store.getVersion(1).getStatus(), VersionStatus.PUSHED);
    verify(currentVersionChangeNotifier, never()).onCurrentVersionChange(any(), anyString(), anyInt(), anyInt());
    Mockito.reset(getMockAccessor());
  }

  @Test
  public void testVersionUpdateWithTargetRegionPushAndSwap() {
    String topic = getTopic();
    Store store = prepareMockStore(topic, VersionStatus.STARTED, Collections.emptyMap(), null, TARGET_REGION_NAME);

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

    for (int i = 0; i < getNumberOfPartition(); i++) {
      Partition partition = mock(Partition.class);
      Map<Instance, HelixState> instanceToStateMap = new HashMap<>();
      instanceToStateMap.put(new Instance("instance0", "host0", 1), HelixState.STANDBY);
      instanceToStateMap.put(new Instance("instance1", "host1", 1), HelixState.STANDBY);
      instanceToStateMap.put(new Instance("instance2", "host2", 1), HelixState.LEADER);
      when(partition.getInstanceToHelixStateMap()).thenReturn(instanceToStateMap);
      when(partition.getId()).thenReturn(i);
      partitionAssignment.addPartition(partition);
      PartitionStatus partitionStatus = mock(ReadOnlyPartitionStatus.class);
      when(partitionStatus.getPartitionId()).thenReturn(i);
      when(partitionStatus.getReplicaHistoricStatusList(anyString()))
          .thenReturn(Collections.singletonList(new StatusSnapshot(COMPLETED, "")));
      pushStatus.setPartitionStatus(partitionStatus);
    }

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

    // The version should be swapped since region matches targetSwapRegion and swap is not deferred any further
    Assert.assertEquals(getMonitor().getOfflinePushOrThrow(topic).getCurrentStatus(), ExecutionStatus.COMPLETED);
    Assert.assertEquals(store.getCurrentVersion(), 1);
    Assert.assertEquals(store.getVersion(1).getStatus(), VersionStatus.ONLINE);
    verify(currentVersionChangeNotifier, atLeastOnce()).onCurrentVersionChange(any(), anyString(), eq(1), anyInt());
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
    for (int i = 0; i < getNumberOfPartition(); i++) {
      Partition partition = mock(Partition.class);
      Map<Instance, HelixState> instanceToStateMap = new HashMap<>();
      instanceToStateMap.put(new Instance("instance0", "host0", 1), HelixState.STANDBY);
      instanceToStateMap.put(new Instance("instance1", "host1", 1), HelixState.STANDBY);
      instanceToStateMap.put(new Instance("instance2", "host2", 1), HelixState.LEADER);
      when(partition.getInstanceToHelixStateMap()).thenReturn(instanceToStateMap);
      when(partition.getId()).thenReturn(i);
      partitionAssignment.addPartition(partition);
      PartitionStatus partitionStatus = mock(ReadOnlyPartitionStatus.class);
      when(partitionStatus.getPartitionId()).thenReturn(i);
      when(partitionStatus.getReplicaHistoricStatusList(anyString()))
          .thenReturn(Collections.singletonList(new StatusSnapshot(ERROR, "")));
      pushStatus.setPartitionStatus(partitionStatus);
    }
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
    Mockito.reset(getMockAccessor());
  }

  @Test(timeOut = 30 * Time.MS_PER_SECOND)
  public void testOnExternalViewChangeDisablePartition() {
    String disabledHostName = "disabled_host";
    Instance[] instances = { new Instance("a", "a", 1), new Instance(disabledHostName, "disabledHostName", 2),
        new Instance("b", disabledHostName, 3), new Instance("d", "d", 4), new Instance("e", "e", 5) };
    // Setup a store where two of its partitions has exactly one error replica.
    Store store = getStoreWithCurrentVersion();
    String resourceName = store.getVersion(store.getCurrentVersion()).kafkaTopicName();
    EnumMap<HelixState, List<Instance>> errorStateInstanceMap = new EnumMap<>(HelixState.class);
    EnumMap<HelixState, List<Instance>> healthyStateInstanceMap = new EnumMap<>(HelixState.class);
    errorStateInstanceMap.put(HelixState.ERROR, Collections.singletonList(instances[0]));
    // if a replica is error, then the left should be 1 leader and 1 standby.
    errorStateInstanceMap.put(HelixState.LEADER, Collections.singletonList(instances[1]));
    errorStateInstanceMap.put(HelixState.OFFLINE, Collections.singletonList(instances[2]));
    healthyStateInstanceMap.put(HelixState.LEADER, Collections.singletonList(instances[0]));
    healthyStateInstanceMap.put(HelixState.STANDBY, Arrays.asList(instances[1], instances[2]));

    Partition errorPartition0 = new Partition(0, errorStateInstanceMap, new EnumMap<>(ExecutionStatus.class));
    Partition errorPartition1 = new Partition(1, errorStateInstanceMap, new EnumMap<>(ExecutionStatus.class));
    Partition healthyPartition2 = new Partition(2, healthyStateInstanceMap, new EnumMap<>(ExecutionStatus.class));
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
    replicaStatuses.add(new ReplicaStatus(disabledHostName));

    replicaStatuses.get(2).updateStatus(ERROR);
    partitionStatus.setReplicaStatuses(replicaStatuses);
    offlinePushStatus.setPartitionStatus(partitionStatus);
    partitionStatus = new PartitionStatus(1);
    List<ReplicaStatus> replicaStatuses1 = new ArrayList<>(3);
    replicaStatuses1.add(new ReplicaStatus("a"));
    replicaStatuses1.add(new ReplicaStatus("c"));
    replicaStatuses1.add(new ReplicaStatus(disabledHostName));
    replicaStatuses1.get(2).updateStatus(ERROR);
    partitionStatus.setReplicaStatuses(replicaStatuses1);
    offlinePushStatus.setPartitionStatus(partitionStatus);
    CachedReadOnlyStoreRepository readOnlyStoreRepository = mock(CachedReadOnlyStoreRepository.class);
    doReturn(Collections.singletonList(store)).when(readOnlyStoreRepository).getAllStores();
    doReturn(store).when(getMockStoreRepo()).getStore(store.getName());
    AbstractPushMonitor pushMonitor = getPushMonitor(new MockStoreCleaner(clusterLockManager));
    Map<String, List<String>> map = new HashMap<>();
    String kafkaTopic = Version.composeKafkaTopic(store.getName(), 1);
    map.put(kafkaTopic, Collections.singletonList(HelixUtils.getPartitionName(kafkaTopic, 0)));
    doReturn(map).when(helixAdminClient).getDisabledPartitionsMap(anyString(), anyString());
    doReturn(true).when(mockRoutingDataRepo).containsKafkaTopic(anyString());
    doReturn(partitionAssignment1).when(mockRoutingDataRepo).getPartitionAssignments(anyString());
    pushMonitor.startMonitorOfflinePush(resourceName, 3, 3, OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION);

    pushMonitor.updatePushStatus(offlinePushStatus, STARTED, Optional.empty());
    pushMonitor.onExternalViewChange(partitionAssignment1);

    ExecutionStatusWithDetails executionStatusWithDetails = offlinePushStatus.getStrategy()
        .getPushStatusDecider()
        .checkPushStatusAndDetailsByPartitionsStatus(offlinePushStatus, partitionAssignment1, null);
    Assert.assertEquals(executionStatusWithDetails.getStatus(), STARTED);

    verify(helixAdminClient, times(1)).getDisabledPartitionsMap(eq(getClusterName()), eq(disabledHostName));

    StatusSnapshot snapshot = new StatusSnapshot(ERROR, "1.2");
    snapshot.setIncrementalPushVersion(KILLED_JOB_MESSAGE + store.getName());
    replicaStatuses1.get(2).setStatusHistory(Arrays.asList(snapshot));
    PartitionStatus partitionStatus1 = new PartitionStatus(0);
    partitionStatus1.updateReplicaStatus(disabledHostName, ERROR, KILLED_JOB_MESSAGE + store.getName());
    offlinePushStatus.setPartitionStatus(partitionStatus1);

    offlinePushStatus.getStrategy()
        .getPushStatusDecider()
        .checkPushStatusAndDetailsByPartitionsStatus(
            offlinePushStatus,
            partitionAssignment1,
            new DisableReplicaCallback() {
              @Override
              public void disableReplica(String instance, int partitionId) {
              }

              @Override
              public boolean isReplicaDisabled(String instance, int partitionId) {
                return false;
              }
            });
    verify(helixAdminClient, times(0)).enablePartition(anyBoolean(), anyString(), anyString(), anyString(), anyList());
  }

  private Store getStoreWithCurrentVersion() {
    Store store = TestUtils.getRandomStore();
    store.addVersion(new VersionImpl(store.getName(), 1, "", 3));
    store.setCurrentVersion(1);
    return store;
  }

  /**
   * Tests that RF tuning is applied during version swap in the push monitor's handleCompletedPush path.
   * When enabled: version RF metadata and Helix IdealState are updated for both current and backup.
   * When disabled: no RF changes occur.
   */
  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testRfTuningOnVersionSwap(boolean rfTuningEnabled) {
    // Configure RF tuning on the mock controller config
    when(getMockControllerConfig().isRfTuningEnabled()).thenReturn(rfTuningEnabled);
    if (rfTuningEnabled) {
      when(getMockControllerConfig().getCurrentVersionRfCount()).thenReturn(4);
      when(getMockControllerConfig().getCurrentVersionMinActiveReplicaCount()).thenReturn(3);
      when(getMockControllerConfig().getBackupVersionRfCount()).thenReturn(2);
      when(getMockControllerConfig().getBackupVersionMinActiveReplicaCount()).thenReturn(1);
    }

    // Create a fresh helixAdminClient mock for this test to isolate verification
    HelixAdminClient testHelixAdminClient = mock(HelixAdminClient.class);

    // Create a monitor with the updated config
    AbstractPushMonitor testMonitor = new PartitionStatusBasedPushMonitor(
        getClusterName(),
        getMockAccessor(),
        getMockStoreCleaner(),
        getMockStoreRepo(),
        getMockRoutingDataRepo(),
        getMockPushHealthStats(),
        mock(RealTimeTopicSwitcher.class),
        getClusterLockManager(),
        getAggregateRealTimeSourceKafkaUrl(),
        Collections.emptyList(),
        testHelixAdminClient,
        getMockControllerConfig(),
        null,
        mock(DisabledPartitionStats.class),
        getMockVeniceWriterFactory(),
        getCurrentVersionChangeNotifier());

    // Prepare mock store with v1 (current) and v2 (new push completing)
    // Use mock versions to avoid ReadOnlyStore wrapper issues
    String storeName = getStoreName();
    String topicV2 = Version.composeKafkaTopic(storeName, 2);

    Store store = mock(Store.class);
    when(store.getName()).thenReturn(storeName);
    when(store.getCurrentVersion()).thenReturn(1); // v1 is current
    when(store.isEnableWrites()).thenReturn(true);

    Version v1 = mock(Version.class);
    when(v1.getNumber()).thenReturn(1);
    when(v1.getStatus()).thenReturn(VersionStatus.ONLINE);
    when(v1.kafkaTopicName()).thenReturn(Version.composeKafkaTopic(storeName, 1));

    Version v2 = mock(Version.class);
    when(v2.getNumber()).thenReturn(2);
    when(v2.getStatus()).thenReturn(VersionStatus.STARTED);
    when(v2.kafkaTopicName()).thenReturn(topicV2);
    when(v2.isVersionSwapDeferred()).thenReturn(false);
    when(v2.getTargetSwapRegion()).thenReturn("");

    when(store.getVersion(1)).thenReturn(v1);
    when(store.getVersion(2)).thenReturn(v2);
    when(store.containsVersion(1)).thenReturn(true);
    when(store.containsVersion(2)).thenReturn(true);
    when(store.getVersions()).thenReturn(Arrays.asList(v1, v2));

    // setCurrentVersion needs to update the return value
    Mockito.doAnswer(inv -> {
      when(store.getCurrentVersion()).thenReturn(inv.getArgument(0));
      return null;
    }).when(store).setCurrentVersion(anyInt());

    doReturn(store).when(getMockStoreRepo()).getStore(storeName);

    // Start monitoring the push for v2
    testMonitor.startMonitorOfflinePush(topicV2, 1, 3, OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION);

    // Trigger completed push which invokes version swap + RF tuning
    testMonitor.handleCompletedPush(topicV2);

    if (rfTuningEnabled) {
      // Verify version metadata RF was updated
      verify(v2).setReplicationFactor(4);
      verify(v1).setReplicationFactor(2);

      // Verify Helix IdealState updates
      verify(testHelixAdminClient).updateIdealState(getClusterName(), Version.composeKafkaTopic(storeName, 2), 3, 4);
      verify(testHelixAdminClient).updateIdealState(getClusterName(), Version.composeKafkaTopic(storeName, 1), 1, 2);
    } else {
      // Verify no RF changes when disabled
      verify(v1, never()).setReplicationFactor(anyInt());
      verify(v2, never()).setReplicationFactor(anyInt());
      verify(testHelixAdminClient, never()).updateIdealState(anyString(), anyString(), anyInt(), anyInt());
    }
  }
}
