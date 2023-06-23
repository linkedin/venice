package com.linkedin.venice.pushmonitor;

import static com.linkedin.venice.pushmonitor.ExecutionStatus.END_OF_INCREMENTAL_PUSH_RECEIVED;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.ERROR;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.STARTED;
import static com.linkedin.venice.pushmonitor.PartitionStatusBasedPushMonitor.MAX_PUSH_TO_KEEP;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.only;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.venice.controller.HelixAdminClient;
import com.linkedin.venice.controller.VeniceControllerConfig;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.CachedReadOnlyStoreRepository;
import com.linkedin.venice.helix.HelixCustomizedViewOfflinePushRepository;
import com.linkedin.venice.helix.HelixState;
import com.linkedin.venice.helix.ResourceAssignment;
import com.linkedin.venice.ingestion.control.RealTimeTopicSwitcher;
import com.linkedin.venice.meta.BufferReplayPolicy;
import com.linkedin.venice.meta.DataReplicationPolicy;
import com.linkedin.venice.meta.HybridStoreConfigImpl;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.OfflinePushStrategy;
import com.linkedin.venice.meta.Partition;
import com.linkedin.venice.meta.PartitionAssignment;
import com.linkedin.venice.meta.ReadWriteStoreRepository;
import com.linkedin.venice.meta.RoutingDataRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreCleaner;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionImpl;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.pushstatushelper.PushStatusStoreReader;
import com.linkedin.venice.utils.HelixUtils;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.locks.AutoCloseableLock;
import com.linkedin.venice.utils.locks.ClusterLockManager;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class PartitionStatusBasedPushMonitorTest {
  private final OfflinePushAccessor mockAccessor = mock(OfflinePushAccessor.class);
  private PartitionStatusBasedPushMonitor monitor;
  private final ReadWriteStoreRepository mockStoreRepo = mock(ReadWriteStoreRepository.class);
  private final RoutingDataRepository mockRoutingDataRepo = mock(RoutingDataRepository.class);
  private final StoreCleaner mockStoreCleaner = mock(StoreCleaner.class);
  private final AggPushHealthStats mockPushHealthStats = mock(AggPushHealthStats.class);

  private final VeniceControllerConfig mockControllerConfig = mock(VeniceControllerConfig.class);
  private final HelixAdminClient helixAdminClient = mock(HelixAdminClient.class);

  private final static String clusterName = Utils.getUniqueString("test_cluster");
  private ClusterLockManager clusterLockManager;

  private final static String aggregateRealTimeSourceKafkaUrl = "aggregate-real-time-source-kafka-url";
  private String storeName;
  private String topic;

  private final static int numberOfPartition = 1;
  private final static int replicationFactor = 3;

  @BeforeMethod
  public void setUp() {
    storeName = Utils.getUniqueString("test_store");
    topic = storeName + "_v1";

    clusterLockManager = new ClusterLockManager(clusterName);
    when(mockControllerConfig.isErrorLeaderReplicaFailOverEnabled()).thenReturn(true);
    when(mockControllerConfig.isDaVinciPushStatusEnabled()).thenReturn(true);
    when(mockControllerConfig.getDaVinciPushStatusScanIntervalInSeconds()).thenReturn(5);
    when(mockControllerConfig.getOffLineJobWaitTimeInMilliseconds()).thenReturn(120000L);
    when(mockControllerConfig.getDaVinciPushStatusScanThreadNumber()).thenReturn(4);
    monitor = getPushMonitor(mockStoreCleaner);
  }

  @Test
  public void testLoadRunningPushWhichIsNotUpdateToDate() {
    String topic = this.topic;
    Store store = prepareMockStore(topic);
    List<OfflinePushStatus> statusList = new ArrayList<>();
    OfflinePushStatus pushStatus = new OfflinePushStatus(
        topic,
        numberOfPartition,
        replicationFactor,
        OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION);
    statusList.add(pushStatus);
    doReturn(statusList).when(mockAccessor).loadOfflinePushStatusesAndPartitionStatuses();
    PartitionAssignment partitionAssignment = new PartitionAssignment(topic, numberOfPartition);
    doReturn(true).when(mockRoutingDataRepo).containsKafkaTopic(eq(topic));
    doReturn(partitionAssignment).when(mockRoutingDataRepo).getPartitionAssignments(topic);
    PushStatusDecider decider = mock(PushStatusDecider.class);
    Pair<ExecutionStatus, Optional<String>> statusAndDetails = new Pair<>(ExecutionStatus.COMPLETED, Optional.empty());
    doReturn(statusAndDetails).when(decider)
        .checkPushStatusAndDetailsByPartitionsStatus(pushStatus, partitionAssignment, null);
    PushStatusDecider.updateDecider(OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION, decider);
    when(mockAccessor.getOfflinePushStatusAndItsPartitionStatuses(Mockito.anyString())).thenAnswer(invocation -> {
      String kafkaTopic = invocation.getArgument(0);
      for (OfflinePushStatus status: statusList) {
        if (status.getKafkaTopic().equals(kafkaTopic)) {
          return status;
        }
      }
      return null;
    });
    monitor.loadAllPushes();
    verify(mockStoreRepo, atLeastOnce()).updateStore(store);
    verify(mockStoreCleaner, atLeastOnce()).retireOldStoreVersions(anyString(), anyString(), eq(false), anyInt());
    Assert.assertEquals(monitor.getOfflinePushOrThrow(topic).getCurrentStatus(), ExecutionStatus.COMPLETED);
    // After offline push completed, bump up the current version of this store.
    Assert.assertEquals(store.getCurrentVersion(), 1);

    // set the push status decider back
    PushStatusDecider.updateDecider(
        OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION,
        new WaitNMinusOnePushStatusDecider());
    Mockito.reset(mockAccessor);
  }

  @Test
  public void testLoadRunningPushWhichIsNotUpdateToDateAndDeletionError() {
    String topic = this.topic;
    Store store = prepareMockStore(topic);
    List<OfflinePushStatus> statusList = new ArrayList<>();
    OfflinePushStatus pushStatus = new OfflinePushStatus(
        topic,
        numberOfPartition,
        replicationFactor,
        OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION);
    statusList.add(pushStatus);
    doReturn(statusList).when(mockAccessor).loadOfflinePushStatusesAndPartitionStatuses();
    PartitionAssignment partitionAssignment = new PartitionAssignment(topic, numberOfPartition);
    doReturn(true).when(mockRoutingDataRepo).containsKafkaTopic(eq(topic));
    doReturn(partitionAssignment).when(mockRoutingDataRepo).getPartitionAssignments(topic);
    PushStatusDecider decider = mock(PushStatusDecider.class);
    Pair<ExecutionStatus, Optional<String>> statusAndDetails = new Pair<>(ExecutionStatus.ERROR, Optional.empty());
    doReturn(statusAndDetails).when(decider)
        .checkPushStatusAndDetailsByPartitionsStatus(pushStatus, partitionAssignment, null);
    PushStatusDecider.updateDecider(OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION, decider);
    doThrow(new VeniceException("Could not delete.")).when(mockStoreCleaner)
        .deleteOneStoreVersion(anyString(), anyString(), anyInt());
    when(mockAccessor.getOfflinePushStatusAndItsPartitionStatuses(Mockito.anyString())).thenAnswer(invocation -> {
      String kafkaTopic = invocation.getArgument(0);
      for (OfflinePushStatus status: statusList) {
        if (status.getKafkaTopic().equals(kafkaTopic)) {
          return status;
        }
      }
      return null;
    });
    monitor.loadAllPushes();
    verify(mockStoreRepo, atLeastOnce()).updateStore(store);
    verify(mockStoreCleaner, atLeastOnce()).deleteOneStoreVersion(anyString(), anyString(), anyInt());
    Assert.assertEquals(monitor.getOfflinePushOrThrow(topic).getCurrentStatus(), ExecutionStatus.ERROR);

    // set the push status decider back
    PushStatusDecider.updateDecider(
        OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION,
        new WaitNMinusOnePushStatusDecider());
    Mockito.reset(mockAccessor);
  }

  @Test(timeOut = 30 * Time.MS_PER_SECOND)
  public void testOnExternalViewChangeDisablePartition() {
    String disabledHostName = "disabled_host";
    Instance[] instances = { new Instance("a", "a", 1), new Instance(disabledHostName, "disabledHostName", 2),
        new Instance("b", disabledHostName, 3), new Instance("d", "d", 4), new Instance("e", "e", 5) };
    // Setup a store where two of its partitions has exactly one error replica.
    Store store = getStoreWithCurrentVersion();
    String resourceName = store.getVersion(store.getCurrentVersion()).get().kafkaTopicName();
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
    doReturn(Arrays.asList(store)).when(readOnlyStoreRepository).getAllStores();
    PartitionStatusBasedPushMonitor pushMonitor = getPushMonitor(new MockStoreCleaner(clusterLockManager));
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

    verify(helixAdminClient, times(1)).getDisabledPartitionsMap(eq(clusterName), eq(disabledHostName));
  }

  private Store getStoreWithCurrentVersion() {
    Store store = TestUtils.getRandomStore();
    store.addVersion(new VersionImpl(store.getName(), 1, "", 3));
    store.setCurrentVersion(1);
    return store;
  }

  @Test
  public void testStartMonitorOfflinePush() {
    monitor.startMonitorOfflinePush(
        topic,
        numberOfPartition,
        replicationFactor,
        OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION);
    OfflinePushStatus pushStatus = monitor.getOfflinePushOrThrow(topic);
    Assert.assertEquals(pushStatus.getCurrentStatus(), ExecutionStatus.STARTED);
    Assert.assertEquals(pushStatus.getKafkaTopic(), topic);
    Assert.assertEquals(pushStatus.getNumberOfPartition(), numberOfPartition);
    Assert.assertEquals(pushStatus.getReplicationFactor(), replicationFactor);
    verify(mockAccessor, atLeastOnce()).createOfflinePushStatusAndItsPartitionStatuses(pushStatus);
    verify(mockAccessor, atLeastOnce()).subscribePartitionStatusChange(pushStatus, monitor);
    verify(mockRoutingDataRepo, atLeastOnce()).subscribeRoutingDataChange(topic, monitor);
    doReturn(mock(ResourceAssignment.class)).when(mockRoutingDataRepo).getResourceAssignment();
    try {
      monitor.startMonitorOfflinePush(
          topic,
          numberOfPartition,
          replicationFactor,
          OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION);
      Assert.fail("Duplicated monitoring is not allowed. ");
    } catch (VeniceException e) {
    }
  }

  @Test
  public void testStartMonitorOfflinePushWhenThereIsAnExistingErrorPush() {
    monitor.startMonitorOfflinePush(
        topic,
        numberOfPartition,
        replicationFactor,
        OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION);
    OfflinePushStatus pushStatus = monitor.getOfflinePushOrThrow(topic);
    Assert.assertEquals(pushStatus.getCurrentStatus(), ExecutionStatus.STARTED);
    Assert.assertEquals(pushStatus.getKafkaTopic(), topic);
    Assert.assertEquals(pushStatus.getNumberOfPartition(), numberOfPartition);
    Assert.assertEquals(pushStatus.getReplicationFactor(), replicationFactor);
    verify(mockAccessor, atLeastOnce()).createOfflinePushStatusAndItsPartitionStatuses(pushStatus);
    verify(mockAccessor, atLeastOnce()).subscribePartitionStatusChange(pushStatus, monitor);
    verify(mockRoutingDataRepo, atLeastOnce()).subscribeRoutingDataChange(topic, monitor);
    monitor.markOfflinePushAsError(topic, "mocked_error_push");
    Assert.assertEquals(monitor.getPushStatus(topic), ExecutionStatus.ERROR);

    // Existing error push will be cleaned up to start a new push
    monitor.startMonitorOfflinePush(
        topic,
        numberOfPartition,
        replicationFactor,
        OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION);
  }

  @Test
  public void testStopMonitorOfflinePush() {
    monitor.startMonitorOfflinePush(
        topic,
        numberOfPartition,
        replicationFactor,
        OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION);
    OfflinePushStatus pushStatus = monitor.getOfflinePushOrThrow(topic);
    monitor.stopMonitorOfflinePush(topic, true, false);
    verify(mockAccessor, atLeastOnce()).deleteOfflinePushStatusAndItsPartitionStatuses(pushStatus.getKafkaTopic());
    verify(mockAccessor, atLeastOnce()).unsubscribePartitionsStatusChange(pushStatus, monitor);
    verify(mockRoutingDataRepo, atLeastOnce()).unSubscribeRoutingDataChange(topic, monitor);

    try {
      monitor.getOfflinePushOrThrow(topic);
      Assert.fail("Push status should be deleted by stopMonitorOfflinePush method");
    } catch (VeniceException e) {
    }
  }

  @Test
  public void testStopMonitorErrorOfflinePush() {
    String store = storeName;
    for (int i = 0; i < MAX_PUSH_TO_KEEP; i++) {
      String topic = Version.composeKafkaTopic(store, i);
      monitor.startMonitorOfflinePush(
          topic,
          numberOfPartition,
          replicationFactor,
          OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION);
      OfflinePushStatus pushStatus = monitor.getOfflinePushOrThrow(topic);
      pushStatus.updateStatus(ExecutionStatus.ERROR);
      monitor.stopMonitorOfflinePush(topic, true, false);
    }
    // We should keep MAX_ERROR_PUSH_TO_KEEP error push for debug.
    for (int i = 0; i < MAX_PUSH_TO_KEEP; i++) {
      Assert.assertNotNull(monitor.getOfflinePushOrThrow(Version.composeKafkaTopic(store, i)));
    }
    // Add a new error push, the oldest one should be collected.
    String topic = Version.composeKafkaTopic(store, MAX_PUSH_TO_KEEP + 1);
    monitor.startMonitorOfflinePush(
        topic,
        numberOfPartition,
        replicationFactor,
        OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION);
    OfflinePushStatus pushStatus = monitor.getOfflinePushOrThrow(topic);
    pushStatus.updateStatus(ExecutionStatus.ERROR);
    monitor.stopMonitorOfflinePush(topic, true, false);
    try {
      monitor.getOfflinePushOrThrow(Version.composeKafkaTopic(store, 0));
      Assert.fail("Oldest error push should be collected.");
    } catch (VeniceException e) {
      // expected
    }
    Assert.assertNotNull(monitor.getOfflinePushOrThrow(topic));
  }

  @Test
  public void testLoadAllPushes() {
    int statusCount = 3;
    List<OfflinePushStatus> statusList = new ArrayList<>(statusCount);
    for (int i = 0; i < statusCount; i++) {
      OfflinePushStatus pushStatus = new OfflinePushStatus(
          "testLoadAllPushes_v" + i,
          numberOfPartition,
          replicationFactor,
          OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION);
      pushStatus.setCurrentStatus(ExecutionStatus.COMPLETED);
      statusList.add(pushStatus);
    }
    doReturn(statusList).when(mockAccessor).loadOfflinePushStatusesAndPartitionStatuses();
    when(mockAccessor.getOfflinePushStatusAndItsPartitionStatuses(Mockito.anyString())).thenAnswer(invocation -> {
      String kafkaTopic = invocation.getArgument(0);
      for (OfflinePushStatus status: statusList) {
        if (status.getKafkaTopic().equals(kafkaTopic)) {
          return status;
        }
      }
      return null;
    });
    monitor.loadAllPushes();
    for (int i = 0; i < statusCount; i++) {
      Assert.assertEquals(
          monitor.getOfflinePushOrThrow("testLoadAllPushes_v" + i).getCurrentStatus(),
          ExecutionStatus.COMPLETED);
    }
  }

  @Test
  public void testClearOldErrorVersion() {
    // creating MAX_PUSH_TO_KEEP * 2 pushes. The first is successful and the rest of them are failed.
    int statusCount = MAX_PUSH_TO_KEEP * 2;
    List<OfflinePushStatus> statusList = new ArrayList<>(statusCount);
    for (int i = 0; i < statusCount; i++) {
      OfflinePushStatus pushStatus = new OfflinePushStatus(
          "testLoadAllPushes_v" + i,
          numberOfPartition,
          replicationFactor,
          OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION);

      // set all push statuses except the first to error
      if (i == 0) {
        pushStatus.setCurrentStatus(ExecutionStatus.COMPLETED);
      } else {
        pushStatus.setCurrentStatus(ExecutionStatus.ERROR);
      }
      statusList.add(pushStatus);
    }
    doReturn(statusList).when(mockAccessor).loadOfflinePushStatusesAndPartitionStatuses();

    when(mockAccessor.getOfflinePushStatusAndItsPartitionStatuses(Mockito.anyString())).thenAnswer(invocation -> {
      String kafkaTopic = invocation.getArgument(0);
      for (OfflinePushStatus status: statusList) {
        if (status.getKafkaTopic().equals(kafkaTopic)) {
          return status;
        }
      }
      return null;
    });

    monitor.loadAllPushes();
    // Make sure we delete old error pushes from accessor.
    verify(mockAccessor, times(statusCount - MAX_PUSH_TO_KEEP)).deleteOfflinePushStatusAndItsPartitionStatuses(any());

    // the first push should be persisted since it succeeded. But the next 5 pushes should be purged.
    int i = 0;
    Assert.assertEquals(monitor.getPushStatus("testLoadAllPushes_v" + i), ExecutionStatus.COMPLETED);

    for (i = 1; i <= MAX_PUSH_TO_KEEP; i++) {
      try {
        monitor.getOfflinePushOrThrow("testLoadAllPushes_v" + i);
        Assert.fail("Old error pushes should be collected after loading.");
      } catch (VeniceException e) {
        // expected
      }
    }

    for (; i < statusCount; i++) {
      Assert.assertEquals(monitor.getPushStatus("testLoadAllPushes_v" + i), ExecutionStatus.ERROR);
    }
  }

  @Test
  public void testOnRoutingDataDeleted() {
    String topic = this.topic;
    prepareMockStore(topic);
    monitor.startMonitorOfflinePush(
        topic,
        numberOfPartition,
        replicationFactor,
        OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION);
    // Resource has been deleted from the external view but still existing in the ideal state.
    doReturn(true).when(mockRoutingDataRepo).doesResourcesExistInIdealState(topic);
    monitor.onRoutingDataDeleted(topic);
    // Job should keep running.
    Assert.assertEquals(monitor.getOfflinePushOrThrow(topic).getCurrentStatus(), ExecutionStatus.STARTED);

    // Resource has been deleted from both external view and ideal state.
    doReturn(false).when(mockRoutingDataRepo).doesResourcesExistInIdealState(topic);
    monitor.onRoutingDataDeleted(topic);
    // Job should be terminated in error status.
    Assert.assertEquals(monitor.getOfflinePushOrThrow(topic).getCurrentStatus(), ExecutionStatus.ERROR);
    verify(mockPushHealthStats, times(1)).recordFailedPush(eq(storeName), anyLong());
  }

  private PushStatusStoreReader statusStoreReaderMock;
  private HelixCustomizedViewOfflinePushRepository customizedViewMock;
  private final static String incrementalPushVersion = "IncPush_42";
  private final static int partitionCountForIncPushTests = 3;
  private final static int replicationFactorForIncPushTests = 3;

  private void prepareForIncrementalPushStatusTest(
      Map<Integer, Map<CharSequence, Integer>> pushStatusMap,
      Map<Integer, Integer> completedStatusMap) {
    statusStoreReaderMock = mock(PushStatusStoreReader.class);
    customizedViewMock = mock(HelixCustomizedViewOfflinePushRepository.class);

    when(
        statusStoreReaderMock.getPartitionStatuses(
            storeName,
            Version.parseVersionFromVersionTopicName(topic),
            incrementalPushVersion,
            partitionCountForIncPushTests)).thenReturn(pushStatusMap);
    when(customizedViewMock.getCompletedStatusReplicas(topic, partitionCountForIncPushTests))
        .thenReturn(completedStatusMap);
  }

  /* Tests mutate/change count of completed status replicas to test various scenarios */
  private Map<Integer, Integer> getCompletedStatusData() {
    Map<Integer, Integer> completedStatusMap = new HashMap<>();
    for (int partitionId = 0; partitionId < partitionCountForIncPushTests; partitionId++) {
      completedStatusMap.put(partitionId, replicationFactorForIncPushTests);
    }
    return completedStatusMap;
  }

  /* Tests mutate inc push status data to test various scenarios */
  private Map<Integer, Map<CharSequence, Integer>> getPushStatusData(ExecutionStatus expectedStatus) {
    Map<Integer, Map<CharSequence, Integer>> partitionPushStatusMap = new HashMap<>();
    for (int partitionId = 0; partitionId < partitionCountForIncPushTests; partitionId++) {
      Map<CharSequence, Integer> replicaPushStatusMap = new HashMap<>();
      for (int replicaId = 0; replicaId < replicationFactorForIncPushTests; replicaId++) {
        replicaPushStatusMap.put("instance-" + replicaId, expectedStatus.getValue());
      }
      partitionPushStatusMap.put(partitionId, replicaPushStatusMap);
    }
    return partitionPushStatusMap;
  }

  @Test(description = "Expect NOT_CREATED status when incremental push statuses are empty for all partitions")
  public void testGetIncrementalPushStatusWhenPushStatusIsEmptyForAllPartitions() {
    // push statuses are missing for all partitions
    Map<Integer, Map<CharSequence, Integer>> pushStatusMap = Collections.emptyMap();
    Map<Integer, Integer> completedReplicas = Collections.emptyMap();
    prepareForIncrementalPushStatusTest(pushStatusMap, completedReplicas);

    ExecutionStatus actualStatus =
        monitor
            .getIncrementalPushStatusFromPushStatusStore(
                topic,
                incrementalPushVersion,
                customizedViewMock,
                statusStoreReaderMock,
                partitionCountForIncPushTests,
                replicationFactorForIncPushTests)
            .getFirst();
    Assert.assertEquals(actualStatus, ExecutionStatus.NOT_CREATED);
  }

  @Test(description = "Expect EOIP status when numberOfReplicasInCompletedState == replicationFactor and all these replicas have seen EOIP")
  public void testCheckIncrementalPushStatusWhenAllPartitionReplicasHaveSeenEoip() {
    Map<Integer, Map<CharSequence, Integer>> pushStatusMap = getPushStatusData(END_OF_INCREMENTAL_PUSH_RECEIVED);
    Map<Integer, Integer> completedReplicas = getCompletedStatusData();
    prepareForIncrementalPushStatusTest(pushStatusMap, completedReplicas);

    ExecutionStatus actualStatus =
        monitor
            .getIncrementalPushStatusFromPushStatusStore(
                topic,
                incrementalPushVersion,
                customizedViewMock,
                statusStoreReaderMock,
                partitionCountForIncPushTests,
                replicationFactorForIncPushTests)
            .getFirst();
    Assert.assertEquals(actualStatus, END_OF_INCREMENTAL_PUSH_RECEIVED);
  }

  @Test(description = "Expect EOIP status when for just one partition numberOfReplicasInCompletedState == (replicationFactor - 1) and only one replica of that partition has not seen EOIP yet")
  void testCheckIncrementalPushStatusWhenAllCompletedStateReplicasOfPartitionHaveSeenEoip() {
    Map<Integer, Map<CharSequence, Integer>> pushStatusMap = getPushStatusData(END_OF_INCREMENTAL_PUSH_RECEIVED);
    // simulate only one replica of partition 0 has not seen EOIP
    pushStatusMap.get(0).remove("instance-0");
    Map<Integer, Integer> completedReplicas = getCompletedStatusData();
    // simulate in partition 0 the number of replicas in completed state are one less than the replication factor
    completedReplicas.put(0, replicationFactor - 1);

    prepareForIncrementalPushStatusTest(pushStatusMap, completedReplicas);

    ExecutionStatus actualStatus =
        monitor
            .getIncrementalPushStatusFromPushStatusStore(
                topic,
                incrementalPushVersion,
                customizedViewMock,
                statusStoreReaderMock,
                partitionCountForIncPushTests,
                replicationFactorForIncPushTests)
            .getFirst();
    Assert.assertEquals(actualStatus, END_OF_INCREMENTAL_PUSH_RECEIVED);
  }

  /* The following case is unlikely but not impossible. This could be an indication of issues in other parts of the system */
  @Test(description = "Expect EOIP status when for one partition numberOfReplicasInCompletedState == (replicationFactor - 2) and only one replica of that partition has not seen EOIP yet")
  public void testCheckIncrementalPushStatusWhenNMinusOneReplicasOfPartitionHaveSeenEoip() {
    Map<Integer, Map<CharSequence, Integer>> pushStatusMap = getPushStatusData(END_OF_INCREMENTAL_PUSH_RECEIVED);
    // simulate only one replica of partition 0 has not seen EOIP
    pushStatusMap.get(0).remove("instance-0");
    Map<Integer, Integer> completedReplicas = getCompletedStatusData();
    // simulate in partition 0 the number of replicas in completed state are two less than the replication factor
    completedReplicas.put(0, replicationFactor - 2);

    prepareForIncrementalPushStatusTest(pushStatusMap, completedReplicas);

    ExecutionStatus actualStatus =
        monitor
            .getIncrementalPushStatusFromPushStatusStore(
                topic,
                incrementalPushVersion,
                customizedViewMock,
                statusStoreReaderMock,
                partitionCountForIncPushTests,
                replicationFactorForIncPushTests)
            .getFirst();
    Assert.assertEquals(actualStatus, END_OF_INCREMENTAL_PUSH_RECEIVED);
  }

  @Test(description = "Expect SOIP status when numberOfReplicasInCompletedState == replicationFactor and just one replica of one partition has not seen EOIP yet")
  public void testCheckIncrementalPushStatusWhenOneCompletedStateReplicaOfPartitionHasNotSeenEoip() {
    Map<Integer, Map<CharSequence, Integer>> pushStatusMap = getPushStatusData(END_OF_INCREMENTAL_PUSH_RECEIVED);
    // simulate only one replica of partition 0 has not seen EOIP
    pushStatusMap.get(0).remove("instance-0");
    Map<Integer, Integer> completedReplicas = getCompletedStatusData();

    prepareForIncrementalPushStatusTest(pushStatusMap, completedReplicas);

    ExecutionStatus actualStatus =
        monitor
            .getIncrementalPushStatusFromPushStatusStore(
                topic,
                incrementalPushVersion,
                customizedViewMock,
                statusStoreReaderMock,
                partitionCountForIncPushTests,
                replicationFactorForIncPushTests)
            .getFirst();
    Assert.assertEquals(actualStatus, ExecutionStatus.START_OF_INCREMENTAL_PUSH_RECEIVED);
  }

  @Test(description = "Expect SOIP status when for one partition numberOfReplicasInCompletedState == (replicationFactor - 2) and two replicas of that partition have not seen EOIP yet")
  public void testCheckIncrementalPushStatusWhenNMinusTwoReplicasOfPartitionHaveSeenEoip() {
    Map<Integer, Map<CharSequence, Integer>> pushStatusMap = getPushStatusData(END_OF_INCREMENTAL_PUSH_RECEIVED);
    // simulate two replicas of partition 0 has not seen EOIP
    pushStatusMap.get(0).put("instance-0", ExecutionStatus.START_OF_INCREMENTAL_PUSH_RECEIVED.getValue());
    pushStatusMap.get(0).put("instance-1", ExecutionStatus.START_OF_INCREMENTAL_PUSH_RECEIVED.getValue());
    Map<Integer, Integer> completedReplicas = getCompletedStatusData();
    // simulate in partition 0 the number of replicas in completed state are two less than the replication factor
    completedReplicas.put(0, replicationFactor - 2);

    prepareForIncrementalPushStatusTest(pushStatusMap, completedReplicas);

    ExecutionStatus actualStatus =
        monitor
            .getIncrementalPushStatusFromPushStatusStore(
                topic,
                incrementalPushVersion,
                customizedViewMock,
                statusStoreReaderMock,
                partitionCountForIncPushTests,
                replicationFactorForIncPushTests)
            .getFirst();
    Assert.assertEquals(actualStatus, ExecutionStatus.START_OF_INCREMENTAL_PUSH_RECEIVED);
  }

  @Test(description = "Expect SOIP status when for one partition numberOfReplicasInCompletedState == replicationFactor and just one replica of only one partition has seen SOIP")
  public void testCheckIncrementalPushStatusOnlyOneReplicasHasSeenSOIP() {
    Map<Integer, Map<CharSequence, Integer>> pushStatusMap =
        getPushStatusData(ExecutionStatus.START_OF_INCREMENTAL_PUSH_RECEIVED);
    // simulate just one replica of only one partition has seen EOIP
    pushStatusMap.get(0).remove("instance-0");
    pushStatusMap.get(0).remove("instance-1");
    for (int partitionId = 1; partitionId < partitionCountForIncPushTests; partitionId++) {
      pushStatusMap.remove(partitionId);
    }

    Map<Integer, Integer> completedReplicas = getCompletedStatusData();

    prepareForIncrementalPushStatusTest(pushStatusMap, completedReplicas);

    ExecutionStatus actualStatus =
        monitor
            .getIncrementalPushStatusFromPushStatusStore(
                topic,
                incrementalPushVersion,
                customizedViewMock,
                statusStoreReaderMock,
                partitionCountForIncPushTests,
                replicationFactorForIncPushTests)
            .getFirst();
    Assert.assertEquals(actualStatus, ExecutionStatus.START_OF_INCREMENTAL_PUSH_RECEIVED);
  }

  @Test(description = "Expect SOIP status when for one partition numberOfReplicasInCompletedState == replicationFactor and just one replica of only one partition has seen EOIP")
  public void testCheckIncrementalPushStatusOnlyOneReplicasHasSeenEOIP() {
    Map<Integer, Map<CharSequence, Integer>> pushStatusMap = getPushStatusData(END_OF_INCREMENTAL_PUSH_RECEIVED);
    // simulate just one replica of only one partition has seen EOIP
    pushStatusMap.get(0).remove("instance-0");
    pushStatusMap.get(0).remove("instance-1");
    for (int partitionId = 1; partitionId < partitionCountForIncPushTests; partitionId++) {
      pushStatusMap.remove(partitionId);
    }

    Map<Integer, Integer> completedReplicas = getCompletedStatusData();

    prepareForIncrementalPushStatusTest(pushStatusMap, completedReplicas);

    ExecutionStatus actualStatus =
        monitor
            .getIncrementalPushStatusFromPushStatusStore(
                topic,
                incrementalPushVersion,
                customizedViewMock,
                statusStoreReaderMock,
                partitionCountForIncPushTests,
                replicationFactorForIncPushTests)
            .getFirst();
    Assert.assertEquals(actualStatus, ExecutionStatus.START_OF_INCREMENTAL_PUSH_RECEIVED);
  }

  /* The following case could happen during re-balancing */
  @Test(description = "Expect EOIP status when numberOfReplicasInCompletedState == (replicationFactor) and (replicationFactor + 1) replicas have seen EOIP")
  public void testCheckIncrementalPushStatusWhenNumberOfReplicasWithEoipAreMoreThanReplicationFactor() {
    Map<Integer, Map<CharSequence, Integer>> pushStatusMap = getPushStatusData(END_OF_INCREMENTAL_PUSH_RECEIVED);
    // simulate (replicationFactor + 1) replicas have EOIP
    pushStatusMap.get(0).put("instance-3", END_OF_INCREMENTAL_PUSH_RECEIVED.getValue());
    Map<Integer, Integer> completedReplicas = getCompletedStatusData();

    prepareForIncrementalPushStatusTest(pushStatusMap, completedReplicas);

    ExecutionStatus actualStatus =
        monitor
            .getIncrementalPushStatusFromPushStatusStore(
                topic,
                incrementalPushVersion,
                customizedViewMock,
                statusStoreReaderMock,
                partitionCountForIncPushTests,
                replicationFactorForIncPushTests)
            .getFirst();
    Assert.assertEquals(actualStatus, END_OF_INCREMENTAL_PUSH_RECEIVED);
  }

  @Test(description = "Expect ERROR status when any one replica belonging to any partition has non-incremental push status")
  public void testCheckIncrementalPushStatusWhenAnyOfTheReplicaHasNonIncPushStatus() {
    Map<Integer, Map<CharSequence, Integer>> pushStatusMap = getPushStatusData(END_OF_INCREMENTAL_PUSH_RECEIVED);
    // simulate one replica has non-incremental push status
    pushStatusMap.get(0).put("instance-0", ExecutionStatus.UNKNOWN.getValue());
    Map<Integer, Integer> completedReplicas = getCompletedStatusData();

    prepareForIncrementalPushStatusTest(pushStatusMap, completedReplicas);

    ExecutionStatus actualStatus =
        monitor
            .getIncrementalPushStatusFromPushStatusStore(
                topic,
                incrementalPushVersion,
                customizedViewMock,
                statusStoreReaderMock,
                partitionCountForIncPushTests,
                replicationFactorForIncPushTests)
            .getFirst();
    Assert.assertEquals(actualStatus, ExecutionStatus.ERROR);
  }

  @Test(description = "Expect NOT_CREATED status when push status map is null")
  public void testCheckIncrementalPushStatusWhenPushStatusMapisNull() {
    Map<Integer, Integer> completedReplicas = getCompletedStatusData();
    prepareForIncrementalPushStatusTest(null, completedReplicas);

    ExecutionStatus actualStatus =
        monitor
            .getIncrementalPushStatusFromPushStatusStore(
                topic,
                incrementalPushVersion,
                customizedViewMock,
                statusStoreReaderMock,
                partitionCountForIncPushTests,
                replicationFactorForIncPushTests)
            .getFirst();
    Assert.assertEquals(actualStatus, ExecutionStatus.NOT_CREATED);
  }

  @Test
  public void testGetOngoingIncrementalPushVersions() {
    Map<CharSequence, Integer> ipVersions = new HashMap<CharSequence, Integer>() {
      {
        put("incPush1", 7);
        put("incPush2", 7);
        put("incPush3", 7);
        put("incPush4", 7);
      }
    };
    statusStoreReaderMock = mock(PushStatusStoreReader.class);
    when(statusStoreReaderMock.getSupposedlyOngoingIncrementalPushVersions(anyString(), anyInt()))
        .thenReturn(ipVersions);
    Set<String> ongoIpVersions = monitor.getOngoingIncrementalPushVersions(topic, statusStoreReaderMock);
    for (CharSequence ipVersion: ipVersions.keySet()) {
      Assert.assertTrue(ongoIpVersions.contains(ipVersion.toString()));
    }
    verify(statusStoreReaderMock).getSupposedlyOngoingIncrementalPushVersions(anyString(), anyInt());
  }

  @Test
  public void testOnPartitionStatusChangeForHybridStore() {
    String topic = this.topic;
    // Prepare a hybrid store.
    Store store = prepareMockStore(topic);
    store.setHybridStoreConfig(
        new HybridStoreConfigImpl(
            100,
            100,
            HybridStoreConfigImpl.DEFAULT_HYBRID_TIME_LAG_THRESHOLD,
            DataReplicationPolicy.NON_AGGREGATE,
            BufferReplayPolicy.REWIND_FROM_EOP));
    // Prepare a mock topic replicator
    RealTimeTopicSwitcher realTimeTopicSwitcher = mock(RealTimeTopicSwitcher.class);
    monitor.setRealTimeTopicSwitcher(realTimeTopicSwitcher);
    // Start a push
    monitor.startMonitorOfflinePush(
        topic,
        numberOfPartition,
        replicationFactor,
        OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION);

    // Prepare the new partition status
    List<ReplicaStatus> replicaStatuses = new ArrayList<>();
    for (int i = 0; i < replicationFactor; i++) {
      ReplicaStatus replicaStatus = new ReplicaStatus("test" + i);
      replicaStatuses.add(replicaStatus);
    }
    // All replicas are in STARTED status
    ReadOnlyPartitionStatus partitionStatus = new ReadOnlyPartitionStatus(0, replicaStatuses);

    // External view exists
    doReturn(true).when(mockRoutingDataRepo).containsKafkaTopic(topic);
    doReturn(new PartitionAssignment(topic, 1)).when(mockRoutingDataRepo).getPartitionAssignments(topic);

    // Check hybrid push status
    monitor.onPartitionStatusChange(topic, partitionStatus);
    // Not ready to send SOBR
    verify(realTimeTopicSwitcher, never()).switchToRealTimeTopic(any(), any(), any(), any(), anyList());
    Assert.assertEquals(
        monitor.getOfflinePushOrThrow(topic).getCurrentStatus(),
        ExecutionStatus.STARTED,
        "Hybrid push is not ready to send SOBR.");

    // One replica received end of push
    replicaStatuses.get(0).updateStatus(ExecutionStatus.END_OF_PUSH_RECEIVED);
    monitor.onPartitionStatusChange(topic, partitionStatus);
    verify(realTimeTopicSwitcher, times(1)).switchToRealTimeTopic(
        eq(Version.composeRealTimeTopic(store.getName())),
        eq(topic),
        eq(store),
        eq(aggregateRealTimeSourceKafkaUrl),
        anyList());
    Assert.assertEquals(
        monitor.getOfflinePushOrThrow(topic).getCurrentStatus(),
        ExecutionStatus.END_OF_PUSH_RECEIVED,
        "At least one replica already received end_of_push, so we send SOBR and update push status to END_OF_PUSH_RECEIVED");

    // Another replica received end of push
    replicaStatuses.get(1).updateStatus(ExecutionStatus.END_OF_PUSH_RECEIVED);
    realTimeTopicSwitcher = mock(RealTimeTopicSwitcher.class);
    monitor.setRealTimeTopicSwitcher(realTimeTopicSwitcher);
    monitor.onPartitionStatusChange(topic, partitionStatus);
    // Should not send SOBR again
    verify(realTimeTopicSwitcher, never()).switchToRealTimeTopic(any(), any(), any(), any(), anyList());
  }

  @Test
  public void testOnPartitionStatusChangeForHybridStoreParallel() throws InterruptedException {
    String topic = this.topic;
    // Prepare a hybrid store.
    Store store = prepareMockStore(topic);
    store.setHybridStoreConfig(
        new HybridStoreConfigImpl(
            100,
            100,
            HybridStoreConfigImpl.DEFAULT_HYBRID_TIME_LAG_THRESHOLD,
            DataReplicationPolicy.NON_AGGREGATE,
            BufferReplayPolicy.REWIND_FROM_EOP));
    // Prepare a mock topic replicator
    RealTimeTopicSwitcher realTimeTopicSwitcher = mock(RealTimeTopicSwitcher.class);
    monitor.setRealTimeTopicSwitcher(realTimeTopicSwitcher);
    // Start a push
    monitor.startMonitorOfflinePush(
        topic,
        numberOfPartition,
        replicationFactor,
        OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION);

    // External view exists
    doReturn(true).when(mockRoutingDataRepo).containsKafkaTopic(topic);
    doReturn(new PartitionAssignment(topic, 1)).when(mockRoutingDataRepo).getPartitionAssignments(topic);

    int threadCount = 8;
    Thread[] threads = new Thread[threadCount];
    for (int i = 0; i < threadCount; i++) {
      Thread t = new Thread(() -> {
        List<ReplicaStatus> replicaStatuses = new ArrayList<>();
        for (int r = 0; r < replicationFactor; r++) {
          ReplicaStatus replicaStatus = new ReplicaStatus("test" + r);
          replicaStatus.updateStatus(ExecutionStatus.END_OF_PUSH_RECEIVED);
          replicaStatuses.add(replicaStatus);
        }
        // All replicas are in END_OF_PUSH_RECEIVED status
        ReadOnlyPartitionStatus partitionStatus = new ReadOnlyPartitionStatus(0, replicaStatuses);
        // Check hybrid push status
        monitor.onPartitionStatusChange(topic, partitionStatus);
      });
      threads[i] = t;
      t.start();
    }
    // After all thread was completely executed.
    for (int i = 0; i < threadCount; i++) {
      threads[i].join();
    }
    // Only send one SOBR
    verify(realTimeTopicSwitcher, only()).switchToRealTimeTopic(
        eq(Version.composeRealTimeTopic(store.getName())),
        eq(topic),
        eq(store),
        eq(aggregateRealTimeSourceKafkaUrl),
        anyList());
    Assert.assertEquals(
        monitor.getOfflinePushOrThrow(topic).getCurrentStatus(),
        ExecutionStatus.END_OF_PUSH_RECEIVED,
        "At least one replica already received end_of_push, so we send SOBR and update push status to END_OF_PUSH_RECEIVED");
  }

  protected class MockStoreCleaner implements StoreCleaner {
    private final ClusterLockManager clusterLockManager;

    public MockStoreCleaner(ClusterLockManager clusterLockManager) {
      this.clusterLockManager = clusterLockManager;
    }

    @Override
    public void deleteOneStoreVersion(String clusterName, String storeName, int versionNumber) {
      try (AutoCloseableLock ignore = clusterLockManager.createStoreWriteLock(storeName)) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }

    @Override
    public void retireOldStoreVersions(
        String clusterName,
        String storeName,
        boolean deleteBackupOnStartPush,
        int currentVersionBeforePush) {
      try (AutoCloseableLock ignore = clusterLockManager.createStoreWriteLock(storeName)) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }

    @Override
    public void topicCleanupWhenPushComplete(String clusterName, String storeName, int versionNumber) {
      // no-op
    }

    @Override
    public boolean containsHelixResource(String clusterName, String resourceName) {
      return true;
    }

    @Override
    public void deleteHelixResource(String clusterName, String resourceName) {
      try (AutoCloseableLock ignore = clusterLockManager.createStoreWriteLock(storeName)) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }
  }

  @Test(timeOut = 30 * Time.MS_PER_SECOND)
  public void testOnExternalViewChangeDeadlock() throws InterruptedException {
    ExecutorService asyncExecutor = Executors.newSingleThreadExecutor();
    try {
      final StoreCleaner mockStoreCleanerWithLock = new MockStoreCleaner(clusterLockManager);
      final PartitionStatusBasedPushMonitor pushMonitor = getPushMonitor(mockStoreCleanerWithLock);
      final String topic = "test-lock_v1";
      final String instanceId = "test_instance";
      prepareMockStore(topic);
      EnumMap<HelixState, List<Instance>> helixStateToInstancesMap = new EnumMap<>(HelixState.class);
      helixStateToInstancesMap.put(HelixState.LEADER, Collections.singletonList(new Instance(instanceId, "a", 1)));
      // Craft a PartitionAssignment that will trigger the StoreCleaner methods as part of handleCompletedPush.
      PartitionAssignment completedPartitionAssignment = new PartitionAssignment(topic, 1);
      completedPartitionAssignment
          .addPartition(new Partition(0, helixStateToInstancesMap, new EnumMap<>(ExecutionStatus.class)));
      ReplicaStatus status = new ReplicaStatus(instanceId);
      status.updateStatus(ExecutionStatus.COMPLETED);
      ReadOnlyPartitionStatus completedPartitionStatus =
          new ReadOnlyPartitionStatus(0, Collections.singletonList(status));
      pushMonitor.startMonitorOfflinePush(topic, 1, 1, OfflinePushStrategy.WAIT_ALL_REPLICAS);
      pushMonitor.onPartitionStatusChange(topic, completedPartitionStatus);

      asyncExecutor.submit(() -> {
        // LEADER -> STANDBY transition thread: onBecomeStandbyFromLeader->stopAllMonitoring
        System.out.println("T1 will acquire cluster level write lock");
        try (AutoCloseableLock ignore = clusterLockManager.createClusterWriteLock()) {
          try {
            Thread.sleep(5000);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
          pushMonitor.stopAllMonitoring();
        }
        System.out.println("T1 released cluster level write lock");
      });
      // Give some time for the other thread to take cluster level write lock.
      Thread.sleep(1000);
      // Controller thread: onExternalViewChange
      // If there is a deadlock then it should hang here
      System.out.println("T2 will acquire cluster level read lock and store level write lock");
      pushMonitor.onExternalViewChange(completedPartitionAssignment);
      System.out.println("T2 released cluster level read lock and store level write lock");
    } finally {
      TestUtils.shutdownExecutor(asyncExecutor);
    }
  }

  @Test(timeOut = 30 * Time.MS_PER_SECOND)
  public void testOnPartitionStatusChangeDeadLock() throws InterruptedException {
    final ExecutorService asyncExecutor = Executors.newSingleThreadExecutor();
    try {
      final StoreCleaner mockStoreCleanerWithLock = new MockStoreCleaner(clusterLockManager);
      final PartitionStatusBasedPushMonitor pushMonitor = getPushMonitor(mockStoreCleanerWithLock);
      final String topic = "test-lock_v1";
      final String instanceId = "test_instance";

      prepareMockStore(topic);
      EnumMap<HelixState, List<Instance>> helixStateToInstancesMap = new EnumMap<>(HelixState.class);
      helixStateToInstancesMap.put(HelixState.LEADER, Collections.singletonList(new Instance(instanceId, "a", 1)));
      PartitionAssignment completedPartitionAssignment = new PartitionAssignment(topic, 1);
      completedPartitionAssignment
          .addPartition(new Partition(0, helixStateToInstancesMap, new EnumMap<>(ExecutionStatus.class)));
      doReturn(true).when(mockRoutingDataRepo).containsKafkaTopic(topic);
      doReturn(completedPartitionAssignment).when(mockRoutingDataRepo).getPartitionAssignments(topic);

      ReplicaStatus status = new ReplicaStatus(instanceId);
      status.updateStatus(ExecutionStatus.COMPLETED);
      ReadOnlyPartitionStatus completedPartitionStatus =
          new ReadOnlyPartitionStatus(0, Collections.singletonList(status));
      pushMonitor.startMonitorOfflinePush(topic, 1, 1, OfflinePushStrategy.WAIT_ALL_REPLICAS);

      asyncExecutor.submit(() -> {
        // Controller thread:
        // onPartitionStatusChange->updatePushStatusByPartitionStatus->handleCompletedPush->retireOldStoreVersions
        System.out.println("T1 will acquire cluster level read lock and store level write lock");
        pushMonitor.onPartitionStatusChange(topic, completedPartitionStatus);
        System.out.println("T1 released cluster level read lock and store level write lock");
      });
      // Give some time for controller thread
      Thread.sleep(1000);
      // LEADER -> STANDBY transition thread: onBecomeStandbyFromLeader->stopAllMonitoring
      System.out.println("T2 will acquire cluster level write lock");
      try (AutoCloseableLock ignore = clusterLockManager.createClusterWriteLock()) {
        pushMonitor.stopAllMonitoring();
      }
      System.out.println("T2 released cluster level write lock");
    } finally {
      TestUtils.shutdownExecutor(asyncExecutor);
    }
  }

  @Test
  public void testGetUncompletedPartitions() {
    monitor.startMonitorOfflinePush(
        topic,
        numberOfPartition,
        replicationFactor,
        OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION);
    Assert.assertEquals(monitor.getUncompletedPartitions(topic).size(), numberOfPartition);
    OfflinePushStatus pushStatus = monitor.getOfflinePushOrThrow(topic);
    monitor.updatePushStatus(pushStatus, ExecutionStatus.COMPLETED, Optional.empty());
    Assert.assertTrue(monitor.getUncompletedPartitions(topic).isEmpty());
  }

  private Store prepareMockStore(String topic) {
    String storeName = Version.parseStoreFromKafkaTopicName(topic);
    int versionNumber = Version.parseVersionFromKafkaTopicName(topic);
    Store store = TestUtils.createTestStore(storeName, "test", System.currentTimeMillis());
    Version version = new VersionImpl(storeName, versionNumber);
    version.setStatus(VersionStatus.STARTED);
    store.addVersion(version);
    doReturn(store).when(mockStoreRepo).getStore(storeName);
    return store;
  }

  private PartitionStatusBasedPushMonitor getPushMonitor(StoreCleaner storeCleaner) {
    return new PartitionStatusBasedPushMonitor(
        clusterName,
        mockAccessor,
        storeCleaner,
        mockStoreRepo,
        mockRoutingDataRepo,
        mockPushHealthStats,
        mock(RealTimeTopicSwitcher.class),
        clusterLockManager,
        aggregateRealTimeSourceKafkaUrl,
        Collections.emptyList(),
        helixAdminClient,
        mockControllerConfig,
        null);
  }
}
