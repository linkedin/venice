package com.linkedin.venice.pushmonitor;

import static com.linkedin.venice.pushmonitor.AbstractPushMonitor.MAX_PUSH_TO_KEEP;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.END_OF_INCREMENTAL_PUSH_RECEIVED;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.NOT_CREATED;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.STARTED;
import static com.linkedin.venice.pushmonitor.OfflinePushStatus.HELIX_ASSIGNMENT_COMPLETED;
import static com.linkedin.venice.pushmonitor.OfflinePushStatus.HELIX_RESOURCE_NOT_CREATED;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.only;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.controller.VeniceControllerClusterConfig;
import com.linkedin.venice.exceptions.VeniceException;
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
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.locks.AutoCloseableLock;
import com.linkedin.venice.utils.locks.ClusterLockManager;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public abstract class AbstractPushMonitorTest {
  private OfflinePushAccessor mockAccessor;
  private AbstractPushMonitor monitor;
  private ReadWriteStoreRepository mockStoreRepo;
  protected RoutingDataRepository mockRoutingDataRepo;
  protected StoreCleaner mockStoreCleaner;
  private AggPushHealthStats mockPushHealthStats;
  protected ClusterLockManager clusterLockManager;

  protected VeniceControllerClusterConfig mockControllerConfig;

  protected MetricsRepository mockMetricRepo;

  private final static String clusterName = Utils.getUniqueString("test_cluster");
  private final static String aggregateRealTimeSourceKafkaUrl = "aggregate-real-time-source-kafka-url";
  private String storeName;
  private String topic;

  private final static int numberOfPartition = 1;
  private final static int replicationFactor = 3;

  protected AbstractPushMonitor getPushMonitor() {
    return getPushMonitor(mock(RealTimeTopicSwitcher.class));
  }

  protected abstract AbstractPushMonitor getPushMonitor(StoreCleaner storeCleaner);

  protected abstract AbstractPushMonitor getPushMonitor(RealTimeTopicSwitcher mockRealTimeTopicSwitcher);

  @BeforeMethod
  public void setUp() {
    storeName = Utils.getUniqueString("test_store");
    topic = storeName + "_v1";

    mockAccessor = mock(OfflinePushAccessor.class);
    mockStoreCleaner = mock(StoreCleaner.class);
    mockStoreRepo = mock(ReadWriteStoreRepository.class);
    mockMetricRepo = mock(MetricsRepository.class);
    mockRoutingDataRepo = mock(RoutingDataRepository.class);
    mockPushHealthStats = mock(AggPushHealthStats.class);
    clusterLockManager = new ClusterLockManager(clusterName);
    mockControllerConfig = mock(VeniceControllerClusterConfig.class);
    when(mockMetricRepo.sensor(anyString(), any())).thenReturn(mock(Sensor.class));
    when(mockControllerConfig.isErrorLeaderReplicaFailOverEnabled()).thenReturn(true);
    when(mockControllerConfig.isDaVinciPushStatusEnabled()).thenReturn(true);
    when(mockControllerConfig.getDaVinciPushStatusScanIntervalInSeconds()).thenReturn(5);
    when(mockControllerConfig.getOffLineJobWaitTimeInMilliseconds()).thenReturn(120000L);
    when(mockControllerConfig.getDaVinciPushStatusScanThreadNumber()).thenReturn(4);
    monitor = getPushMonitor();
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
    verify(mockRoutingDataRepo, atLeastOnce()).subscribeRoutingDataChange(getTopic(), monitor);
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
    verify(mockRoutingDataRepo, atLeastOnce()).subscribeRoutingDataChange(getTopic(), monitor);
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
    verify(mockRoutingDataRepo, atLeastOnce()).unSubscribeRoutingDataChange(getTopic(), monitor);

    try {
      monitor.getOfflinePushOrThrow(topic);
      Assert.fail("Push status should be deleted by stopMonitorOfflinePush method");
    } catch (VeniceException e) {
    }
  }

  @Test
  public void testStopMonitorErrorOfflinePush() {
    String store = getStoreName();
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
  public void testLoadAllPushesWithPartitionChangeNotification() {
    int statusCount = 3;
    doReturn(true).when(mockRoutingDataRepo).containsKafkaTopic("testLoadAllPushes_v3");
    PartitionAssignment partitionAssignment = mock(PartitionAssignment.class);
    doReturn(false).when(partitionAssignment).isMissingAssignedPartitions();
    Partition partition = mock(Partition.class);
    doReturn(partition).when(partitionAssignment).getPartition(0);
    Map<Instance, HelixState> instanceToStateMap = new HashMap<>();
    instanceToStateMap.put(new Instance("instance0", "host0", 1), HelixState.STANDBY);
    instanceToStateMap.put(new Instance("instance1", "host1", 1), HelixState.STANDBY);
    instanceToStateMap.put(new Instance("instance2", "host2", 1), HelixState.LEADER);
    when(partition.getInstanceToHelixStateMap()).thenReturn(instanceToStateMap);
    doReturn(partitionAssignment).when(mockRoutingDataRepo).getPartitionAssignments("testLoadAllPushes_v3");

    List<OfflinePushStatus> statusList = new ArrayList<>(statusCount);
    for (int i = 1; i <= statusCount; i++) {
      String kafkaTopic = "testLoadAllPushes_v" + i;
      OfflinePushStatus pushStatus = new OfflinePushStatus(
          kafkaTopic,
          numberOfPartition,
          replicationFactor,
          OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION);
      if (i == 3) {
        pushStatus.setCurrentStatus(ExecutionStatus.STARTED);
      } else {
        pushStatus.setCurrentStatus(ExecutionStatus.COMPLETED);
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

    OfflinePushStatus v3NewStatus = statusList.get(2).clonePushStatus();
    doAnswer((Answer<Void>) invocation -> {
      /**
       * This is where we make the test case: We want to make sure ZK status changes after initial refresh.
       * Previously, the offline push status manual refresh happened before subscribing to ZK changes, so there is race
       * condition when server update ZK status during these two action.
       * Now push status will be manually refreshed after ZK change subscription, so it should capture this change.
       */
      v3NewStatus.setCurrentStatus(ExecutionStatus.COMPLETED);
      return null;
    }).when(mockAccessor).subscribePartitionStatusChange(statusList.get(2), monitor);
    when(mockAccessor.getOfflinePushStatusAndItsPartitionStatuses("testLoadAllPushes_v3")).thenReturn(v3NewStatus);

    monitor.loadAllPushes();

    TestUtils.waitForNonDeterministicAssertion(3, TimeUnit.SECONDS, () -> {
      for (int i = 1; i <= statusCount; i++) {
        Assert.assertEquals(
            monitor.getOfflinePushOrThrow("testLoadAllPushes_v" + i).getCurrentStatus(),
            ExecutionStatus.COMPLETED);
      }
    });
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
    String topic = getTopic();
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
    verify(mockPushHealthStats, times(1)).recordFailedPush(eq(getStoreName()), anyLong());
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
            getStoreName(),
            Version.parseVersionFromVersionTopicName(getTopic()),
            incrementalPushVersion,
            partitionCountForIncPushTests)).thenReturn(pushStatusMap);
    when(customizedViewMock.getCompletedStatusReplicas(getTopic(), partitionCountForIncPushTests))
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
                getTopic(),
                incrementalPushVersion,
                customizedViewMock,
                statusStoreReaderMock,
                partitionCountForIncPushTests,
                replicationFactorForIncPushTests)
            .getStatus();
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
                getTopic(),
                incrementalPushVersion,
                customizedViewMock,
                statusStoreReaderMock,
                partitionCountForIncPushTests,
                replicationFactorForIncPushTests)
            .getStatus();
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
                getTopic(),
                incrementalPushVersion,
                customizedViewMock,
                statusStoreReaderMock,
                partitionCountForIncPushTests,
                replicationFactorForIncPushTests)
            .getStatus();
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
                getTopic(),
                incrementalPushVersion,
                customizedViewMock,
                statusStoreReaderMock,
                partitionCountForIncPushTests,
                replicationFactorForIncPushTests)
            .getStatus();
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
                getTopic(),
                incrementalPushVersion,
                customizedViewMock,
                statusStoreReaderMock,
                partitionCountForIncPushTests,
                replicationFactorForIncPushTests)
            .getStatus();
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
                getTopic(),
                incrementalPushVersion,
                customizedViewMock,
                statusStoreReaderMock,
                partitionCountForIncPushTests,
                replicationFactorForIncPushTests)
            .getStatus();
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
                getTopic(),
                incrementalPushVersion,
                customizedViewMock,
                statusStoreReaderMock,
                partitionCountForIncPushTests,
                replicationFactorForIncPushTests)
            .getStatus();
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
                getTopic(),
                incrementalPushVersion,
                customizedViewMock,
                statusStoreReaderMock,
                partitionCountForIncPushTests,
                replicationFactorForIncPushTests)
            .getStatus();
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
                getTopic(),
                incrementalPushVersion,
                customizedViewMock,
                statusStoreReaderMock,
                partitionCountForIncPushTests,
                replicationFactorForIncPushTests)
            .getStatus();
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
                getTopic(),
                incrementalPushVersion,
                customizedViewMock,
                statusStoreReaderMock,
                partitionCountForIncPushTests,
                replicationFactorForIncPushTests)
            .getStatus();
    Assert.assertEquals(actualStatus, ExecutionStatus.ERROR);
  }

  @Test(description = "Expect NOT_CREATED status when push status map is null")
  public void testCheckIncrementalPushStatusWhenPushStatusMapisNull() {
    Map<Integer, Integer> completedReplicas = getCompletedStatusData();
    prepareForIncrementalPushStatusTest(null, completedReplicas);

    ExecutionStatus actualStatus =
        monitor
            .getIncrementalPushStatusFromPushStatusStore(
                getTopic(),
                incrementalPushVersion,
                customizedViewMock,
                statusStoreReaderMock,
                partitionCountForIncPushTests,
                replicationFactorForIncPushTests)
            .getStatus();
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
    Set<String> ongoIpVersions = monitor.getOngoingIncrementalPushVersions(getTopic(), statusStoreReaderMock);
    for (CharSequence ipVersion: ipVersions.keySet()) {
      Assert.assertTrue(ongoIpVersions.contains(ipVersion.toString()));
    }
    verify(statusStoreReaderMock).getSupposedlyOngoingIncrementalPushVersions(anyString(), anyInt());
  }

  @Test
  public void testOnPartitionStatusChangeForHybridStore() {
    String topic = getTopic();
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
        eq(Utils.getRealTimeTopicName(store)),
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
    String topic = getTopic();
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
        eq(Utils.getRealTimeTopicName(store)),
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
      final AbstractPushMonitor pushMonitor = getPushMonitor(mockStoreCleanerWithLock);
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
      final AbstractPushMonitor pushMonitor = getPushMonitor(mockStoreCleanerWithLock);
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

  @Test(timeOut = 30 * Time.MS_PER_SECOND)
  public void testKilledVersionStatus() {
    final String topic = "test-killed-version_v1";
    final String instanceId = "test_instance";

    Store store = prepareMockStore(topic, VersionStatus.KILLED);
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
    monitor.startMonitorOfflinePush(topic, 1, 1, OfflinePushStrategy.WAIT_ALL_REPLICAS);

    try {
      monitor.onPartitionStatusChange(topic, completedPartitionStatus);
    } catch (VeniceException e) {
      Assert.assertTrue(e.getMessage().contains("Abort version status update and current version swapping"));
    }
    OfflinePushStatus pushStatus = monitor.getOfflinePushOrThrow(topic);
    Assert.assertEquals(pushStatus.getCurrentStatus(), ExecutionStatus.STARTED);

    int versionNumber = Version.parseVersionFromKafkaTopicName(topic);
    Assert.assertEquals(store.getVersionStatus(versionNumber), VersionStatus.KILLED);
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

  @Test
  public void testGetPushStatusAndDetails() {
    ResourceAssignment resourceAssignment = mock(ResourceAssignment.class);
    doReturn(true).when(resourceAssignment).containsResource(topic);
    doReturn(mock(PartitionAssignment.class)).when(resourceAssignment).getPartitionAssignment(topic);
    doReturn(resourceAssignment).when(mockRoutingDataRepo).getResourceAssignment();

    // Initial conditions
    OfflinePushStatus offlinePushStatus = monitor.getOfflinePush(topic);
    assertNull(offlinePushStatus);

    ExecutionStatusWithDetails statusWithDetails = monitor.getPushStatusAndDetails(topic);
    assertNotNull(statusWithDetails);
    assertEquals(statusWithDetails.getStatus(), NOT_CREATED);

    // After starting to monitor, the push should be initialized as STARTED
    monitor.startMonitorOfflinePush(
        topic,
        numberOfPartition,
        replicationFactor,
        OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION);

    offlinePushStatus = monitor.getOfflinePush(topic);
    assertNotNull(offlinePushStatus);
    assertEquals(offlinePushStatus.getCurrentStatus(), STARTED);
    Optional<String> statusDetails = offlinePushStatus.getOptionalStatusDetails();
    assertTrue(statusDetails.isPresent());
    assertEquals(statusDetails.get(), HELIX_RESOURCE_NOT_CREATED);

    statusWithDetails = monitor.getPushStatusAndDetails(topic);
    assertNotNull(statusWithDetails);
    assertEquals(statusWithDetails.getStatus(), STARTED);
    assertEquals(
        statusWithDetails.getDetails(),
        HELIX_ASSIGNMENT_COMPLETED,
        "Details should change as a side effect of calling getPushStatusAndDetails.");
  }

  protected Store prepareMockStore(String topic, VersionStatus status) {
    String storeName = Version.parseStoreFromKafkaTopicName(topic);
    int versionNumber = Version.parseVersionFromKafkaTopicName(topic);
    Store store = TestUtils.createTestStore(storeName, "test", System.currentTimeMillis());
    Version version = new VersionImpl(storeName, versionNumber);
    version.setStatus(status);
    store.addVersion(version);
    doReturn(store).when(mockStoreRepo).getStore(storeName);
    return store;
  }

  protected Store prepareMockStore(String topic) {
    return prepareMockStore(topic, VersionStatus.STARTED);
  }

  protected OfflinePushAccessor getMockAccessor() {
    return mockAccessor;
  }

  protected AbstractPushMonitor getMonitor() {
    return monitor;
  }

  protected ReadWriteStoreRepository getMockStoreRepo() {
    return mockStoreRepo;
  }

  protected VeniceControllerClusterConfig getMockControllerConfig() {
    return mockControllerConfig;
  }

  protected RoutingDataRepository getMockRoutingDataRepo() {
    return mockRoutingDataRepo;
  }

  protected StoreCleaner getMockStoreCleaner() {
    return mockStoreCleaner;
  }

  protected AggPushHealthStats getMockPushHealthStats() {
    return mockPushHealthStats;
  }

  protected String getStoreName() {
    return storeName;
  }

  protected String getClusterName() {
    return clusterName;
  }

  protected String getTopic() {
    return topic;
  }

  protected int getNumberOfPartition() {
    return numberOfPartition;
  }

  protected int getReplicationFactor() {
    return replicationFactor;
  }

  protected ClusterLockManager getClusterLockManager() {
    return clusterLockManager;
  }

  protected String getAggregateRealTimeSourceKafkaUrl() {
    return aggregateRealTimeSourceKafkaUrl;
  }
}
