package com.linkedin.venice.pushmonitor;

import com.linkedin.venice.controller.MetadataStoreWriter;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.HelixState;
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
import com.linkedin.venice.replication.TopicReplicator;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.locks.AutoCloseableLock;
import com.linkedin.venice.utils.locks.ClusterLockManager;
import io.tehuti.metrics.MetricsRepository;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;


public abstract class AbstractPushMonitorTest {
  private OfflinePushAccessor mockAccessor;
  private AbstractPushMonitor monitor;
  private ReadWriteStoreRepository mockStoreRepo;
  private RoutingDataRepository mockRoutingDataRepo;
  private StoreCleaner mockStoreCleaner;
  private AggPushHealthStats mockPushHealthStats;
  private MetricsRepository metricsRepository;
  private MetadataStoreWriter metadataStoreWriter;
  private ClusterLockManager clusterLockManager;

  private String clusterName = TestUtils.getUniqueString("test_cluster");
  private String aggregateRealTimeSourceKafkaUrl = "aggregate-real-time-source-kafka-url";
  private String storeName;
  private String topic;

  private int numberOfPartition = 1;
  private int replicationFactor = 3;

  protected AbstractPushMonitor getPushMonitor() {
    return getPushMonitor(false, mock(TopicReplicator.class));
  }

  protected abstract AbstractPushMonitor getPushMonitor(StoreCleaner storeCleaner);

  protected abstract AbstractPushMonitor getPushMonitor(boolean skipBufferReplayForHybrid, TopicReplicator mockReplicator);

  @BeforeMethod
  public void setup() {
    storeName = TestUtils.getUniqueString("test_store");
    topic = storeName + "_v1";

    mockAccessor = mock(OfflinePushAccessor.class);
    mockStoreCleaner = mock(StoreCleaner.class);
    mockStoreRepo = mock(ReadWriteStoreRepository.class);
    mockRoutingDataRepo = mock(RoutingDataRepository.class);
    mockPushHealthStats = mock(AggPushHealthStats.class);
    metricsRepository = new MetricsRepository();
    metadataStoreWriter = mock(MetadataStoreWriter.class);
    clusterLockManager = new ClusterLockManager(clusterName);
    monitor = getPushMonitor();
  }

  @Test
  public void testStartMonitorOfflinePush() {
    monitor.startMonitorOfflinePush(topic, numberOfPartition, replicationFactor,
        OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION);
    OfflinePushStatus pushStatus = monitor.getOfflinePushOrThrow(topic);
    Assert.assertEquals(pushStatus.getCurrentStatus(), ExecutionStatus.STARTED);
    Assert.assertEquals(pushStatus.getKafkaTopic(), topic);
    Assert.assertEquals(pushStatus.getNumberOfPartition(), numberOfPartition);
    Assert.assertEquals(pushStatus.getReplicationFactor(), replicationFactor);
    verify(mockAccessor, atLeastOnce()).createOfflinePushStatusAndItsPartitionStatuses(pushStatus);
    verify(mockAccessor, atLeastOnce()).subscribePartitionStatusChange(pushStatus, monitor);
    verify(mockRoutingDataRepo, atLeastOnce()).subscribeRoutingDataChange(getTopic(), monitor);
    try {
      monitor.startMonitorOfflinePush(topic, numberOfPartition, replicationFactor,
          OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION);
      Assert.fail("Duplicated monitoring is not allowed. ");
    } catch (VeniceException e) {
    }
  }

  @Test
  public void testStartMonitorOfflinePushWhenThereIsAnExistingErrorPush() {
    monitor.startMonitorOfflinePush(topic, numberOfPartition, replicationFactor,
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
    monitor.startMonitorOfflinePush(topic, numberOfPartition, replicationFactor,
        OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION);
  }

  @Test
  public void testStopMonitorOfflinePush() {
    monitor.startMonitorOfflinePush(topic, numberOfPartition, replicationFactor,
        OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION);
    OfflinePushStatus pushStatus = monitor.getOfflinePushOrThrow(topic);
    monitor.stopMonitorOfflinePush(topic, true);
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
    for (int i = 0; i < HelixEVBasedPushMonitor.MAX_PUSH_TO_KEEP; i++) {
      String topic = Version.composeKafkaTopic(store, i);
      monitor.startMonitorOfflinePush(topic, numberOfPartition, replicationFactor,
          OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION);
      OfflinePushStatus pushStatus = monitor.getOfflinePushOrThrow(topic);
      pushStatus.updateStatus(ExecutionStatus.ERROR);
      monitor.stopMonitorOfflinePush(topic, true);
    }
    // We should keep MAX_ERROR_PUSH_TO_KEEP error push for debug.
    for (int i = 0; i < HelixEVBasedPushMonitor.MAX_PUSH_TO_KEEP; i++) {
      Assert.assertNotNull(monitor.getOfflinePushOrThrow(Version.composeKafkaTopic(store, i)));
    }
    // Add a new error push, the oldest one should be collected.
    String topic = Version.composeKafkaTopic(store, HelixEVBasedPushMonitor.MAX_PUSH_TO_KEEP + 1);
    monitor.startMonitorOfflinePush(topic, numberOfPartition, replicationFactor,
        OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION);
    OfflinePushStatus pushStatus = monitor.getOfflinePushOrThrow(topic);
    pushStatus.updateStatus(ExecutionStatus.ERROR);
    monitor.stopMonitorOfflinePush(topic, true);
    try {
      monitor.getOfflinePushOrThrow(Version.composeKafkaTopic(store, 0));
      Assert.fail("Oldest error push should be collected.");
    } catch (VeniceException e) {
      //expected
    }
    Assert.assertNotNull(monitor.getOfflinePushOrThrow(topic));
  }

  @Test
  public void testLoadAllPushes() {
    int statusCount = 3;
    List<OfflinePushStatus> statusList = new ArrayList<>(statusCount);
    for (int i = 0; i < statusCount; i++) {
      OfflinePushStatus pushStatus =
          new OfflinePushStatus("testLoadAllPushes_v" + i, numberOfPartition, replicationFactor,
              OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION);
      pushStatus.setCurrentStatus(ExecutionStatus.COMPLETED);
      statusList.add(pushStatus);
    }
    doReturn(statusList).when(mockAccessor).loadOfflinePushStatusesAndPartitionStatuses();
    when(mockAccessor.getOfflinePushStatusAndItsPartitionStatuses(Mockito.anyString())).thenAnswer(invocation ->
    {
      String kafkaTopic = invocation.getArgument(0);
      for(OfflinePushStatus status : statusList) {
        if(status.getKafkaTopic().equals(kafkaTopic)) {
          return status;
        }
      }
      return null;
    });
    monitor.loadAllPushes();
    for (int i = 0; i < statusCount; i++) {
      Assert.assertEquals(monitor.getOfflinePushOrThrow("testLoadAllPushes_v" + i).getCurrentStatus(),
          ExecutionStatus.COMPLETED);
    }
  }

  @Test
  public void testClearOldErrorVersion() {
    //creating MAX_PUSH_TO_KEEP * 2 pushes. The first is successful and the rest of them are failed.
    int statusCount = HelixEVBasedPushMonitor.MAX_PUSH_TO_KEEP * 2;
    List<OfflinePushStatus> statusList = new ArrayList<>(statusCount);
    for (int i = 0; i < statusCount; i++) {
      OfflinePushStatus pushStatus =
          new OfflinePushStatus("testLoadAllPushes_v" + i, numberOfPartition, replicationFactor,
              OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION);

      //set all push statuses except the first to error
      if (i == 0) {
        pushStatus.setCurrentStatus(ExecutionStatus.COMPLETED);
      } else {
        pushStatus.setCurrentStatus(ExecutionStatus.ERROR);
      }
      statusList.add(pushStatus);
    }
    doReturn(statusList).when(mockAccessor).loadOfflinePushStatusesAndPartitionStatuses();

    when(mockAccessor.getOfflinePushStatusAndItsPartitionStatuses(Mockito.anyString())).thenAnswer(invocation ->
    {
      String kafkaTopic = invocation.getArgument(0);
      for(OfflinePushStatus status : statusList) {
        if(status.getKafkaTopic().equals(kafkaTopic)) {
          return status;
        }
      }
      return null;
    });

    monitor.loadAllPushes();
    // Make sure we delete old error pushes from accessor.
    verify(mockAccessor, times(statusCount - HelixEVBasedPushMonitor.MAX_PUSH_TO_KEEP))
        .deleteOfflinePushStatusAndItsPartitionStatuses(any());

    //the first push should be persisted since it succeeded. But the next 5 pushes should be purged.
    int i = 0;
    Assert.assertEquals(monitor.getPushStatus("testLoadAllPushes_v" + i), ExecutionStatus.COMPLETED);

    for (i = 1; i <= HelixEVBasedPushMonitor.MAX_PUSH_TO_KEEP; i++) {
      try {
        monitor.getOfflinePushOrThrow("testLoadAllPushes_v" + i);
        Assert.fail("Old error pushes should be collected after loading.");
      } catch (VeniceException e) {
        //expected
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
    monitor.startMonitorOfflinePush(topic, numberOfPartition, replicationFactor,
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

  @Test
  public void testQueryingIncrementalPushJobStatus() {
    String topic = getTopic();
    String incrementalPushVersion = String.valueOf(System.currentTimeMillis());

    monitor.startMonitorOfflinePush(topic, numberOfPartition, replicationFactor,
        OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION);

    //prepare new partition status
    List<ReplicaStatus> replicaStatuses = new ArrayList<>();
    for (int i = 0; i < replicationFactor; i++) {
      ReplicaStatus replicaStatus = new ReplicaStatus("test" + i);
      replicaStatus.updateStatus(ExecutionStatus.STARTED);
      replicaStatus.updateStatus(ExecutionStatus.COMPLETED);
      replicaStatuses.add(replicaStatus);
    }

    Map<String, List<Instance>> onlineInstanceMap = new HashMap<>();
    List<Instance> onlineInstance = new ArrayList<>(replicationFactor);
    for (int i = 0; i < replicationFactor; i++) {
      onlineInstance.add(new Instance("test" + i, "test", i));
    }
    onlineInstanceMap.put(HelixState.ONLINE_STATE, onlineInstance);
    PartitionAssignment partitionAssignment = new PartitionAssignment(topic, numberOfPartition);
    partitionAssignment.addPartition(new Partition(0, onlineInstanceMap));
    doReturn(partitionAssignment).when(mockRoutingDataRepo).getPartitionAssignments(topic);

    Assert.assertEquals(monitor.getPushStatus(topic, Optional.of(incrementalPushVersion)),
        ExecutionStatus.NOT_CREATED);

    //update one of the replica status START -> COMPLETE -> START_OF_INCREMENTAL_PUSH_RECEIVED (SOIP_RECEIVED)
    replicaStatuses.get(0).updateStatus(ExecutionStatus.START_OF_INCREMENTAL_PUSH_RECEIVED, incrementalPushVersion);
    prepareMockStore(topic);

    monitor.onPartitionStatusChange(topic, new ReadOnlyPartitionStatus(0, replicaStatuses));
    //OfflinePushMonitor should return SOIP_RECEIVED if any of replica receives SOIP_RECEIVED
    Assert.assertEquals(monitor.getPushStatus(topic, Optional.of(incrementalPushVersion)),
        ExecutionStatus.START_OF_INCREMENTAL_PUSH_RECEIVED);

    //update 2 of the replica status to END_OF_INCREMENTAL_PUSH_RECEIVED (EOIP_RECEIVED)
    //and update the third one to EOIP_RECEIVED with wrong version
    replicaStatuses.get(0).updateStatus(ExecutionStatus.END_OF_INCREMENTAL_PUSH_RECEIVED, incrementalPushVersion);

    replicaStatuses.get(1).updateStatus(ExecutionStatus.START_OF_INCREMENTAL_PUSH_RECEIVED, incrementalPushVersion);
    replicaStatuses.get(1).updateStatus(ExecutionStatus.END_OF_INCREMENTAL_PUSH_RECEIVED, incrementalPushVersion);

    replicaStatuses.get(2).updateStatus(ExecutionStatus.START_OF_INCREMENTAL_PUSH_RECEIVED, incrementalPushVersion);
    replicaStatuses.get(2).updateStatus(ExecutionStatus.END_OF_INCREMENTAL_PUSH_RECEIVED, "incorrect_version");

    monitor.onPartitionStatusChange(topic, new ReadOnlyPartitionStatus(0, replicaStatuses));
    //OfflinePushMonitor should be able to filter out irrelevant IP versions
    Assert.assertEquals(monitor.getPushStatus(topic, Optional.of(incrementalPushVersion)),
        ExecutionStatus.START_OF_INCREMENTAL_PUSH_RECEIVED);

    replicaStatuses.get(2).updateStatus(ExecutionStatus.END_OF_INCREMENTAL_PUSH_RECEIVED, incrementalPushVersion);

    monitor.onPartitionStatusChange(topic, new ReadOnlyPartitionStatus(0, replicaStatuses));
    Assert.assertEquals(monitor.getPushStatus(topic, Optional.of(incrementalPushVersion)),
        ExecutionStatus.END_OF_INCREMENTAL_PUSH_RECEIVED);

    replicaStatuses.get(0).updateStatus(ExecutionStatus.WARNING, incrementalPushVersion);

    monitor.onPartitionStatusChange(topic, new ReadOnlyPartitionStatus(0, replicaStatuses));
    Assert.assertEquals(monitor.getPushStatus(topic, Optional.of(incrementalPushVersion)),
        ExecutionStatus.ERROR);
  }

  @Test
  public void testDisableBufferReplayForHybrid() {
    String topic = "hybridTestStore_v1";
    // Prepare a hybrid store.
    Store store = prepareMockStore(topic);
    store.setHybridStoreConfig(new HybridStoreConfigImpl(100, 100, HybridStoreConfigImpl.DEFAULT_HYBRID_TIME_LAG_THRESHOLD,
        DataReplicationPolicy.NON_AGGREGATE, BufferReplayPolicy.REWIND_FROM_EOP));
    // Prepare a mock topic replicator
    TopicReplicator mockReplicator = mock(TopicReplicator.class);
    AbstractPushMonitor testMonitor = getPushMonitor(true, mockReplicator);
    // Start a push
    testMonitor.startMonitorOfflinePush(topic, numberOfPartition, replicationFactor,
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

    // Check hybrid push status
    testMonitor.onPartitionStatusChange(topic, partitionStatus);
    // Not ready to send SOBR
    verify(mockReplicator, never()).prepareAndStartReplication(any(), any(), any(), any());
    Assert.assertEquals(testMonitor.getOfflinePushOrThrow(topic).getCurrentStatus(), ExecutionStatus.STARTED,
        "Hybrid push is not ready to send SOBR.");

    // One replica received end of push
    replicaStatuses.get(0).updateStatus(ExecutionStatus.END_OF_PUSH_RECEIVED);
    testMonitor.onPartitionStatusChange(topic, partitionStatus);
    // no buffer replay should be sent
    verify(mockReplicator, never())
        .prepareAndStartReplication(eq(Version.composeRealTimeTopic(store.getName())), eq(topic), eq(store), eq(aggregateRealTimeSourceKafkaUrl));
    Assert.assertEquals(testMonitor.getOfflinePushOrThrow(topic).getCurrentStatus(), ExecutionStatus.END_OF_PUSH_RECEIVED,
        "At least one replica already received end_of_push, so we send SOBR and update push status to END_OF_PUSH_RECEIVED");

    // Another replica received end of push
    replicaStatuses.get(1).updateStatus(ExecutionStatus.END_OF_PUSH_RECEIVED);
    mockReplicator = mock(TopicReplicator.class);
    testMonitor.setTopicReplicator(Optional.of(mockReplicator));
    testMonitor.onPartitionStatusChange(topic, partitionStatus);
    // Should not send SOBR again
    verify(mockReplicator, never()).prepareAndStartReplication(any(), any(), any(), any());
  }

  @Test
  public void testOnPartitionStatusChangeForHybridStore() {
    String topic = getTopic();
    // Prepare a hybrid store.
    Store store = prepareMockStore(topic);
    store.setHybridStoreConfig(new HybridStoreConfigImpl(100, 100, HybridStoreConfigImpl.DEFAULT_HYBRID_TIME_LAG_THRESHOLD,
        DataReplicationPolicy.NON_AGGREGATE, BufferReplayPolicy.REWIND_FROM_EOP));
    // Prepare a mock topic replicator
    TopicReplicator mockReplicator = mock(TopicReplicator.class);
    monitor.setTopicReplicator(Optional.of(mockReplicator));
    // Start a push
    monitor.startMonitorOfflinePush(topic, numberOfPartition, replicationFactor,
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

    // Check hybrid push status
    monitor.onPartitionStatusChange(topic, partitionStatus);
    // Not ready to send SOBR
    verify(mockReplicator, never()).prepareAndStartReplication(any(), any(), any(), any());
    Assert.assertEquals(monitor.getOfflinePushOrThrow(topic).getCurrentStatus(), ExecutionStatus.STARTED,
        "Hybrid push is not ready to send SOBR.");

    // One replica received end of push
    replicaStatuses.get(0).updateStatus(ExecutionStatus.END_OF_PUSH_RECEIVED);
    monitor.onPartitionStatusChange(topic, partitionStatus);
    verify(mockReplicator,times(1))
        .prepareAndStartReplication(eq(Version.composeRealTimeTopic(store.getName())), eq(topic), eq(store), eq(aggregateRealTimeSourceKafkaUrl));
    Assert.assertEquals(monitor.getOfflinePushOrThrow(topic).getCurrentStatus(), ExecutionStatus.END_OF_PUSH_RECEIVED,
        "At least one replica already received end_of_push, so we send SOBR and update push status to END_OF_PUSH_RECEIVED");

    // Another replica received end of push
    replicaStatuses.get(1).updateStatus(ExecutionStatus.END_OF_PUSH_RECEIVED);
    mockReplicator = mock(TopicReplicator.class);
    monitor.setTopicReplicator(Optional.of(mockReplicator));
    monitor.onPartitionStatusChange(topic, partitionStatus);
    // Should not send SOBR again
    verify(mockReplicator, never()).prepareAndStartReplication(any(), any(), any(), any());
  }

  @Test
  public void testOnPartitionStatusChangeForHybridStoreParallel()
      throws InterruptedException {
    String topic = getTopic();
    // Prepare a hybrid store.
    Store store = prepareMockStore(topic);
    store.setHybridStoreConfig(new HybridStoreConfigImpl(100, 100, HybridStoreConfigImpl.DEFAULT_HYBRID_TIME_LAG_THRESHOLD,
        DataReplicationPolicy.NON_AGGREGATE, BufferReplayPolicy.REWIND_FROM_EOP));
    // Prepare a mock topic replicator
    TopicReplicator mockReplicator = mock(TopicReplicator.class);
    monitor.setTopicReplicator(Optional.of(mockReplicator));
    // Start a push
    monitor.startMonitorOfflinePush(topic, numberOfPartition, replicationFactor,
        OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION);

    // External view exists
    doReturn(true).when(mockRoutingDataRepo).containsKafkaTopic(topic);

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
    verify(mockReplicator, only())
        .prepareAndStartReplication(eq(Version.composeRealTimeTopic(store.getName())), eq(topic), eq(store), eq(aggregateRealTimeSourceKafkaUrl));
    Assert.assertEquals(monitor.getOfflinePushOrThrow(topic).getCurrentStatus(), ExecutionStatus.END_OF_PUSH_RECEIVED,
        "At least one replica already received end_of_push, so we send SOBR and update push status to END_OF_PUSH_RECEIVED");
  }

  private class MockStoreCleaner implements StoreCleaner {
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
    public void retireOldStoreVersions(String clusterName, String storeName, boolean deleteBackupOnStartPush) {
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
  }

  @Test(timeOut = 30 * Time.MS_PER_SECOND)
  public void testOnExternalViewChangeDeadlock() throws InterruptedException {
    ExecutorService asyncExecutor = Executors.newSingleThreadExecutor();
    final StoreCleaner mockStoreCleanerWithLock = new MockStoreCleaner(clusterLockManager);
    final AbstractPushMonitor pushMonitor = getPushMonitor(mockStoreCleanerWithLock);
    final String topic = "test-lock_v1";
    final String instanceId = "test_instance";
    prepareMockStore(topic);
    Map<String, List<Instance>> onlineInstanceMap = new HashMap<>();
    onlineInstanceMap.put(HelixState.ONLINE_STATE, Collections.singletonList(new Instance(instanceId, "a", 1)));
    // Craft a PartitionAssignment that will trigger the StoreCleaner methods as part of handleCompletedPush.
    PartitionAssignment completedPartitionAssignment = new PartitionAssignment(topic, 1);
    completedPartitionAssignment.addPartition(new Partition(0, onlineInstanceMap));
    ReplicaStatus status = new ReplicaStatus(instanceId);
    status.updateStatus(ExecutionStatus.COMPLETED);
    ReadOnlyPartitionStatus completedPartitionStatus = new ReadOnlyPartitionStatus(0, Collections.singletonList(status));
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
    asyncExecutor.shutdownNow();
  }

  @Test(timeOut = 30 * Time.MS_PER_SECOND)
  public void testOnPartitionStatusChangeDeadLock() throws InterruptedException {
    final ExecutorService asyncExecutor = Executors.newSingleThreadExecutor();
    final StoreCleaner mockStoreCleanerWithLock = new MockStoreCleaner(clusterLockManager);
    final AbstractPushMonitor pushMonitor = getPushMonitor(mockStoreCleanerWithLock);
    final String topic = "test-lock_v1";
    final String instanceId = "test_instance";

    prepareMockStore(topic);
    Map<String, List<Instance>> onlineInstanceMap = new HashMap<>();
    onlineInstanceMap.put(HelixState.ONLINE_STATE, Collections.singletonList(new Instance(instanceId, "a", 1)));
    PartitionAssignment completedPartitionAssignment = new PartitionAssignment(topic, 1);
    completedPartitionAssignment.addPartition(new Partition(0, onlineInstanceMap));
    doReturn(true).when(mockRoutingDataRepo).containsKafkaTopic(topic);
    doReturn(completedPartitionAssignment).when(mockRoutingDataRepo).getPartitionAssignments(topic);

    ReplicaStatus status = new ReplicaStatus(instanceId);
    status.updateStatus(ExecutionStatus.COMPLETED);
    ReadOnlyPartitionStatus completedPartitionStatus = new ReadOnlyPartitionStatus(0, Collections.singletonList(status));
    pushMonitor.startMonitorOfflinePush(topic, 1, 1, OfflinePushStrategy.WAIT_ALL_REPLICAS);

    asyncExecutor.submit(() -> {
      // Controller thread: onPartitionStatusChange->updatePushStatusByPartitionStatus->handleCompletedPush->retireOldStoreVersions
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
    asyncExecutor.shutdownNow();
  }

  protected Store prepareMockStore(String topic) {
    String storeName = Version.parseStoreFromKafkaTopicName(topic);
    int versionNumber = Version.parseVersionFromKafkaTopicName(topic);
    Store store = TestUtils.createTestStore(storeName, "test", System.currentTimeMillis());
    Version version = new VersionImpl(storeName, versionNumber);
    version.setStatus(VersionStatus.STARTED);
    store.addVersion(version);
    doReturn(store).when(mockStoreRepo).getStore(storeName);
    return store;
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

  protected MetadataStoreWriter getMockMetadataStoreWriter() {
    return metadataStoreWriter;
  }

  protected ClusterLockManager getClusterLockManager() {
    return clusterLockManager;
  }

  protected String getAggregateRealTimeSourceKafkaUrl() {
    return aggregateRealTimeSourceKafkaUrl;
  }
}
