package com.linkedin.venice.pushmonitor;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.HybridStoreConfig;
import com.linkedin.venice.meta.OfflinePushStrategy;
import com.linkedin.venice.meta.PartitionAssignment;
import com.linkedin.venice.meta.ReadWriteStoreRepository;
import com.linkedin.venice.meta.RoutingDataRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreCleaner;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.replication.TopicReplicator;
import com.linkedin.venice.utils.TestUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.any;


public class OfflinePushMonitorTest {
  private RoutingDataRepository mockRoutingDataRepo;
  private OfflinePushAccessor mockAccessor;
  private OfflinePushMonitor monitor;
  private ReadWriteStoreRepository mockStoreRepo;
  private StoreCleaner mockStoreCleaner;
  private int numberOfPartition = 1;
  private int replicationFactor = 3;

  @BeforeMethod
  public void setup() {
    mockRoutingDataRepo = Mockito.mock(RoutingDataRepository.class);
    mockAccessor = Mockito.mock(OfflinePushAccessor.class);
    mockStoreCleaner = Mockito.mock(StoreCleaner.class);
    mockStoreRepo = Mockito.mock(ReadWriteStoreRepository.class);
    monitor = new OfflinePushMonitor("OfflinePushMonitorTest", mockRoutingDataRepo, mockAccessor, mockStoreCleaner,
        mockStoreRepo);
  }

  @Test
  public void testStartMonitorOfflinePush() {
    String topic = "testStartMonitorOfflinePush";
    monitor.startMonitorOfflinePush(topic, numberOfPartition, replicationFactor,
        OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION);
    OfflinePushStatus pushStatus = monitor.getOfflinePush(topic);
    Assert.assertEquals(pushStatus.getCurrentStatus(), ExecutionStatus.STARTED);
    Assert.assertEquals(pushStatus.getKafkaTopic(), topic);
    Assert.assertEquals(pushStatus.getNumberOfPartition(), numberOfPartition);
    Assert.assertEquals(pushStatus.getReplicationFactor(), replicationFactor);
    Mockito.verify(mockAccessor, Mockito.atLeastOnce()).createOfflinePushStatusAndItsPartitionStatuses(pushStatus);
    Mockito.verify(mockAccessor, Mockito.atLeastOnce()).subscribePartitionStatusChange(pushStatus, monitor);
    Mockito.verify(mockRoutingDataRepo, Mockito.atLeastOnce()).subscribeRoutingDataChange(topic, monitor);
    try {
      monitor.startMonitorOfflinePush(topic, numberOfPartition, replicationFactor,
          OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION);
      Assert.fail("Duplicated monitoring is not allowed. ");
    } catch (VeniceException e) {
    }
  }

  @Test
  public void testStopMonitorOfflinePush() {
    String topic = "testStopMonitorOfflinePush";
    monitor.startMonitorOfflinePush(topic, numberOfPartition, replicationFactor,
        OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION);
    OfflinePushStatus pushStatus = monitor.getOfflinePush(topic);
    monitor.stopMonitorOfflinePush(topic);
    Mockito.verify(mockAccessor, Mockito.atLeastOnce()).deleteOfflinePushStatusAndItsPartitionStatuses(pushStatus);
    Mockito.verify(mockAccessor, Mockito.atLeastOnce()).unsubscribePartitionsStatusChange(pushStatus, monitor);
    Mockito.verify(mockRoutingDataRepo, Mockito.atLeastOnce()).unSubscribeRoutingDataChange(topic, monitor);

    try {
      monitor.getOfflinePush(topic);
      Assert.fail("Push status should be deleted by stopMonitorOfflinePush method");
    } catch (VeniceException e) {
    }
  }

  @Test
  public void testStopMonitorErrorOfflinePush() {
    String store = "testStopMonitorErrorOfflinePush";
    for (int i = 0; i < OfflinePushMonitor.MAX_ERROR_PUSH_TO_KEEP; i++) {
      String topic = Version.composeKafkaTopic(store, i);
      monitor.startMonitorOfflinePush(topic, numberOfPartition, replicationFactor,
          OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION);
      OfflinePushStatus pushStatus = monitor.getOfflinePush(topic);
      pushStatus.updateStatus(ExecutionStatus.ERROR);
      monitor.stopMonitorOfflinePush(topic);
    }
    // We should keeep MAX_ERROR_PUSH_TO_KEEP error push for debug.
    for (int i = 0; i < OfflinePushMonitor.MAX_ERROR_PUSH_TO_KEEP; i++) {
      Assert.assertNotNull(monitor.getOfflinePush(Version.composeKafkaTopic(store, i)));
    }
    // Add a new error push, the oldest one should be collected.
    String topic = Version.composeKafkaTopic(store, OfflinePushMonitor.MAX_ERROR_PUSH_TO_KEEP + 1);
    monitor.startMonitorOfflinePush(topic, numberOfPartition, replicationFactor,
        OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION);
    OfflinePushStatus pushStatus = monitor.getOfflinePush(topic);
    pushStatus.updateStatus(ExecutionStatus.ERROR);
    monitor.stopMonitorOfflinePush(topic);
    try {
      monitor.getOfflinePush(Version.composeKafkaTopic(store, 0));
      Assert.fail("Oldest error push should be collected.");
    } catch (VeniceException e) {
      //expected
    }
    Assert.assertNotNull(monitor.getOfflinePush(topic));
  }

  @Test
  public void testLoadAllPushes() {
    int statusCount = 3;
    List<OfflinePushStatus> statusList = new ArrayList<>(statusCount);
    for (int i = 0; i < statusCount; i++) {
      OfflinePushStatus pushStatus = new OfflinePushStatus("testLoadAllPushes" + i, numberOfPartition,
          replicationFactor,
          OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION);
      pushStatus.setCurrentStatus(ExecutionStatus.COMPLETED);
      statusList.add(pushStatus);
    }
    Mockito.doReturn(statusList).when(mockAccessor).loadOfflinePushStatusesAndPartitionStatuses();
    monitor.loadAllPushes();
    for (int i = 0; i < statusCount; i++) {
      Assert.assertEquals(monitor.getOfflinePush("testLoadAllPushes" + i).getCurrentStatus(),
          ExecutionStatus.COMPLETED);
    }
  }

  @Test
  public void testLoadRunningPushWhichIsNotUpdateToDate() {
    String topic = "testLoadRunningPushWhichIsNotUpdateToDate_v1";
    Store store = prepareMockStore(topic);
    List<OfflinePushStatus> statusList = new ArrayList<>();
    OfflinePushStatus pushStatus = new OfflinePushStatus(topic, numberOfPartition, replicationFactor,
        OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION);
    statusList.add(pushStatus);
    Mockito.doReturn(statusList).when(mockAccessor).loadOfflinePushStatusesAndPartitionStatuses();
    PartitionAssignment partitionAssignment = new PartitionAssignment(topic, numberOfPartition);
    Mockito.doReturn(partitionAssignment).when(mockRoutingDataRepo).getPartitionAssignments(topic);
    PushStatusDecider decider = Mockito.mock(PushStatusDecider.class);
    Mockito.doReturn(ExecutionStatus.COMPLETED).when(decider).checkPushStatus(pushStatus, partitionAssignment);
    PushStatusDecider.updateDecider(OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION, decider);

    monitor.loadAllPushes();
    Mockito.verify(mockStoreRepo, Mockito.atLeastOnce()).updateStore(store);
    Mockito.verify(mockStoreCleaner, Mockito.atLeastOnce()).retireOldStoreVersions(Mockito.anyString(), Mockito.anyString());
    Assert.assertEquals(monitor.getOfflinePush(topic).getCurrentStatus(), ExecutionStatus.COMPLETED);
    // After offline push completed, bump up the current version of this store.
    Assert.assertEquals(store.getCurrentVersion(), 1);
  }

  @Test
  public void testLoadRunningPushWhichIsNotUpdateToDateAndDeletionError() {
    String topic = "testLoadRunningPushWhichIsNotUpdateToDate_v1";
    Store store = prepareMockStore(topic);
    List<OfflinePushStatus> statusList = new ArrayList<>();
    OfflinePushStatus pushStatus = new OfflinePushStatus(topic, numberOfPartition, replicationFactor,
        OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION);
    statusList.add(pushStatus);
    Mockito.doReturn(statusList).when(mockAccessor).loadOfflinePushStatusesAndPartitionStatuses();
    PartitionAssignment partitionAssignment = new PartitionAssignment(topic, numberOfPartition);
    Mockito.doReturn(partitionAssignment).when(mockRoutingDataRepo).getPartitionAssignments(topic);
    PushStatusDecider decider = Mockito.mock(PushStatusDecider.class);
    Mockito.doReturn(ExecutionStatus.ERROR).when(decider).checkPushStatus(pushStatus, partitionAssignment);
    PushStatusDecider.updateDecider(OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION, decider);
    Mockito.doThrow(new VeniceException("Could not delete.")).when(mockStoreCleaner).deleteOneStoreVersion(Mockito.anyString(), Mockito.anyString(), Mockito.anyInt());


    monitor.loadAllPushes();
    Mockito.verify(mockStoreRepo, Mockito.atLeastOnce()).updateStore(store);
    Mockito.verify(mockStoreCleaner, Mockito.atLeastOnce()).deleteOneStoreVersion(Mockito.anyString(), Mockito.anyString(), Mockito.anyInt());
    Assert.assertEquals(monitor.getOfflinePush(topic).getCurrentStatus(), ExecutionStatus.ERROR);
  }

  @Test
  public void testOnPartitionStatusChangeForHybridStore(){
    String topic = "hybridTestStore_v1";
    // Prepare a hybrid store.
    Store store = prepareMockStore(topic);
    store.setHybridStoreConfig(new HybridStoreConfig(100,100));
    // Prepare a mock topic replicator
    TopicReplicator mockReplicator = Mockito.mock(TopicReplicator.class);
    monitor.setTopicReplicator(Optional.of(mockReplicator));
    // Start a push
    monitor.startMonitorOfflinePush(topic, numberOfPartition, replicationFactor,
        OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION);

    // Prepare the new partition status
    List<ReplicaStatus> replicaStatuses = new ArrayList<>();
    for(int i=0;i<replicationFactor;i++){
      ReplicaStatus replicaStatus = new ReplicaStatus("test"+i);
      replicaStatuses.add(replicaStatus);
    }
    // All replicas are in STARTED status
    ReadOnlyPartitionStatus partitionStatus = new ReadOnlyPartitionStatus(0, replicaStatuses);

    // Check hybrid push status
    monitor.onPartitionStatusChange(topic, partitionStatus);
    // Not ready to send SOBR
    Mockito.verify(mockReplicator, Mockito.never()).startBufferReplay(any(), any(), any());
    Assert.assertEquals(monitor.getOfflinePush(topic).getCurrentStatus(), ExecutionStatus.STARTED,
        "Hybrid push is not ready to send SOBR.");

    // One replica received end of push
    replicaStatuses.get(0).updateStatus(ExecutionStatus.END_OF_PUSH_RECEIVED);
    monitor.onPartitionStatusChange(topic, partitionStatus);
    Mockito.verify(mockReplicator, Mockito.times(1))
        .startBufferReplay(eq(Version.composeRealTimeTopic(store.getName())), eq(topic), eq(store));
    Assert.assertEquals(monitor.getOfflinePush(topic).getCurrentStatus(), ExecutionStatus.END_OF_PUSH_RECEIVED,
        "At least one replica already received end_of_push, so we send SOBR and update push status to END_OF_PUSH_RECEIVED");

    // Another replica received end of push
    replicaStatuses.get(1).updateStatus(ExecutionStatus.END_OF_PUSH_RECEIVED);
    mockReplicator = Mockito.mock(TopicReplicator.class);
    monitor.setTopicReplicator(Optional.of(mockReplicator));
    monitor.onPartitionStatusChange(topic, partitionStatus);
    // Should not send SOBR again
    Mockito.verify(mockReplicator, Mockito.never()).startBufferReplay(any(), any(), any());
  }

  @Test
  public void testOnPartitionStatusChangeForHybridStoreParallel()
      throws InterruptedException {
    String topic = "hybridTestStore_v2";
    // Prepare a hybrid store.
    Store store = prepareMockStore(topic);
    store.setHybridStoreConfig(new HybridStoreConfig(100,100));
    // Prepare a mock topic replicator
    TopicReplicator mockReplicator = Mockito.mock(TopicReplicator.class);
    monitor.setTopicReplicator(Optional.of(mockReplicator));
    // Start a push
    monitor.startMonitorOfflinePush(topic, numberOfPartition, replicationFactor,
        OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION);

    int threadCount = 8;
    Thread[] threads = new Thread[threadCount];
    for(int i = 0;i<threadCount;i++ ) {
      Thread t = new Thread(()->{
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
      threads[i]=t;
      t.start();
    }
    // After all thread was completely executed.
    for(int i=0;i<threadCount;i++){
      threads[i].join();
    }
    // Only send one SOBR
    Mockito.verify(mockReplicator, Mockito.only())
        .startBufferReplay(eq(Version.composeRealTimeTopic(store.getName())), eq(topic), eq(store));
    Assert.assertEquals(monitor.getOfflinePush(topic).getCurrentStatus(), ExecutionStatus.END_OF_PUSH_RECEIVED,
        "At least one replica already received end_of_push, so we send SOBR and update push status to END_OF_PUSH_RECEIVED");
  }

  @DataProvider(name = "pushStatues")
  public static Object[][] pushStatues() {
    return new Object[][]{{ExecutionStatus.COMPLETED}, {ExecutionStatus.STARTED}, {ExecutionStatus.ERROR}};
  }

  @Test(dataProvider = "pushStatues")
  public void testOnRoutingDataChanged(ExecutionStatus expectedStatus) {
    String topic = "testOnRoutingDataChanged_v1";
    prepareMockStore(topic);

    monitor.startMonitorOfflinePush(topic, numberOfPartition, numberOfPartition,
        OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION);
    PartitionAssignment partitionAssignment = new PartitionAssignment(topic, numberOfPartition);
    OfflinePushStatus pushStatus = monitor.getOfflinePush(topic);
    PushStatusDecider decider = Mockito.mock(PushStatusDecider.class);
    Mockito.doReturn(expectedStatus).when(decider).checkPushStatus(pushStatus, partitionAssignment);
    PushStatusDecider.updateDecider(OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION, decider);
    monitor.onRoutingDataChanged(partitionAssignment);
    Assert.assertEquals(monitor.getOfflinePush(topic).getCurrentStatus(), expectedStatus);
  }


  private Store prepareMockStore(String topic){
    String storeName = Version.parseStoreFromKafkaTopicName(topic);
    int versionNumber = Version.parseVersionFromKafkaTopicName(topic);
    Store store = TestUtils.createTestStore(storeName, "test",System.currentTimeMillis());
    Version version = new Version(storeName, versionNumber);
    version.setStatus(VersionStatus.STARTED);
    store.addVersion(version);
    Mockito.doReturn(store).when(mockStoreRepo).getStore(storeName);
    return store;
  }
}
