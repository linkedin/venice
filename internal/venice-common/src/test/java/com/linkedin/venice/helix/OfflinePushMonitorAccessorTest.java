package com.linkedin.venice.helix;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.pushmonitor.OfflinePushStatus;
import com.linkedin.venice.pushmonitor.PartitionStatus;
import com.linkedin.venice.pushmonitor.ReplicaStatus;
import com.linkedin.venice.pushmonitor.StatusSnapshot;
import com.linkedin.venice.utils.LogContext;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.zookeeper.zkclient.DataUpdater;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


public class OfflinePushMonitorAccessorTest {
  @Test
  public void testErrorInGettingOfflinePushStatusCreationTimeIsHandled() {
    ZkBaseDataAccessor<OfflinePushStatus> mockOfflinePushStatusAccessor = mock(ZkBaseDataAccessor.class);
    doThrow(new RuntimeException()).when(mockOfflinePushStatusAccessor).getStat(anyString(), anyInt());
    VeniceOfflinePushMonitorAccessor accessor = new VeniceOfflinePushMonitorAccessor(
        "cluster0",
        mockOfflinePushStatusAccessor,
        mock(ZkBaseDataAccessor.class),
        LogContext.EMPTY);
    Optional<Long> ctime = accessor.getOfflinePushStatusCreationTime("test");
    Assert.assertFalse(ctime.isPresent());
  }

  @Test
  public void testNullStatWillReturnEmptyOptional() {
    ZkBaseDataAccessor<OfflinePushStatus> mockOfflinePushStatusAccessor = mock(ZkBaseDataAccessor.class);
    doReturn(null).when(mockOfflinePushStatusAccessor).getStat(anyString(), anyInt());
    VeniceOfflinePushMonitorAccessor accessor = new VeniceOfflinePushMonitorAccessor(
        "cluster0",
        mockOfflinePushStatusAccessor,
        mock(ZkBaseDataAccessor.class),
        LogContext.EMPTY);
    Optional<Long> ctime = accessor.getOfflinePushStatusCreationTime("test");
    Assert.assertFalse(ctime.isPresent());
  }

  @Test
  public void testBatchUpdateEOIP() {
    ZkBaseDataAccessor<OfflinePushStatus> mockOfflinePushStatusAccessor = mock(ZkBaseDataAccessor.class);
    ZkBaseDataAccessor<PartitionStatus> mockPartitionStatusAccessor = mock(ZkBaseDataAccessor.class);
    doReturn(null).when(mockOfflinePushStatusAccessor).getStat(anyString(), anyInt());
    VeniceOfflinePushMonitorAccessor accessor = new VeniceOfflinePushMonitorAccessor(
        "cluster0",
        mockOfflinePushStatusAccessor,
        mockPartitionStatusAccessor,
        LogContext.EMPTY);
    when(mockPartitionStatusAccessor.exists(anyString(), anyInt())).thenReturn(true);
    when(mockPartitionStatusAccessor.update(anyString(), any(), anyInt())).thenReturn(true);
    accessor.batchUpdateReplicaIncPushStatus("test_topic", 0, "test_instance_id", Arrays.asList("a", "b"));
    Mockito.verify(mockPartitionStatusAccessor, Mockito.times(1)).update(anyString(), any(), anyInt());
  }

  @Test
  public void testUpdatePartitionStatus() {
    ZkBaseDataAccessor<OfflinePushStatus> mockOfflinePushStatusAccessor = mock(ZkBaseDataAccessor.class);
    ZkBaseDataAccessor<PartitionStatus> mockPartitionStatusAccessor = mock(ZkBaseDataAccessor.class);
    VeniceOfflinePushMonitorAccessor accessor = new VeniceOfflinePushMonitorAccessor(
        "cluster0",
        mockOfflinePushStatusAccessor,
        mockPartitionStatusAccessor,
        LogContext.EMPTY);

    // Mock push status exists - note: pushStatusExists() uses partitionStatusAccessor, not offlinePushStatusAccessor
    when(mockPartitionStatusAccessor.exists(anyString(), anyInt())).thenReturn(true);
    // Mock update() method since HelixUtils.compareAndUpdate() calls dataAccessor.update()
    when(mockPartitionStatusAccessor.update(anyString(), any(), anyInt())).thenReturn(true);

    // Create a partition status to update
    PartitionStatus partitionStatus = new PartitionStatus(0);

    // Update partition status
    accessor.updatePartitionStatus("test_topic_v1", partitionStatus);

    // Verify that the partition status was updated in ZK via compareAndUpdate
    Mockito.verify(mockPartitionStatusAccessor, Mockito.times(1)).update(anyString(), any(), anyInt());
  }

  @Test
  public void testUpdatePartitionStatusWhenPushStatusDoesNotExist() {
    ZkBaseDataAccessor<OfflinePushStatus> mockOfflinePushStatusAccessor = mock(ZkBaseDataAccessor.class);
    ZkBaseDataAccessor<PartitionStatus> mockPartitionStatusAccessor = mock(ZkBaseDataAccessor.class);
    VeniceOfflinePushMonitorAccessor accessor = new VeniceOfflinePushMonitorAccessor(
        "cluster0",
        mockOfflinePushStatusAccessor,
        mockPartitionStatusAccessor,
        LogContext.EMPTY);

    // Mock push status does not exist - note: pushStatusExists() uses partitionStatusAccessor, not
    // offlinePushStatusAccessor
    when(mockPartitionStatusAccessor.exists(anyString(), anyInt())).thenReturn(false);

    // Create a partition status to update
    PartitionStatus partitionStatus = new PartitionStatus(0);

    // Update partition status - should be skipped because push status doesn't exist
    accessor.updatePartitionStatus("test_topic_v1", partitionStatus);

    // Verify that the partition status was NOT updated in ZK
    Mockito.verify(mockPartitionStatusAccessor, Mockito.times(0)).update(anyString(), any(), anyInt());
  }

  @Test
  public void testUpdatePartitionStatusMergeLogicWithNullCurrentData() {
    ZkBaseDataAccessor<OfflinePushStatus> mockOfflinePushStatusAccessor = mock(ZkBaseDataAccessor.class);
    ZkBaseDataAccessor<PartitionStatus> mockPartitionStatusAccessor = mock(ZkBaseDataAccessor.class);
    VeniceOfflinePushMonitorAccessor accessor = new VeniceOfflinePushMonitorAccessor(
        "cluster0",
        mockOfflinePushStatusAccessor,
        mockPartitionStatusAccessor,
        LogContext.EMPTY);

    when(mockPartitionStatusAccessor.exists(anyString(), anyInt())).thenReturn(true);

    // Capture the DataUpdater to test merge logic
    ArgumentCaptor<DataUpdater<PartitionStatus>> updaterCaptor = ArgumentCaptor.forClass(DataUpdater.class);
    when(mockPartitionStatusAccessor.update(anyString(), updaterCaptor.capture(), anyInt())).thenReturn(true);

    // Create a partition status to update with one replica
    PartitionStatus partitionStatus = new PartitionStatus(0);
    partitionStatus.updateReplicaStatus("instance1", ExecutionStatus.COMPLETED, "push1");

    accessor.updatePartitionStatus("test_topic_v1", partitionStatus);

    // Execute the captured updater with null current data
    DataUpdater<PartitionStatus> updater = updaterCaptor.getValue();
    PartitionStatus result = updater.update(null);

    // Should return the provided partition status as-is when currentData is null
    Assert.assertNotNull(result);
    Assert.assertEquals(result.getPartitionId(), 0);
    Assert.assertEquals(result.getReplicaStatuses().size(), 1);
    Assert.assertEquals(result.getReplicaStatuses().iterator().next().getInstanceId(), "instance1");
  }

  @Test
  public void testUpdatePartitionStatusMergeLogicRemovesStaleReplicas() {
    ZkBaseDataAccessor<OfflinePushStatus> mockOfflinePushStatusAccessor = mock(ZkBaseDataAccessor.class);
    ZkBaseDataAccessor<PartitionStatus> mockPartitionStatusAccessor = mock(ZkBaseDataAccessor.class);
    VeniceOfflinePushMonitorAccessor accessor = new VeniceOfflinePushMonitorAccessor(
        "cluster0",
        mockOfflinePushStatusAccessor,
        mockPartitionStatusAccessor,
        LogContext.EMPTY);

    when(mockPartitionStatusAccessor.exists(anyString(), anyInt())).thenReturn(true);

    ArgumentCaptor<DataUpdater<PartitionStatus>> updaterCaptor = ArgumentCaptor.forClass(DataUpdater.class);
    when(mockPartitionStatusAccessor.update(anyString(), updaterCaptor.capture(), anyInt())).thenReturn(true);

    // Create partition status to update with only instance1 (instance2 should be removed)
    PartitionStatus partitionStatusToUpdate = new PartitionStatus(0);
    partitionStatusToUpdate.updateReplicaStatus("instance1", ExecutionStatus.STARTED, "push1");

    accessor.updatePartitionStatus("test_topic_v1", partitionStatusToUpdate);

    // Create current data with two replicas (instance1 and instance2)
    PartitionStatus currentData = new PartitionStatus(0);
    currentData.updateReplicaStatus("instance1", ExecutionStatus.PROGRESS, "push1");
    currentData.updateReplicaStatus("instance2", ExecutionStatus.COMPLETED, "push1");

    // Execute the captured updater
    DataUpdater<PartitionStatus> updater = updaterCaptor.getValue();
    PartitionStatus result = updater.update(currentData);

    // Should only keep instance1, remove instance2 (stale replica)
    Assert.assertEquals(result.getReplicaStatuses().size(), 1);
    ReplicaStatus keptReplica = result.getReplicaStatuses().iterator().next();
    Assert.assertEquals(keptReplica.getInstanceId(), "instance1");
  }

  @Test
  public void testUpdatePartitionStatusMergeLogicPreservesConcurrentUpdates() {
    ZkBaseDataAccessor<OfflinePushStatus> mockOfflinePushStatusAccessor = mock(ZkBaseDataAccessor.class);
    ZkBaseDataAccessor<PartitionStatus> mockPartitionStatusAccessor = mock(ZkBaseDataAccessor.class);
    VeniceOfflinePushMonitorAccessor accessor = new VeniceOfflinePushMonitorAccessor(
        "cluster0",
        mockOfflinePushStatusAccessor,
        mockPartitionStatusAccessor,
        LogContext.EMPTY);

    when(mockPartitionStatusAccessor.exists(anyString(), anyInt())).thenReturn(true);

    ArgumentCaptor<DataUpdater<PartitionStatus>> updaterCaptor = ArgumentCaptor.forClass(DataUpdater.class);
    when(mockPartitionStatusAccessor.update(anyString(), updaterCaptor.capture(), anyInt())).thenReturn(true);

    // Create partition status to update with instance1 in STARTED state
    PartitionStatus partitionStatusToUpdate = new PartitionStatus(0);
    partitionStatusToUpdate.updateReplicaStatus("instance1", ExecutionStatus.STARTED, "push1");

    accessor.updatePartitionStatus("test_topic_v1", partitionStatusToUpdate);

    // Create current data with instance1 in COMPLETED state (concurrent update from server)
    PartitionStatus currentData = new PartitionStatus(0);
    currentData.updateReplicaStatus("instance1", ExecutionStatus.COMPLETED, "push1");

    // Execute the captured updater
    DataUpdater<PartitionStatus> updater = updaterCaptor.getValue();
    PartitionStatus result = updater.update(currentData);

    // Should preserve the COMPLETED status from current data, not overwrite with STARTED
    Assert.assertEquals(result.getReplicaStatuses().size(), 1);
    ReplicaStatus replica = result.getReplicaStatuses().iterator().next();
    Assert.assertEquals(replica.getInstanceId(), "instance1");
    Assert.assertEquals(replica.getCurrentStatus(), ExecutionStatus.COMPLETED);
  }

  @Test
  public void testUpdatePartitionStatusMergeLogicPreservesStatusHistory() {
    ZkBaseDataAccessor<OfflinePushStatus> mockOfflinePushStatusAccessor = mock(ZkBaseDataAccessor.class);
    ZkBaseDataAccessor<PartitionStatus> mockPartitionStatusAccessor = mock(ZkBaseDataAccessor.class);
    VeniceOfflinePushMonitorAccessor accessor = new VeniceOfflinePushMonitorAccessor(
        "cluster0",
        mockOfflinePushStatusAccessor,
        mockPartitionStatusAccessor,
        LogContext.EMPTY);

    when(mockPartitionStatusAccessor.exists(anyString(), anyInt())).thenReturn(true);

    ArgumentCaptor<DataUpdater<PartitionStatus>> updaterCaptor = ArgumentCaptor.forClass(DataUpdater.class);
    when(mockPartitionStatusAccessor.update(anyString(), updaterCaptor.capture(), anyInt())).thenReturn(true);

    // Create partition status to update
    PartitionStatus partitionStatusToUpdate = new PartitionStatus(0);
    partitionStatusToUpdate.updateReplicaStatus("instance1", ExecutionStatus.STARTED, "push1");

    accessor.updatePartitionStatus("test_topic_v1", partitionStatusToUpdate);

    // Create current data with status history
    PartitionStatus currentData = new PartitionStatus(0);
    currentData.updateReplicaStatus("instance1", ExecutionStatus.COMPLETED, "push1");
    ReplicaStatus currentReplica = currentData.getReplicaStatuses().iterator().next();
    List<StatusSnapshot> history = new ArrayList<>();
    history.add(new StatusSnapshot(ExecutionStatus.STARTED, String.valueOf(System.currentTimeMillis())));
    history.add(new StatusSnapshot(ExecutionStatus.PROGRESS, String.valueOf(System.currentTimeMillis())));
    currentReplica.setStatusHistory(history);

    // Execute the captured updater
    DataUpdater<PartitionStatus> updater = updaterCaptor.getValue();
    PartitionStatus result = updater.update(currentData);

    // Should preserve status history from current data
    ReplicaStatus resultReplica = result.getReplicaStatuses().iterator().next();
    Assert.assertNotNull(resultReplica.getStatusHistory());
    Assert.assertEquals(resultReplica.getStatusHistory().size(), 2);
    Assert.assertEquals(resultReplica.getStatusHistory().get(0).getStatus(), ExecutionStatus.STARTED);
    Assert.assertEquals(resultReplica.getStatusHistory().get(1).getStatus(), ExecutionStatus.PROGRESS);
  }

  @Test
  public void testUpdatePartitionStatusMergeLogicMixedScenario() {
    ZkBaseDataAccessor<OfflinePushStatus> mockOfflinePushStatusAccessor = mock(ZkBaseDataAccessor.class);
    ZkBaseDataAccessor<PartitionStatus> mockPartitionStatusAccessor = mock(ZkBaseDataAccessor.class);
    VeniceOfflinePushMonitorAccessor accessor = new VeniceOfflinePushMonitorAccessor(
        "cluster0",
        mockOfflinePushStatusAccessor,
        mockPartitionStatusAccessor,
        LogContext.EMPTY);

    when(mockPartitionStatusAccessor.exists(anyString(), anyInt())).thenReturn(true);

    ArgumentCaptor<DataUpdater<PartitionStatus>> updaterCaptor = ArgumentCaptor.forClass(DataUpdater.class);
    when(mockPartitionStatusAccessor.update(anyString(), updaterCaptor.capture(), anyInt())).thenReturn(true);

    // Create partition status to update - keep instance1 and instance2, remove instance3
    PartitionStatus partitionStatusToUpdate = new PartitionStatus(0);
    partitionStatusToUpdate.updateReplicaStatus("instance1", ExecutionStatus.STARTED, "push1");
    partitionStatusToUpdate.updateReplicaStatus("instance2", ExecutionStatus.STARTED, "push1");

    accessor.updatePartitionStatus("test_topic_v1", partitionStatusToUpdate);

    // Current data has three replicas with different states
    PartitionStatus currentData = new PartitionStatus(0);
    currentData.updateReplicaStatus("instance1", ExecutionStatus.COMPLETED, "push1");
    currentData.updateReplicaStatus("instance2", ExecutionStatus.PROGRESS, "push1");
    currentData.updateReplicaStatus("instance3", ExecutionStatus.ERROR, "push1"); // Should be removed

    // Execute the captured updater
    DataUpdater<PartitionStatus> updater = updaterCaptor.getValue();
    PartitionStatus result = updater.update(currentData);

    // Should keep instance1 and instance2 with their current states, remove instance3
    Assert.assertEquals(result.getReplicaStatuses().size(), 2);
    boolean foundInstance1 = false;
    boolean foundInstance2 = false;
    boolean foundInstance3 = false;
    for (ReplicaStatus replica: result.getReplicaStatuses()) {
      if (replica.getInstanceId().equals("instance1")) {
        foundInstance1 = true;
        Assert.assertEquals(replica.getCurrentStatus(), ExecutionStatus.COMPLETED);
      } else if (replica.getInstanceId().equals("instance2")) {
        foundInstance2 = true;
        Assert.assertEquals(replica.getCurrentStatus(), ExecutionStatus.PROGRESS);
      } else if (replica.getInstanceId().equals("instance3")) {
        foundInstance3 = true;
      }
    }
    Assert.assertTrue(foundInstance1, "instance1 should be present");
    Assert.assertTrue(foundInstance2, "instance2 should be present");
    Assert.assertFalse(foundInstance3, "instance3 should be removed (stale replica)");
  }
}
