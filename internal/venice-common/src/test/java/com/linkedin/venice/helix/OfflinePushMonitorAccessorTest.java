package com.linkedin.venice.helix;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.pushmonitor.OfflinePushStatus;
import com.linkedin.venice.pushmonitor.PartitionStatus;
import com.linkedin.venice.pushmonitor.ReplicaStatus;
import com.linkedin.venice.pushmonitor.StatusSnapshot;
import com.linkedin.venice.utils.LogContext;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
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
  public void testRemoveStaleReplicasFromPartitionStatus() {
    ZkBaseDataAccessor<OfflinePushStatus> mockOfflinePushStatusAccessor = mock(ZkBaseDataAccessor.class);
    ZkBaseDataAccessor<PartitionStatus> mockPartitionStatusAccessor = mock(ZkBaseDataAccessor.class);
    VeniceOfflinePushMonitorAccessor accessor = new VeniceOfflinePushMonitorAccessor(
        "cluster0",
        mockOfflinePushStatusAccessor,
        mockPartitionStatusAccessor,
        LogContext.EMPTY);

    when(mockPartitionStatusAccessor.exists(anyString(), anyInt())).thenReturn(true);
    when(mockPartitionStatusAccessor.update(anyString(), any(), anyInt())).thenReturn(true);

    Set<String> staleInstanceIds = new HashSet<>(Arrays.asList("instance2"));
    accessor.removeStaleReplicasFromPartitionStatus("test_topic_v1", 0, staleInstanceIds);

    Mockito.verify(mockPartitionStatusAccessor, Mockito.times(1)).update(anyString(), any(), anyInt());
  }

  @Test(expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = "Push status does not exist for topic test_topic_v1.*")
  public void testRemoveStaleReplicasWhenPushStatusDoesNotExist() {
    ZkBaseDataAccessor<OfflinePushStatus> mockOfflinePushStatusAccessor = mock(ZkBaseDataAccessor.class);
    ZkBaseDataAccessor<PartitionStatus> mockPartitionStatusAccessor = mock(ZkBaseDataAccessor.class);
    VeniceOfflinePushMonitorAccessor accessor = new VeniceOfflinePushMonitorAccessor(
        "cluster0",
        mockOfflinePushStatusAccessor,
        mockPartitionStatusAccessor,
        LogContext.EMPTY);

    when(mockPartitionStatusAccessor.exists(anyString(), anyInt())).thenReturn(false);

    Set<String> staleInstanceIds = new HashSet<>(Arrays.asList("instance2"));
    accessor.removeStaleReplicasFromPartitionStatus("test_topic_v1", 0, staleInstanceIds);
  }

  @Test
  public void testRemoveStaleReplicasWithNullCurrentData() {
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

    Set<String> staleInstanceIds = new HashSet<>(Arrays.asList("instance2"));
    accessor.removeStaleReplicasFromPartitionStatus("test_topic_v1", 0, staleInstanceIds);

    DataUpdater<PartitionStatus> updater = updaterCaptor.getValue();
    PartitionStatus result = updater.update(null);

    // Should return null when currentData is null (node was deleted)
    Assert.assertNull(result);
  }

  @Test
  public void testRemoveStaleReplicasRemovesOnlyStaleOnes() {
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

    // Remove instance2, keep instance1
    Set<String> staleInstanceIds = new HashSet<>(Arrays.asList("instance2"));
    accessor.removeStaleReplicasFromPartitionStatus("test_topic_v1", 0, staleInstanceIds);

    // Current data has two replicas
    PartitionStatus currentData = new PartitionStatus(0);
    currentData.updateReplicaStatus("instance1", ExecutionStatus.PROGRESS, "push1");
    currentData.updateReplicaStatus("instance2", ExecutionStatus.COMPLETED, "push1");

    DataUpdater<PartitionStatus> updater = updaterCaptor.getValue();
    PartitionStatus result = updater.update(currentData);

    Assert.assertEquals(result.getReplicaStatuses().size(), 1);
    ReplicaStatus keptReplica = result.getReplicaStatuses().iterator().next();
    Assert.assertEquals(keptReplica.getInstanceId(), "instance1");
    Assert.assertEquals(keptReplica.getCurrentStatus(), ExecutionStatus.PROGRESS);
  }

  @Test
  public void testRemoveStaleReplicasPreservesConcurrentlyAddedReplicas() {
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

    // We want to remove instance_D (stale)
    Set<String> staleInstanceIds = new HashSet<>(Arrays.asList("instance_D"));
    accessor.removeStaleReplicasFromPartitionStatus("test_topic_v1", 0, staleInstanceIds);

    // Simulate: between the stale read and the CAS, instance_E joined concurrently
    // currentData from ZK now has [A, B, C, D, E]
    PartitionStatus currentData = new PartitionStatus(0);
    currentData.updateReplicaStatus("instance_A", ExecutionStatus.COMPLETED, "push1");
    currentData.updateReplicaStatus("instance_B", ExecutionStatus.COMPLETED, "push1");
    currentData.updateReplicaStatus("instance_C", ExecutionStatus.COMPLETED, "push1");
    currentData.updateReplicaStatus("instance_D", ExecutionStatus.STARTED, "push1"); // stale
    currentData.updateReplicaStatus("instance_E", ExecutionStatus.PROGRESS, "push1"); // concurrently added

    DataUpdater<PartitionStatus> updater = updaterCaptor.getValue();
    PartitionStatus result = updater.update(currentData);

    // Should remove only D, keep A, B, C, and E (the concurrently added one)
    Assert.assertEquals(result.getReplicaStatuses().size(), 4);
    Set<String> resultInstanceIds = new HashSet<>();
    for (ReplicaStatus rs: result.getReplicaStatuses()) {
      resultInstanceIds.add(rs.getInstanceId());
    }
    Assert.assertTrue(resultInstanceIds.contains("instance_A"));
    Assert.assertTrue(resultInstanceIds.contains("instance_B"));
    Assert.assertTrue(resultInstanceIds.contains("instance_C"));
    Assert.assertFalse(resultInstanceIds.contains("instance_D"), "Stale instance_D should be removed");
    Assert.assertTrue(resultInstanceIds.contains("instance_E"), "Concurrently added instance_E should be preserved");
  }

  @Test
  public void testRemoveStaleReplicasPreservesStatusHistory() {
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

    Set<String> staleInstanceIds = new HashSet<>(Arrays.asList("instance2"));
    accessor.removeStaleReplicasFromPartitionStatus("test_topic_v1", 0, staleInstanceIds);

    // Create current data with status history
    PartitionStatus currentData = new PartitionStatus(0);
    currentData.updateReplicaStatus("instance1", ExecutionStatus.COMPLETED, "push1");
    currentData.updateReplicaStatus("instance2", ExecutionStatus.ERROR, "push1");
    ReplicaStatus currentReplica = null;
    for (ReplicaStatus rs: currentData.getReplicaStatuses()) {
      if (rs.getInstanceId().equals("instance1")) {
        currentReplica = rs;
        break;
      }
    }
    List<StatusSnapshot> history = new ArrayList<>();
    history.add(new StatusSnapshot(ExecutionStatus.STARTED, String.valueOf(System.currentTimeMillis())));
    history.add(new StatusSnapshot(ExecutionStatus.PROGRESS, String.valueOf(System.currentTimeMillis())));
    currentReplica.setStatusHistory(history);

    DataUpdater<PartitionStatus> updater = updaterCaptor.getValue();
    PartitionStatus result = updater.update(currentData);

    Assert.assertEquals(result.getReplicaStatuses().size(), 1);
    ReplicaStatus resultReplica = result.getReplicaStatuses().iterator().next();
    Assert.assertEquals(resultReplica.getInstanceId(), "instance1");
    Assert.assertNotNull(resultReplica.getStatusHistory());
    Assert.assertEquals(resultReplica.getStatusHistory().size(), 2);
    Assert.assertEquals(resultReplica.getStatusHistory().get(0).getStatus(), ExecutionStatus.STARTED);
    Assert.assertEquals(resultReplica.getStatusHistory().get(1).getStatus(), ExecutionStatus.PROGRESS);
  }

  @Test
  public void testRemoveStaleReplicasMixedScenario() {
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

    // Remove instance3
    Set<String> staleInstanceIds = new HashSet<>(Arrays.asList("instance3"));
    accessor.removeStaleReplicasFromPartitionStatus("test_topic_v1", 0, staleInstanceIds);

    // Current data has three replicas with different states
    PartitionStatus currentData = new PartitionStatus(0);
    currentData.updateReplicaStatus("instance1", ExecutionStatus.COMPLETED, "push1");
    currentData.updateReplicaStatus("instance2", ExecutionStatus.PROGRESS, "push1");
    currentData.updateReplicaStatus("instance3", ExecutionStatus.ERROR, "push1"); // Should be removed

    DataUpdater<PartitionStatus> updater = updaterCaptor.getValue();
    PartitionStatus result = updater.update(currentData);

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
