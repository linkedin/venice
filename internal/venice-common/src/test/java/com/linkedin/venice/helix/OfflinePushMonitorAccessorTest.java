package com.linkedin.venice.helix;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.linkedin.venice.pushmonitor.OfflinePushStatus;
import com.linkedin.venice.pushmonitor.PartitionStatus;
import com.linkedin.venice.utils.LogContext;
import java.util.Arrays;
import java.util.Optional;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
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
}
