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
    VeniceOfflinePushMonitorAccessor accessor =
        new VeniceOfflinePushMonitorAccessor("cluster0", mockOfflinePushStatusAccessor, mock(ZkBaseDataAccessor.class));
    Optional<Long> ctime = accessor.getOfflinePushStatusCreationTime("test");
    Assert.assertFalse(ctime.isPresent());
  }

  @Test
  public void testNullStatWillReturnEmptyOptional() {
    ZkBaseDataAccessor<OfflinePushStatus> mockOfflinePushStatusAccessor = mock(ZkBaseDataAccessor.class);
    doReturn(null).when(mockOfflinePushStatusAccessor).getStat(anyString(), anyInt());
    VeniceOfflinePushMonitorAccessor accessor =
        new VeniceOfflinePushMonitorAccessor("cluster0", mockOfflinePushStatusAccessor, mock(ZkBaseDataAccessor.class));
    Optional<Long> ctime = accessor.getOfflinePushStatusCreationTime("test");
    Assert.assertFalse(ctime.isPresent());
  }

  @Test
  public void testBatchUpdateEOIP() {
    ZkBaseDataAccessor<OfflinePushStatus> mockOfflinePushStatusAccessor = mock(ZkBaseDataAccessor.class);
    ZkBaseDataAccessor<PartitionStatus> mockPartitionStatusAccessor = mock(ZkBaseDataAccessor.class);
    doReturn(null).when(mockOfflinePushStatusAccessor).getStat(anyString(), anyInt());
    VeniceOfflinePushMonitorAccessor accessor =
        new VeniceOfflinePushMonitorAccessor("cluster0", mockOfflinePushStatusAccessor, mockPartitionStatusAccessor);
    when(mockPartitionStatusAccessor.exists(anyString(), anyInt())).thenReturn(true);
    when(mockPartitionStatusAccessor.update(anyString(), any(), anyInt())).thenReturn(true);
    accessor.batchUpdateReplicaIncPushStatus("test_topic", 0, "test_instance_id", 100L, Arrays.asList("a", "b"));
    Mockito.verify(mockPartitionStatusAccessor, Mockito.times(1)).update(anyString(), any(), anyInt());
  }
}
