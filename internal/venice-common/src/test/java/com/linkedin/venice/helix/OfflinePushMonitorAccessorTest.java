package com.linkedin.venice.helix;

import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

import com.linkedin.venice.pushmonitor.OfflinePushStatus;
import java.util.Optional;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
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
}
