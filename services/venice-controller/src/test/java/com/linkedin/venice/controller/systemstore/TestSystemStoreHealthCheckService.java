package com.linkedin.venice.controller.systemstore;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.pushstatushelper.PushStatusStoreReader;
import com.linkedin.venice.system.store.MetaStoreReader;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestSystemStoreHealthCheckService {
  @Test
  public void testCheckHeartbeat() {
    MetaStoreReader metaStoreReader = mock(MetaStoreReader.class);
    PushStatusStoreReader pushStatusStoreReader = mock(PushStatusStoreReader.class);
    SystemStoreHealthCheckService systemStoreHealthCheckService = mock(SystemStoreHealthCheckService.class);
    when(systemStoreHealthCheckService.getMetaStoreReader()).thenReturn(metaStoreReader);
    when(systemStoreHealthCheckService.getPushStatusStoreReader()).thenReturn(pushStatusStoreReader);
    when(systemStoreHealthCheckService.isSystemStoreIngesting(anyString(), anyLong())).thenCallRealMethod();

    when(pushStatusStoreReader.getHeartbeat(anyString(), anyString())).thenReturn(1L, 1L, 10L);
    when(metaStoreReader.getHeartbeat(anyString())).thenReturn(1L, 1L, 10L);
    String userStoreName = "testStore";
    String metaStoreName = VeniceSystemStoreType.META_STORE.getSystemStoreName(userStoreName);
    // Eventually should succeed.
    Assert.assertTrue(systemStoreHealthCheckService.isSystemStoreIngesting(metaStoreName, 10L));
    // Eventually should fail.
    Assert.assertFalse(systemStoreHealthCheckService.isSystemStoreIngesting(metaStoreName, 11L));

    String pushStatusStoreName = VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE.getSystemStoreName(userStoreName);
    Assert.assertTrue(systemStoreHealthCheckService.isSystemStoreIngesting(pushStatusStoreName, 10L));
    Assert.assertFalse(systemStoreHealthCheckService.isSystemStoreIngesting(pushStatusStoreName, 11L));

  }
}
