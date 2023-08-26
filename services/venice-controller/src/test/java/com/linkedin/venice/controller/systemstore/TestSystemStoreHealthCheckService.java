package com.linkedin.venice.controller.systemstore;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.meta.ReadWriteStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.pushstatushelper.PushStatusStoreReader;
import com.linkedin.venice.pushstatushelper.PushStatusStoreWriter;
import com.linkedin.venice.system.store.MetaStoreReader;
import com.linkedin.venice.system.store.MetaStoreWriter;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
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

  @Test
  public void testSendHeartbeat() {
    MetaStoreWriter metaStoreWriter = mock(MetaStoreWriter.class);
    PushStatusStoreWriter pushStatusStoreWriter = mock(PushStatusStoreWriter.class);
    ReadWriteStoreRepository storeRepository = mock(ReadWriteStoreRepository.class);
    Store metaStoreWithNoVersion = mock(Store.class);
    when(metaStoreWithNoVersion.getName())
        .thenReturn(VeniceSystemStoreType.META_STORE.getSystemStoreName("test_store"));
    when(metaStoreWithNoVersion.getCurrentVersion()).thenReturn(0);

    Store pushStatusStore = mock(Store.class);
    when(pushStatusStore.getName())
        .thenReturn(VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE.getSystemStoreName("test_store"));
    when(pushStatusStore.getCurrentVersion()).thenReturn(1);

    Store userStore = mock(Store.class);
    when(userStore.getName()).thenReturn("test_store");

    when(storeRepository.getAllStores()).thenReturn(Arrays.asList(metaStoreWithNoVersion, pushStatusStore, userStore));
    AtomicBoolean isRunning = new AtomicBoolean(false);

    SystemStoreHealthCheckService systemStoreHealthCheckService = mock(SystemStoreHealthCheckService.class);
    when(systemStoreHealthCheckService.getMetaStoreWriter()).thenReturn(metaStoreWriter);
    when(systemStoreHealthCheckService.getPushStatusStoreWriter()).thenReturn(pushStatusStoreWriter);
    when(systemStoreHealthCheckService.getStoreRepository()).thenReturn(storeRepository);
    doCallRealMethod().when(systemStoreHealthCheckService).sendHeartbeatToSystemStores(anySet(), anyMap());
    when(systemStoreHealthCheckService.getIsRunning()).thenReturn(isRunning);

    Set<String> newUnhealthySystemStoreSet = new HashSet<>();
    Map<String, Long> systemStoreToHeartbeatTimestampMap = new VeniceConcurrentHashMap<>();

    systemStoreHealthCheckService
        .sendHeartbeatToSystemStores(newUnhealthySystemStoreSet, systemStoreToHeartbeatTimestampMap);
    Assert.assertTrue(newUnhealthySystemStoreSet.isEmpty());
    Assert.assertTrue(systemStoreToHeartbeatTimestampMap.isEmpty());

    isRunning.set(true);
    systemStoreHealthCheckService
        .sendHeartbeatToSystemStores(newUnhealthySystemStoreSet, systemStoreToHeartbeatTimestampMap);
    Assert.assertEquals(newUnhealthySystemStoreSet.size(), 1);
    Assert.assertTrue(
        newUnhealthySystemStoreSet.contains(VeniceSystemStoreType.META_STORE.getSystemStoreName("test_store")));
    Assert.assertEquals(systemStoreToHeartbeatTimestampMap.size(), 1);
    Assert.assertNotNull(
        systemStoreToHeartbeatTimestampMap
            .get(VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE.getSystemStoreName("test_store")));
  }
}
