package com.linkedin.venice.controller.systemstore;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
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
  void testCheckSystemStoreHeartbeat() {
    AtomicBoolean isRunning = new AtomicBoolean(false);
    MetaStoreReader metaStoreReader = mock(MetaStoreReader.class);
    PushStatusStoreReader pushStatusStoreReader = mock(PushStatusStoreReader.class);
    SystemStoreHealthCheckService systemStoreHealthCheckService = mock(SystemStoreHealthCheckService.class);
    when(systemStoreHealthCheckService.getMetaStoreReader()).thenReturn(metaStoreReader);
    when(systemStoreHealthCheckService.getPushStatusStoreReader()).thenReturn(pushStatusStoreReader);
    when(systemStoreHealthCheckService.isSystemStoreIngesting(anyString(), anyLong())).thenReturn(true, false);
    when(systemStoreHealthCheckService.getIsRunning()).thenReturn(isRunning);
    Set<String> newUnhealthySystemStoreSet = new HashSet<>();
    Map<String, Long> systemStoreToHeartbeatTimestampMap = new VeniceConcurrentHashMap<>();
    systemStoreToHeartbeatTimestampMap.put(VeniceSystemStoreType.META_STORE.getSystemStoreName("test_store"), 1L);
    systemStoreToHeartbeatTimestampMap
        .put(VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE.getSystemStoreName("test_store"), 1L);
    doCallRealMethod().when(systemStoreHealthCheckService)
        .checkSystemStoreHeartbeat(newUnhealthySystemStoreSet, systemStoreToHeartbeatTimestampMap);
    systemStoreHealthCheckService
        .checkSystemStoreHeartbeat(newUnhealthySystemStoreSet, systemStoreToHeartbeatTimestampMap);
    Assert.assertTrue(newUnhealthySystemStoreSet.isEmpty());
    verify(systemStoreHealthCheckService, times(0)).isSystemStoreIngesting(anyString(), anyLong());
    isRunning.set(true);
    systemStoreHealthCheckService
        .checkSystemStoreHeartbeat(newUnhealthySystemStoreSet, systemStoreToHeartbeatTimestampMap);
    Assert.assertEquals(newUnhealthySystemStoreSet.size(), 1);
    Assert.assertTrue(
        newUnhealthySystemStoreSet
            .contains(VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE.getSystemStoreName("test_store")));
    verify(systemStoreHealthCheckService, times(2)).isSystemStoreIngesting(anyString(), anyLong());

  }

  @Test
  public void testSendHeartbeat() {
    MetaStoreWriter metaStoreWriter = mock(MetaStoreWriter.class);
    PushStatusStoreWriter pushStatusStoreWriter = mock(PushStatusStoreWriter.class);
    ReadWriteStoreRepository storeRepository = mock(ReadWriteStoreRepository.class);
    String testStore1 = "test_store_1";
    Store userStore1 = mock(Store.class);
    when(userStore1.getName()).thenReturn(testStore1);
    when(userStore1.isStoreMetaSystemStoreEnabled()).thenReturn(true);
    when(userStore1.isDaVinciPushStatusStoreEnabled()).thenReturn(true);
    Store metaStore1 = mock(Store.class);
    when(metaStore1.getName()).thenReturn(VeniceSystemStoreType.META_STORE.getSystemStoreName(testStore1));
    when(metaStore1.getCurrentVersion()).thenReturn(0);
    Store pushStatusStore1 = mock(Store.class);
    when(pushStatusStore1.getName())
        .thenReturn(VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE.getSystemStoreName(testStore1));
    when(pushStatusStore1.getCurrentVersion()).thenReturn(1);

    String testStore2 = "test_store_2";
    Store userStore2 = mock(Store.class);
    when(userStore2.getName()).thenReturn(testStore2);
    when(userStore2.isStoreMetaSystemStoreEnabled()).thenReturn(true);
    when(userStore2.isDaVinciPushStatusStoreEnabled()).thenReturn(true);
    Store metaStore2 = mock(Store.class);
    when(metaStore2.getName()).thenReturn(VeniceSystemStoreType.META_STORE.getSystemStoreName(testStore2));
    when(metaStore2.getCurrentVersion()).thenReturn(1);
    Store pushStatusStore2 = mock(Store.class);
    when(pushStatusStore2.getName())
        .thenReturn(VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE.getSystemStoreName(testStore2));
    when(pushStatusStore2.getCurrentVersion()).thenReturn(0);

    String testStore3 = "test_store_3";
    Store userStore3 = mock(Store.class);
    when(userStore3.getName()).thenReturn(testStore3);
    when(userStore3.isStoreMetaSystemStoreEnabled()).thenReturn(false);
    when(userStore3.isDaVinciPushStatusStoreEnabled()).thenReturn(false);

    when(storeRepository.getAllStores()).thenReturn(
        Arrays.asList(metaStore1, pushStatusStore1, userStore1, metaStore2, pushStatusStore2, userStore2, userStore3));
    AtomicBoolean isRunning = new AtomicBoolean(false);

    SystemStoreHealthCheckService systemStoreHealthCheckService = mock(SystemStoreHealthCheckService.class);
    when(systemStoreHealthCheckService.getMetaStoreWriter()).thenReturn(metaStoreWriter);
    when(systemStoreHealthCheckService.getPushStatusStoreWriter()).thenReturn(pushStatusStoreWriter);
    when(systemStoreHealthCheckService.getStoreRepository()).thenReturn(storeRepository);
    doCallRealMethod().when(systemStoreHealthCheckService).checkAndSendHeartbeatToSystemStores(anySet(), anyMap());
    when(systemStoreHealthCheckService.getIsRunning()).thenReturn(isRunning);

    Set<String> newUnhealthySystemStoreSet = new HashSet<>();
    Map<String, Long> systemStoreToHeartbeatTimestampMap = new VeniceConcurrentHashMap<>();

    systemStoreHealthCheckService
        .checkAndSendHeartbeatToSystemStores(newUnhealthySystemStoreSet, systemStoreToHeartbeatTimestampMap);
    Assert.assertTrue(newUnhealthySystemStoreSet.isEmpty());
    Assert.assertTrue(systemStoreToHeartbeatTimestampMap.isEmpty());

    isRunning.set(true);
    systemStoreHealthCheckService
        .checkAndSendHeartbeatToSystemStores(newUnhealthySystemStoreSet, systemStoreToHeartbeatTimestampMap);
    Assert.assertEquals(newUnhealthySystemStoreSet.size(), 4);
    Assert.assertTrue(
        newUnhealthySystemStoreSet.contains(VeniceSystemStoreType.META_STORE.getSystemStoreName(testStore1)));
    Assert.assertTrue(
        newUnhealthySystemStoreSet
            .contains(VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE.getSystemStoreName(testStore2)));
    Assert.assertTrue(
        newUnhealthySystemStoreSet.contains(VeniceSystemStoreType.META_STORE.getSystemStoreName(testStore3)));
    Assert.assertTrue(
        newUnhealthySystemStoreSet
            .contains(VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE.getSystemStoreName(testStore3)));

    Assert.assertEquals(systemStoreToHeartbeatTimestampMap.size(), 2);
    Assert.assertNotNull(
        systemStoreToHeartbeatTimestampMap
            .get(VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE.getSystemStoreName(testStore1)));
    Assert.assertNotNull(
        systemStoreToHeartbeatTimestampMap.get(VeniceSystemStoreType.META_STORE.getSystemStoreName(testStore2)));
  }
}
