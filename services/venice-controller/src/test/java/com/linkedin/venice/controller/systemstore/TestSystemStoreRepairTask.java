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
import com.linkedin.venice.controller.VeniceParentHelixAdmin;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.HeartbeatResponse;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestSystemStoreRepairTask {
  @Test
  public void testCheckHeartbeat() {
    SystemStoreRepairTask systemStoreRepairTask = mock(SystemStoreRepairTask.class);
    ControllerClient client = mock(ControllerClient.class);
    String clusterName = "venice";
    String region = "region";
    String userStoreName = "testStore";
    String metaStoreName = VeniceSystemStoreType.META_STORE.getSystemStoreName(userStoreName);
    String pushStatusStoreName = VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE.getSystemStoreName(userStoreName);
    HeartbeatResponse heartbeatResponse = new HeartbeatResponse();
    heartbeatResponse.setHeartbeatTimestamp(10L);
    when(client.getHeartbeatFromSystemStore(metaStoreName)).thenReturn(heartbeatResponse);
    when(client.getHeartbeatFromSystemStore(pushStatusStoreName)).thenReturn(heartbeatResponse);
    when(systemStoreRepairTask.isHeartbeatReceivedBySystemStore(anyString(), anyString(), anyLong()))
        .thenCallRealMethod();
    when(systemStoreRepairTask.getControllerClientMap(clusterName))
        .thenReturn(Collections.singletonMap(region, client));

    // Eventually should succeed.
    Assert.assertTrue(systemStoreRepairTask.isHeartbeatReceivedBySystemStore(clusterName, metaStoreName, 10L));
    // Eventually should fail.
    Assert.assertFalse(systemStoreRepairTask.isHeartbeatReceivedBySystemStore(clusterName, metaStoreName, 11L));

    Assert.assertTrue(systemStoreRepairTask.isHeartbeatReceivedBySystemStore(clusterName, pushStatusStoreName, 10L));
    Assert.assertFalse(systemStoreRepairTask.isHeartbeatReceivedBySystemStore(clusterName, pushStatusStoreName, 11L));
  }

  @Test
  void testCheckSystemStoreHeartbeat() {
    AtomicBoolean isRunning = new AtomicBoolean(false);
    SystemStoreRepairTask systemStoreRepairTask = mock(SystemStoreRepairTask.class);
    when(systemStoreRepairTask.isHeartbeatReceivedBySystemStore(anyString(), anyString(), anyLong()))
        .thenReturn(true, false);
    when(systemStoreRepairTask.getIsRunning()).thenReturn(isRunning);
    String clusterName = "venice";
    Set<String> newUnhealthySystemStoreSet = new HashSet<>();
    Map<String, Long> systemStoreToHeartbeatTimestampMap = new VeniceConcurrentHashMap<>();
    systemStoreToHeartbeatTimestampMap.put(VeniceSystemStoreType.META_STORE.getSystemStoreName("test_store"), 1L);
    systemStoreToHeartbeatTimestampMap
        .put(VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE.getSystemStoreName("test_store"), 1L);
    doCallRealMethod().when(systemStoreRepairTask)
        .checkHeartbeatFromSystemStores(clusterName, newUnhealthySystemStoreSet, systemStoreToHeartbeatTimestampMap);

    when(systemStoreRepairTask.shouldContinue(anyString())).thenReturn(false);
    systemStoreRepairTask
        .checkHeartbeatFromSystemStores(clusterName, newUnhealthySystemStoreSet, systemStoreToHeartbeatTimestampMap);
    Assert.assertTrue(newUnhealthySystemStoreSet.isEmpty());
    verify(systemStoreRepairTask, times(0)).isHeartbeatReceivedBySystemStore(anyString(), anyString(), anyLong());
    isRunning.set(true);
    when(systemStoreRepairTask.shouldContinue(anyString())).thenReturn(true);
    systemStoreRepairTask
        .checkHeartbeatFromSystemStores(clusterName, newUnhealthySystemStoreSet, systemStoreToHeartbeatTimestampMap);
    Assert.assertEquals(newUnhealthySystemStoreSet.size(), 1);
    Assert.assertTrue(
        newUnhealthySystemStoreSet
            .contains(VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE.getSystemStoreName("test_store")));
    verify(systemStoreRepairTask, times(2)).isHeartbeatReceivedBySystemStore(anyString(), anyString(), anyLong());

  }

  @Test
  public void testSendHeartbeat() {
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

    String cluster = "venice";

    VeniceParentHelixAdmin veniceParentHelixAdmin = mock(VeniceParentHelixAdmin.class);
    when(veniceParentHelixAdmin.getAllStores(cluster)).thenReturn(
        Arrays.asList(metaStore1, pushStatusStore1, userStore1, metaStore2, pushStatusStore2, userStore2, userStore3));
    AtomicBoolean isRunning = new AtomicBoolean(false);

    SystemStoreRepairTask systemStoreRepairTask = mock(SystemStoreRepairTask.class);
    when(systemStoreRepairTask.getParentAdmin()).thenReturn(veniceParentHelixAdmin);
    doCallRealMethod().when(systemStoreRepairTask).checkAndSendHeartbeatToSystemStores(anyString(), anySet(), anyMap());
    when(systemStoreRepairTask.getIsRunning()).thenReturn(isRunning);

    when(systemStoreRepairTask.shouldContinue(cluster)).thenReturn(false);
    Set<String> newUnhealthySystemStoreSet = new HashSet<>();
    Map<String, Long> systemStoreToHeartbeatTimestampMap = new VeniceConcurrentHashMap<>();

    systemStoreRepairTask
        .checkAndSendHeartbeatToSystemStores(cluster, newUnhealthySystemStoreSet, systemStoreToHeartbeatTimestampMap);
    Assert.assertTrue(newUnhealthySystemStoreSet.isEmpty());
    Assert.assertTrue(systemStoreToHeartbeatTimestampMap.isEmpty());

    isRunning.set(true);
    when(systemStoreRepairTask.shouldContinue(cluster)).thenReturn(true);
    systemStoreRepairTask
        .checkAndSendHeartbeatToSystemStores(cluster, newUnhealthySystemStoreSet, systemStoreToHeartbeatTimestampMap);
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
