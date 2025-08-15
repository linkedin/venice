package com.linkedin.venice.controller.systemstore;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.VeniceParentHelixAdmin;
import com.linkedin.venice.controller.stats.SystemStoreHealthCheckStats;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.SystemStoreHeartbeatResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.utils.LogContext;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.logging.log4j.LogManager;
import org.testng.Assert;
import org.testng.annotations.Test;


public class SystemStoreRepairTaskTest {
  @Test
  public void testFilterDisabledCluster() {
    SystemStoreRepairTask systemStoreRepairTask = mock(SystemStoreRepairTask.class);

    Map<String, SystemStoreHealthCheckStats> systemStoreHealthCheckStatsMap = new HashMap<>();
    systemStoreHealthCheckStatsMap.put("venice-2", mock(SystemStoreHealthCheckStats.class));
    systemStoreHealthCheckStatsMap.put("venice-3", mock(SystemStoreHealthCheckStats.class));
    doReturn(systemStoreHealthCheckStatsMap).when(systemStoreRepairTask).getClusterToSystemStoreHealthCheckStatsMap();

    VeniceParentHelixAdmin parentHelixAdmin = mock(VeniceParentHelixAdmin.class);
    List<String> leaderClusterList = Arrays.asList("venice-1", "venice-3");
    doReturn(leaderClusterList).when(parentHelixAdmin).getClustersLeaderOf();
    doReturn(LogContext.EMPTY).when(parentHelixAdmin).getLogContext();
    doReturn(parentHelixAdmin).when(systemStoreRepairTask).getParentAdmin();

    doCallRealMethod().when(systemStoreRepairTask).run();
    systemStoreRepairTask.run();

    // Only venice-3 is the leader cluster and has system store repair task enabled.
    verify(systemStoreRepairTask, never()).checkSystemStoresHealth(eq("venice-1"), anySet(), anySet(), anyMap());
    verify(systemStoreRepairTask, never()).checkSystemStoresHealth(eq("venice-2"), anySet(), anySet(), anyMap());
    verify(systemStoreRepairTask).checkSystemStoresHealth(eq("venice-3"), anySet(), anySet(), anyMap());

  }

  @Test
  public void testPeriodicCheckHeartbeat() {

    SystemStoreRepairTask systemStoreRepairTask = mock(SystemStoreRepairTask.class);

    String clusterName = "venice";

    String userStoreName1 = "testStore";
    String metaStoreName1 = VeniceSystemStoreType.META_STORE.getSystemStoreName(userStoreName1);
    String pushStatusStoreName1 = VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE.getSystemStoreName(userStoreName1);

    String userStoreName2 = "testStore2";
    String metaStoreName2 = VeniceSystemStoreType.META_STORE.getSystemStoreName(userStoreName2);
    String pushStatusStoreName2 = VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE.getSystemStoreName(userStoreName2);

    Map<String, Long> systemStoreToHeartbeatTimestampMap = new HashMap<>();
    systemStoreToHeartbeatTimestampMap.put(metaStoreName1, 100L);
    systemStoreToHeartbeatTimestampMap.put(pushStatusStoreName1, 100L);
    systemStoreToHeartbeatTimestampMap.put(metaStoreName2, 100L);
    systemStoreToHeartbeatTimestampMap.put(pushStatusStoreName2, 100L);

    Set<String> unhealthySystemStoreSet = new HashSet<>();
    Set<String> unreachableSystemStoreSet = new HashSet<>();
    doCallRealMethod().when(systemStoreRepairTask)
        .checkHeartbeatFromSystemStores(anyString(), anySet(), anySet(), anyMap(), anyInt());
    // Check is not enabled.
    when(systemStoreRepairTask.shouldContinue(anyString())).thenReturn(false);
    doCallRealMethod().when(systemStoreRepairTask).periodicCheckTask(anyString(), anyInt(), anyInt(), any());
    doReturn(1).when(systemStoreRepairTask).getHeartbeatCheckIntervalInSeconds();
    systemStoreRepairTask.checkHeartbeatFromSystemStores(
        clusterName,
        unhealthySystemStoreSet,
        unreachableSystemStoreSet,
        systemStoreToHeartbeatTimestampMap,
        5);
    Assert.assertEquals(unhealthySystemStoreSet.size(), 0);
    Assert.assertEquals(unreachableSystemStoreSet.size(), 0);

    // Always stale
    when(systemStoreRepairTask.getHeartbeatFromSystemStore(clusterName, metaStoreName1)).thenReturn(10L);
    // First stale, then healthy HB
    when(systemStoreRepairTask.getHeartbeatFromSystemStore(clusterName, pushStatusStoreName1)).thenReturn(10L)
        .thenReturn(100L);
    // Always not reachable
    when(systemStoreRepairTask.getHeartbeatFromSystemStore(clusterName, metaStoreName2)).thenReturn(-1L);
    // First not reachable, then healthy HB
    when(systemStoreRepairTask.getHeartbeatFromSystemStore(clusterName, pushStatusStoreName2)).thenReturn(-1L)
        .thenReturn(100L);

    // Check is enabled.
    when(systemStoreRepairTask.shouldContinue(anyString())).thenReturn(true);
    systemStoreRepairTask.checkHeartbeatFromSystemStores(
        clusterName,
        unhealthySystemStoreSet,
        unreachableSystemStoreSet,
        systemStoreToHeartbeatTimestampMap,
        5);
    Assert.assertEquals(unhealthySystemStoreSet.size(), 2);
    Assert.assertEquals(unreachableSystemStoreSet.size(), 1);
  }

  @Test
  public void testCheckHeartbeat() {
    SystemStoreRepairTask systemStoreRepairTask = mock(SystemStoreRepairTask.class);
    String clusterName = "venice";
    String userStoreName1 = "testStore";
    String metaStoreName1 = VeniceSystemStoreType.META_STORE.getSystemStoreName(userStoreName1);
    String pushStatusStoreName1 = VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE.getSystemStoreName(userStoreName1);

    String userStoreName2 = "testStore2";
    String metaStoreName2 = VeniceSystemStoreType.META_STORE.getSystemStoreName(userStoreName2);
    String pushStatusStoreName2 = VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE.getSystemStoreName(userStoreName2);

    SystemStoreHeartbeatResponse heartbeatResponse1 = new SystemStoreHeartbeatResponse();
    heartbeatResponse1.setHeartbeatTimestamp(10L);
    SystemStoreHeartbeatResponse heartbeatResponse2 = new SystemStoreHeartbeatResponse();
    heartbeatResponse2.setHeartbeatTimestamp(11L);
    SystemStoreHeartbeatResponse heartbeatResponse3 = new SystemStoreHeartbeatResponse();
    heartbeatResponse3.setHeartbeatTimestamp(-1L);

    ControllerClient client1 = mock(ControllerClient.class);
    when(client1.getHeartbeatFromSystemStore(metaStoreName1)).thenReturn(heartbeatResponse1);
    when(client1.getHeartbeatFromSystemStore(pushStatusStoreName1)).thenReturn(heartbeatResponse1);
    when(client1.getHeartbeatFromSystemStore(metaStoreName2)).thenReturn(heartbeatResponse1);
    when(client1.getHeartbeatFromSystemStore(pushStatusStoreName2)).thenReturn(heartbeatResponse1);

    ControllerClient client2 = mock(ControllerClient.class);
    when(client2.getHeartbeatFromSystemStore(metaStoreName1)).thenReturn(heartbeatResponse2);
    when(client2.getHeartbeatFromSystemStore(pushStatusStoreName1)).thenReturn(heartbeatResponse3);
    when(client2.getHeartbeatFromSystemStore(metaStoreName2)).thenReturn(heartbeatResponse1);
    when(client2.getHeartbeatFromSystemStore(pushStatusStoreName2)).thenReturn(heartbeatResponse1);

    when(systemStoreRepairTask.getHeartbeatFromSystemStore(anyString(), anyString())).thenCallRealMethod();
    doCallRealMethod().when(systemStoreRepairTask)
        .checkHeartbeatFromSystemStores(anyString(), anySet(), anySet(), anyMap(), anyInt());
    Map<String, ControllerClient> controllerClientMap = new HashMap<>();

    controllerClientMap.put("region1", client1);
    controllerClientMap.put("region2", client2);
    when(systemStoreRepairTask.getControllerClientMap(clusterName)).thenReturn(controllerClientMap);

    Assert.assertEquals(systemStoreRepairTask.getHeartbeatFromSystemStore(clusterName, metaStoreName1), 10L);
    Assert.assertEquals(systemStoreRepairTask.getHeartbeatFromSystemStore(clusterName, pushStatusStoreName1), -1L);
    Assert.assertEquals(systemStoreRepairTask.getHeartbeatFromSystemStore(clusterName, metaStoreName2), 10L);
    Assert.assertEquals(systemStoreRepairTask.getHeartbeatFromSystemStore(clusterName, pushStatusStoreName2), 10L);
  }

  @Test
  public void testSendHeartbeat() {
    Version staleVersion = mock(Version.class);
    doReturn(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(100)).when(staleVersion).getCreatedTime();
    Version newVersion = mock(Version.class);
    doReturn(System.currentTimeMillis()).when(newVersion).getCreatedTime();

    String testStore1 = "test_store_1";
    Store userStore1 = mock(Store.class);
    when(userStore1.getName()).thenReturn(testStore1);
    when(userStore1.isStoreMetaSystemStoreEnabled()).thenReturn(true);
    when(userStore1.isDaVinciPushStatusStoreEnabled()).thenReturn(true);
    Store metaStore1 = mock(Store.class);
    when(metaStore1.getName()).thenReturn(VeniceSystemStoreType.META_STORE.getSystemStoreName(testStore1));
    when(metaStore1.getVersions()).thenReturn(Collections.emptyList());
    Store pushStatusStore1 = mock(Store.class);
    when(pushStatusStore1.getName())
        .thenReturn(VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE.getSystemStoreName(testStore1));
    when(pushStatusStore1.getVersions()).thenReturn(Collections.singletonList(newVersion));

    String testStore2 = "test_store_2";
    Store userStore2 = mock(Store.class);
    when(userStore2.getName()).thenReturn(testStore2);
    when(userStore2.isStoreMetaSystemStoreEnabled()).thenReturn(true);
    when(userStore2.isDaVinciPushStatusStoreEnabled()).thenReturn(true);
    Store metaStore2 = mock(Store.class);
    when(metaStore2.getName()).thenReturn(VeniceSystemStoreType.META_STORE.getSystemStoreName(testStore2));
    when(metaStore2.getVersions()).thenReturn(Arrays.asList(staleVersion, newVersion));
    Store pushStatusStore2 = mock(Store.class);
    when(pushStatusStore2.getName())
        .thenReturn(VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE.getSystemStoreName(testStore2));
    when(pushStatusStore2.getVersions()).thenReturn(Collections.singletonList(staleVersion));

    String testStore3 = "test_store_3";
    Store userStore3 = mock(Store.class);
    when(userStore3.getName()).thenReturn(testStore3);
    when(userStore3.isStoreMetaSystemStoreEnabled()).thenReturn(false);
    when(userStore3.isDaVinciPushStatusStoreEnabled()).thenReturn(false);

    String testStore4 = "test_store_4";
    Store userStore4 = mock(Store.class);
    when(userStore4.getName()).thenReturn(testStore4);
    when(userStore4.isMigrating()).thenReturn(true);
    when(userStore4.isStoreMetaSystemStoreEnabled()).thenReturn(true);
    when(userStore4.isDaVinciPushStatusStoreEnabled()).thenReturn(true);
    Store metaStore4 = mock(Store.class);
    when(metaStore4.getName()).thenReturn(VeniceSystemStoreType.META_STORE.getSystemStoreName(testStore4));
    when(metaStore4.getVersions()).thenReturn(Collections.singletonList(staleVersion));
    when(metaStore4.isMigrating()).thenReturn(true);
    Store pushStatusStore4 = mock(Store.class);
    when(pushStatusStore4.getName())
        .thenReturn(VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE.getSystemStoreName(testStore4));
    when(pushStatusStore4.getVersions()).thenReturn(Collections.singletonList(staleVersion));
    when(pushStatusStore4.isMigrating()).thenReturn(true);

    String cluster = "venice";

    VeniceParentHelixAdmin veniceParentHelixAdmin = mock(VeniceParentHelixAdmin.class);
    when(veniceParentHelixAdmin.getAllStores(cluster)).thenReturn(
        Arrays.asList(
            metaStore1,
            pushStatusStore1,
            userStore1,
            metaStore2,
            pushStatusStore2,
            userStore2,
            userStore3,
            userStore4,
            metaStore4,
            pushStatusStore4));
    AtomicBoolean isRunning = new AtomicBoolean(false);

    SystemStoreRepairTask systemStoreRepairTask = mock(SystemStoreRepairTask.class);
    doReturn(30).when(systemStoreRepairTask).getVersionRefreshThresholdInDays();
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
    /**
     * It is expected to NOT check current version as it is always 0 in parent controller.
     */
    Assert.assertEquals(newUnhealthySystemStoreSet.size(), 4);
    Assert.assertTrue(
        newUnhealthySystemStoreSet.contains(VeniceSystemStoreType.META_STORE.getSystemStoreName(testStore3)));
    Assert.assertTrue(
        newUnhealthySystemStoreSet
            .contains(VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE.getSystemStoreName(testStore3)));

    /**
     * All other stores should have new timestamp sent.
     */
    LogManager.getLogger().info("DEBUGGING: {}", systemStoreToHeartbeatTimestampMap);
    Assert.assertEquals(systemStoreToHeartbeatTimestampMap.size(), 2);
    Assert.assertNotNull(
        systemStoreToHeartbeatTimestampMap
            .get(VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE.getSystemStoreName(testStore1)));
    Assert.assertNotNull(
        systemStoreToHeartbeatTimestampMap.get(VeniceSystemStoreType.META_STORE.getSystemStoreName(testStore2)));
  }

  @Test
  public void testRepairSystemStore() {
    String clusterName = "test-cluster";
    SystemStoreRepairTask systemStoreRepairTask = mock(SystemStoreRepairTask.class);
    doCallRealMethod().when(systemStoreRepairTask).repairBadSystemStore(anyString(), anySet());
    doCallRealMethod().when(systemStoreRepairTask).pollSystemStorePushStatus(anyString(), anyMap(), anySet(), anyInt());
    doReturn(true).when(systemStoreRepairTask).shouldContinue(anyString());
    doCallRealMethod().when(systemStoreRepairTask).periodicCheckTask(anyString(), anyInt(), anyInt(), any());

    VeniceParentHelixAdmin parentHelixAdmin = mock(VeniceParentHelixAdmin.class);
    doReturn(parentHelixAdmin).when(systemStoreRepairTask).getParentAdmin();

    doReturn(1).when(systemStoreRepairTask).getRepairJobCheckIntervalInSeconds();
    doReturn(1).when(systemStoreRepairTask).getHeartbeatWaitTimeInSeconds();
    doReturn(3).when(systemStoreRepairTask).getRepairJobCheckTimeoutInSeconds();

    String systemStore = VeniceSystemStoreUtils.getMetaStoreName("testStore");
    Set<String> unhealthySystemStoreSet = new HashSet<>();
    unhealthySystemStoreSet.add(systemStore);

    Version version = mock(Version.class);
    doReturn(5).when(version).getNumber();
    doReturn(version).when(systemStoreRepairTask).getNewSystemStoreVersion(anyString(), anyString(), anyString());

    Admin.OfflinePushStatusInfo midPushStatus = mock(Admin.OfflinePushStatusInfo.class);
    Admin.OfflinePushStatusInfo goodPushStatus = mock(Admin.OfflinePushStatusInfo.class);
    Admin.OfflinePushStatusInfo errorPushStatus = mock(Admin.OfflinePushStatusInfo.class);
    doReturn(ExecutionStatus.STARTED).when(midPushStatus).getExecutionStatus();
    doReturn(ExecutionStatus.COMPLETED).when(goodPushStatus).getExecutionStatus();
    doReturn(ExecutionStatus.ERROR).when(errorPushStatus).getExecutionStatus();

    // Push Failed.
    when(parentHelixAdmin.getOffLinePushStatus(clusterName, Version.composeKafkaTopic(systemStore, 5)))
        .thenReturn(midPushStatus)
        .thenReturn(errorPushStatus);
    systemStoreRepairTask.repairBadSystemStore(clusterName, unhealthySystemStoreSet);
    Assert.assertFalse(unhealthySystemStoreSet.isEmpty());

    // Push Timeout.
    when(parentHelixAdmin.getOffLinePushStatus(clusterName, Version.composeKafkaTopic(systemStore, 5)))
        .thenReturn(midPushStatus);
    systemStoreRepairTask.repairBadSystemStore(clusterName, unhealthySystemStoreSet);
    Assert.assertFalse(unhealthySystemStoreSet.isEmpty());

    // Push Completed.
    when(parentHelixAdmin.getOffLinePushStatus(clusterName, Version.composeKafkaTopic(systemStore, 5)))
        .thenReturn(midPushStatus)
        .thenReturn(goodPushStatus);
    systemStoreRepairTask.repairBadSystemStore(clusterName, unhealthySystemStoreSet);
    Assert.assertTrue(unhealthySystemStoreSet.isEmpty());

    // Poll throws exception, should be caught inside.
    unhealthySystemStoreSet.add(systemStore);
    doThrow(VeniceException.class).when(parentHelixAdmin)
        .getOffLinePushStatus(clusterName, Version.composeKafkaTopic(systemStore, 5));
    systemStoreRepairTask.repairBadSystemStore(clusterName, unhealthySystemStoreSet);
    Assert.assertFalse(unhealthySystemStoreSet.isEmpty());

  }
}
