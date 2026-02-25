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
import com.linkedin.venice.controller.systemstore.SystemStoreHealthChecker.HealthCheckResult;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.utils.LogContext;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
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
    verify(systemStoreRepairTask, never()).checkSystemStoresHealth(eq("venice-1"), anySet());
    verify(systemStoreRepairTask, never()).checkSystemStoresHealth(eq("venice-2"), anySet());
    verify(systemStoreRepairTask).checkSystemStoresHealth(eq("venice-3"), anySet());
  }

  @Test
  public void testPreFilterSystemStores() {
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
    doCallRealMethod().when(systemStoreRepairTask).preFilterSystemStores(anyString(), anySet());
    when(systemStoreRepairTask.getIsRunning()).thenReturn(isRunning);

    // shouldContinue returns false -> early exit
    when(systemStoreRepairTask.shouldContinue(cluster)).thenReturn(false);
    Set<String> unhealthySet = new HashSet<>();
    Set<String> candidates = systemStoreRepairTask.preFilterSystemStores(cluster, unhealthySet);
    Assert.assertTrue(unhealthySet.isEmpty());
    Assert.assertTrue(candidates.isEmpty());

    // shouldContinue returns true -> full filter
    isRunning.set(true);
    when(systemStoreRepairTask.shouldContinue(cluster)).thenReturn(true);
    unhealthySet = new HashSet<>();
    candidates = systemStoreRepairTask.preFilterSystemStores(cluster, unhealthySet);

    // testStore3 has system store flags disabled -> 2 unhealthy
    // metaStore1 has no version -> 1 more unhealthy (stale)
    // pushStatusStore2 has stale version -> 1 more unhealthy (stale)
    // Total unhealthy: 4
    Assert.assertEquals(unhealthySet.size(), 4);
    Assert.assertTrue(unhealthySet.contains(VeniceSystemStoreType.META_STORE.getSystemStoreName(testStore3)));
    Assert.assertTrue(
        unhealthySet.contains(VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE.getSystemStoreName(testStore3)));

    // Candidates should be stores that passed all filters (pushStatusStore1 and metaStore2)
    Assert.assertEquals(candidates.size(), 2);
    Assert.assertTrue(
        candidates.contains(VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE.getSystemStoreName(testStore1)));
    Assert.assertTrue(candidates.contains(VeniceSystemStoreType.META_STORE.getSystemStoreName(testStore2)));
  }

  @Test
  public void testCheckSystemStoresHealthMixedResults() {
    // Checker returns HEALTHY, UNHEALTHY, and UNKNOWN; UNKNOWN is treated as unhealthy
    SystemStoreRepairTask task = mock(SystemStoreRepairTask.class);
    String cluster = "venice";

    String storeA = VeniceSystemStoreType.META_STORE.getSystemStoreName("testStoreA");
    String storeB = VeniceSystemStoreType.META_STORE.getSystemStoreName("testStoreB");
    String storeC = VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE.getSystemStoreName("testStoreC");
    Set<String> candidates = new HashSet<>(Arrays.asList(storeA, storeB, storeC));
    doCallRealMethod().when(task).checkSystemStoresHealth(anyString(), anySet());
    doReturn(candidates).when(task).preFilterSystemStores(anyString(), anySet());

    SystemStoreHealthChecker checker = mock(SystemStoreHealthChecker.class);
    Map<String, HealthCheckResult> results = new HashMap<>();
    results.put(storeA, HealthCheckResult.HEALTHY);
    results.put(storeB, HealthCheckResult.UNHEALTHY);
    results.put(storeC, HealthCheckResult.UNKNOWN);
    doReturn(results).when(checker).checkHealth(eq(cluster), anySet());
    doReturn(checker).when(task).getHealthChecker();

    SystemStoreHealthCheckStats stats = mock(SystemStoreHealthCheckStats.class);
    when(stats.getBadMetaSystemStoreCounter()).thenReturn(new AtomicLong(0));
    when(stats.getBadPushStatusSystemStoreCounter()).thenReturn(new AtomicLong(0));
    doReturn(stats).when(task).getClusterSystemStoreHealthCheckStats(cluster);

    Set<String> unhealthySet = new HashSet<>();
    task.checkSystemStoresHealth(cluster, unhealthySet);

    Assert.assertEquals(unhealthySet.size(), 2);
    Assert.assertFalse(unhealthySet.contains(storeA));
    Assert.assertTrue(unhealthySet.contains(storeB));
    Assert.assertTrue(unhealthySet.contains(storeC));
  }

  @Test
  public void testCheckSystemStoresHealthAllHealthy() {
    SystemStoreRepairTask task = mock(SystemStoreRepairTask.class);
    String cluster = "venice";

    String storeA = VeniceSystemStoreType.META_STORE.getSystemStoreName("testStoreA");
    String storeB = VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE.getSystemStoreName("testStoreB");
    Set<String> candidates = new HashSet<>(Arrays.asList(storeA, storeB));
    doCallRealMethod().when(task).checkSystemStoresHealth(anyString(), anySet());
    doReturn(candidates).when(task).preFilterSystemStores(anyString(), anySet());

    SystemStoreHealthChecker checker = mock(SystemStoreHealthChecker.class);
    Map<String, HealthCheckResult> results = new HashMap<>();
    results.put(storeA, HealthCheckResult.HEALTHY);
    results.put(storeB, HealthCheckResult.HEALTHY);
    doReturn(results).when(checker).checkHealth(eq(cluster), anySet());
    doReturn(checker).when(task).getHealthChecker();

    SystemStoreHealthCheckStats stats = mock(SystemStoreHealthCheckStats.class);
    when(stats.getBadMetaSystemStoreCounter()).thenReturn(new AtomicLong(0));
    when(stats.getBadPushStatusSystemStoreCounter()).thenReturn(new AtomicLong(0));
    doReturn(stats).when(task).getClusterSystemStoreHealthCheckStats(cluster);

    Set<String> unhealthySet = new HashSet<>();
    task.checkSystemStoresHealth(cluster, unhealthySet);

    Assert.assertTrue(unhealthySet.isEmpty());
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
    doReturn(3).when(systemStoreRepairTask).getRepairJobCheckTimeoutInSeconds();
    doReturn(-1).when(systemStoreRepairTask).getMaxRepairPerRound();

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

  @Test
  public void testRepairMaxPerRoundLimit() {
    String clusterName = "test-cluster";
    SystemStoreRepairTask systemStoreRepairTask = mock(SystemStoreRepairTask.class);
    doCallRealMethod().when(systemStoreRepairTask).repairBadSystemStore(anyString(), anySet());
    doCallRealMethod().when(systemStoreRepairTask).pollSystemStorePushStatus(anyString(), anyMap(), anySet(), anyInt());
    doReturn(true).when(systemStoreRepairTask).shouldContinue(anyString());
    doCallRealMethod().when(systemStoreRepairTask).periodicCheckTask(anyString(), anyInt(), anyInt(), any());

    VeniceParentHelixAdmin parentHelixAdmin = mock(VeniceParentHelixAdmin.class);
    doReturn(parentHelixAdmin).when(systemStoreRepairTask).getParentAdmin();

    doReturn(1).when(systemStoreRepairTask).getRepairJobCheckIntervalInSeconds();
    doReturn(3).when(systemStoreRepairTask).getRepairJobCheckTimeoutInSeconds();
    // Set limit to 1
    doReturn(1).when(systemStoreRepairTask).getMaxRepairPerRound();

    String systemStore1 = VeniceSystemStoreUtils.getMetaStoreName("testStore1");
    String systemStore2 = VeniceSystemStoreUtils.getMetaStoreName("testStore2");
    String systemStore3 = VeniceSystemStoreUtils.getMetaStoreName("testStore3");
    Set<String> unhealthySystemStoreSet = new HashSet<>();
    unhealthySystemStoreSet.add(systemStore1);
    unhealthySystemStoreSet.add(systemStore2);
    unhealthySystemStoreSet.add(systemStore3);

    Version version = mock(Version.class);
    doReturn(5).when(version).getNumber();
    doReturn(version).when(systemStoreRepairTask).getNewSystemStoreVersion(anyString(), anyString(), anyString());

    Admin.OfflinePushStatusInfo goodPushStatus = mock(Admin.OfflinePushStatusInfo.class);
    doReturn(ExecutionStatus.COMPLETED).when(goodPushStatus).getExecutionStatus();
    when(parentHelixAdmin.getOffLinePushStatus(anyString(), anyString())).thenReturn(goodPushStatus);

    systemStoreRepairTask.repairBadSystemStore(clusterName, unhealthySystemStoreSet);
    // Only 1 store should have been repaired (removed from unhealthy set), 2 remain
    Assert.assertEquals(unhealthySystemStoreSet.size(), 2);
  }
}
