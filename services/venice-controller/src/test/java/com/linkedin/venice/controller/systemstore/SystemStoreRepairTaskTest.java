package com.linkedin.venice.controller.systemstore;

import static com.linkedin.venice.controller.VeniceController.CONTROLLER_SERVICE_METRIC_ENTITIES;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_SYSTEM_STORE_TYPE;
import static com.linkedin.venice.stats.dimensions.VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE;
import static com.linkedin.venice.stats.dimensions.VeniceSystemStoreType.META_STORE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.venice.acl.VeniceComponent;
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
import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.stats.VeniceMetricsConfig;
import com.linkedin.venice.stats.VeniceMetricsRepository;
import com.linkedin.venice.utils.LogContext;
import com.linkedin.venice.utils.OpenTelemetryDataTestUtils;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
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
    doReturn(
        LogContext.newBuilder()
            .setComponentName(VeniceComponent.CONTROLLER.name())
            .setRegionName("test-region")
            .build()).when(parentHelixAdmin).getLogContext();
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
    // Checker returns HEALTHY and UNHEALTHY for the stores it decided on; candidates missing from the result map
    // are deferred to the next round (NOT marked unhealthy), so an aborted checker can't inflate unhealthy counts.
    SystemStoreRepairTask task = mock(SystemStoreRepairTask.class);
    String cluster = "venice";

    Set<String> candidates = new HashSet<>(Arrays.asList("store_a", "store_b", "store_c"));
    doCallRealMethod().when(task).checkSystemStoresHealth(anyString(), anySet());
    doReturn(candidates).when(task).preFilterSystemStores(anyString(), anySet());

    SystemStoreHealthChecker checker = mock(SystemStoreHealthChecker.class);
    Map<String, HealthCheckResult> results = new HashMap<>();
    results.put("store_a", HealthCheckResult.HEALTHY);
    results.put("store_b", HealthCheckResult.UNHEALTHY);
    // store_c has no result — checker aborted before reaching a decision; should be deferred.
    doReturn(results).when(checker).checkHealth(eq(cluster), anySet());
    doReturn(checker).when(task).getHealthChecker();

    SystemStoreHealthCheckStats stats = mock(SystemStoreHealthCheckStats.class);
    when(stats.getBadMetaSystemStoreCounter()).thenReturn(new AtomicLong(0));
    when(stats.getBadPushStatusSystemStoreCounter()).thenReturn(new AtomicLong(0));
    doReturn(stats).when(task).getClusterSystemStoreHealthCheckStats(cluster);

    Set<String> unhealthySet = new HashSet<>();
    task.checkSystemStoresHealth(cluster, unhealthySet);

    Assert.assertEquals(unhealthySet.size(), 1);
    Assert.assertFalse(unhealthySet.contains("store_a"));
    Assert.assertTrue(unhealthySet.contains("store_b"));
    Assert.assertFalse(unhealthySet.contains("store_c"), "stores missing from checker result should be deferred");
  }

  @Test
  public void testCheckSystemStoresHealthEmptyResultsDoesNotInflateUnhealthy() {
    // When the checker aborts very early (e.g., leadership lost) and returns an empty result map, candidates
    // should be deferred to the next round instead of all being marked unhealthy.
    SystemStoreRepairTask task = mock(SystemStoreRepairTask.class);
    String cluster = "venice";

    Set<String> candidates = new HashSet<>(Arrays.asList("store_a", "store_b", "store_c"));
    doCallRealMethod().when(task).checkSystemStoresHealth(anyString(), anySet());
    doReturn(candidates).when(task).preFilterSystemStores(anyString(), anySet());

    SystemStoreHealthChecker checker = mock(SystemStoreHealthChecker.class);
    doReturn(Collections.emptyMap()).when(checker).checkHealth(eq(cluster), anySet());
    doReturn(checker).when(task).getHealthChecker();

    SystemStoreHealthCheckStats stats = mock(SystemStoreHealthCheckStats.class);
    when(stats.getBadMetaSystemStoreCounter()).thenReturn(new AtomicLong(0));
    when(stats.getBadPushStatusSystemStoreCounter()).thenReturn(new AtomicLong(0));
    doReturn(stats).when(task).getClusterSystemStoreHealthCheckStats(cluster);

    Set<String> unhealthySet = new HashSet<>();
    task.checkSystemStoresHealth(cluster, unhealthySet);

    Assert.assertTrue(unhealthySet.isEmpty(), "empty checker result should not flag any store as unhealthy");
  }

  @Test
  public void testCheckSystemStoresHealthAllHealthy() {
    SystemStoreRepairTask task = mock(SystemStoreRepairTask.class);
    String cluster = "venice";

    Set<String> candidates = new HashSet<>(Arrays.asList("store_a", "store_b"));
    doCallRealMethod().when(task).checkSystemStoresHealth(anyString(), anySet());
    doReturn(candidates).when(task).preFilterSystemStores(anyString(), anySet());

    SystemStoreHealthChecker checker = mock(SystemStoreHealthChecker.class);
    Map<String, HealthCheckResult> results = new HashMap<>();
    results.put("store_a", HealthCheckResult.HEALTHY);
    results.put("store_b", HealthCheckResult.HEALTHY);
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
  public void testCheckSystemStoresHealthRecoversFromCheckerException() {
    // A misbehaving (e.g., pluggable override) health checker that throws must not take down the cluster round.
    // Stores already flagged unhealthy by pre-filtering must still be repairable; only the health-check
    // candidates are deferred.
    SystemStoreRepairTask task = mock(SystemStoreRepairTask.class);
    String cluster = "venice";

    Set<String> candidates = new HashSet<>(Arrays.asList("candidate_a", "candidate_b"));
    doCallRealMethod().when(task).checkSystemStoresHealth(anyString(), anySet());
    // Simulate pre-filter already adding one store as unhealthy via the side-effect set.
    doAnswer(invocation -> {
      Set<String> unhealthy = invocation.getArgument(1);
      unhealthy.add("pre_filtered_bad");
      return candidates;
    }).when(task).preFilterSystemStores(anyString(), anySet());

    SystemStoreHealthChecker checker = mock(SystemStoreHealthChecker.class);
    when(checker.checkHealth(eq(cluster), anySet())).thenThrow(new RuntimeException("checker boom"));
    doReturn(checker).when(task).getHealthChecker();

    SystemStoreHealthCheckStats stats = mock(SystemStoreHealthCheckStats.class);
    when(stats.getBadMetaSystemStoreCounter()).thenReturn(new AtomicLong(0));
    when(stats.getBadPushStatusSystemStoreCounter()).thenReturn(new AtomicLong(0));
    doReturn(stats).when(task).getClusterSystemStoreHealthCheckStats(cluster);
    AtomicLong errorCounter = new AtomicLong(0);
    doReturn(errorCounter).when(task).getSystemStoreHealthCheckErrorCounter(cluster);

    Set<String> unhealthySet = new HashSet<>();
    task.checkSystemStoresHealth(cluster, unhealthySet);

    // Pre-filtered store stays unhealthy (will be repaired); candidates are deferred (not falsely flagged).
    Assert.assertTrue(unhealthySet.contains("pre_filtered_bad"));
    Assert.assertFalse(unhealthySet.contains("candidate_a"));
    Assert.assertFalse(unhealthySet.contains("candidate_b"));
    Assert.assertEquals(unhealthySet.size(), 1);
    // A checker that throws must bump the error metric so a persistently broken checker is alertable rather than
    // silently deferring every round.
    Assert.assertEquals(errorCounter.get(), 1L, "a thrown checker exception should increment the error counter");
  }

  @Test
  public void testCheckSystemStoresHealthIgnoresNonCandidateResults() {
    // A misbehaving (e.g., pluggable override) checker may return a store that was not among the candidates.
    // Such entries must be ignored so they cannot pollute the unhealthy set or the bad-store metric (which would
    // otherwise throw on an arbitrary, non-system-store name).
    SystemStoreRepairTask task = mock(SystemStoreRepairTask.class);
    String cluster = "venice";

    Set<String> candidates = new HashSet<>(Collections.singletonList("store_a"));
    doCallRealMethod().when(task).checkSystemStoresHealth(anyString(), anySet());
    doReturn(candidates).when(task).preFilterSystemStores(anyString(), anySet());

    SystemStoreHealthChecker checker = mock(SystemStoreHealthChecker.class);
    Map<String, HealthCheckResult> results = new HashMap<>();
    results.put("store_a", HealthCheckResult.HEALTHY);
    // Not a candidate; an arbitrary name a buggy override should never be able to inject into repair.
    results.put("rogue_non_candidate_store", HealthCheckResult.UNHEALTHY);
    doReturn(results).when(checker).checkHealth(eq(cluster), anySet());
    doReturn(checker).when(task).getHealthChecker();

    SystemStoreHealthCheckStats stats = mock(SystemStoreHealthCheckStats.class);
    when(stats.getBadMetaSystemStoreCounter()).thenReturn(new AtomicLong(0));
    when(stats.getBadPushStatusSystemStoreCounter()).thenReturn(new AtomicLong(0));
    doReturn(stats).when(task).getClusterSystemStoreHealthCheckStats(cluster);

    Set<String> unhealthySet = new HashSet<>();
    task.checkSystemStoresHealth(cluster, unhealthySet);

    Assert.assertTrue(
        unhealthySet.isEmpty(),
        "a result for a non-candidate store must be ignored and never added to the unhealthy set");
  }

  @Test
  public void testCheckSystemStoresHealthHandlesNullResult() {
    // A misbehaving checker that returns null (instead of a possibly-empty map) must not abort the cluster round.
    // Stores already flagged by pre-filtering must still be repairable; the health-check candidates are deferred.
    SystemStoreRepairTask task = mock(SystemStoreRepairTask.class);
    String cluster = "venice";

    Set<String> candidates = new HashSet<>(Arrays.asList("candidate_a", "candidate_b"));
    doCallRealMethod().when(task).checkSystemStoresHealth(anyString(), anySet());
    doAnswer(invocation -> {
      Set<String> unhealthy = invocation.getArgument(1);
      unhealthy.add("pre_filtered_bad");
      return candidates;
    }).when(task).preFilterSystemStores(anyString(), anySet());

    SystemStoreHealthChecker checker = mock(SystemStoreHealthChecker.class);
    doReturn(null).when(checker).checkHealth(eq(cluster), anySet());
    doReturn(checker).when(task).getHealthChecker();

    SystemStoreHealthCheckStats stats = mock(SystemStoreHealthCheckStats.class);
    when(stats.getBadMetaSystemStoreCounter()).thenReturn(new AtomicLong(0));
    when(stats.getBadPushStatusSystemStoreCounter()).thenReturn(new AtomicLong(0));
    doReturn(stats).when(task).getClusterSystemStoreHealthCheckStats(cluster);
    AtomicLong errorCounter = new AtomicLong(0);
    doReturn(errorCounter).when(task).getSystemStoreHealthCheckErrorCounter(cluster);

    Set<String> unhealthySet = new HashSet<>();
    task.checkSystemStoresHealth(cluster, unhealthySet);

    Assert.assertTrue(unhealthySet.contains("pre_filtered_bad"));
    Assert.assertFalse(unhealthySet.contains("candidate_a"));
    Assert.assertFalse(unhealthySet.contains("candidate_b"));
    Assert.assertEquals(unhealthySet.size(), 1);
    // A checker that returns null must bump the error metric, same as a thrown exception.
    Assert.assertEquals(errorCounter.get(), 1L, "a null checker result should increment the error counter");
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

    // Real stats for metric verification
    String metricPrefix = "controller";
    InMemoryMetricReader metricReader = InMemoryMetricReader.create();
    VeniceMetricsRepository metricsRepo = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricPrefix(metricPrefix)
            .setMetricEntities(CONTROLLER_SERVICE_METRIC_ENTITIES)
            .setEmitOtelMetrics(true)
            .setOtelAdditionalMetricsReader(metricReader)
            .build());
    SystemStoreHealthCheckStats realStats = new SystemStoreHealthCheckStats(metricsRepo, clusterName);
    Map<String, SystemStoreHealthCheckStats> statsMap = new HashMap<>();
    statsMap.put(clusterName, realStats);

    doCallRealMethod().when(systemStoreRepairTask).updateBadSystemStoreCount(anyString(), anySet());
    doCallRealMethod().when(systemStoreRepairTask).updateNotRepairableSystemStoreCount(anyString(), anySet());
    doCallRealMethod().when(systemStoreRepairTask).getBadMetaStoreCount(anyString());
    doCallRealMethod().when(systemStoreRepairTask).getBadPushStatusStoreCount(anyString());
    doCallRealMethod().when(systemStoreRepairTask).getNotRepairableSystemStoreCounter(anyString());
    doCallRealMethod().when(systemStoreRepairTask).getClusterSystemStoreHealthCheckStats(anyString());
    doReturn(statsMap).when(systemStoreRepairTask).getClusterToSystemStoreHealthCheckStatsMap();

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

    // After Push Completed: all counts should be 0
    verifySystemStoreMetrics(metricsRepo, metricReader, clusterName, metricPrefix, 0, 0, 0);

    // Poll throws exception, should be caught inside.
    unhealthySystemStoreSet.add(systemStore);
    doThrow(VeniceException.class).when(parentHelixAdmin)
        .getOffLinePushStatus(clusterName, Version.composeKafkaTopic(systemStore, 5));
    systemStoreRepairTask.repairBadSystemStore(clusterName, unhealthySystemStoreSet);
    Assert.assertFalse(unhealthySystemStoreSet.isEmpty());

    // After Poll throws: systemStore is a meta store, so badMeta=1, badPushStatus=0, notRepairable=1
    verifySystemStoreMetrics(metricsRepo, metricReader, clusterName, metricPrefix, 1, 0, 1);
  }

  /**
   * Verify both Tehuti and OTel metrics report consistent expected values for SystemStoreHealthCheckStats.
   */
  private static void verifySystemStoreMetrics(
      VeniceMetricsRepository metricsRepo,
      InMemoryMetricReader metricReader,
      String clusterName,
      String metricPrefix,
      long expectedBadMeta,
      long expectedBadPushStatus,
      long expectedNotRepairable) {
    String tehutiResourceName = "." + clusterName;

    // Tehuti verification
    String badMetaName =
        AbstractVeniceStats.getSensorFullName(tehutiResourceName, "bad_meta_system_store_count") + ".Gauge";
    Assert.assertNotNull(metricsRepo.getMetric(badMetaName), "Tehuti bad meta metric should exist");
    Assert.assertEquals(
        metricsRepo.getMetric(badMetaName).value(),
        (double) expectedBadMeta,
        "Tehuti bad meta count mismatch");

    String badPushStatusName =
        AbstractVeniceStats.getSensorFullName(tehutiResourceName, "bad_push_status_system_store_count") + ".Gauge";
    Assert.assertNotNull(metricsRepo.getMetric(badPushStatusName), "Tehuti bad push status metric should exist");
    Assert.assertEquals(
        metricsRepo.getMetric(badPushStatusName).value(),
        (double) expectedBadPushStatus,
        "Tehuti bad push status count mismatch");

    String notRepairableName =
        AbstractVeniceStats.getSensorFullName(tehutiResourceName, "not_repairable_system_store_count") + ".Gauge";
    Assert.assertNotNull(metricsRepo.getMetric(notRepairableName), "Tehuti not repairable metric should exist");
    Assert.assertEquals(
        metricsRepo.getMetric(notRepairableName).value(),
        (double) expectedNotRepairable,
        "Tehuti not repairable count mismatch");

    // OTel verification
    Attributes metaStoreAttrs = Attributes.builder()
        .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), clusterName)
        .put(VENICE_SYSTEM_STORE_TYPE.getDimensionNameInDefaultFormat(), META_STORE.getDimensionValue())
        .build();
    Attributes pushStatusStoreAttrs = Attributes.builder()
        .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), clusterName)
        .put(VENICE_SYSTEM_STORE_TYPE.getDimensionNameInDefaultFormat(), DAVINCI_PUSH_STATUS_STORE.getDimensionValue())
        .build();
    Attributes clusterAttrs =
        Attributes.builder().put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), clusterName).build();

    OpenTelemetryDataTestUtils.validateLongPointDataFromGauge(
        metricReader,
        expectedBadMeta,
        metaStoreAttrs,
        "system_store.health_check.unhealthy_count",
        metricPrefix);
    OpenTelemetryDataTestUtils.validateLongPointDataFromGauge(
        metricReader,
        expectedBadPushStatus,
        pushStatusStoreAttrs,
        "system_store.health_check.unhealthy_count",
        metricPrefix);
    OpenTelemetryDataTestUtils.validateLongPointDataFromGauge(
        metricReader,
        expectedNotRepairable,
        clusterAttrs,
        "system_store.health_check.unrepairable_count",
        metricPrefix);
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
    // Only 1 store should have been repaired (removed from unhealthy set), 2 remain. The repaired store is
    // picked randomly from the unhealthy set (we shuffle to avoid both HashSet-order starvation and
    // sort-order starvation), so we only assert the count, not the identity.
    Assert.assertEquals(unhealthySystemStoreSet.size(), 2);
    // Sanity check: exactly one of the original three was repaired (removed) — not zero, not multiple.
    int remaining = 0;
    if (unhealthySystemStoreSet.contains(systemStore1)) {
      remaining++;
    }
    if (unhealthySystemStoreSet.contains(systemStore2)) {
      remaining++;
    }
    if (unhealthySystemStoreSet.contains(systemStore3)) {
      remaining++;
    }
    Assert.assertEquals(remaining, 2, "exactly one of the seeded stores should have been repaired");
  }

  @Test
  public void testRepairMaxPerRoundCapCountsFailedAttempts() {
    // Repairs that fail fast (materialization throws) must still consume the per-round cap. Otherwise a burst of
    // failing stores would let one round attempt the entire unhealthy set and defeat the throttle.
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
    // Cap of 1 repair per round.
    doReturn(1).when(systemStoreRepairTask).getMaxRepairPerRound();
    // Wire the real not-repairable accounting so we can assert the metric, not just the unhealthy-set size.
    doCallRealMethod().when(systemStoreRepairTask).updateNotRepairableSystemStoreCount(anyString(), anySet());
    AtomicLong notRepairableCounter = new AtomicLong(0);
    doReturn(notRepairableCounter).when(systemStoreRepairTask).getNotRepairableSystemStoreCounter(clusterName);

    Set<String> unhealthySystemStoreSet = new HashSet<>();
    unhealthySystemStoreSet.add(VeniceSystemStoreUtils.getMetaStoreName("testStore1"));
    unhealthySystemStoreSet.add(VeniceSystemStoreUtils.getMetaStoreName("testStore2"));
    unhealthySystemStoreSet.add(VeniceSystemStoreUtils.getMetaStoreName("testStore3"));

    // Every materialization fails fast.
    doThrow(new VeniceException("materialization failed")).when(systemStoreRepairTask)
        .getNewSystemStoreVersion(anyString(), anyString(), anyString());

    systemStoreRepairTask.repairBadSystemStore(clusterName, unhealthySystemStoreSet);

    // Despite all attempts failing, the cap of 1 must stop the loop after a single attempt.
    verify(systemStoreRepairTask, times(1)).getNewSystemStoreVersion(anyString(), anyString(), anyString());
    // No store was repaired, so all three remain unhealthy (the other two were deferred, never attempted).
    Assert.assertEquals(unhealthySystemStoreSet.size(), 3);
    // Only the single attempted-but-failed store is "not repairable"; the two cap-deferred stores were never
    // attempted and must NOT inflate the metric.
    Assert.assertEquals(
        notRepairableCounter.get(),
        1L,
        "cap-deferred stores (never attempted) must not be counted as not-repairable");
  }

  @Test
  public void testRepairMaxPerRoundExactlyAtLimitRepairsAll() {
    // Boundary: when the cap equals the number of unhealthy stores, every store is repaired and none is deferred.
    // Guards the `>=` comparison in SystemStoreRepairTask#repairBadSystemStore against an off-by-one regression.
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
    // Cap exactly equals the unhealthy-store count: all should be repaired, none deferred.
    doReturn(3).when(systemStoreRepairTask).getMaxRepairPerRound();

    Set<String> unhealthySystemStoreSet = new HashSet<>();
    unhealthySystemStoreSet.add(VeniceSystemStoreUtils.getMetaStoreName("testStore1"));
    unhealthySystemStoreSet.add(VeniceSystemStoreUtils.getMetaStoreName("testStore2"));
    unhealthySystemStoreSet.add(VeniceSystemStoreUtils.getMetaStoreName("testStore3"));

    Version version = mock(Version.class);
    doReturn(5).when(version).getNumber();
    doReturn(version).when(systemStoreRepairTask).getNewSystemStoreVersion(anyString(), anyString(), anyString());

    Admin.OfflinePushStatusInfo goodPushStatus = mock(Admin.OfflinePushStatusInfo.class);
    doReturn(ExecutionStatus.COMPLETED).when(goodPushStatus).getExecutionStatus();
    when(parentHelixAdmin.getOffLinePushStatus(anyString(), anyString())).thenReturn(goodPushStatus);

    systemStoreRepairTask.repairBadSystemStore(clusterName, unhealthySystemStoreSet);

    // Cap == unhealthy count, so every store is attempted and (push COMPLETED) repaired; none deferred, none left.
    verify(systemStoreRepairTask, times(3)).getNewSystemStoreVersion(anyString(), anyString(), anyString());
    Assert.assertTrue(
        unhealthySystemStoreSet.isEmpty(),
        "when the cap equals the unhealthy-store count, every store should be repaired and none deferred");
  }
}
