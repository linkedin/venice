package com.linkedin.venice.controller;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.davinci.listener.response.ServerCurrentVersionResponse;
import com.linkedin.venice.controllerapi.CurrentVersionResponse;
import com.linkedin.venice.helix.ZkRoutersClusterManager;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.LiveInstanceMonitor;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.utils.ObjectMapperFactory;
import com.linkedin.venice.utils.SystemTime;
import com.linkedin.venice.utils.TestMockTime;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TestStoreBackupVersionCleanupService {
  private static final ObjectMapper OBJECT_MAPPER = ObjectMapperFactory.getInstance();

  private static final String CLUSTER_NAME = "test_cluster";
  private static final long DEFAULT_RETENTION_MS = TimeUnit.DAYS.toMillis(7);

  private VeniceHelixAdmin admin;
  private ZkRoutersClusterManager clusterManager;
  private HelixVeniceClusterResources mockClusterResource;
  private VeniceControllerMultiClusterConfig config;
  private VeniceControllerClusterConfig controllerConfig;
  private LiveInstanceMonitor liveInstanceMonitor;
  private MetricsRepository metricsRepository;

  @BeforeMethod
  public void setUp() throws Exception {
    // Initialize common mocks
    admin = mock(VeniceHelixAdmin.class);
    mockClusterResource = mock(HelixVeniceClusterResources.class);
    clusterManager = mock(ZkRoutersClusterManager.class);
    config = mock(VeniceControllerMultiClusterConfig.class);
    controllerConfig = mock(VeniceControllerClusterConfig.class);
    liveInstanceMonitor = mock(LiveInstanceMonitor.class);
    metricsRepository = mock(MetricsRepository.class);

    // Setup default mock behaviors
    when(mockClusterResource.getRoutersClusterManager()).thenReturn(clusterManager);
    when(admin.getHelixVeniceClusterResources(anyString())).thenReturn(mockClusterResource);
    when(admin.getLiveInstanceMonitor(anyString())).thenReturn(liveInstanceMonitor);
    when(config.getControllerConfig(anyString())).thenReturn(controllerConfig);
    when(config.getBackupVersionDefaultRetentionMs()).thenReturn(DEFAULT_RETENTION_MS);
    when(metricsRepository.sensor(anyString(), any())).thenReturn(mock(Sensor.class));

    // Default test cluster setup
    Set<String> clusters = new HashSet<>();
    clusters.add(CLUSTER_NAME);
    when(config.getClusters()).thenReturn(clusters);
    when(admin.isLeaderControllerFor(any())).thenReturn(true);
  }

  private Store mockStore(
      long backupVersionRetentionMs,
      long latestVersionPromoteToCurrentTimestamp,
      Map<Integer, VersionStatus> versions,
      int currentVersion) {
    Store store = mock(Store.class);
    doReturn(Utils.getUniqueString()).when(store).getName();
    doReturn(backupVersionRetentionMs).when(store).getBackupVersionRetentionMs();
    doReturn(latestVersionPromoteToCurrentTimestamp).when(store).getLatestVersionPromoteToCurrentTimestamp();
    doReturn(currentVersion).when(store).getCurrentVersion();
    List<Version> versionList = new ArrayList<>();
    versions.forEach((n, s) -> {
      Version v = mock(Version.class);
      doReturn(n).when(v).getNumber();
      doReturn(s).when(v).getStatus();
      doReturn(System.currentTimeMillis()).when(v).getCreatedTime();
      versionList.add(v);
    });
    doReturn(versionList).when(store).getVersions();
    for (Version version: versionList) {
      when(store.getVersion(version.getNumber())).thenReturn(version);
    }
    doReturn(versionList.get(versionList.size() - 1)).when(store).getVersionOrThrow(currentVersion);
    return store;
  }

  @Test
  public void testWhetherStoreReadyToBeCleanup() {
    Map<Integer, VersionStatus> versions = new HashMap<>();
    versions.put(1, VersionStatus.ONLINE);
    versions.put(2, VersionStatus.ONLINE);
    long defaultBackupVersionRetentionMs = TimeUnit.DAYS.toMillis(7);
    Store storeNotReadyForCleanupWithDefaultRetentionPolicy = mockStore(-1, System.currentTimeMillis(), versions, -1);
    Assert.assertFalse(
        StoreBackupVersionCleanupService.whetherStoreReadyToBeCleanup(
            storeNotReadyForCleanupWithDefaultRetentionPolicy,
            defaultBackupVersionRetentionMs,
            new SystemTime(),
            -1));

    Store storeReadyForCleanupWithDefaultRetentionPolicy =
        mockStore(-1, System.currentTimeMillis() - 2 * defaultBackupVersionRetentionMs, versions, -1);
    Assert.assertTrue(
        StoreBackupVersionCleanupService.whetherStoreReadyToBeCleanup(
            storeReadyForCleanupWithDefaultRetentionPolicy,
            defaultBackupVersionRetentionMs,
            new SystemTime(),
            -1));

    long storeBackupRetentionMs = TimeUnit.DAYS.toMillis(3);
    Store storeNotReadyForCleanupWithSpecifiedRetentionPolicy =
        mockStore(storeBackupRetentionMs, System.currentTimeMillis(), versions, -1);
    Assert.assertFalse(
        StoreBackupVersionCleanupService.whetherStoreReadyToBeCleanup(
            storeNotReadyForCleanupWithSpecifiedRetentionPolicy,
            defaultBackupVersionRetentionMs,
            new SystemTime(),
            -1));

    Store storeReadyForCleanupWithSpecifiedRetentionPolicy =
        mockStore(storeBackupRetentionMs, System.currentTimeMillis() - 2 * storeBackupRetentionMs, versions, -1);
    Assert.assertTrue(
        StoreBackupVersionCleanupService.whetherStoreReadyToBeCleanup(
            storeReadyForCleanupWithSpecifiedRetentionPolicy,
            defaultBackupVersionRetentionMs,
            new SystemTime(),
            -1));

    long storeBackupRetentionMsZero = 0;
    Store storeNotReadyForCleanupWithZeroRetentionPolicy1 =
        mockStore(storeBackupRetentionMsZero, System.currentTimeMillis(), versions, -1);
    Assert.assertFalse(
        StoreBackupVersionCleanupService.whetherStoreReadyToBeCleanup(
            storeNotReadyForCleanupWithZeroRetentionPolicy1,
            defaultBackupVersionRetentionMs,
            new SystemTime(),
            -1));

    Store storeNotReadyForCleanupWithZeroRetentionPolicy2 =
        mockStore(storeBackupRetentionMsZero, System.currentTimeMillis() - 10, versions, -1);
    Assert.assertFalse(
        StoreBackupVersionCleanupService.whetherStoreReadyToBeCleanup(
            storeNotReadyForCleanupWithZeroRetentionPolicy2,
            defaultBackupVersionRetentionMs,
            new SystemTime(),
            -1));

    Store storeReadyForCleanupWithZeroRetentionPolicy =
        mockStore(storeBackupRetentionMsZero, System.currentTimeMillis() - 2 * storeBackupRetentionMs, versions, -1);
    Assert.assertTrue(
        StoreBackupVersionCleanupService.whetherStoreReadyToBeCleanup(
            storeReadyForCleanupWithZeroRetentionPolicy,
            defaultBackupVersionRetentionMs,
            new SystemTime(),
            -1));
  }

  private StoreBackupVersionCleanupService createService() {
    return new StoreBackupVersionCleanupService(admin, config, metricsRepository);
  }

  private void setupStoreWithVersions(Store store, Map<Integer, VersionStatus> versions, int currentVersion) {
    doReturn(versions.keySet().stream().map(store::getVersion).collect(Collectors.toList())).when(store).getVersions();
    for (Map.Entry<Integer, VersionStatus> entry: versions.entrySet()) {
      when(store.getVersion(entry.getKey())).thenReturn(store.getVersion(entry.getKey()));
    }
    doReturn(store.getVersion(currentVersion)).when(store).getVersionOrThrow(currentVersion);
  }

  @Test
  public void testCleanupBackupVersion() {
    StoreBackupVersionCleanupService service = createService();

    // Test 1: Store not qualified due to recent backup version
    Map<Integer, VersionStatus> versions = new HashMap<>();
    versions.put(1, VersionStatus.ONLINE);
    versions.put(2, VersionStatus.ONLINE);
    Store storeWithFreshBackupVersion = mockStore(-1, System.currentTimeMillis(), versions, 2);
    Assert.assertFalse(service.cleanupBackupVersion(storeWithFreshBackupVersion, CLUSTER_NAME));

    // Test 2: Store qualified but only has one version
    versions.clear();
    versions.put(2, VersionStatus.ONLINE);
    Store storeWithOneVersion = mockStore(-1, System.currentTimeMillis() - DEFAULT_RETENTION_MS * 2, versions, 2);
    Assert.assertFalse(service.cleanupBackupVersion(storeWithOneVersion, CLUSTER_NAME));

    // Test 3: Store qualified with one removable version
    versions.clear();
    versions.put(1, VersionStatus.ONLINE);
    versions.put(2, VersionStatus.ONLINE);
    Store storeWithTwoVersions = mockStore(-1, System.currentTimeMillis() - DEFAULT_RETENTION_MS * 2, versions, 2);
    Assert.assertTrue(service.cleanupBackupVersion(storeWithTwoVersions, CLUSTER_NAME));
    verify(admin).deleteOldVersionInStore(CLUSTER_NAME, storeWithTwoVersions.getName(), 1);

    // Test 4: Store qualified but rollback was executed
    versions.clear();
    versions.put(1, VersionStatus.ONLINE);
    versions.put(2, VersionStatus.ONLINE);
    versions.put(3, VersionStatus.STARTED);
    Store storeWithRollback = mockStore(-1, System.currentTimeMillis() - DEFAULT_RETENTION_MS * 2, versions, 1);
    Assert.assertFalse(service.cleanupBackupVersion(storeWithRollback, CLUSTER_NAME));

    // Test 5: Store with push in progress
    StoreBackupVersionCleanupService.setMinBackupVersionCleanupDelay(100L);
    doReturn(true).when(controllerConfig).isBackupVersionReplicaReductionEnabled();
    versions.clear();
    versions.put(1, VersionStatus.ONLINE);
    versions.put(2, VersionStatus.ONLINE);
    versions.put(3, VersionStatus.STARTED);
    Store storeWithPush = mockStore(-1, System.currentTimeMillis() - DEFAULT_RETENTION_MS * 2, versions, 2);
    doReturn(1509711434L).when(storeWithPush).getBackupVersionRetentionMs();
    Assert.assertFalse(service.cleanupBackupVersion(storeWithPush, CLUSTER_NAME));
    verify(admin).updateIdealState(CLUSTER_NAME, Version.composeKafkaTopic(storeWithPush.getName(), 1), 2);
  }

  @Test
  public void testCleanupBackupVersionRepush() {
    StoreBackupVersionCleanupService service = createService();
    long rolledbackTimestamp = System.currentTimeMillis() - DEFAULT_RETENTION_MS * 2;
    long waitTimeDeleteRepushSourceVersion = 100; // bypasses whetherStoreReadyToBeCleanup() for repush cases
    StoreBackupVersionCleanupService.setWaitTimeDeleteRepushSourceVersion(waitTimeDeleteRepushSourceVersion);

    // Test case: One repush creating one backup version
    Map<Integer, VersionStatus> versions = new HashMap<>();
    versions.put(1, VersionStatus.ONLINE);
    versions.put(2, VersionStatus.ONLINE);
    Store storeWithFreshBackupVersion = mockStore(-1, System.currentTimeMillis(), versions, 2);
    Version currentVersion = storeWithFreshBackupVersion.getVersion(2);
    when(currentVersion.getRepushSourceVersion()).thenReturn(1);
    Assert.assertFalse(service.cleanupBackupVersion(storeWithFreshBackupVersion, CLUSTER_NAME));

    // Even if the repush was on a version created 6 months ago, the backup version should not be immediately deleted
    Version backupVersion = storeWithFreshBackupVersion.getVersion(1);
    doReturn(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(6 * 30)).when(backupVersion).getCreatedTime();
    Assert.assertFalse(service.cleanupBackupVersion(storeWithFreshBackupVersion, CLUSTER_NAME));

    // The deletion condition should hinge on latestVersionPromoteToCurrentTimestamp
    doReturn(rolledbackTimestamp).when(storeWithFreshBackupVersion).getLatestVersionPromoteToCurrentTimestamp();
    Assert.assertTrue(service.cleanupBackupVersion(storeWithFreshBackupVersion, CLUSTER_NAME));

    versions.clear();
    versions.put(1, VersionStatus.ONLINE);
    versions.put(2, VersionStatus.ONLINE);
    versions.put(3, VersionStatus.ONLINE);
    Store storeWithRollback = mockStore(-1, rolledbackTimestamp, versions, 3);
    Version version = storeWithRollback.getVersion(3);
    doReturn(2).when(version).getRepushSourceVersion();

    // should delete version 2 as 1 is the true backup
    Assert.assertTrue(service.cleanupBackupVersion(storeWithRollback, CLUSTER_NAME));
    TestUtils.waitForNonDeterministicAssertion(
        1,
        TimeUnit.SECONDS,
        () -> verify(admin, atLeast(1)).deleteOldVersionInStore(CLUSTER_NAME, storeWithRollback.getName(), 2));

    versions.clear();
    versions.put(1, VersionStatus.ONLINE);
    versions.put(3, VersionStatus.ONLINE);
    Store storeLingeringVersion = mockStore(-1, rolledbackTimestamp, versions, 3);
    version = storeLingeringVersion.getVersion(3);
    doReturn(2).when(version).getRepushSourceVersion();
    version = storeLingeringVersion.getVersion(1);
    doReturn(1L).when(version).getCreatedTime();

    // should delete version 1 as it's lingering and not a repush source version.
    Assert.assertTrue(service.cleanupBackupVersion(storeLingeringVersion, CLUSTER_NAME));
    TestUtils.waitForNonDeterministicAssertion(
        1,
        TimeUnit.SECONDS,
        () -> verify(admin, atLeast(1)).deleteOldVersionInStore(CLUSTER_NAME, storeLingeringVersion.getName(), 1));

    // Test case: Store with multiple repushed versions
    // Version 2 is repushed from Version 3 until Version 10
    versions.clear();
    int minRepushedVersion = 3;
    int maxRepushedVersion = 10;
    for (int v = 1; v <= maxRepushedVersion; v++) {
      versions.put(v, VersionStatus.ONLINE);
    }
    Store repushedStore = mockStore(-1, System.currentTimeMillis(), versions, maxRepushedVersion);
    for (int v = minRepushedVersion; v <= maxRepushedVersion; v++) {
      version = repushedStore.getVersion(v);
      doReturn(v - 1).when(version).getRepushSourceVersion();
    }

    // Cleanup service should not run, since it hasn't been long enough since the latest version was promoted to current
    Assert.assertFalse(service.cleanupBackupVersion(repushedStore, CLUSTER_NAME), "No versions should be cleaned up");
    for (int v = minRepushedVersion; v < maxRepushedVersion; v++) {
      verify(admin, never()).deleteOldVersionInStore(CLUSTER_NAME, repushedStore.getName(), v);
    }

    // Versions 2..9 should be deleted, but not Version 1 or Version 10
    doReturn(System.currentTimeMillis() - 2 * waitTimeDeleteRepushSourceVersion).when(repushedStore)
        .getLatestVersionPromoteToCurrentTimestamp(); // cleanup service can run again
    Assert.assertTrue(service.cleanupBackupVersion(repushedStore, CLUSTER_NAME));
    verify(admin, never()).deleteOldVersionInStore(CLUSTER_NAME, repushedStore.getName(), 1);
    for (int v = minRepushedVersion - 1; v < maxRepushedVersion; v++) { // version 2, 3, 4, ..., 9
      int versionNumber = v; // for compiler warning
      TestUtils.waitForNonDeterministicAssertion(
          1,
          TimeUnit.SECONDS,
          () -> verify(admin, atLeast(1))
              .deleteOldVersionInStore(CLUSTER_NAME, repushedStore.getName(), versionNumber));
    }
    verify(admin, never()).deleteOldVersionInStore(CLUSTER_NAME, repushedStore.getName(), maxRepushedVersion);

    // Test case: Version 1 is repushed from Version 2 until Version 10
    // The latest backup version (9) should not be deleted unless retention time has passed
    clearInvocations(admin);
    version = repushedStore.getVersion(2);
    doReturn(1).when(version).getRepushSourceVersion();
    Assert.assertTrue(service.cleanupBackupVersion(repushedStore, CLUSTER_NAME));
    for (int v = 1; v < maxRepushedVersion - 1; v++) { // version 1, 2, 3, ..., 8
      int versionNumber = v; // for compiler warning
      TestUtils.waitForNonDeterministicAssertion(
          1,
          TimeUnit.SECONDS,
          () -> verify(admin, atLeast(1))
              .deleteOldVersionInStore(CLUSTER_NAME, repushedStore.getName(), versionNumber));
    }
    verify(admin, never()).deleteOldVersionInStore(CLUSTER_NAME, repushedStore.getName(), maxRepushedVersion - 1);
    verify(admin, never()).deleteOldVersionInStore(CLUSTER_NAME, repushedStore.getName(), maxRepushedVersion);

    // If the retention period has passed since promotion to current version, that version (9) should be deleted as well
    clearInvocations(admin);
    doReturn(0L).when(repushedStore).getLatestVersionPromoteToCurrentTimestamp();
    Assert.assertTrue(service.cleanupBackupVersion(repushedStore, CLUSTER_NAME));
    for (int v = 1; v < maxRepushedVersion; v++) { // version 1, 2, 3, ..., 9
      int versionNumber = v; // for compiler warning
      TestUtils.waitForNonDeterministicAssertion(
          1,
          TimeUnit.SECONDS,
          () -> verify(admin, atLeast(1))
              .deleteOldVersionInStore(CLUSTER_NAME, repushedStore.getName(), versionNumber));
    }
    verify(admin, never()).deleteOldVersionInStore(CLUSTER_NAME, repushedStore.getName(), maxRepushedVersion);
  }

  @Test
  public void testMetadataBasedCleanupBackupVersion() throws IOException {
    CloseableHttpAsyncClient asyncClient = mock(CloseableHttpAsyncClient.class);
    StatusLine statusLine = mock(StatusLine.class);
    CurrentVersionResponse currentVersionResponse = new CurrentVersionResponse();
    currentVersionResponse.setCurrentVersion(2);
    HttpResponse response = mock(HttpResponse.class);
    doReturn(CompletableFuture.completedFuture(response)).when(asyncClient).execute(any(), eq(null));
    doReturn(HttpStatus.SC_OK).when(statusLine).getStatusCode();
    doReturn(statusLine).when(response).getStatusLine();
    HttpEntity entity = mock(HttpEntity.class);
    doReturn(entity).when(response).getEntity();

    doReturn(new ByteArrayInputStream(OBJECT_MAPPER.writeValueAsBytes(currentVersionResponse))).when(entity)
        .getContent();
    doReturn(true).when(controllerConfig).isBackupVersionMetadataFetchBasedCleanupEnabled();
    Set<Instance> instSet = new HashSet<>();
    instSet.add(new Instance("0", "localhost1", 1234));

    StoreBackupVersionCleanupService service = spy(createService());
    doReturn(asyncClient).when(service).getHttpAsyncClient();

    Map<Integer, VersionStatus> versions = new HashMap<>();
    // Store is qualified, and contains one removable version
    versions.put(1, VersionStatus.ONLINE);
    versions.put(2, VersionStatus.ONLINE);
    doReturn(instSet).when(clusterManager).getLiveRouterInstances();
    doReturn(instSet).when(liveInstanceMonitor).getAllLiveInstances();

    Store storeWithTwoVersions = mockStore(-1, System.currentTimeMillis() - DEFAULT_RETENTION_MS * 2, versions, 2);
    Assert.assertFalse(service.cleanupBackupVersion(storeWithTwoVersions, CLUSTER_NAME));

    ServerCurrentVersionResponse versionResponse = new ServerCurrentVersionResponse();
    versionResponse.setCurrentVersion(2);

    // both server and router returns valid version. delete backup.
    doReturn(new ByteArrayInputStream(OBJECT_MAPPER.writeValueAsBytes(versionResponse)))
        .doReturn(new ByteArrayInputStream(OBJECT_MAPPER.writeValueAsBytes(currentVersionResponse)))
        .when(entity)
        .getContent();
    Assert.assertTrue(service.cleanupBackupVersion(storeWithTwoVersions, CLUSTER_NAME));
  }

  @Test
  public void testCleanupBackupVersionSleepValidation() throws Exception {
    doReturn(true).when(controllerConfig).isBackupVersionRetentionBasedCleanupEnabled();
    doReturn(Collections.emptySet()).when(clusterManager).getLiveRouterInstances();
    Map<Integer, VersionStatus> versions = new HashMap<>();
    versions.put(1, VersionStatus.ONLINE);
    versions.put(2, VersionStatus.ONLINE);

    Store storeWithTwoVersions = mockStore(-1, System.currentTimeMillis() - DEFAULT_RETENTION_MS * 2, versions, 2);
    List<Store> storeList = Collections.singletonList(storeWithTwoVersions);
    doReturn(storeList).when(admin).getAllStores(any());
    TestMockTime time = new TestMockTime();
    StoreBackupVersionCleanupService service =
        new StoreBackupVersionCleanupService(admin, config, time, metricsRepository);
    service.startInner();
    TestUtils.waitForNonDeterministicAssertion(
        1,
        TimeUnit.SECONDS,
        () -> verify(admin, atLeast(1)).deleteOldVersionInStore(CLUSTER_NAME, storeWithTwoVersions.getName(), 1));
  }
}
