package com.linkedin.venice.controller;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
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
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
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

  private VeniceHelixAdmin admin;
  private ZkRoutersClusterManager clusterManager;
  private HelixVeniceClusterResources mockClusterResource;

  @BeforeMethod
  public void setUp() throws Exception {
    admin = mock(VeniceHelixAdmin.class);
    mockClusterResource = mock(HelixVeniceClusterResources.class);
    clusterManager = mock(ZkRoutersClusterManager.class);
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
      versionList.add(v);
    });
    doReturn(versionList).when(store).getVersions();
    for (int i = 0; i < versionList.size(); i++) {
      doReturn(versionList.get(i)).when(store).getVersion(i + 1);
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

  @Test
  public void testCleanupBackupVersion() {
    VeniceHelixAdmin admin = mock(VeniceHelixAdmin.class);
    VeniceControllerMultiClusterConfig config = mock(VeniceControllerMultiClusterConfig.class);
    long defaultRetentionMs = TimeUnit.DAYS.toMillis(7);
    doReturn(defaultRetentionMs).when(config).getBackupVersionDefaultRetentionMs();
    VeniceControllerClusterConfig controllerConfig = mock(VeniceControllerClusterConfig.class);
    doReturn(controllerConfig).when(config).getControllerConfig(anyString());
    doReturn(mockClusterResource).when(admin).getHelixVeniceClusterResources(anyString());
    doReturn(clusterManager).when(mockClusterResource).getRoutersClusterManager();
    StoreBackupVersionCleanupService service =
        new StoreBackupVersionCleanupService(admin, config, mock(MetricsRepository.class));

    String clusterName = "test_cluster";
    // Store is not qualified because of short life time of backup version
    Map<Integer, VersionStatus> versions = new HashMap<>();
    versions.put(1, VersionStatus.ONLINE);
    versions.put(2, VersionStatus.ONLINE);
    Store storeWithFreshBackupVersion = mockStore(-1, System.currentTimeMillis(), versions, 2);
    Assert.assertFalse(service.cleanupBackupVersion(storeWithFreshBackupVersion, clusterName));

    // Store is qualified, but only one version left
    versions.clear();
    versions.put(2, VersionStatus.ONLINE);
    Store storeWithOneVersion = mockStore(-1, System.currentTimeMillis() - defaultRetentionMs * 2, versions, 2);
    Assert.assertFalse(service.cleanupBackupVersion(storeWithOneVersion, clusterName));

    // Store is qualified, and contains one removable version
    versions.clear();
    versions.put(1, VersionStatus.ONLINE);
    versions.put(2, VersionStatus.ONLINE);
    Store storeWithTwoVersions = mockStore(-1, System.currentTimeMillis() - defaultRetentionMs * 2, versions, 2);
    Assert.assertTrue(service.cleanupBackupVersion(storeWithTwoVersions, clusterName));
    verify(admin).deleteOldVersionInStore(clusterName, storeWithTwoVersions.getName(), 1);

    // Store is qualified, but rollback was executed
    versions.clear();
    versions.put(1, VersionStatus.ONLINE);
    versions.put(2, VersionStatus.ONLINE);
    versions.put(3, VersionStatus.STARTED);
    Store storeWithRollback = mockStore(-1, System.currentTimeMillis() - defaultRetentionMs * 2, versions, 1);
    Assert.assertFalse(service.cleanupBackupVersion(storeWithRollback, clusterName));
    StoreBackupVersionCleanupService.setMinBackupVersionCleanupDelay(100L);
    doReturn(true).when(controllerConfig).isBackupVersionReplicaReductionEnabled();
    versions.clear();
    versions.put(1, VersionStatus.ONLINE);
    versions.put(2, VersionStatus.ONLINE);
    versions.put(3, VersionStatus.STARTED);
    Store storeWithPush = mockStore(-1, System.currentTimeMillis() - defaultRetentionMs * 2, versions, 2);
    doReturn(1509711434L).when(storeWithPush).getBackupVersionRetentionMs();
    Assert.assertFalse(service.cleanupBackupVersion(storeWithPush, clusterName));
    verify(admin).updateIdealState(clusterName, Version.composeKafkaTopic(storeWithPush.getName(), 1), 2);

  }

  @Test
  public void testCleanupBackupVersionRepush() {
    VeniceHelixAdmin admin = mock(VeniceHelixAdmin.class);
    VeniceControllerMultiClusterConfig config = mock(VeniceControllerMultiClusterConfig.class);
    long defaultRetentionMs = TimeUnit.DAYS.toMillis(7);
    doReturn(defaultRetentionMs).when(config).getBackupVersionDefaultRetentionMs();
    VeniceControllerClusterConfig controllerConfig = mock(VeniceControllerClusterConfig.class);
    doReturn(controllerConfig).when(config).getControllerConfig(anyString());
    doReturn(mockClusterResource).when(admin).getHelixVeniceClusterResources(anyString());
    doReturn(clusterManager).when(mockClusterResource).getRoutersClusterManager();
    StoreBackupVersionCleanupService service =
        new StoreBackupVersionCleanupService(admin, config, mock(MetricsRepository.class));

    String clusterName = "test_cluster";
    // Store is not qualified because of short life time of backup version
    Map<Integer, VersionStatus> versions = new HashMap<>();
    versions.put(1, VersionStatus.ONLINE);
    versions.put(2, VersionStatus.ONLINE);
    Store storeWithFreshBackupVersion = mockStore(-1, System.currentTimeMillis(), versions, 2);
    storeWithFreshBackupVersion.getVersion(2).setRepushSourceVersion(1);
    Assert.assertFalse(service.cleanupBackupVersion(storeWithFreshBackupVersion, clusterName));

    versions.clear();
    versions.put(1, VersionStatus.ONLINE);
    versions.put(2, VersionStatus.ONLINE);
    versions.put(3, VersionStatus.ONLINE);
    Store storeWithRollback = mockStore(-1, System.currentTimeMillis() - defaultRetentionMs * 2, versions, 3);
    Version version = storeWithRollback.getVersion(3);
    doReturn(2).when(version).getRepushSourceVersion();

    // should delete version 2 as 1 is the true backup
    Assert.assertTrue(service.cleanupBackupVersion(storeWithRollback, clusterName));
    TestUtils.waitForNonDeterministicAssertion(
        1,
        TimeUnit.SECONDS,
        () -> verify(admin, atLeast(1)).deleteOldVersionInStore(clusterName, storeWithRollback.getName(), 2));

    versions.clear();
    versions.put(1, VersionStatus.ONLINE);
    versions.put(3, VersionStatus.ONLINE);
    Store storeLingeringVersion = mockStore(-1, System.currentTimeMillis() - defaultRetentionMs * 2, versions, 3);
    version = storeLingeringVersion.getVersion(2);
    doReturn(2).when(version).getRepushSourceVersion();
    doReturn(1L).when(version).getCreatedTime();

    // should delete version 1 as its lingerning and not a repush source version.
    Assert.assertTrue(service.cleanupBackupVersion(storeLingeringVersion, clusterName));
    TestUtils.waitForNonDeterministicAssertion(
        1,
        TimeUnit.SECONDS,
        () -> verify(admin, atLeast(1)).deleteOldVersionInStore(clusterName, storeLingeringVersion.getName(), 1));
  }

  @Test
  public void testMetadataBasedCleanupBackupVersion() throws IOException {
    VeniceHelixAdmin admin = mock(VeniceHelixAdmin.class);
    VeniceControllerMultiClusterConfig config = mock(VeniceControllerMultiClusterConfig.class);
    long defaultRetentionMs = TimeUnit.DAYS.toMillis(7);
    LiveInstanceMonitor liveInstanceMonitor = mock(LiveInstanceMonitor.class);
    doReturn(liveInstanceMonitor).when(admin).getLiveInstanceMonitor(anyString());
    doReturn(defaultRetentionMs).when(config).getBackupVersionDefaultRetentionMs();
    CloseableHttpAsyncClient asyncClient = mock(CloseableHttpAsyncClient.class);
    StatusLine statusLine = mock(StatusLine.class);
    CurrentVersionResponse currentVersionResponse = new CurrentVersionResponse();
    currentVersionResponse.setCurrentVersion(2);
    HttpResponse response = mock(HttpResponse.class);
    Future future = CompletableFuture.completedFuture(response);
    doReturn(future).when(asyncClient).execute(any(), eq(null));
    doReturn(HttpStatus.SC_OK).when(statusLine).getStatusCode();
    doReturn(statusLine).when(response).getStatusLine();
    HttpEntity entity = mock(HttpEntity.class);
    doReturn(entity).when(response).getEntity();

    doReturn(new ByteArrayInputStream(OBJECT_MAPPER.writeValueAsBytes(currentVersionResponse))).when(entity)
        .getContent();
    VeniceControllerClusterConfig controllerConfig = mock(VeniceControllerClusterConfig.class);
    doReturn(controllerConfig).when(config).getControllerConfig(anyString());
    doReturn(true).when(controllerConfig).isBackupVersionMetadataFetchBasedCleanupEnabled();
    doReturn(mockClusterResource).when(admin).getHelixVeniceClusterResources(anyString());
    doReturn(clusterManager).when(mockClusterResource).getRoutersClusterManager();
    Set<Instance> instSet = new HashSet<>();
    instSet.add(new Instance("0", "localhost1", 1234));
    MetricsRepository metricsRepository = mock(MetricsRepository.class);
    doReturn(mock(Sensor.class)).when(metricsRepository).sensor(anyString(), any());

    doReturn(Collections.emptySet()).when(clusterManager).getLiveRouterInstances();
    StoreBackupVersionCleanupService service =
        spy(new StoreBackupVersionCleanupService(admin, config, metricsRepository));
    doReturn(asyncClient).when(service).getHttpAsyncClient();

    String clusterName = "test_cluster";
    Map<Integer, VersionStatus> versions = new HashMap<>();
    // Store is qualified, and contains one removable version
    versions.put(1, VersionStatus.ONLINE);
    versions.put(2, VersionStatus.ONLINE);
    doReturn(instSet).when(clusterManager).getLiveRouterInstances();
    doReturn(instSet).when(liveInstanceMonitor).getAllLiveInstances();

    Store storeWithTwoVersions = mockStore(-1, System.currentTimeMillis() - defaultRetentionMs * 2, versions, 2);
    Assert.assertFalse(service.cleanupBackupVersion(storeWithTwoVersions, clusterName));

    ServerCurrentVersionResponse versionResponse = new ServerCurrentVersionResponse();
    versionResponse.setCurrentVersion(2);

    // both server and router returns valid version. delete backup.
    doReturn(new ByteArrayInputStream(OBJECT_MAPPER.writeValueAsBytes(versionResponse)))
        .doReturn(new ByteArrayInputStream(OBJECT_MAPPER.writeValueAsBytes(currentVersionResponse)))
        .when(entity)
        .getContent();
    Assert.assertTrue(service.cleanupBackupVersion(storeWithTwoVersions, clusterName));
  }

  @Test
  public void testCleanupBackupVersionSleepValidation() throws Exception {
    VeniceControllerMultiClusterConfig config = mock(VeniceControllerMultiClusterConfig.class);
    long defaultRetentionMs = TimeUnit.DAYS.toMillis(7);
    LiveInstanceMonitor liveInstanceMonitor = mock(LiveInstanceMonitor.class);
    doReturn(defaultRetentionMs).when(config).getBackupVersionDefaultRetentionMs();
    doReturn(defaultRetentionMs).when(config).getBackupVersionDefaultRetentionMs();
    VeniceControllerClusterConfig controllerConfig = mock(VeniceControllerClusterConfig.class);
    doReturn(controllerConfig).when(config).getControllerConfig(any());
    doReturn(true).when(controllerConfig).isBackupVersionRetentionBasedCleanupEnabled();
    doReturn(true).when(admin).isLeaderControllerFor(any());
    doReturn(liveInstanceMonitor).when(admin).getLiveInstanceMonitor(anyString());
    doReturn(mockClusterResource).when(admin).getHelixVeniceClusterResources(anyString());
    doReturn(clusterManager).when(mockClusterResource).getRoutersClusterManager();
    doReturn(Collections.emptySet()).when(clusterManager).getLiveRouterInstances();
    Map<Integer, VersionStatus> versions = new HashMap<>();
    versions.put(1, VersionStatus.ONLINE);
    versions.put(2, VersionStatus.ONLINE);
    Store storeWithTwoVersions = mockStore(-1, System.currentTimeMillis() - defaultRetentionMs * 2, versions, 2);

    String clusterName = "test_cluster";
    Set<String> clusters = new HashSet<>();
    clusters.add(clusterName);
    doReturn(clusters).when(config).getClusters();
    List<Store> storeList = new ArrayList<>();
    storeList.add(storeWithTwoVersions);
    doReturn(storeList).when(admin).getAllStores(any());
    TestMockTime time = new TestMockTime();
    MetricsRepository metricsRepository = mock(MetricsRepository.class);
    when(metricsRepository.sensor(anyString(), any())).thenReturn(mock(Sensor.class));
    StoreBackupVersionCleanupService service =
        new StoreBackupVersionCleanupService(admin, config, time, metricsRepository);
    service.startInner();
    TestUtils.waitForNonDeterministicAssertion(
        1,
        TimeUnit.SECONDS,
        () -> verify(admin, atLeast(1)).deleteOldVersionInStore(clusterName, storeWithTwoVersions.getName(), 1));
  }

}
