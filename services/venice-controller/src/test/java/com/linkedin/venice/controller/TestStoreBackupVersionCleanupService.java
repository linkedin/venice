package com.linkedin.venice.controller;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.utils.SystemTime;
import com.linkedin.venice.utils.TestMockTime;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestStoreBackupVersionCleanupService {
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

    return store;
  }

  @Test
  public void testWhetherStoreReadyToBeCleanup() {
    long defaultBackupVersionRetentionMs = TimeUnit.DAYS.toMillis(7);
    Store storeNotReadyForCleanupWithDefaultRetentionPolicy =
        mockStore(-1, System.currentTimeMillis(), Collections.emptyMap(), -1);
    Assert.assertFalse(
        StoreBackupVersionCleanupService.whetherStoreReadyToBeCleanup(
            storeNotReadyForCleanupWithDefaultRetentionPolicy,
            defaultBackupVersionRetentionMs,
            new SystemTime()));

    Store storeReadyForCleanupWithDefaultRetentionPolicy =
        mockStore(-1, System.currentTimeMillis() - 2 * defaultBackupVersionRetentionMs, Collections.emptyMap(), -1);
    Assert.assertTrue(
        StoreBackupVersionCleanupService.whetherStoreReadyToBeCleanup(
            storeReadyForCleanupWithDefaultRetentionPolicy,
            defaultBackupVersionRetentionMs,
            new SystemTime()));

    long storeBackupRetentionMs = TimeUnit.DAYS.toMillis(3);
    Store storeNotReadyForCleanupWithSpecifiedRetentionPolicy =
        mockStore(storeBackupRetentionMs, System.currentTimeMillis(), Collections.emptyMap(), -1);
    Assert.assertFalse(
        StoreBackupVersionCleanupService.whetherStoreReadyToBeCleanup(
            storeNotReadyForCleanupWithSpecifiedRetentionPolicy,
            defaultBackupVersionRetentionMs,
            new SystemTime()));

    Store storeReadyForCleanupWithSpecifiedRetentionPolicy = mockStore(
        storeBackupRetentionMs,
        System.currentTimeMillis() - 2 * storeBackupRetentionMs,
        Collections.emptyMap(),
        -1);
    Assert.assertTrue(
        StoreBackupVersionCleanupService.whetherStoreReadyToBeCleanup(
            storeReadyForCleanupWithSpecifiedRetentionPolicy,
            defaultBackupVersionRetentionMs,
            new SystemTime()));

    long storeBackupRetentionMsZero = 0;
    Store storeNotReadyForCleanupWithZeroRetentionPolicy1 =
        mockStore(storeBackupRetentionMsZero, System.currentTimeMillis(), Collections.emptyMap(), -1);
    Assert.assertFalse(
        StoreBackupVersionCleanupService.whetherStoreReadyToBeCleanup(
            storeNotReadyForCleanupWithZeroRetentionPolicy1,
            defaultBackupVersionRetentionMs,
            new SystemTime()));

    Store storeNotReadyForCleanupWithZeroRetentionPolicy2 =
        mockStore(storeBackupRetentionMsZero, System.currentTimeMillis() - 10, Collections.emptyMap(), -1);
    Assert.assertFalse(
        StoreBackupVersionCleanupService.whetherStoreReadyToBeCleanup(
            storeNotReadyForCleanupWithZeroRetentionPolicy2,
            defaultBackupVersionRetentionMs,
            new SystemTime()));

    Store storeReadyForCleanupWithZeroRetentionPolicy = mockStore(
        storeBackupRetentionMsZero,
        System.currentTimeMillis() - 2 * storeBackupRetentionMs,
        Collections.emptyMap(),
        -1);
    Assert.assertTrue(
        StoreBackupVersionCleanupService.whetherStoreReadyToBeCleanup(
            storeReadyForCleanupWithZeroRetentionPolicy,
            defaultBackupVersionRetentionMs,
            new SystemTime()));
  }

  @Test
  public void testCleanupBackupVersion() {
    VeniceHelixAdmin admin = mock(VeniceHelixAdmin.class);
    VeniceControllerMultiClusterConfig config = mock(VeniceControllerMultiClusterConfig.class);
    long defaultRetentionMs = TimeUnit.DAYS.toMillis(7);
    doReturn(defaultRetentionMs).when(config).getBackupVersionDefaultRetentionMs();
    StoreBackupVersionCleanupService service = new StoreBackupVersionCleanupService(admin, config);

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
  }

  @Test
  public void testCleanupBackupVersionSleepValidation() throws Exception {
    VeniceHelixAdmin admin = mock(VeniceHelixAdmin.class);
    VeniceControllerMultiClusterConfig config = mock(VeniceControllerMultiClusterConfig.class);
    long defaultRetentionMs = TimeUnit.DAYS.toMillis(7);
    doReturn(defaultRetentionMs).when(config).getBackupVersionDefaultRetentionMs();
    doReturn(defaultRetentionMs).when(config).getBackupVersionDefaultRetentionMs();
    VeniceControllerConfig controllerConfig = mock(VeniceControllerConfig.class);
    doReturn(controllerConfig).when(config).getControllerConfig(any());
    doReturn(true).when(controllerConfig).isBackupVersionRetentionBasedCleanupEnabled();
    doReturn(true).when(admin).isLeaderControllerFor(any());
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
    StoreBackupVersionCleanupService service = new StoreBackupVersionCleanupService(admin, config, time);
    service.startInner();
    TestUtils.waitForNonDeterministicAssertion(
        1,
        TimeUnit.SECONDS,
        () -> verify(admin, atLeast(1)).deleteOldVersionInStore(clusterName, storeWithTwoVersions.getName(), 1));
  }

}
