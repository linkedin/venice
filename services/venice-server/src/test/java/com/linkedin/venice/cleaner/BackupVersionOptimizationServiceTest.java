package com.linkedin.venice.cleaner;

import static com.linkedin.venice.meta.VersionStatus.ONLINE;
import static org.mockito.Mockito.after;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.linkedin.davinci.storage.StorageEngineRepository;
import com.linkedin.davinci.store.AbstractStorageEngine;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.stats.BackupVersionOptimizationServiceStats;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.Test;


public class BackupVersionOptimizationServiceTest {
  private static final int PARTITION_ID_0 = 0;
  private static final int NO_READ_THRESHOLD_MS_FOR_DATABASE_OPTIMIZATION = 3;

  private StorageEngineRepository mockStorageEngineRepository(String storeName, int... versions) {
    Set<Integer> partitionIdSet = new HashSet<>();
    partitionIdSet.add(PARTITION_ID_0);
    List<AbstractStorageEngine> engineList = new ArrayList<>(versions.length);

    StorageEngineRepository storageEngineRepository = mock(StorageEngineRepository.class);
    for (int version: versions) {
      String resourceName = Version.composeKafkaTopic(storeName, version);
      AbstractStorageEngine storageEngine = mock(AbstractStorageEngine.class);
      doReturn(resourceName).when(storageEngine).getStoreName();
      doReturn(partitionIdSet).when(storageEngine).getPartitionIds();
      engineList.add(storageEngine);
      doReturn(storageEngine).when(storageEngineRepository).getLocalStorageEngine(resourceName);
    }
    doReturn(engineList).when(storageEngineRepository).getAllLocalStorageEngines();

    return storageEngineRepository;
  }

  private ReadOnlyStoreRepository mockStoreRepository(
      String storeName,
      int currentVersion,
      Map<Integer, VersionStatus> versionStatusMap) {
    ReadOnlyStoreRepository storeRepository = mock(ReadOnlyStoreRepository.class);
    Store store = mock(Store.class);
    doReturn(currentVersion).when(store).getCurrentVersion();
    versionStatusMap.forEach((version, status) -> {
      Version versionInfo = mock(Version.class);
      doReturn(status).when(versionInfo).getStatus();
      doReturn(Optional.of(versionInfo)).when(store).getVersion(version);

    });
    doReturn(store).when(storeRepository).getStore(storeName);

    return storeRepository;
  }

  @Test
  public void testBackupVersionShouldBeOptimizedAfterBecomingInactive() throws Exception {
    // Construct storage engines for two versions
    String storeName = Utils.getUniqueString();
    int backupVersion = 1;
    int newVersion = 2;
    String backupResourceName = Version.composeKafkaTopic(storeName, backupVersion);

    StorageEngineRepository storageEngineRepository = mockStorageEngineRepository(storeName, backupVersion, newVersion);
    AbstractStorageEngine backupStorageEngine = storageEngineRepository.getLocalStorageEngine(backupResourceName);

    Map<Integer, VersionStatus> versionStatusMap = new HashMap<>();
    versionStatusMap.put(backupVersion, ONLINE);
    versionStatusMap.put(newVersion, ONLINE);

    ReadOnlyStoreRepository storeRepository = mockStoreRepository(storeName, newVersion, versionStatusMap);

    BackupVersionOptimizationService optimizationService = new BackupVersionOptimizationService(
        storeRepository,
        storageEngineRepository,
        NO_READ_THRESHOLD_MS_FOR_DATABASE_OPTIMIZATION,
        1,
        mock(BackupVersionOptimizationServiceStats.class));
    // Record a read
    optimizationService.recordReadUsage(backupResourceName);

    optimizationService.start();
    try {
      TestUtils.waitForNonDeterministicAssertion(
          10,
          TimeUnit.SECONDS,
          () -> verify(backupStorageEngine).reopenStoragePartition(PARTITION_ID_0));
    } finally {
      optimizationService.stop();
    }
  }

  @Test
  public void testBothVersionShouldBeOptimizedAfterRollback() throws Exception {
    // Construct storage engines for two versions
    String storeName = Utils.getUniqueString();
    int backupVersion = 1;
    int newVersion = 2;
    String backupResourceName = Version.composeKafkaTopic(storeName, backupVersion);
    String newResourceName = Version.composeKafkaTopic(storeName, newVersion);

    StorageEngineRepository storageEngineRepository = mockStorageEngineRepository(storeName, backupVersion, newVersion);
    AbstractStorageEngine backupStorageEngine = storageEngineRepository.getLocalStorageEngine(backupResourceName);
    AbstractStorageEngine newStorageEngine = storageEngineRepository.getLocalStorageEngine(newResourceName);

    Map<Integer, VersionStatus> versionStatusMap = new HashMap<>();
    versionStatusMap.put(backupVersion, ONLINE);
    versionStatusMap.put(newVersion, ONLINE);

    ReadOnlyStoreRepository storeRepository = mockStoreRepository(storeName, newVersion, versionStatusMap);

    BackupVersionOptimizationService optimizationService = new BackupVersionOptimizationService(
        storeRepository,
        storageEngineRepository,
        NO_READ_THRESHOLD_MS_FOR_DATABASE_OPTIMIZATION,
        1,
        mock(BackupVersionOptimizationServiceStats.class));
    // Record a read to the backup version
    optimizationService.recordReadUsage(backupResourceName);

    optimizationService.start();
    try {
      TestUtils.waitForNonDeterministicAssertion(
          10,
          TimeUnit.SECONDS,
          () -> verify(backupStorageEngine).reopenStoragePartition(PARTITION_ID_0));

      // Record a read to the new version
      optimizationService.recordReadUsage(newResourceName);

      // Perform a rollback
      Store store = storeRepository.getStore(storeName);
      doReturn(backupVersion).when(store).getCurrentVersion();

      TestUtils.waitForNonDeterministicAssertion(
          10,
          TimeUnit.SECONDS,
          () -> verify(newStorageEngine).reopenStoragePartition(PARTITION_ID_0));
    } finally {
      optimizationService.stop();
    }
  }

  @Test
  public void testBackupVersionShouldBeOptimizedAgainAfterMoreReadsAfterOptimization() throws Exception {
    // Construct storage engines for two versions
    String storeName = Utils.getUniqueString();
    int backupVersion = 1;
    int newVersion = 2;
    String backupResourceName = Version.composeKafkaTopic(storeName, backupVersion);
    String newResourceName = Version.composeKafkaTopic(storeName, newVersion);

    StorageEngineRepository storageEngineRepository = mockStorageEngineRepository(storeName, backupVersion, newVersion);
    AbstractStorageEngine backupStorageEngine = storageEngineRepository.getLocalStorageEngine(backupResourceName);
    AbstractStorageEngine newStorageEngine = storageEngineRepository.getLocalStorageEngine(newResourceName);

    Map<Integer, VersionStatus> versionStatusMap = new HashMap<>();
    versionStatusMap.put(backupVersion, ONLINE);
    versionStatusMap.put(newVersion, ONLINE);

    ReadOnlyStoreRepository storeRepository = mockStoreRepository(storeName, newVersion, versionStatusMap);

    BackupVersionOptimizationService optimizationService = new BackupVersionOptimizationService(
        storeRepository,
        storageEngineRepository,
        NO_READ_THRESHOLD_MS_FOR_DATABASE_OPTIMIZATION,
        1,
        mock(BackupVersionOptimizationServiceStats.class));
    // Record a read to the backup version
    optimizationService.recordReadUsage(backupResourceName);

    optimizationService.start();
    try {
      TestUtils.waitForNonDeterministicAssertion(
          10,
          TimeUnit.SECONDS,
          () -> verify(backupStorageEngine).reopenStoragePartition(PARTITION_ID_0));

      // Record a read to the old version again after optimization
      optimizationService.recordReadUsage(backupResourceName);

      TestUtils.waitForNonDeterministicAssertion(
          10,
          TimeUnit.SECONDS,
          () -> verify(backupStorageEngine, times(2)).reopenStoragePartition(PARTITION_ID_0));

      verify(newStorageEngine, never()).reopenStoragePartition(PARTITION_ID_0);
    } finally {
      optimizationService.stop();
    }
  }

  @Test
  public void testBackupVersionShouldNotBeOptimizedIfNoRead() throws Exception {
    // Construct storage engines for two versions
    String storeName = Utils.getUniqueString();
    int backupVersion = 1;
    int newVersion = 2;
    String backupResourceName = Version.composeKafkaTopic(storeName, backupVersion);

    StorageEngineRepository storageEngineRepository = mockStorageEngineRepository(storeName, backupVersion, newVersion);
    AbstractStorageEngine backupStorageEngine = storageEngineRepository.getLocalStorageEngine(backupResourceName);

    Map<Integer, VersionStatus> versionStatusMap = new HashMap<>();
    versionStatusMap.put(backupVersion, ONLINE);
    versionStatusMap.put(newVersion, ONLINE);

    ReadOnlyStoreRepository storeRepository = mockStoreRepository(storeName, newVersion, versionStatusMap);

    BackupVersionOptimizationService optimizationService = new BackupVersionOptimizationService(
        storeRepository,
        storageEngineRepository,
        NO_READ_THRESHOLD_MS_FOR_DATABASE_OPTIMIZATION,
        1,
        mock(BackupVersionOptimizationServiceStats.class));

    optimizationService.start();
    try {
      TestUtils.waitForNonDeterministicAssertion(
          3,
          TimeUnit.SECONDS,
          () -> verify(storageEngineRepository, atLeastOnce()).getAllLocalStorageEngines());
      TestUtils.waitForNonDeterministicAssertion(
          3,
          TimeUnit.SECONDS,
          () -> verify(storeRepository, atLeastOnce()).getStore(storeName));
      verify(backupStorageEngine, after(TimeUnit.SECONDS.toMillis(10)).never()).reopenStoragePartition(PARTITION_ID_0);
    } finally {
      optimizationService.stop();
    }
  }
}
