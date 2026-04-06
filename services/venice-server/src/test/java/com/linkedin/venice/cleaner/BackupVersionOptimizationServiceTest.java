package com.linkedin.venice.cleaner;

import static com.linkedin.venice.meta.VersionStatus.ONLINE;
import static com.linkedin.venice.meta.VersionStatus.STARTED;
import static org.mockito.Mockito.after;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.linkedin.davinci.storage.StorageEngineRepository;
import com.linkedin.davinci.store.StorageEngine;
import com.linkedin.venice.acl.VeniceComponent;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.stats.BackupVersionOptimizationServiceStats;
import com.linkedin.venice.utils.LogContext;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.Test;


public class BackupVersionOptimizationServiceTest {
  private static final int PARTITION_ID_0 = 0;
  private static final int NO_READ_THRESHOLD_MS_FOR_DATABASE_OPTIMIZATION = 3;

  private StorageEngineRepository mockStorageEngineRepository(String storeName, int... versions) {
    Set<Integer> partitionIdSet = new HashSet<>();
    partitionIdSet.add(PARTITION_ID_0);
    List<StorageEngine> engineList = new ArrayList<>(versions.length);

    StorageEngineRepository storageEngineRepository = mock(StorageEngineRepository.class);
    for (int version: versions) {
      String resourceName = Version.composeKafkaTopic(storeName, version);
      StorageEngine storageEngine = mock(StorageEngine.class);
      doReturn(resourceName).when(storageEngine).getStoreVersionName();
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
      doReturn(versionInfo).when(store).getVersion(version);
    });
    doReturn(store).when(storeRepository).getStore(storeName);

    return storeRepository;
  }

  @Test
  public void testBackupVersionShouldBeOptimizedAfterBecomingInactive() throws Exception {
    // Construct storage engines for two versions
    String storeName = Utils.getUniqueString();
    int backupVersion = 1;
    int currentVersion = 2;
    int futureVersionWhichDoesNotExistAnymore = 3;
    String backupResourceName = Version.composeKafkaTopic(storeName, backupVersion);

    StorageEngineRepository storageEngineRepository =
        mockStorageEngineRepository(storeName, backupVersion, currentVersion, futureVersionWhichDoesNotExistAnymore);
    StorageEngine backupStorageEngine = storageEngineRepository.getLocalStorageEngine(backupResourceName);

    Map<Integer, VersionStatus> versionStatusMap = new HashMap<>();
    versionStatusMap.put(backupVersion, ONLINE);
    versionStatusMap.put(currentVersion, ONLINE);

    ReadOnlyStoreRepository storeRepository = mockStoreRepository(storeName, currentVersion, versionStatusMap);

    BackupVersionOptimizationService optimizationService = new BackupVersionOptimizationService(
        storeRepository,
        storageEngineRepository,
        NO_READ_THRESHOLD_MS_FOR_DATABASE_OPTIMIZATION,
        1,
        mock(BackupVersionOptimizationServiceStats.class),
        LogContext.forTests(VeniceComponent.SERVER.name()));
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
    int currentVersion = 2;
    int futureVersion = 3;
    String backupResourceName = Version.composeKafkaTopic(storeName, backupVersion);
    String newResourceName = Version.composeKafkaTopic(storeName, currentVersion);

    StorageEngineRepository storageEngineRepository =
        mockStorageEngineRepository(storeName, backupVersion, currentVersion, futureVersion);
    StorageEngine backupStorageEngine = storageEngineRepository.getLocalStorageEngine(backupResourceName);
    StorageEngine newStorageEngine = storageEngineRepository.getLocalStorageEngine(newResourceName);

    Map<Integer, VersionStatus> versionStatusMap = new HashMap<>();
    versionStatusMap.put(backupVersion, ONLINE);
    versionStatusMap.put(currentVersion, ONLINE);
    versionStatusMap.put(futureVersion, STARTED);

    ReadOnlyStoreRepository storeRepository = mockStoreRepository(storeName, currentVersion, versionStatusMap);

    BackupVersionOptimizationService optimizationService = new BackupVersionOptimizationService(
        storeRepository,
        storageEngineRepository,
        NO_READ_THRESHOLD_MS_FOR_DATABASE_OPTIMIZATION,
        1,
        mock(BackupVersionOptimizationServiceStats.class),
        LogContext.forTests(VeniceComponent.SERVER.name()));
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
    StorageEngine backupStorageEngine = storageEngineRepository.getLocalStorageEngine(backupResourceName);
    StorageEngine newStorageEngine = storageEngineRepository.getLocalStorageEngine(newResourceName);

    Map<Integer, VersionStatus> versionStatusMap = new HashMap<>();
    versionStatusMap.put(backupVersion, ONLINE);
    versionStatusMap.put(newVersion, ONLINE);

    ReadOnlyStoreRepository storeRepository = mockStoreRepository(storeName, newVersion, versionStatusMap);

    BackupVersionOptimizationService optimizationService = new BackupVersionOptimizationService(
        storeRepository,
        storageEngineRepository,
        NO_READ_THRESHOLD_MS_FOR_DATABASE_OPTIMIZATION,
        1,
        mock(BackupVersionOptimizationServiceStats.class),
        LogContext.forTests(VeniceComponent.SERVER.name()));
    // Record a read to the backup version
    optimizationService.recordReadUsage(backupResourceName);

    optimizationService.start();
    try {
      TestUtils.waitForNonDeterministicAssertion(
          10,
          TimeUnit.SECONDS,
          () -> verify(backupStorageEngine).reopenStoragePartition(PARTITION_ID_0));

      // Sleep to ensure the next recordReadUsage timestamp is strictly after
      // recordDatabaseOptimization timestamp. Without this, both can land on the same
      // millisecond, causing whetherToOptimize() to return false permanently
      // (lastOptimizationTimestamp >= lastReadUsageTimestamp).
      Thread.sleep(NO_READ_THRESHOLD_MS_FOR_DATABASE_OPTIMIZATION + 1);

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

  /**
   * Pins per-partition error scaling: N partition failures must produce N
   * {@code recordBackupVersionDatabaseOptimizationError} calls. Catches a regression that hoists
   * the error-recording out of the partition loop (pre-PR behavior) — that would silently scale
   * the error counter at 1 per store-cycle instead of 1 per failing partition.
   */
  @Test
  public void testPerPartitionErrorRecordingScalesWithFailingPartitions() throws Exception {
    String storeName = Utils.getUniqueString();
    int backupVersion = 1;
    int currentVersion = 2;
    String backupResourceName = Version.composeKafkaTopic(storeName, backupVersion);

    // Build a store with 4 partitions on the backup version, ALL of which throw on reopen.
    Set<Integer> partitionIds = new HashSet<>();
    partitionIds.add(0);
    partitionIds.add(1);
    partitionIds.add(2);
    partitionIds.add(3);

    StorageEngineRepository storageEngineRepository = mock(StorageEngineRepository.class);
    StorageEngine backupEngine = mock(StorageEngine.class);
    doReturn(backupResourceName).when(backupEngine).getStoreVersionName();
    doReturn(partitionIds).when(backupEngine).getPartitionIds();
    doThrow(new RuntimeException("simulated reopen failure")).when(backupEngine).reopenStoragePartition(0);
    doThrow(new RuntimeException("simulated reopen failure")).when(backupEngine).reopenStoragePartition(1);
    doThrow(new RuntimeException("simulated reopen failure")).when(backupEngine).reopenStoragePartition(2);
    doThrow(new RuntimeException("simulated reopen failure")).when(backupEngine).reopenStoragePartition(3);

    // Current-version engine: real but empty (won't be optimized; only the backup is read-active).
    String currentResourceName = Version.composeKafkaTopic(storeName, currentVersion);
    StorageEngine currentEngine = mock(StorageEngine.class);
    doReturn(currentResourceName).when(currentEngine).getStoreVersionName();
    doReturn(new HashSet<Integer>()).when(currentEngine).getPartitionIds();

    List<StorageEngine> engineList = new ArrayList<>();
    engineList.add(backupEngine);
    engineList.add(currentEngine);
    doReturn(engineList).when(storageEngineRepository).getAllLocalStorageEngines();
    doReturn(backupEngine).when(storageEngineRepository).getLocalStorageEngine(backupResourceName);
    doReturn(currentEngine).when(storageEngineRepository).getLocalStorageEngine(currentResourceName);

    Map<Integer, VersionStatus> versionStatusMap = new HashMap<>();
    versionStatusMap.put(backupVersion, ONLINE);
    versionStatusMap.put(currentVersion, ONLINE);
    ReadOnlyStoreRepository storeRepository = mockStoreRepository(storeName, currentVersion, versionStatusMap);

    BackupVersionOptimizationServiceStats stats = mock(BackupVersionOptimizationServiceStats.class);
    BackupVersionOptimizationService optimizationService = new BackupVersionOptimizationService(
        storeRepository,
        storageEngineRepository,
        NO_READ_THRESHOLD_MS_FOR_DATABASE_OPTIMIZATION,
        1,
        stats,
        LogContext.forTests(VeniceComponent.SERVER.name()));
    optimizationService.recordReadUsage(backupResourceName);

    optimizationService.start();
    try {
      // Wait until exactly one cycle has run all 4 partition reopens. We stop the service AS PART
      // of the wait condition so the strict times(4) verify below isn't racing against the next
      // cycle: the all-partitions-throw path never calls resourceState.recordDatabaseOptimization,
      // so whetherToOptimize stays true and a short scheduleIntervalSeconds would let the cycle
      // re-fire and inflate the recorded counts.
      TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
        verify(backupEngine).reopenStoragePartition(0);
        verify(backupEngine).reopenStoragePartition(1);
        verify(backupEngine).reopenStoragePartition(2);
        verify(backupEngine).reopenStoragePartition(3);
      });
      optimizationService.stop();

      // Per-partition error recording: 4 partitions all fail → 4 error calls (NOT 1 per store).
      verify(stats, times(4)).recordBackupVersionDatabaseOptimizationError(storeName);
      // No success recordings since every partition threw.
      verify(stats, never()).recordBackupVersionDatabaseOptimization(storeName);
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
    StorageEngine backupStorageEngine = storageEngineRepository.getLocalStorageEngine(backupResourceName);

    Map<Integer, VersionStatus> versionStatusMap = new HashMap<>();
    versionStatusMap.put(backupVersion, ONLINE);
    versionStatusMap.put(newVersion, ONLINE);

    ReadOnlyStoreRepository storeRepository = mockStoreRepository(storeName, newVersion, versionStatusMap);

    BackupVersionOptimizationService optimizationService = new BackupVersionOptimizationService(
        storeRepository,
        storageEngineRepository,
        NO_READ_THRESHOLD_MS_FOR_DATABASE_OPTIMIZATION,
        1,
        mock(BackupVersionOptimizationServiceStats.class),
        LogContext.forTests(VeniceComponent.SERVER.name()));

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
