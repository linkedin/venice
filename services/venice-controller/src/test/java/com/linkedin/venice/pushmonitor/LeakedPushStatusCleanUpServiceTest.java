package com.linkedin.venice.pushmonitor;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreCleaner;
import com.linkedin.venice.meta.Version;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.Test;


public class LeakedPushStatusCleanUpServiceTest {
  private static final long TEST_TIMEOUT = TimeUnit.SECONDS.toMillis(10);

  @Test
  public void testLeakedZKNodeShouldBeDeleted() throws Exception {
    String clusterName = "test-cluster";
    long sleepIntervalInMs = 10;
    long allowedLingerTimeInMs = 0;
    OfflinePushAccessor accessor = mock(OfflinePushAccessor.class);
    ReadOnlyStoreRepository metadataRepository = mock(ReadOnlyStoreRepository.class);
    AggPushStatusCleanUpStats aggPushStatusCleanUpStats = mock(AggPushStatusCleanUpStats.class);

    /**
     * Define good and leaked push statues
     */
    String storeName = "test_store";
    int leakedVersion1 = 1;
    int leakedVersion2 = 2;
    int currentVersion = 3;
    String leakedStoreVersion1 = Version.composeKafkaTopic(storeName, leakedVersion1);
    String leakedStoreVersion2 = Version.composeKafkaTopic(storeName, leakedVersion2);
    String goodStoreVersion = Version.composeKafkaTopic(storeName, currentVersion);
    List<String> loadedStoreVersionList = Arrays.asList(leakedStoreVersion1, leakedStoreVersion2, goodStoreVersion);
    doReturn(loadedStoreVersionList).when(accessor).loadOfflinePushStatusPaths();
    // Return empty creation time for the second leaked push status, so that it will be kept for debugging
    doReturn(Optional.empty()).when(accessor).getOfflinePushStatusCreationTime(leakedStoreVersion2);

    /**
     * Define the behavior of store config; the leaked version will not be in the version list of the store
     */
    Store mockStore = mock(Store.class);
    doReturn(mockStore).when(metadataRepository).getStoreOrThrow(any());
    doReturn(currentVersion).when(mockStore).getCurrentVersion();
    doReturn(false).when(mockStore).containsVersion(leakedVersion1);
    doReturn(false).when(mockStore).containsVersion(leakedVersion2);

    /**
     * The actual test; the clean up service will try to delete the leaked push status
     */
    try (LeakedPushStatusCleanUpService cleanUpService = new LeakedPushStatusCleanUpService(
        clusterName,
        accessor,
        metadataRepository,
        mock(StoreCleaner.class),
        aggPushStatusCleanUpStats,
        sleepIntervalInMs,
        allowedLingerTimeInMs)) {
      cleanUpService.start();
      verify(accessor, timeout(TEST_TIMEOUT).atLeastOnce())
          .deleteOfflinePushStatusAndItsPartitionStatuses(leakedStoreVersion1);
      /**
       * At most {@link LeakedPushStatusCleanUpService#MAX_LEAKED_VERSION_TO_KEEP} leaked push statues before the current
       * version will be kept for debugging.
       */
      verify(accessor, never()).deleteOfflinePushStatusAndItsPartitionStatuses(leakedStoreVersion2);
    }

    /**
     * Return an old creation time for the second leaked push status, so that it will be deleted due to be lingering too long.
     */
    doReturn(Optional.of(0l)).when(accessor).getOfflinePushStatusCreationTime(leakedStoreVersion2);
    try (LeakedPushStatusCleanUpService cleanUpService = new LeakedPushStatusCleanUpService(
        clusterName,
        accessor,
        metadataRepository,
        mock(StoreCleaner.class),
        aggPushStatusCleanUpStats,
        sleepIntervalInMs,
        allowedLingerTimeInMs)) {
      cleanUpService.start();
      // Both leaked resources should be deleted.
      verify(accessor, timeout(TEST_TIMEOUT).atLeastOnce())
          .deleteOfflinePushStatusAndItsPartitionStatuses(leakedStoreVersion1);
      verify(accessor, timeout(TEST_TIMEOUT).atLeastOnce())
          .deleteOfflinePushStatusAndItsPartitionStatuses(leakedStoreVersion2);
    }
  }
}
