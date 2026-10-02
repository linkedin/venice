package com.linkedin.venice.pushmonitor;

import static com.linkedin.venice.pushmonitor.PushStatusCleanUpServiceState.FAILED;
import static com.linkedin.venice.pushmonitor.PushStatusCleanUpServiceState.STOPPED;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.HelixReadOnlyStoreRepositoryAdapter;
import com.linkedin.venice.helix.HelixReadOnlyZKSharedSystemStoreRepository;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreCleaner;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.LatencyUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.locks.AutoCloseableLock;
import com.linkedin.venice.utils.locks.ClusterLockManager;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.helix.HelixException;
import org.apache.helix.zookeeper.zkclient.exception.ZkException;
import org.mockito.MockedStatic;
import org.mockito.stubbing.Stubber;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class LeakedPushStatusCleanUpServiceTest {
  private static final long TEST_TIMEOUT = TimeUnit.SECONDS.toMillis(30);
  private static final String CLUSTER = "test-cluster";
  private static final String OWNER = "test_store";

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
    String retainedStoreVersion = Version.composeKafkaTopic(storeName, 0);
    String futureStoreVersion = Version.composeKafkaTopic(storeName, currentVersion + 1);
    List<String> loadedStoreVersionList = Arrays
        .asList(leakedStoreVersion1, leakedStoreVersion2, goodStoreVersion, retainedStoreVersion, futureStoreVersion);
    doReturn(loadedStoreVersionList).when(accessor).loadOfflinePushStatusPaths();
    // Return empty creation time for the second leaked push status, so that it will be kept for debugging
    doReturn(Optional.empty()).when(accessor).getOfflinePushStatusCreationTime(leakedStoreVersion2);

    /**
     * Define the behavior of store config; the leaked version will not be in the version list of the store
     */
    Store mockStore = mock(Store.class);
    doReturn(mockStore).when(metadataRepository).getStore(any());
    doReturn(currentVersion).when(mockStore).getCurrentVersion();
    doReturn(false).when(mockStore).containsVersion(leakedVersion1);
    doReturn(false).when(mockStore).containsVersion(leakedVersion2);
    doReturn(true).when(mockStore).containsVersion(0);

    /**
     * The actual test; the clean up service will try to delete the leaked push status
     */
    try (LeakedPushStatusCleanUpService cleanUpService = new LeakedPushStatusCleanUpService(
        clusterName,
        accessor,
        metadataRepository,
        mock(StoreCleaner.class),
        new ClusterLockManager(clusterName),
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
        new ClusterLockManager(clusterName),
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
    verify(accessor, never()).deleteOfflinePushStatusAndItsPartitionStatuses(goodStoreVersion);
    verify(accessor, never()).deleteOfflinePushStatusAndItsPartitionStatuses(retainedStoreVersion);
    verify(accessor, never()).deleteOfflinePushStatusAndItsPartitionStatuses(futureStoreVersion);
  }

  private static class Fixture {
    final OfflinePushAccessor accessor = mock(OfflinePushAccessor.class);
    final StoreCleaner cleaner = mock(StoreCleaner.class);
    final AggPushStatusCleanUpStats stats = mock(AggPushStatusCleanUpStats.class);
    final ReadOnlyStoreRepository regularRepository = mock(ReadOnlyStoreRepository.class);
    final HelixReadOnlyZKSharedSystemStoreRepository sharedRepository =
        mock(HelixReadOnlyZKSharedSystemStoreRepository.class);
    final HelixReadOnlyStoreRepositoryAdapter repository =
        new HelixReadOnlyStoreRepositoryAdapter(sharedRepository, regularRepository, CLUSTER);
    final ClusterLockManager locks = new ClusterLockManager(CLUSTER);
    final AtomicReference<Thread> cleanupThread = new AtomicReference<>();
    final LeakedPushStatusCleanUpService service =
        new LeakedPushStatusCleanUpService(CLUSTER, accessor, repository, cleaner, locks, stats, 10, 0);

    Fixture() {
      doReturn(Optional.of(0L)).when(accessor).getOfflinePushStatusCreationTime(anyString());
    }

    void paths(String... topics) {
      doAnswer(invocation -> {
        cleanupThread.set(Thread.currentThread());
        return Arrays.asList(topics);
      }).when(accessor).loadOfflinePushStatusPaths();
    }
  }

  @DataProvider
  public Object[][] storeNames() {
    return new Object[][] { { OWNER }, { VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE.getSystemStoreName(OWNER) },
        { VeniceSystemStoreType.META_STORE.getSystemStoreName(OWNER) } };
  }

  @Test(dataProvider = "storeNames")
  public void testAbsentStoreRetentionAndCleanupOrder(String storeName) {
    Fixture f = new Fixture();
    String newest = Version.composeKafkaTopic(storeName, 2);
    String oldest = Version.composeKafkaTopic(storeName, 1);
    f.paths(oldest, newest, newest);
    assertNull(f.repository.getStore(storeName));
    doReturn(Optional.empty()).doReturn(Optional.of(0L)).when(f.accessor).getOfflinePushStatusCreationTime(newest);
    doReturn(true).doReturn(false).when(f.cleaner).containsHelixResource(CLUSTER, newest);

    try (MockedStatic<LatencyUtils> clock = mockStatic(LatencyUtils.class)) {
      f.service.cleanUpLeakedPushStatuses();
      verify(f.accessor).deleteOfflinePushStatusAndItsPartitionStatuses(oldest);
      verify(f.cleaner, never()).containsHelixResource(CLUSTER, newest);
      f.paths(newest, newest);
      clock.when(() -> LatencyUtils.getElapsedTimeFromMsToMs(0L)).thenReturn(0L);
      f.service.cleanUpLeakedPushStatuses();
      verify(f.cleaner, never()).containsHelixResource(CLUSTER, newest);
      clock.when(() -> LatencyUtils.getElapsedTimeFromMsToMs(0L)).thenReturn(1L);
      f.service.cleanUpLeakedPushStatuses();
      verify(f.cleaner).deleteHelixResource(CLUSTER, newest);
      verify(f.accessor, never()).deleteOfflinePushStatusAndItsPartitionStatuses(newest);
      f.service.cleanUpLeakedPushStatuses();
      verify(f.accessor).deleteOfflinePushStatusAndItsPartitionStatuses(newest);
    }
  }

  @DataProvider
  public Object[][] systemStoreTypes() {
    return new Object[][] { { VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE }, { VeniceSystemStoreType.META_STORE } };
  }

  @Test(dataProvider = "systemStoreTypes")
  public void testMissingSharedMetadataWithLiveOwnerIsNotCleaned(VeniceSystemStoreType type) {
    Fixture f = new Fixture();
    doReturn(mock(Store.class)).when(f.regularRepository).getStore(OWNER);
    String storeName = type.getSystemStoreName(OWNER);
    String topic = Version.composeKafkaTopic(storeName, 1);
    f.paths(topic);
    assertNull(f.repository.getStore(storeName));

    f.service.cleanUpLeakedPushStatuses();

    verifyNoInteractions(f.cleaner);
    verify(f.accessor, never()).deleteOfflinePushStatusAndItsPartitionStatuses(anyString());
  }

  @DataProvider
  public Object[][] resourceFailures() {
    return new Object[][] { { true }, { false } };
  }

  @Test(dataProvider = "resourceFailures")
  public void testDeletionFailureOnLastVersionDoesNotStopWorker(boolean inHelix) throws Exception {
    Fixture f = new Fixture();
    Store store = mock(Store.class);
    doReturn(2).when(store).getCurrentVersion();
    doReturn(store).when(f.regularRepository).getStore(OWNER);
    String topic = Version.composeKafkaTopic(OWNER, 1);
    String other = Version.composeKafkaTopic("other_store", 1);
    doReturn(Arrays.asList(topic, other)).doReturn(Collections.singletonList(topic))
        .when(f.accessor)
        .loadOfflinePushStatusPaths();
    doReturn(inHelix).when(f.cleaner).containsHelixResource(CLUSTER, topic);
    Stubber failureThenRetry =
        doThrow(inHelix ? new HelixException("drop failed") : new ZkException("remove failed")).doAnswer(invocation -> {
          f.service.stopInner();
          return null;
        });
    if (inHelix) {
      failureThenRetry.when(f.cleaner).deleteHelixResource(CLUSTER, topic);
    } else {
      failureThenRetry.when(f.accessor).deleteOfflinePushStatusAndItsPartitionStatuses(topic);
    }
    CountDownLatch stopped = onStopped(f);
    try {
      f.service.start();
      await(stopped);
    } finally {
      f.service.stop();
    }
    verify(f.accessor).deleteOfflinePushStatusAndItsPartitionStatuses(other);
    verify(f.cleaner, times(inHelix ? 2 : 0)).deleteHelixResource(CLUSTER, topic);
    verify(f.accessor, times(inHelix ? 0 : 2)).deleteOfflinePushStatusAndItsPartitionStatuses(topic);
    verify(f.stats).recordSuccessfulLeakedPushStatusCleanUpCount(0);
    verify(f.stats, times(2)).recordSuccessfulLeakedPushStatusCleanUpCount(1);
    verify(f.stats).recordFailedLeakedPushStatusCleanUpCount(1);
    verify(f.stats, times(2)).recordFailedLeakedPushStatusCleanUpCount(0);
    verify(f.stats, never()).recordLeakedPushStatusCleanUpServiceState(FAILED);
  }

  @Test
  public void testMetadataFailureIsNotAbsenceAndRetriesNextSweep() {
    Fixture f = new Fixture();
    String topic = Version.composeKafkaTopic(OWNER, 1);
    String other = Version.composeKafkaTopic("other_store", 1);
    f.paths(topic, other);
    doThrow(new VeniceException("metadata unavailable")).doReturn(null).when(f.regularRepository).getStore(OWNER);

    f.service.cleanUpLeakedPushStatuses();

    verify(f.cleaner, never()).containsHelixResource(CLUSTER, topic);
    verify(f.accessor).deleteOfflinePushStatusAndItsPartitionStatuses(other);
    f.service.cleanUpLeakedPushStatuses();
    verify(f.accessor).deleteOfflinePushStatusAndItsPartitionStatuses(topic);
  }

  @Test
  public void testOwnerRecreatedBeforeLockAcquisitionIsNotCleaned() throws Exception {
    Fixture f = new Fixture();
    String storeName = VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE.getSystemStoreName(CLUSTER);
    String topic = Version.composeKafkaTopic(storeName, 1);
    f.paths(topic);
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      Future<?> cleanup;
      try (AutoCloseableLock ignored = f.locks.createStoreWriteLock(CLUSTER)) {
        cleanup = executor.submit(f.service::cleanUpLeakedPushStatuses);
        awaitLockWait(f.cleanupThread);
        verify(f.regularRepository, never()).getStore(anyString());
        doReturn(mock(Store.class)).when(f.regularRepository).getStore(CLUSTER);
      }
      cleanup.get(TEST_TIMEOUT, TimeUnit.MILLISECONDS);
      verifyNoInteractions(f.cleaner);
      verify(f.accessor, never()).deleteOfflinePushStatusAndItsPartitionStatuses(anyString());
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void testRecreationWaitsUntilCleanupFinishes() throws Exception {
    Fixture f = new Fixture();
    String storeName = VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE.getSystemStoreName(OWNER);
    String topic = Version.composeKafkaTopic(storeName, 1);
    f.paths(topic);
    CountDownLatch deleting = new CountDownLatch(1);
    CountDownLatch allowDelete = new CountDownLatch(1);
    AtomicReference<Thread> creator = new AtomicReference<>();
    doReturn(true).when(f.cleaner).containsHelixResource(CLUSTER, topic);
    doAnswer(invocation -> {
      deleting.countDown();
      await(allowDelete);
      return null;
    }).when(f.cleaner).deleteHelixResource(CLUSTER, topic);
    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      Future<?> cleanup = executor.submit(f.service::cleanUpLeakedPushStatuses);
      await(deleting);
      Future<?> recreate = executor.submit(() -> {
        creator.set(Thread.currentThread());
        try (AutoCloseableLock ignored = f.locks.createStoreWriteLock(OWNER)) {
          return null;
        }
      });
      awaitLockWait(creator);
      assertFalse(recreate.isDone());
      allowDelete.countDown();
      cleanup.get(TEST_TIMEOUT, TimeUnit.MILLISECONDS);
      recreate.get(TEST_TIMEOUT, TimeUnit.MILLISECONDS);
    } finally {
      allowDelete.countDown();
      executor.shutdownNow();
    }
  }

  @Test
  public void testShutdownUnderClusterLockDoesNotUseClearedMetadata() throws Exception {
    Fixture f = new Fixture();
    CountDownLatch stopped = onStopped(f);
    f.paths(Version.composeKafkaTopic(OWNER, 1));
    try {
      try (AutoCloseableLock ignored = f.locks.createClusterWriteLock()) {
        f.service.start();
        awaitLockWait(f.cleanupThread);
        f.service.stop();
        doReturn(null).when(f.regularRepository).getStore(OWNER);
      }
      await(stopped);
      verify(f.regularRepository, never()).getStore(anyString());
      verifyNoInteractions(f.cleaner);
      verify(f.accessor, never()).deleteOfflinePushStatusAndItsPartitionStatuses(anyString());
    } finally {
      f.service.stop();
    }
  }

  private static CountDownLatch onStopped(Fixture f) {
    CountDownLatch latch = new CountDownLatch(1);
    doAnswer(invocation -> {
      latch.countDown();
      return null;
    }).when(f.stats).recordLeakedPushStatusCleanUpServiceState(STOPPED);
    return latch;
  }

  private static void await(CountDownLatch latch) throws InterruptedException {
    assertTrue(latch.await(TEST_TIMEOUT, TimeUnit.MILLISECONDS), "Timed out waiting for test synchronization");
  }

  private static void awaitLockWait(AtomicReference<Thread> thread) {
    TestUtils.waitForNonDeterministicAssertion(TEST_TIMEOUT, TimeUnit.MILLISECONDS, () -> {
      assertNotNull(thread.get());
      assertEquals(thread.get().getState(), Thread.State.WAITING);
    });
  }
}
