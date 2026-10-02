package com.linkedin.venice.pushmonitor;

import static com.linkedin.venice.pushmonitor.PushStatusCleanUpServiceState.FAILED;
import static com.linkedin.venice.pushmonitor.PushStatusCleanUpServiceState.RUNNING;
import static com.linkedin.venice.pushmonitor.PushStatusCleanUpServiceState.STOPPED;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
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
import com.linkedin.venice.meta.VersionImpl;
import com.linkedin.venice.utils.TestMockTime;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.locks.AutoCloseableLock;
import com.linkedin.venice.utils.locks.ClusterLockManager;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.helix.HelixException;
import org.apache.helix.zookeeper.zkclient.exception.ZkException;
import org.mockito.InOrder;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class LeakedPushStatusCleanUpServiceTest {
  private static final String CLUSTER = "test-cluster";
  private static final String OWNER = "test_store";
  private static final long NOW = 10_000;
  private static final long LINGER = 100;
  private static final long TEST_TIMEOUT_SECONDS = 10;

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
    final TestMockTime time = new TestMockTime(NOW);
    final LeakedPushStatusCleanUpService service =
        new LeakedPushStatusCleanUpService(CLUSTER, accessor, repository, cleaner, locks, stats, 10, LINGER, time);

    Fixture() {
      doReturn(Optional.of(0L)).when(accessor).getOfflinePushStatusCreationTime(anyString());
    }

    void paths(String... topics) {
      doReturn(Arrays.asList(topics)).when(accessor).loadOfflinePushStatusPaths();
    }

    Store addStore(String storeName, int currentVersion, int... retainedVersions) {
      VeniceSystemStoreType type = VeniceSystemStoreType.getSystemStoreType(storeName);
      if (type != null && type.isNewMedataRepositoryAdopted()) {
        Store owner = TestUtils.createTestStore(type.extractRegularStoreName(storeName), "owner", 0);
        doReturn(owner).when(regularRepository).getStore(owner.getName());
        Store shared = TestUtils.createTestStore(type.getZkSharedStoreName(), "owner", 0);
        doReturn(shared).when(sharedRepository).getStore(shared.getName());
      } else {
        doReturn(TestUtils.createTestStore(storeName, "owner", 0)).when(regularRepository).getStore(storeName);
      }
      Store store = repository.getStoreOrThrow(storeName);
      for (int version: retainedVersions) {
        store.addVersion(new VersionImpl(storeName, version, "test-push"));
      }
      store.setCurrentVersionWithoutCheck(currentVersion);
      return store;
    }
  }

  @DataProvider
  public Object[][] sharedStoreTypes() {
    return new Object[][] { { VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE }, { VeniceSystemStoreType.META_STORE } };
  }

  @Test(dataProvider = "sharedStoreTypes")
  public void testAbsentOwnerExpiredPushStatusIsReclaimed(VeniceSystemStoreType type) {
    Fixture f = new Fixture();
    String storeName = type.getSystemStoreName(OWNER);
    String topic = Version.composeKafkaTopic(storeName, 1);
    f.paths(topic);
    assertNull(f.repository.getStore(storeName));
    AtomicBoolean inHelix = new AtomicBoolean(true);
    doAnswer(invocation -> inHelix.get()).when(f.cleaner).containsHelixResource(CLUSTER, topic);
    doAnswer(invocation -> {
      inHelix.set(false);
      return null;
    }).when(f.cleaner).deleteHelixResource(CLUSTER, topic);
    doAnswer(invocation -> {
      f.paths();
      return null;
    }).when(f.accessor).deleteOfflinePushStatusAndItsPartitionStatuses(topic);

    f.service.cleanUpLeakedPushStatuses();
    verify(f.cleaner).deleteHelixResource(CLUSTER, topic);
    verify(f.accessor, never()).deleteOfflinePushStatusAndItsPartitionStatuses(topic);
    f.service.cleanUpLeakedPushStatuses();
    f.service.cleanUpLeakedPushStatuses();

    InOrder order = inOrder(f.cleaner, f.accessor);
    order.verify(f.cleaner).deleteHelixResource(CLUSTER, topic);
    order.verify(f.accessor).deleteOfflinePushStatusAndItsPartitionStatuses(topic);
    verify(f.cleaner, times(2)).containsHelixResource(CLUSTER, topic);
    verify(f.stats, times(2)).recordSuccessfulLeakedPushStatusCleanUpCount(1);
  }

  @DataProvider
  public Object[][] retentionCases() {
    List<Object[]> cases = new ArrayList<>();
    for (boolean present: new boolean[] { false, true }) {
      for (int count: new int[] { 1, 2, 4 }) {
        for (Long creationTime: new Long[] { NOW - LINGER + 1, NOW - LINGER, NOW - LINGER - 1, null }) {
          cases.add(new Object[] { present, count, creationTime });
        }
      }
    }
    return cases.toArray(new Object[0][]);
  }

  @Test(dataProvider = "retentionCases")
  public void testNewestLeakedVersionRetention(boolean present, int count, Long creationTime) {
    Fixture f = new Fixture();
    if (present) {
      f.addStore(OWNER, count + 1);
    }
    List<String> paths = new ArrayList<>();
    for (int version = 1; version <= count; version++) {
      paths.add(Version.composeKafkaTopic(OWNER, version));
    }
    // Duplicate discoveries must not consume the one-version debug allowance.
    paths.add(paths.get(paths.size() - 1));
    f.paths(paths.toArray(new String[0]));
    String newest = Version.composeKafkaTopic(OWNER, count);
    doReturn(Optional.ofNullable(creationTime)).when(f.accessor).getOfflinePushStatusCreationTime(newest);

    f.service.cleanUpLeakedPushStatuses();

    boolean expired = creationTime != null && NOW - creationTime > LINGER;
    verify(f.accessor, expired ? times(1) : never()).deleteOfflinePushStatusAndItsPartitionStatuses(newest);
    for (int version = 1; version < count; version++) {
      String older = Version.composeKafkaTopic(OWNER, version);
      verify(f.accessor).deleteOfflinePushStatusAndItsPartitionStatuses(older);
      verify(f.accessor, never()).getOfflinePushStatusCreationTime(older);
    }
    verify(f.stats).recordLeakedPushStatusCount(count);
    verify(f.stats).recordSuccessfulLeakedPushStatusCleanUpCount(count - (expired ? 0 : 1));
    verify(f.stats).recordFailedLeakedPushStatusCleanUpCount(0);
  }

  @DataProvider
  public Object[][] storeNames() {
    return new Object[][] { { OWNER }, { VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE.getSystemStoreName(OWNER) },
        { VeniceSystemStoreType.META_STORE.getSystemStoreName(OWNER) },
        { VeniceSystemStoreType.BATCH_JOB_HEARTBEAT_STORE.getZkSharedStoreNameInCluster(CLUSTER) },
        { VeniceSystemStoreType.BATCH_JOB_HEARTBEAT_STORE.getPrefix() } };
  }

  @Test(dataProvider = "storeNames")
  public void testCurrentFutureAndRetainedVersionsAreProtected(String storeName) {
    Fixture f = new Fixture();
    f.addStore(storeName, 3, 1);
    String retained = Version.composeKafkaTopic(storeName, 1);
    String leaked = Version.composeKafkaTopic(storeName, 2);
    String current = Version.composeKafkaTopic(storeName, 3);
    String future = Version.composeKafkaTopic(storeName, 4);
    f.paths(future, retained, current, leaked);

    f.service.cleanUpLeakedPushStatuses();

    verify(f.accessor).deleteOfflinePushStatusAndItsPartitionStatuses(leaked);
    for (String protectedTopic: Arrays.asList(retained, current, future)) {
      verify(f.cleaner, never()).containsHelixResource(CLUSTER, protectedTopic);
      verify(f.accessor, never()).getOfflinePushStatusCreationTime(protectedTopic);
      verify(f.accessor, never()).deleteOfflinePushStatusAndItsPartitionStatuses(protectedTopic);
    }
    verify(f.stats).recordLeakedPushStatusCount(1);
  }

  @Test(dataProvider = "sharedStoreTypes")
  public void testMissingSharedMetadataWithLiveOwnerIsNotDeletion(VeniceSystemStoreType type) {
    Fixture f = new Fixture();
    f.addStore(OWNER, 3);
    String systemStore = type.getSystemStoreName(OWNER);
    f.paths(Version.composeKafkaTopic(systemStore, 1), Version.composeKafkaTopic(systemStore, 2));
    assertNull(f.repository.getStore(systemStore), "The real adapter hides system stores with missing shared metadata");

    f.service.cleanUpLeakedPushStatuses();

    verifyNoInteractions(f.cleaner);
    verify(f.accessor, never()).getOfflinePushStatusCreationTime(anyString());
    verify(f.accessor, never()).deleteOfflinePushStatusAndItsPartitionStatuses(anyString());
  }

  @Test
  public void testAbsentLegacySystemStoreUsesItsOwnMetadata() {
    Fixture f = new Fixture();
    // This suffix is a cluster name, not an owner. A same-named user store must not block legacy cleanup.
    f.addStore(CLUSTER, 1);
    String topic = Version
        .composeKafkaTopic(VeniceSystemStoreType.BATCH_JOB_HEARTBEAT_STORE.getZkSharedStoreNameInCluster(CLUSTER), 1);
    f.paths(topic);
    clearInvocations(f.regularRepository);

    f.service.cleanUpLeakedPushStatuses();

    verify(f.accessor).deleteOfflinePushStatusAndItsPartitionStatuses(topic);
    verify(f.regularRepository, never()).getStore(CLUSTER);
    verify(f.sharedRepository, never()).getStore(anyString());
  }

  @DataProvider
  public Object[][] resourceFailures() {
    return new Object[][] { { "timestamp" }, { "presence" }, { "helix" }, { "zk" } };
  }

  @Test(dataProvider = "resourceFailures")
  public void testResourceFailureIsIsolatedCountedAndRetried(String phase) {
    Fixture f = new Fixture();
    String newest = Version.composeKafkaTopic(OWNER, 3);
    String middle = Version.composeKafkaTopic(OWNER, 2);
    String oldest = Version.composeKafkaTopic(OWNER, 1);
    String otherStore = Version.composeKafkaTopic("other_store", 1);
    f.paths(oldest, middle, newest, otherStore);
    switch (phase) {
      case "timestamp":
        doThrow(new ZkException("timestamp unavailable")).doReturn(Optional.of(0L))
            .when(f.accessor)
            .getOfflinePushStatusCreationTime(newest);
        break;
      case "presence":
        doThrow(new VeniceException("presence unavailable")).doReturn(false)
            .when(f.cleaner)
            .containsHelixResource(CLUSTER, middle);
        break;
      case "helix":
        doReturn(true).when(f.cleaner).containsHelixResource(CLUSTER, middle);
        doThrow(new HelixException("drop failed")).doNothing().when(f.cleaner).deleteHelixResource(CLUSTER, middle);
        break;
      case "zk":
        doThrow(new ZkException("remove failed")).doNothing()
            .when(f.accessor)
            .deleteOfflinePushStatusAndItsPartitionStatuses(middle);
        break;
      default:
        throw new AssertionError(phase);
    }

    f.service.cleanUpLeakedPushStatuses();

    verify(f.accessor).deleteOfflinePushStatusAndItsPartitionStatuses(oldest);
    verify(f.accessor).deleteOfflinePushStatusAndItsPartitionStatuses(otherStore);
    verify(f.stats).recordSuccessfulLeakedPushStatusCleanUpCount(2);
    verify(f.stats).recordSuccessfulLeakedPushStatusCleanUpCount(1);
    verify(f.stats).recordFailedLeakedPushStatusCleanUpCount(1);
    if (phase.equals("helix") || phase.equals("presence")) {
      verify(f.accessor, never()).deleteOfflinePushStatusAndItsPartitionStatuses(middle);
    }
    clearInvocations(f.stats);

    f.service.cleanUpLeakedPushStatuses();

    verify(f.stats).recordSuccessfulLeakedPushStatusCleanUpCount(3);
    verify(f.stats).recordSuccessfulLeakedPushStatusCleanUpCount(1);
    verify(f.stats, times(2)).recordFailedLeakedPushStatusCleanUpCount(0);
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
  public void testOwnerLookupFailureIsNotAbsence() {
    Fixture f = new Fixture();
    String topic =
        Version.composeKafkaTopic(VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE.getSystemStoreName(OWNER), 1);
    f.paths(topic);
    // The adapter's first lookup returns null; the explicit owner check fails.
    doReturn(null).doThrow(new VeniceException("owner lookup failed")).when(f.regularRepository).getStore(OWNER);

    f.service.cleanUpLeakedPushStatuses();

    verifyNoInteractions(f.cleaner);
    verify(f.accessor, never()).deleteOfflinePushStatusAndItsPartitionStatuses(anyString());
  }

  @Test
  public void testEmptyAndMalformedPathsDoNotPreventValidCleanup() {
    Fixture f = new Fixture();
    f.paths();
    f.service.cleanUpLeakedPushStatuses();
    String valid = Version.composeKafkaTopic(OWNER, 1);
    f.paths(
        null,
        "",
        "_v1",
        "not-a-version",
        "store_v",
        "store_vno",
        "store_rt",
        "store_v1_sr",
        "store_v99999999999999999999",
        valid);
    f.service.cleanUpLeakedPushStatuses();
    verify(f.cleaner).containsHelixResource(CLUSTER, valid);
    verify(f.accessor).deleteOfflinePushStatusAndItsPartitionStatuses(valid);
    verify(f.stats).recordLeakedPushStatusCount(1);
  }

  @Test
  public void testDeletionFailureAfterQueueDrainedDoesNotStopWorker() throws Exception {
    Fixture f = new Fixture();
    f.addStore(OWNER, 2);
    String topic = Version.composeKafkaTopic(OWNER, 1);
    f.paths(topic);
    CountDownLatch retried = new CountDownLatch(1);
    CountDownLatch stopped = onState(f, STOPPED);
    doThrow(new VeniceException("injected deletion failure")).doAnswer(invocation -> {
      retried.countDown();
      return null;
    }).when(f.accessor).deleteOfflinePushStatusAndItsPartitionStatuses(topic);
    try {
      f.service.start();
      await(retried);
    } finally {
      f.service.stop();
    }
    await(stopped);
    verify(f.stats).recordFailedLeakedPushStatusCleanUpCount(1);
    verify(f.stats, never()).recordLeakedPushStatusCleanUpServiceState(FAILED);
    InOrder order = inOrder(f.stats);
    order.verify(f.stats).recordLeakedPushStatusCleanUpServiceState(RUNNING);
    order.verify(f.stats).recordLeakedPushStatusCleanUpServiceState(STOPPED);
  }

  @Test
  public void testPathScanFailureRetries() throws Exception {
    Fixture f = new Fixture();
    CountDownLatch retried = new CountDownLatch(1);
    CountDownLatch stopped = onState(f, STOPPED);
    doThrow(new ZkException("listing failed")).doAnswer(invocation -> {
      retried.countDown();
      return Collections.emptyList();
    }).when(f.accessor).loadOfflinePushStatusPaths();
    try {
      f.service.start();
      await(retried);
    } finally {
      f.service.stop();
    }
    await(stopped);
    verify(f.stats, never()).recordLeakedPushStatusCleanUpServiceState(FAILED);
  }

  @Test
  public void testLeadershipCheckFailureDoesNotDeleteZkOrHelix() {
    Fixture f = new Fixture();
    String topic = Version.composeKafkaTopic(OWNER, 1);
    f.paths(topic);
    doThrow(new VeniceException("not controller leader")).when(f.cleaner).containsHelixResource(CLUSTER, topic);

    f.service.cleanUpLeakedPushStatuses();

    verify(f.cleaner, never()).deleteHelixResource(anyString(), anyString());
    verify(f.accessor, never()).deleteOfflinePushStatusAndItsPartitionStatuses(anyString());
    verify(f.stats).recordFailedLeakedPushStatusCleanUpCount(1);
  }

  @Test
  public void testStopDuringPresenceCheckPreventsDeletion() throws Exception {
    Fixture f = new Fixture();
    String topic = Version.composeKafkaTopic(OWNER, 1);
    f.paths(topic);
    doAnswer(invocation -> {
      f.service.stopInner();
      return false;
    }).when(f.cleaner).containsHelixResource(CLUSTER, topic);

    f.service.cleanUpLeakedPushStatuses();

    verify(f.accessor, never()).deleteOfflinePushStatusAndItsPartitionStatuses(anyString());
    verify(f.stats).recordSuccessfulLeakedPushStatusCleanUpCount(0);
    verify(f.stats).recordFailedLeakedPushStatusCleanUpCount(0);
  }

  @DataProvider
  public Object[][] lockingStoreNames() {
    return new Object[][] { { OWNER }, { VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE.getSystemStoreName(OWNER) },
        { VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE.getSystemStoreName(CLUSTER) } };
  }

  @Test(dataProvider = "lockingStoreNames")
  public void testRecreationBeforeLockAcquisitionProtectsVersion(String storeName) throws Exception {
    Fixture f = new Fixture();
    String topic = Version.composeKafkaTopic(storeName, 1);
    AtomicReference<Thread> worker = new AtomicReference<>();
    CountDownLatch discovered = new CountDownLatch(1);
    doAnswer(invocation -> {
      worker.set(Thread.currentThread());
      discovered.countDown();
      return Collections.singletonList(topic);
    }).when(f.accessor).loadOfflinePushStatusPaths();
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      Future<?> cleanup;
      try (AutoCloseableLock ignored =
          f.locks.createStoreWriteLock(VeniceSystemStoreType.extractUserStoreName(storeName))) {
        cleanup = executor.submit(f.service::cleanUpLeakedPushStatuses);
        await(discovered);
        awaitLockWait(worker.get());
        f.addStore(storeName, 1);
      }
      cleanup.get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
      verifyNoInteractions(f.cleaner);
    } finally {
      executor.shutdownNow();
    }
  }

  @Test(dataProvider = "lockingStoreNames")
  public void testRecreationWaitsUntilCleanupFinishes(String storeName) throws Exception {
    Fixture f = new Fixture();
    String topic = Version.composeKafkaTopic(storeName, 1);
    f.paths(topic);
    CountDownLatch deleting = new CountDownLatch(1);
    CountDownLatch allowDelete = new CountDownLatch(1);
    CountDownLatch recreating = new CountDownLatch(1);
    AtomicReference<Thread> creator = new AtomicReference<>();
    AtomicBoolean recreated = new AtomicBoolean();
    doReturn(true).when(f.cleaner).containsHelixResource(CLUSTER, topic);
    doAnswer(invocation -> {
      deleting.countDown();
      await(allowDelete);
      assertFalse(recreated.get(), "Owner recreation must be excluded through the delete action");
      return null;
    }).when(f.cleaner).deleteHelixResource(CLUSTER, topic);
    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      Future<?> cleanup = executor.submit(f.service::cleanUpLeakedPushStatuses);
      await(deleting);
      Future<?> recreate = executor.submit(() -> {
        creator.set(Thread.currentThread());
        recreating.countDown();
        try (AutoCloseableLock ignored =
            f.locks.createStoreWriteLock(VeniceSystemStoreType.extractUserStoreName(storeName))) {
          f.addStore(storeName, 1);
          recreated.set(true);
        }
      });
      await(recreating);
      awaitLockWait(creator.get());
      allowDelete.countDown();
      cleanup.get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
      recreate.get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
      assertTrue(recreated.get());
      verify(f.cleaner).deleteHelixResource(CLUSTER, topic);
    } finally {
      allowDelete.countDown();
      executor.shutdownNow();
    }
  }

  @DataProvider
  public Object[][] interruptionPhases() {
    return new Object[][] { { "scan" }, { "metadata" }, { "resource" }, { "flag" } };
  }

  @Test(dataProvider = "interruptionPhases")
  public void testInterruptionStopsWithoutFurtherDeletion(String phase) throws Exception {
    Fixture f = new Fixture();
    String newest = Version.composeKafkaTopic(OWNER, 2);
    f.paths(Version.composeKafkaTopic(OWNER, 1), newest);
    CountDownLatch stopped = onState(f, STOPPED);
    VeniceException interrupted = new VeniceException(new InterruptedException("interrupted operation"));
    switch (phase) {
      case "scan":
        doThrow(interrupted).when(f.accessor).loadOfflinePushStatusPaths();
        break;
      case "metadata":
        doThrow(interrupted).when(f.regularRepository).getStore(OWNER);
        break;
      case "resource":
        doThrow(interrupted).when(f.cleaner).containsHelixResource(CLUSTER, newest);
        break;
      case "flag":
        doAnswer(invocation -> {
          Thread.currentThread().interrupt();
          return Collections.singletonList(newest);
        }).when(f.accessor).loadOfflinePushStatusPaths();
        break;
      default:
        throw new AssertionError(phase);
    }
    try {
      f.service.start();
      await(stopped);
      verify(f.accessor, times(1)).loadOfflinePushStatusPaths();
      verify(f.accessor, never()).deleteOfflinePushStatusAndItsPartitionStatuses(anyString());
      verify(f.cleaner, never()).deleteHelixResource(anyString(), anyString());
      verify(f.stats, never()).recordLeakedPushStatusCleanUpServiceState(FAILED);
    } finally {
      f.service.stop();
    }
  }

  @Test
  public void testShutdownUnderClusterLockDoesNotUseClearedMetadata() throws Exception {
    Fixture f = new Fixture();
    AtomicReference<Thread> worker = new AtomicReference<>();
    CountDownLatch discovered = new CountDownLatch(1);
    CountDownLatch stopped = onState(f, STOPPED);
    doAnswer(invocation -> {
      worker.set(Thread.currentThread());
      discovered.countDown();
      return Collections.singletonList(Version.composeKafkaTopic(OWNER, 1));
    }).when(f.accessor).loadOfflinePushStatusPaths();
    try {
      try (AutoCloseableLock ignored = f.locks.createClusterWriteLock()) {
        f.service.start();
        await(discovered);
        awaitLockWait(worker.get());
        // Match controller teardown: stop while holding the cluster write lock, then clear metadata.
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

  @Test
  public void testUnexpectedFailureMarksWorkerFailed() throws Exception {
    Fixture f = new Fixture();
    CountDownLatch failed = onState(f, FAILED);
    CountDownLatch uncaught = new CountDownLatch(1);
    AtomicInteger scans = new AtomicInteger();
    doAnswer(invocation -> {
      Thread.currentThread().setUncaughtExceptionHandler((thread, error) -> uncaught.countDown());
      scans.incrementAndGet();
      throw new IllegalStateException("invalid state");
    }).when(f.accessor).loadOfflinePushStatusPaths();
    try {
      f.service.start();
      await(failed);
      await(uncaught);
      assertEquals(scans.get(), 1);
      verify(f.stats, never()).recordLeakedPushStatusCleanUpServiceState(STOPPED);
    } finally {
      f.service.stop();
    }
  }

  private static CountDownLatch onState(Fixture f, PushStatusCleanUpServiceState state) {
    CountDownLatch latch = new CountDownLatch(1);
    doAnswer(invocation -> {
      latch.countDown();
      return null;
    }).when(f.stats).recordLeakedPushStatusCleanUpServiceState(state);
    return latch;
  }

  private static void await(CountDownLatch latch) throws InterruptedException {
    assertTrue(latch.await(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS), "Timed out waiting for test synchronization");
  }

  private static void awaitLockWait(Thread thread) {
    TestUtils.waitForNonDeterministicAssertion(
        TEST_TIMEOUT_SECONDS,
        TimeUnit.SECONDS,
        () -> assertEquals(thread.getState(), Thread.State.WAITING));
  }
}
