package com.linkedin.davinci.stats;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionImpl;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.utils.DataProviderUtils;
import io.tehuti.metrics.MetricsRepository;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;
import org.testng.annotations.Test;


/** Tests the per-store OTel stats registry of {@link AbstractVeniceAggVersionedStats}. */
public class AbstractVeniceAggVersionedStatsTest {
  private static final String STORE_NAME = "test-store";

  @Test
  public void testStatsCreationAndStoreChangePropagateVersionInfo() {
    Store store = createStore(STORE_NAME, 1, createVersion(STORE_NAME, 1, VersionStatus.ONLINE));
    TestAggStats stats = createStats(store, true);

    TestStoreOtelStats storeStats = stats.getOrCreateOtelStats(STORE_NAME);
    assertEquals(storeStats.currentVersion, 1);
    assertEquals(storeStats.futureVersion, Store.NON_EXISTING_VERSION);
    assertEquals(storeStats.updateCount, 1);

    Store updated = createStore(
        STORE_NAME,
        2,
        createVersion(STORE_NAME, 1, VersionStatus.ONLINE),
        createVersion(STORE_NAME, 2, VersionStatus.ONLINE),
        createVersion(STORE_NAME, 3, VersionStatus.STARTED));
    stats.handleStoreChanged(updated);

    assertSame(stats.getOtelStats(STORE_NAME), storeStats);
    assertEquals(storeStats.currentVersion, 2);
    assertEquals(storeStats.futureVersion, 3);
    assertEquals(storeStats.updateCount, 2);
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testStoreDeletionClosesAndRemovesStats(boolean unregisterMetricForDeletedStoreEnabled) {
    assertStoreDeletionClosesAndRemovesStats(unregisterMetricForDeletedStoreEnabled);
  }

  @Test
  public void testRemoveIfClosesOnlyWhenPredicateMatches() {
    Store store = createStore(STORE_NAME, 1, createVersion(STORE_NAME, 1, VersionStatus.ONLINE));
    TestAggStats stats = createStats(store, true);

    TestStoreOtelStats storeStats = stats.getOrCreateOtelStats(STORE_NAME);
    stats.removeOtelStatsIf(STORE_NAME, existing -> false);
    assertSame(stats.getOtelStats(STORE_NAME), storeStats);
    assertFalse(storeStats.closed);

    stats.removeOtelStatsIf(STORE_NAME, existing -> true);
    assertNull(stats.getOtelStats(STORE_NAME));
    assertTrue(storeStats.closed);
    assertEquals(storeStats.closeCount, 1);
  }

  @Test
  public void testComputeCreatesAbsentStatsWithVersionInfoBeforeAction() {
    Store store = createStore(
        STORE_NAME,
        5,
        createVersion(STORE_NAME, 5, VersionStatus.ONLINE),
        createVersion(STORE_NAME, 6, VersionStatus.PUSHED));
    TestAggStats stats = createStats(store, true);
    AtomicBoolean actionCalled = new AtomicBoolean(false);

    stats.computeOtelStats(STORE_NAME, storeStats -> {
      actionCalled.set(true);
      assertEquals(storeStats.currentVersion, 5);
      assertEquals(storeStats.futureVersion, 6);
      assertFalse(storeStats.closed);
    });

    assertTrue(actionCalled.get());
    assertEquals(stats.getOtelStats(STORE_NAME).updateCount, 1);
  }

  @Test
  public void testStoreChangeWhileCreatingStatsIsNotLost() {
    Store store = createStore(STORE_NAME, 1, createVersion(STORE_NAME, 1, VersionStatus.ONLINE));
    TestAggStats stats = createStats(store, true);
    Store updated = createStore(
        STORE_NAME,
        2,
        createVersion(STORE_NAME, 1, VersionStatus.ONLINE),
        createVersion(STORE_NAME, 2, VersionStatus.ONLINE),
        createVersion(STORE_NAME, 3, VersionStatus.STARTED));
    // The store changes after getOrCreate reads its versions but before it creates the stats, so the change finds no
    // stats to update.
    stats.afterReadingVersions = () -> stats.handleStoreChanged(updated);

    TestStoreOtelStats storeStats = stats.getOrCreateOtelStats(STORE_NAME);

    assertEquals(storeStats.currentVersion, 2);
    assertEquals(storeStats.futureVersion, 3);
  }

  private void assertStoreDeletionClosesAndRemovesStats(boolean unregisterMetricForDeletedStoreEnabled) {
    Store store = createStore(STORE_NAME, 1, createVersion(STORE_NAME, 1, VersionStatus.ONLINE));
    TestAggStats stats = createStats(store, unregisterMetricForDeletedStoreEnabled);

    TestStoreOtelStats storeStats = stats.getOrCreateOtelStats(STORE_NAME);
    stats.handleStoreDeleted(STORE_NAME);

    assertTrue(storeStats.closed);
    assertEquals(storeStats.closeCount, 1);
    assertNull(stats.getOtelStats(STORE_NAME));
  }

  private static TestAggStats createStats(Store store, boolean unregisterMetricForDeletedStoreEnabled) {
    ReadOnlyStoreRepository metadataRepository = mock(ReadOnlyStoreRepository.class);
    doReturn(Collections.singletonList(store)).when(metadataRepository).getAllStores();
    doReturn(store).when(metadataRepository).getStoreOrThrow(STORE_NAME);
    return new TestAggStats(new MetricsRepository(), metadataRepository, unregisterMetricForDeletedStoreEnabled);
  }

  private static Store createStore(String storeName, int currentVersion, Version... versions) {
    Store store = mock(Store.class);
    doReturn(storeName).when(store).getName();
    doReturn(currentVersion).when(store).getCurrentVersion();
    doReturn(Arrays.asList(versions)).when(store).getVersions();
    return store;
  }

  private static Version createVersion(String storeName, int versionNumber, VersionStatus status) {
    Version version = new VersionImpl(storeName, versionNumber, "push-" + versionNumber);
    version.setStatus(status);
    return version;
  }

  private static class TestAggStats extends AbstractVeniceAggVersionedStats<Object, TestStatsReporter> {
    private final PerStoreVersionedOtelStats<TestStoreOtelStats> otelStats;
    /** Runs once, right after the registry reads a store's future version. */
    private Runnable afterReadingVersions;

    TestAggStats(
        MetricsRepository metricsRepository,
        ReadOnlyStoreRepository metadataRepository,
        boolean unregisterMetricForDeletedStoreEnabled) {
      super(
          metricsRepository,
          metadataRepository,
          Object::new,
          TestStatsReporter::new,
          unregisterMetricForDeletedStoreEnabled);
      otelStats = createPerStoreOtelStats(TestStoreOtelStats::new);
    }

    @Override
    protected int getFutureVersion(String storeName) {
      int futureVersion = super.getFutureVersion(storeName);
      Runnable hook = afterReadingVersions;
      afterReadingVersions = null;
      if (hook != null) {
        hook.run();
      }
      return futureVersion;
    }

    TestStoreOtelStats getOrCreateOtelStats(String storeName) {
      return otelStats.getOrCreate(storeName);
    }

    TestStoreOtelStats getOtelStats(String storeName) {
      return otelStats.get(storeName);
    }

    void removeOtelStatsIf(String storeName, java.util.function.Predicate<TestStoreOtelStats> predicate) {
      otelStats.removeIf(storeName, predicate);
    }

    void computeOtelStats(String storeName, java.util.function.Consumer<TestStoreOtelStats> action) {
      otelStats.compute(storeName, action);
    }
  }

  private static class TestStoreOtelStats implements AbstractVeniceAggVersionedStats.StoreOtelStats {
    final String storeName;
    int currentVersion = Integer.MIN_VALUE;
    int futureVersion = Integer.MIN_VALUE;
    int updateCount;
    int closeCount;
    boolean closed;

    TestStoreOtelStats(String storeName) {
      this.storeName = storeName;
    }

    @Override
    public void updateVersionInfo(int currentVersion, int futureVersion) {
      this.currentVersion = currentVersion;
      this.futureVersion = futureVersion;
      updateCount++;
    }

    @Override
    public void close() {
      closed = true;
      closeCount++;
    }
  }

  private static class TestStatsReporter extends AbstractVeniceStatsReporter<Object> {
    TestStatsReporter(MetricsRepository metricsRepository, String storeName, String clusterName) {
      super(metricsRepository, storeName);
    }

    @Override
    protected void registerStats() {
    }
  }
}
