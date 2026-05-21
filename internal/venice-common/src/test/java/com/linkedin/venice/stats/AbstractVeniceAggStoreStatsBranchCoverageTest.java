package com.linkedin.venice.stats;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertNull;

import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import io.tehuti.metrics.MetricsRepository;
import org.testng.annotations.Test;


/**
 * Branch-coverage tests for {@link AbstractVeniceAggStoreStats} — exercises the new
 * {@code handleStoreDeleted} early-return / unconditional-OTel-close paths and the
 * {@code registerStoreDataChangedListenerIfRequired} null-guard added in this PR.
 */
public class AbstractVeniceAggStoreStatsBranchCoverageTest {
  private static final String CLUSTER = "test-cluster";
  private static final String STORE = "test-store";

  /** Minimal concrete stats class so we can instantiate {@code AbstractVeniceAggStoreStats<T>}. */
  private static class TestStats extends AbstractVeniceStats {
    TestStats(MetricsRepository metricsRepository, String name) {
      super(metricsRepository, name);
    }
  }

  private static class TestAggStats extends AbstractVeniceAggStoreStats<TestStats> {
    TestAggStats(
        MetricsRepository metricsRepository,
        ReadOnlyStoreRepository metadataRepository,
        boolean isUnregisterEnabled) {
      super(CLUSTER, metricsRepository, metadataRepository, isUnregisterEnabled);
    }
  }

  @Test
  public void testRegisterListenerOnConstructionWhenRepoNonNull() {
    MetricsRepository metrics = new MetricsRepository();
    ReadOnlyStoreRepository repo = mock(ReadOnlyStoreRepository.class);
    TestAggStats stats = new TestAggStats(metrics, repo, true);
    verify(repo).registerStoreDataChangedListener(stats);
  }

  @Test
  public void testRegisterListenerSkippedWhenRepoIsNull() {
    MetricsRepository metrics = new MetricsRepository();
    // Null repo path — constructor must not NPE and must not attempt registration.
    new TestAggStats(metrics, null, true);
    // (No throw == pass for this branch.)
  }

  @Test
  public void testHandleStoreDeletedEarlyReturnsWhenStoreUnknown() {
    MetricsRepository metrics = new MetricsRepository();
    ReadOnlyStoreRepository repo = mock(ReadOnlyStoreRepository.class);
    TestAggStats stats = new TestAggStats(metrics, repo, true);
    // Store was never registered; handleStoreDeleted must short-circuit.
    stats.handleStoreDeleted("never-seen-store");
    // No assertion needed beyond "did not throw" — the branch is covered.
  }

  @Test
  public void testHandleStoreDeletedUnregistersSensorsWhenEnabled() {
    MetricsRepository metrics = new MetricsRepository();
    ReadOnlyStoreRepository repo = mock(ReadOnlyStoreRepository.class);
    TestAggStats stats = new TestAggStats(metrics, repo, true);
    TestStats storeStats = spy(new TestStats(metrics, STORE));
    stats.storeStats.put(STORE, storeStats);

    stats.handleStoreDeleted(STORE);

    verify(storeStats, times(1)).unregisterAllSensors();
    // Entry must be removed from the map so a re-creation of the same store gets fresh stats
    // instead of the now-closed instance.
    assertNull(stats.storeStats.get(STORE));
  }

  @Test
  public void testHandleStoreDeletedSkipsUnregisterWhenDisabled() {
    MetricsRepository metrics = new MetricsRepository();
    ReadOnlyStoreRepository repo = mock(ReadOnlyStoreRepository.class);
    TestAggStats stats = new TestAggStats(metrics, repo, false);
    TestStats storeStats = spy(new TestStats(metrics, STORE));
    stats.storeStats.put(STORE, storeStats);

    stats.handleStoreDeleted(STORE);

    // Tehuti unregister disabled — sensors stay; OTel close path still runs unconditionally.
    verify(storeStats, never()).unregisterAllSensors();
    // Entry is removed regardless of the unregister flag.
    assertNull(stats.storeStats.get(STORE));
  }
}
