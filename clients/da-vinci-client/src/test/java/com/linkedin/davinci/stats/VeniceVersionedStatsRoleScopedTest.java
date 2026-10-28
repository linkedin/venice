package com.linkedin.davinci.stats;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.utils.metrics.MetricsRepositoryUtils;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.stats.AsyncGauge;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/** Values recorded while a version is future must stay on the future reporter after that version is promoted. */
public class VeniceVersionedStatsRoleScopedTest {
  private static final String STORE_NAME = "testStore";

  private MetricsRepository metricsRepository;
  private ReadOnlyStoreRepository storeRepository;

  /** Cumulative counter, so tests can tell which stats object a reporter reads. */
  static class CounterStats {
    long count;
  }

  /** Mirrors IngestionStatsReporter's traffic gauges. */
  static class CounterStatsReporter extends AbstractVeniceStatsReporter<CounterStats> {
    CounterStatsReporter(MetricsRepository metricsRepository, String storeName, String clusterName) {
      super(metricsRepository, storeName);
    }

    @Override
    protected void registerStats() {
      registerSensor(new AsyncGauge((ignored, ignored2) -> {
        CounterStats stats = getRoleStats() != null ? getRoleStats() : getStats();
        return stats == null ? -1 : stats.count;
      }, "count"));
    }
  }

  static class CounterAggStats extends AbstractVeniceAggVersionedStats<CounterStats, CounterStatsReporter> {
    CounterAggStats(MetricsRepository metricsRepository, ReadOnlyStoreRepository repo, boolean roleScoped) {
      super(metricsRepository, repo, CounterStats::new, CounterStatsReporter::new, true, roleScoped);
    }

    void record(String storeName, int version) {
      recordVersionedRoleAndTotalStat(storeName, version, stats -> stats.count++);
    }
  }

  @BeforeMethod
  public void setUp() {
    metricsRepository = MetricsRepositoryUtils.createSingleThreadedMetricsRepository();
    storeRepository = mock(ReadOnlyStoreRepository.class);
    when(storeRepository.getAllStores()).thenReturn(Collections.emptyList());
  }

  private static Version version(int number, VersionStatus status) {
    Version version = mock(Version.class);
    when(version.getNumber()).thenReturn(number);
    when(version.getStatus()).thenReturn(status);
    return version;
  }

  private Store store(String storeName, int currentVersion, Version... versions) {
    Store store = mock(Store.class);
    when(store.getName()).thenReturn(storeName);
    when(store.getCurrentVersion()).thenReturn(currentVersion);
    List<Version> versionList = new ArrayList<>();
    Collections.addAll(versionList, versions);
    when(store.getVersions()).thenReturn(versionList);
    doReturn(store).when(storeRepository).getStoreOrThrow(storeName);
    return store;
  }

  private double gauge(String reporterName) {
    return metricsRepository.getMetric("." + reporterName + "--count.Gauge").value();
  }

  private void record(CounterAggStats aggStats, String storeName, int version, int times) {
    for (int i = 0; i < times; i++) {
      aggStats.record(storeName, version);
    }
  }

  @Test
  public void testFutureBacklogStaysFutureAfterSwap() {
    CounterAggStats aggStats = new CounterAggStats(metricsRepository, storeRepository, true);
    // v1 current, v2 future.
    aggStats
        .handleStoreCreated(store(STORE_NAME, 1, version(1, VersionStatus.ONLINE), version(2, VersionStatus.STARTED)));

    record(aggStats, STORE_NAME, 1, 3);
    record(aggStats, STORE_NAME, 2, 5);
    assertEquals(gauge(STORE_NAME + "_current"), 3.0);
    assertEquals(gauge(STORE_NAME + "_future"), 5.0);
    assertEquals(gauge(STORE_NAME + "_total"), 8.0);

    // Promote v2 before its backlog is read. Previously _current would report v2's 5.
    aggStats
        .handleStoreChanged(store(STORE_NAME, 2, version(1, VersionStatus.ONLINE), version(2, VersionStatus.ONLINE)));
    assertEquals(gauge(STORE_NAME + "_current"), 3.0, "v2's pre-swap backlog must not move to _current");
    assertEquals(gauge(STORE_NAME + "_future"), 5.0, "v2's pre-swap backlog stays attributed to _future");

    // v2 now counts as current; backup v1 only counts toward total.
    record(aggStats, STORE_NAME, 2, 2);
    record(aggStats, STORE_NAME, 1, 4);
    assertEquals(gauge(STORE_NAME + "_current"), 5.0);
    assertEquals(gauge(STORE_NAME + "_future"), 5.0);
    assertEquals(gauge(STORE_NAME + "_total"), 14.0);
  }

  @Test
  public void testDisabledKeepsPerVersionReporting() {
    CounterAggStats aggStats = new CounterAggStats(metricsRepository, storeRepository, false);
    aggStats
        .handleStoreCreated(store(STORE_NAME, 1, version(1, VersionStatus.ONLINE), version(2, VersionStatus.STARTED)));
    record(aggStats, STORE_NAME, 2, 5);

    aggStats
        .handleStoreChanged(store(STORE_NAME, 2, version(1, VersionStatus.ONLINE), version(2, VersionStatus.ONLINE)));
    // Old behavior: _current follows v2's per-version stats, backlog included.
    assertEquals(gauge(STORE_NAME + "_current"), 5.0);
    assertEquals(gauge(STORE_NAME + "_total"), 5.0);
  }

  @Test
  public void testRecordWithNoCurrentOrFutureVersion() {
    CounterAggStats aggStats = new CounterAggStats(metricsRepository, storeRepository, true);
    aggStats.handleStoreCreated(store(STORE_NAME, 0));

    // Neither current nor future: total only.
    record(aggStats, STORE_NAME, 7, 2);
    assertEquals(gauge(STORE_NAME + "_current"), 0.0);
    assertEquals(gauge(STORE_NAME + "_future"), 0.0);
    assertEquals(gauge(STORE_NAME + "_total"), 2.0);
  }

  @Test
  public void testSystemStoreHasNoFutureRoleStats() {
    String systemStoreName = VeniceSystemStoreUtils.getMetaStoreName(STORE_NAME);
    VeniceVersionedStats<CounterStats, CounterStatsReporter> versionedStats = new VeniceVersionedStats<>(
        metricsRepository,
        systemStoreName,
        CounterStats::new,
        CounterStatsReporter::new,
        true);
    versionedStats.setCurrentVersion(1);
    versionedStats.setFutureVersion(2);

    assertEquals(versionedStats.getRoleStats(1).count, 0L);
    assertNull(versionedStats.getRoleStats(2), "System stores have no future reporter, so no future role stats");
    assertNull(metricsRepository.getMetric("." + systemStoreName + "_future--count.Gauge"));
  }

  @Test
  public void testIngestionReporterReadsRoleStatsForTrafficAndVersionStatsForState() {
    IngestionStats versionStats = mock(IngestionStats.class);
    IngestionStats roleStats = mock(IngestionStats.class);
    when(versionStats.getLeaderRecordsConsumed()).thenReturn(90_000.0);
    when(roleStats.getLeaderRecordsConsumed()).thenReturn(80.0);
    when(versionStats.getLeaderBytesProduced()).thenReturn(1_000.0);
    when(roleStats.getLeaderBytesProduced()).thenReturn(10.0);
    when(versionStats.getIngestionTaskErroredGauge()).thenReturn(1);
    when(roleStats.getIngestionTaskErroredGauge()).thenReturn(0);

    IngestionStatsReporter reporter = new IngestionStatsReporter(metricsRepository, STORE_NAME + "_current", null);
    reporter.setStats(versionStats);
    reporter.setRoleStats(roleStats);

    String prefix = "." + STORE_NAME + "_current--";
    assertEquals(metricsRepository.getMetric(prefix + "leader_records_consumed.IngestionStatsGauge").value(), 80.0);
    assertEquals(metricsRepository.getMetric(prefix + "leader_bytes_produced.IngestionStatsGauge").value(), 10.0);
    // State gauges keep reading per-version stats.
    assertEquals(metricsRepository.getMetric(prefix + "ingestion_task_errored_gauge.IngestionStatsGauge").value(), 1.0);

    // No role stats (e.g. total reporter): fall back to linked stats.
    reporter.setRoleStats(null);
    assertEquals(metricsRepository.getMetric(prefix + "leader_records_consumed.IngestionStatsGauge").value(), 90_000.0);
  }
}
