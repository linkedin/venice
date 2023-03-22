package com.linkedin.venice.stats;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.stats.AggVersionedIngestionStats;
import com.linkedin.davinci.stats.IngestionStats;
import com.linkedin.venice.meta.OfflinePushStrategy;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.ReadStrategy;
import com.linkedin.venice.meta.RoutingStrategy;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionImpl;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.meta.ZKStore;
import com.linkedin.venice.tehuti.MockTehutiReporter;
import com.linkedin.venice.utils.Utils;
import io.tehuti.metrics.MetricsRepository;
import it.unimi.dsi.fastutil.ints.Int2ObjectMaps;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;


public class AggVersionedIngestionStatsTest {
  private AggVersionedIngestionStats aggIngestionStats;
  private final MetricsRepository metricsRepo = new MetricsRepository();
  private final MockTehutiReporter reporter = new MockTehutiReporter();
  private final VeniceServerConfig mockVeniceServerConfig = Mockito.mock(VeniceServerConfig.class);

  private String totalKey;
  private String currentKey;
  private String futureKey;

  private Version backupVer;
  private Version currentVer;
  private Version futureVer;

  private static final String STORE_FOO = Utils.getUniqueString("store_foo");

  @BeforeTest
  public void setUp() {
    String prefix = "." + STORE_FOO;
    String postfix = IngestionStats.VERSION_TOPIC_END_OFFSET_REWIND_COUNT + ".IngestionStatsGauge";
    totalKey = prefix + "_total--" + postfix;
    currentKey = prefix + "_current--" + postfix;
    futureKey = prefix + "_future--" + postfix;

    metricsRepo.addReporter(reporter);
    ReadOnlyStoreRepository mockMetaRepository = mock(ReadOnlyStoreRepository.class);
    doReturn(Int2ObjectMaps.emptyMap()).when(mockVeniceServerConfig).getKafkaClusterIdToAliasMap();
    doReturn(true).when(mockVeniceServerConfig).isUnregisterMetricForDeletedStoreEnabled();

    aggIngestionStats = new AggVersionedIngestionStats(metricsRepo, mockMetaRepository, mockVeniceServerConfig);

    Store mockStore = createStore();

    backupVer = new VersionImpl(STORE_FOO, 1);
    backupVer.setStatus(VersionStatus.ONLINE);
    mockStore.addVersion(backupVer);

    currentVer = new VersionImpl(STORE_FOO, 2);
    currentVer.setStatus(VersionStatus.ONLINE);
    mockStore.addVersion(currentVer);

    futureVer = new VersionImpl(STORE_FOO, 3);
    futureVer.setStatus(VersionStatus.PUSHED);
    mockStore.addVersion(futureVer);
    mockStore.setCurrentVersion(currentVer.getNumber());

    Mockito.doReturn(mockStore).when(mockMetaRepository).getStoreOrThrow(any());
  }

  @AfterTest
  public void cleanUp() {
    metricsRepo.close();
  }

  /**
   * testIngestionOffsetRewind does the following steps:
   *
   * 1. Create a store with 3 versions: backup, current, and future.
   * 2. Verify that no metrics exist initially.
   * 3. Increase the offset rewind counter for the store's backup, current, and future version.
   * 4. Verify that metrics repository contains the right counter value.
   * 5. Verify that no metrics after store is deleted, if UNREGISTER_METRIC_FOR_DELETED_STORE_ENABLED is enabled.
   */
  @Test
  public void testIngestionOffsetRewind() {
    // No metrics initially.
    verifyNoMetrics();

    int backupVerCnt = 2, curVerCnt = 3, futureVerCnt = 4;
    for (int i = 0; i < backupVerCnt; i++) {
      aggIngestionStats.recordVersionTopicEndOffsetRewind(STORE_FOO, backupVer.getNumber());
    }
    verifyCounters(backupVerCnt, 0, 0);

    for (int i = 0; i < curVerCnt; i++) {
      aggIngestionStats.recordVersionTopicEndOffsetRewind(STORE_FOO, currentVer.getNumber());
    }
    verifyCounters(backupVerCnt + curVerCnt, curVerCnt, 0);

    for (int i = 0; i < futureVerCnt; i++) {
      aggIngestionStats.recordVersionTopicEndOffsetRewind(STORE_FOO, futureVer.getNumber());
    }
    verifyCounters(backupVerCnt + curVerCnt + futureVerCnt, curVerCnt, futureVerCnt);

    aggIngestionStats.handleStoreDeleted(STORE_FOO);
    // Metrics are unregistered when UNREGISTER_METRIC_FOR_DELETED_STORE_ENABLED is enabled.
    if (mockVeniceServerConfig.isUnregisterMetricForDeletedStoreEnabled()) {
      verifyNoMetrics();
    }
  }

  private Store createStore() {
    return new ZKStore(
        STORE_FOO,
        "",
        10,
        PersistenceType.ROCKS_DB,
        RoutingStrategy.CONSISTENT_HASH,
        ReadStrategy.ANY_OF_ONLINE,
        OfflinePushStrategy.WAIT_ALL_REPLICAS,
        1);
  }

  private void verifyCounters(double total, double current, double future) {
    Assert.assertEquals(reporter.query(totalKey).value(), total);
    Assert.assertEquals(reporter.query(currentKey).value(), current);
    Assert.assertEquals(reporter.query(futureKey).value(), future);
  }

  private void verifyNoMetrics() {
    Assert.assertNull(metricsRepo.getMetric(totalKey));
    Assert.assertNull(metricsRepo.getMetric(currentKey));
    Assert.assertNull(metricsRepo.getMetric(futureKey));
  }
}
