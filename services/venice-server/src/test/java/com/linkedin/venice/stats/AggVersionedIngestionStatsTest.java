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
import com.linkedin.venice.utils.metrics.MetricsRepositoryUtils;
import io.tehuti.metrics.MetricsRepository;
import it.unimi.dsi.fastutil.ints.Int2ObjectMaps;
import java.util.ArrayList;
import java.util.List;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


public class AggVersionedIngestionStatsTest {
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
    MetricsRepository metricsRepo = MetricsRepositoryUtils.createSingleThreadedMetricsRepository();
    MockTehutiReporter reporter = new MockTehutiReporter();
    VeniceServerConfig mockVeniceServerConfig = Mockito.mock(VeniceServerConfig.class);

    String storeName = Utils.getUniqueString("store_foo");
    String prefix = "." + storeName;
    String postfix = IngestionStats.VERSION_TOPIC_END_OFFSET_REWIND_COUNT + ".IngestionStatsGauge";
    String totalKey = prefix + "_total--" + postfix;
    String currentKey = prefix + "_current--" + postfix;
    String futureKey = prefix + "_future--" + postfix;

    metricsRepo.addReporter(reporter);
    ReadOnlyStoreRepository mockMetaRepository = mock(ReadOnlyStoreRepository.class);
    doReturn(Int2ObjectMaps.emptyMap()).when(mockVeniceServerConfig).getKafkaClusterIdToAliasMap();
    doReturn(true).when(mockVeniceServerConfig).isUnregisterMetricForDeletedStoreEnabled();

    AggVersionedIngestionStats stats =
        new AggVersionedIngestionStats(metricsRepo, mockMetaRepository, mockVeniceServerConfig);
    Store mockStore = createStore(storeName);
    List<Store> storeList = new ArrayList<>();
    storeList.add(mockStore);

    doReturn(mockStore).when(mockMetaRepository).getStoreOrThrow(any());
    doReturn(storeList).when(mockMetaRepository).getAllStores();

    // No metrics initially.
    verifyNoMetrics(metricsRepo, totalKey);
    verifyNoMetrics(metricsRepo, currentKey);
    verifyNoMetrics(metricsRepo, futureKey);
    Version backupVer = new VersionImpl(storeName, 1);
    backupVer.setStatus(VersionStatus.ONLINE);
    mockStore.addVersion(backupVer);

    Version currentVer = new VersionImpl(storeName, 2);
    currentVer.setStatus(VersionStatus.ONLINE);
    mockStore.addVersion(currentVer);

    Version futureVer = new VersionImpl(storeName, 3);
    futureVer.setStatus(VersionStatus.PUSHED);
    mockStore.addVersion(futureVer);
    mockStore.setCurrentVersion(currentVer.getNumber());

    int backupVerCnt = 2, curVerCnt = 3, futureVerCnt = 4;
    for (int i = 0; i < backupVerCnt; i++) {
      stats.recordVersionTopicEndOffsetRewind(storeName, backupVer.getNumber());
    }
    verifyCounters(reporter, totalKey, backupVerCnt);
    verifyCounters(reporter, currentKey, 0);
    verifyCounters(reporter, futureKey, 0);

    for (int i = 0; i < curVerCnt; i++) {
      stats.recordVersionTopicEndOffsetRewind(storeName, currentVer.getNumber());
    }
    verifyCounters(reporter, totalKey, backupVerCnt + curVerCnt);
    verifyCounters(reporter, currentKey, curVerCnt);
    verifyCounters(reporter, futureKey, 0);

    for (int i = 0; i < futureVerCnt; i++) {
      stats.recordVersionTopicEndOffsetRewind(storeName, futureVer.getNumber());
    }

    verifyCounters(reporter, totalKey, backupVerCnt + curVerCnt + futureVerCnt);
    verifyCounters(reporter, currentKey, curVerCnt);
    verifyCounters(reporter, futureKey, futureVerCnt);

    stats.handleStoreDeleted(storeName);
    // Metrics are unregistered when UNREGISTER_METRIC_FOR_DELETED_STORE_ENABLED is enabled.
    if (mockVeniceServerConfig.isUnregisterMetricForDeletedStoreEnabled()) {
      verifyNoMetrics(metricsRepo, totalKey);
      verifyNoMetrics(metricsRepo, currentKey);
      verifyNoMetrics(metricsRepo, futureKey);
    }

    metricsRepo.close();
    reporter.close();
  }

  @Test
  public void testStatsCanUpdateVersionStatus() {
    MetricsRepository metricsRepo = MetricsRepositoryUtils.createSingleThreadedMetricsRepository();
    MockTehutiReporter reporter = new MockTehutiReporter();
    VeniceServerConfig mockVeniceServerConfig = Mockito.mock(VeniceServerConfig.class);

    String storeName = Utils.getUniqueString("store_foo");

    metricsRepo.addReporter(reporter);
    ReadOnlyStoreRepository mockMetaRepository = mock(ReadOnlyStoreRepository.class);
    doReturn(Int2ObjectMaps.emptyMap()).when(mockVeniceServerConfig).getKafkaClusterIdToAliasMap();
    doReturn(true).when(mockVeniceServerConfig).isUnregisterMetricForDeletedStoreEnabled();

    AggVersionedIngestionStats stats =
        new AggVersionedIngestionStats(metricsRepo, mockMetaRepository, mockVeniceServerConfig);
    Store mockStore = createStore(storeName);
    List<Store> storeList = new ArrayList<>();
    storeList.add(mockStore);

    doReturn(mockStore).when(mockMetaRepository).getStoreOrThrow(any());
    doReturn(storeList).when(mockMetaRepository).getAllStores();

    stats.loadAllStats();

    storeName = mockStore.getName();
    Assert.assertEquals(reporter.query("." + storeName + "--future_version.Gauge").value(), 0d);

    // v1 starts pushing
    Version version = new VersionImpl(storeName, 1);
    mockStore.addVersion(version);
    stats.handleStoreChanged(mockStore);

    // expect to see v1's stats on future reporter
    Assert.assertEquals(reporter.query("." + storeName + "--future_version.Gauge").value(), 1d);

    long consumerTimestampMs = System.currentTimeMillis();

    double v1ProducerToSourceBrokerLatencyMs = 811d;
    double v1SourceBrokerToLeaderConsumerLatencyMs = 211d;
    stats.recordLeaderLatencies(
        storeName,
        1,
        consumerTimestampMs,
        v1ProducerToSourceBrokerLatencyMs,
        v1SourceBrokerToLeaderConsumerLatencyMs);

    double v1ProducerToLocalBrokerLatencyMs = 821d;
    double v1LocalBrokerToFollowerConsumerLatencyMs = 221d;
    stats.recordFollowerLatencies(
        storeName,
        1,
        consumerTimestampMs,
        v1ProducerToLocalBrokerLatencyMs,
        v1LocalBrokerToFollowerConsumerLatencyMs);

    // v1 becomes the current version and v2 starts pushing
    version.setStatus(VersionStatus.ONLINE);
    mockStore.setCurrentVersionWithoutCheck(1);
    Version version2 = new VersionImpl(storeName, 2);
    mockStore.addVersion(version2);

    stats.handleStoreChanged(mockStore);

    // expect to see v1's stats on current reporter and v2's stats on future reporter
    Assert.assertEquals(reporter.query("." + storeName + "--future_version.Gauge").value(), 2d);
    Assert.assertEquals(reporter.query("." + storeName + "--current_version.Gauge").value(), 1d);

    double v2ProducerToSourceBrokerLatencyMs = 812d;
    double v2SourceBrokerToLeaderConsumerLatencyMs = 212d;
    stats.recordLeaderLatencies(
        storeName,
        2,
        consumerTimestampMs,
        v2ProducerToSourceBrokerLatencyMs,
        v2SourceBrokerToLeaderConsumerLatencyMs);

    Assert.assertEquals(
        reporter.query("." + storeName + "_current--producer_to_source_broker_latency_avg_ms.IngestionStatsGauge")
            .value(),
        v1ProducerToSourceBrokerLatencyMs);
    Assert.assertEquals(
        reporter.query("." + storeName + "_current--producer_to_source_broker_latency_max_ms.IngestionStatsGauge")
            .value(),
        v1ProducerToSourceBrokerLatencyMs);
    Assert.assertEquals(
        reporter.query("." + storeName + "_future--source_broker_to_leader_consumer_latency_avg_ms.IngestionStatsGauge")
            .value(),
        v2SourceBrokerToLeaderConsumerLatencyMs);
    Assert.assertEquals(
        reporter.query("." + storeName + "_future--source_broker_to_leader_consumer_latency_max_ms.IngestionStatsGauge")
            .value(),
        v2SourceBrokerToLeaderConsumerLatencyMs);

    double v2ProducerToLocalBrokerLatencyMs = 822d;
    double v2LocalBrokerToFollowerConsumerLatencyMs = 222d;
    stats.recordFollowerLatencies(
        storeName,
        2,
        consumerTimestampMs,
        v2ProducerToLocalBrokerLatencyMs,
        v2LocalBrokerToFollowerConsumerLatencyMs);

    Assert.assertEquals(
        reporter.query("." + storeName + "_current--producer_to_local_broker_latency_avg_ms.IngestionStatsGauge")
            .value(),
        v1ProducerToLocalBrokerLatencyMs);
    Assert.assertEquals(
        reporter.query("." + storeName + "_current--producer_to_local_broker_latency_max_ms.IngestionStatsGauge")
            .value(),
        v1ProducerToLocalBrokerLatencyMs);
    Assert
        .assertEquals(
            reporter
                .query(
                    "." + storeName + "_future--local_broker_to_follower_consumer_latency_avg_ms.IngestionStatsGauge")
                .value(),
            v2LocalBrokerToFollowerConsumerLatencyMs);
    Assert
        .assertEquals(
            reporter
                .query(
                    "." + storeName + "_future--local_broker_to_follower_consumer_latency_max_ms.IngestionStatsGauge")
                .value(),
            v2LocalBrokerToFollowerConsumerLatencyMs);

    // v2 finishes pushing
    version2.setStatus(VersionStatus.ONLINE);
    stats.handleStoreChanged(mockStore);

    // v2 becomes the current version
    mockStore.setCurrentVersionWithoutCheck(2);
    stats.handleStoreChanged(mockStore);

    // v3 finishes pushing and the status becomes to be online
    Version version3 = new VersionImpl(storeName, 3);
    version3.setStatus(VersionStatus.ONLINE);
    mockStore.addVersion(version3);
    mockStore.deleteVersion(1);
    stats.handleStoreChanged(mockStore);

    metricsRepo.close();
    reporter.close();
  }

  private Store createStore(String storeName) {
    return new ZKStore(
        storeName,
        "",
        10,
        PersistenceType.ROCKS_DB,
        RoutingStrategy.CONSISTENT_HASH,
        ReadStrategy.ANY_OF_ONLINE,
        OfflinePushStrategy.WAIT_ALL_REPLICAS,
        1);
  }

  private void verifyNoMetrics(MetricsRepository metricsRepository, String key) {
    Assert.assertNull(metricsRepository.getMetric(key));
  }

  private void verifyCounters(MockTehutiReporter reporter, String key, double value) {
    Assert.assertEquals(reporter.query(key).value(), value);
  }
}
