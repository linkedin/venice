package com.linkedin.venice.stats;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.stats.AggVersionedIngestionStats;
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

  @Test
  public void testStorageEngineLatencyMetrics() {
    MetricsRepository metricsRepo = MetricsRepositoryUtils.createSingleThreadedMetricsRepository();
    MockTehutiReporter reporter = new MockTehutiReporter();
    VeniceServerConfig mockVeniceServerConfig = Mockito.mock(VeniceServerConfig.class);

    String storeName = Utils.getUniqueString("store_latency_test");

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

    // v1 starts pushing
    Version version = new VersionImpl(storeName, 1);
    mockStore.addVersion(version);
    stats.handleStoreChanged(mockStore);

    long currentTimeMs = System.currentTimeMillis();

    // Test storage engine put latency
    double v1PutLatencyMs = 5.5d;
    stats.recordStorageEnginePutLatency(storeName, 1, v1PutLatencyMs, currentTimeMs);

    // Test storage engine delete latency
    double v1DeleteLatencyMs = 3.2d;
    stats.recordStorageEngineDeleteLatency(storeName, 1, v1DeleteLatencyMs, currentTimeMs);

    // Verify put latency metrics are recorded correctly
    Assert.assertEquals(
        reporter.query("." + storeName + "_future--storage_engine_put_latency_avg_ms.IngestionStatsGauge").value(),
        v1PutLatencyMs);
    Assert.assertEquals(
        reporter.query("." + storeName + "_future--storage_engine_put_latency_max_ms.IngestionStatsGauge").value(),
        v1PutLatencyMs);

    // Verify delete latency metrics are recorded correctly
    Assert.assertEquals(
        reporter.query("." + storeName + "_future--storage_engine_delete_latency_avg_ms.IngestionStatsGauge").value(),
        v1DeleteLatencyMs);
    Assert.assertEquals(
        reporter.query("." + storeName + "_future--storage_engine_delete_latency_max_ms.IngestionStatsGauge").value(),
        v1DeleteLatencyMs);

    // v1 becomes the current version and v2 starts pushing
    version.setStatus(VersionStatus.ONLINE);
    mockStore.setCurrentVersionWithoutCheck(1);
    Version version2 = new VersionImpl(storeName, 2);
    mockStore.addVersion(version2);

    stats.handleStoreChanged(mockStore);

    // Record metrics for v2
    double v2PutLatencyMs = 7.8d;
    double v2DeleteLatencyMs = 4.1d;
    stats.recordStorageEnginePutLatency(storeName, 2, v2PutLatencyMs, currentTimeMs);
    stats.recordStorageEngineDeleteLatency(storeName, 2, v2DeleteLatencyMs, currentTimeMs);

    // Verify v1 (current version) metrics are still accessible
    Assert.assertEquals(
        reporter.query("." + storeName + "_current--storage_engine_put_latency_avg_ms.IngestionStatsGauge").value(),
        v1PutLatencyMs);
    Assert.assertEquals(
        reporter.query("." + storeName + "_current--storage_engine_put_latency_max_ms.IngestionStatsGauge").value(),
        v1PutLatencyMs);
    Assert.assertEquals(
        reporter.query("." + storeName + "_current--storage_engine_delete_latency_avg_ms.IngestionStatsGauge").value(),
        v1DeleteLatencyMs);
    Assert.assertEquals(
        reporter.query("." + storeName + "_current--storage_engine_delete_latency_max_ms.IngestionStatsGauge").value(),
        v1DeleteLatencyMs);

    // Verify v2 (future version) metrics
    Assert.assertEquals(
        reporter.query("." + storeName + "_future--storage_engine_put_latency_avg_ms.IngestionStatsGauge").value(),
        v2PutLatencyMs);
    Assert.assertEquals(
        reporter.query("." + storeName + "_future--storage_engine_put_latency_max_ms.IngestionStatsGauge").value(),
        v2PutLatencyMs);
    Assert.assertEquals(
        reporter.query("." + storeName + "_future--storage_engine_delete_latency_avg_ms.IngestionStatsGauge").value(),
        v2DeleteLatencyMs);
    Assert.assertEquals(
        reporter.query("." + storeName + "_future--storage_engine_delete_latency_max_ms.IngestionStatsGauge").value(),
        v2DeleteLatencyMs);

    // Record multiple values to test avg and max calculations
    double v2PutLatency2Ms = 10.5d;
    double v2DeleteLatency2Ms = 6.3d;
    stats.recordStorageEnginePutLatency(storeName, 2, v2PutLatency2Ms, currentTimeMs);
    stats.recordStorageEngineDeleteLatency(storeName, 2, v2DeleteLatency2Ms, currentTimeMs);

    // Verify max is updated correctly
    Assert.assertEquals(
        reporter.query("." + storeName + "_future--storage_engine_put_latency_max_ms.IngestionStatsGauge").value(),
        v2PutLatency2Ms); // Should be the max
    Assert.assertEquals(
        reporter.query("." + storeName + "_future--storage_engine_delete_latency_max_ms.IngestionStatsGauge").value(),
        v2DeleteLatency2Ms); // Should be the max

    // Verify avg is calculated correctly
    double expectedPutAvg = (v2PutLatencyMs + v2PutLatency2Ms) / 2;
    double expectedDeleteAvg = (v2DeleteLatencyMs + v2DeleteLatency2Ms) / 2;
    Assert.assertEquals(
        reporter.query("." + storeName + "_future--storage_engine_put_latency_avg_ms.IngestionStatsGauge").value(),
        expectedPutAvg);
    Assert.assertEquals(
        reporter.query("." + storeName + "_future--storage_engine_delete_latency_avg_ms.IngestionStatsGauge").value(),
        expectedDeleteAvg);

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
}
