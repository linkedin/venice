package com.linkedin.venice.stats;

import static com.linkedin.venice.stats.StatsErrorCode.NULL_DIV_STATS;
import static org.mockito.Mockito.mock;

import com.linkedin.davinci.stats.AggVersionedDIVStats;
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
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import io.tehuti.metrics.MetricsRepository;
import java.util.ArrayList;
import java.util.List;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;


public class AggVersionedDIVStatsTest {
  private static final int TEST_TIME = 10 * Time.MS_PER_SECOND;

  private AggVersionedDIVStats stats;
  protected MetricsRepository metricsRepository;
  protected MockTehutiReporter reporter;

  private ReadOnlyStoreRepository mockMetaRepository;
  private Store mockStore;
  List<Store> storeList;

  @BeforeTest
  public void setUp() {
    metricsRepository = new MetricsRepository();
    this.reporter = new MockTehutiReporter();
    metricsRepository.addReporter(reporter);

    mockMetaRepository = mock(ReadOnlyStoreRepository.class);

    mockStore = createStore(Utils.getUniqueString("store"));
    storeList = new ArrayList<>();
    storeList.add(mockStore);

    stats = new AggVersionedDIVStats(metricsRepository, mockMetaRepository, true);
  }

  @Test(timeOut = TEST_TIME)
  public void testStatsCanLoadAllStoresInTime() {
    // try to load 5000 stores
    for (int i = 0; i < 5000; i++) {
      storeList.add(createStore("store" + i));
    }

    Mockito.doReturn(storeList).when(mockMetaRepository).getAllStores();
    stats.loadAllStats();
  }

  @Test(dependsOnMethods = { "testStatsCanLoadAllStoresInTime" })
  public void testStatsCanLoadStores() {
    String storeName = mockStore.getName();

    /**
     * Since this is a brand new store, it does not yet contain any versions, which means we should get
     * {@link NULL_DIV_STATS} on the version-specific stats...
     */

    Assert.assertEquals(
        reporter.query("." + storeName + "_future--success_msg.DIVStatsCounter").value(),
        (double) NULL_DIV_STATS.code);
    Assert.assertEquals(
        reporter.query("." + storeName + "_current--duplicate_msg.DIVStatsCounter").value(),
        (double) NULL_DIV_STATS.code);
    Assert.assertEquals(
        reporter.query("." + storeName + "_future--corrupted_msg.DIVStatsCounter").value(),
        (double) NULL_DIV_STATS.code);
  }

  @Test(dependsOnMethods = { "testStatsCanLoadAllStoresInTime" })
  public void testStatsCanCreateStore() {
    Store newStore = createStore("store2");
    stats.handleStoreCreated(newStore);

    String storeName = newStore.getName();

    Assert.assertEquals(reporter.query("." + storeName + "_total--corrupted_msg.DIVStatsCounter").value(), 0d);
    Assert.assertEquals(reporter.query("." + storeName + "_total--success_msg.DIVStatsCounter").value(), 0d);
  }

  @Test(dependsOnMethods = { "testStatsCanLoadAllStoresInTime" })
  public void testStatsCanUpdateVersionStatus() {
    String storeName = mockStore.getName();
    Assert.assertEquals(reporter.query("." + storeName + "--future_version.VersionStat").value(), 0d);

    // v1 starts pushing
    Version version = new VersionImpl(storeName, 1);
    mockStore.addVersion(version);
    stats.handleStoreChanged(mockStore);

    // expect to see v1's stats on future reporter
    Assert.assertEquals(reporter.query("." + storeName + "--future_version.VersionStat").value(), 1d);

    long consumerTimestampMs = System.currentTimeMillis();
    double v1ProducerBrokerLatencyMs = 801d;
    double v1BrokerConsumerLatencyMs = 201d;
    double v1ProducerConsumerLatencyMs = 1001d;
    stats.recordLatencies(
        storeName,
        1,
        consumerTimestampMs,
        v1ProducerBrokerLatencyMs,
        v1BrokerConsumerLatencyMs,
        v1ProducerConsumerLatencyMs);
    Assert.assertEquals(
        reporter.query("." + storeName + "_future--producer_to_consumer_latency_avg_ms.DIVStatsCounter").value(),
        v1ProducerConsumerLatencyMs);
    Assert.assertEquals(
        reporter.query("." + storeName + "_future--producer_to_consumer_latency_max_ms.DIVStatsCounter").value(),
        v1ProducerConsumerLatencyMs);

    double v1ProducerToSourceBrokerLatencyMs = 811d;
    double v1SourceBrokerToLeaderConsumerLatencyMs = 211d;
    double v1ProducerToLeaderConsumerLatencyMs = 1011d;
    stats.recordLeaderLatencies(
        storeName,
        1,
        consumerTimestampMs,
        v1ProducerToSourceBrokerLatencyMs,
        v1SourceBrokerToLeaderConsumerLatencyMs,
        v1ProducerToLeaderConsumerLatencyMs);
    Assert.assertEquals(
        reporter.query("." + storeName + "_future--producer_to_leader_consumer_latency_avg_ms.DIVStatsCounter").value(),
        v1ProducerToLeaderConsumerLatencyMs);
    Assert.assertEquals(
        reporter.query("." + storeName + "_future--producer_to_leader_consumer_latency_max_ms.DIVStatsCounter").value(),
        v1ProducerToLeaderConsumerLatencyMs);

    double v1ProducerToLocalBrokerLatencyMs = 821d;
    double v1LocalBrokerToFollowerConsumerLatencyMs = 221d;
    double v1ProducerToFollowerConsumerLatencyMs = 1021d;
    stats.recordFollowerLatencies(
        storeName,
        1,
        consumerTimestampMs,
        v1ProducerToLocalBrokerLatencyMs,
        v1LocalBrokerToFollowerConsumerLatencyMs,
        v1ProducerToFollowerConsumerLatencyMs);
    Assert.assertEquals(
        reporter.query("." + storeName + "_future--producer_to_follower_consumer_latency_avg_ms.DIVStatsCounter")
            .value(),
        v1ProducerToFollowerConsumerLatencyMs);
    Assert.assertEquals(
        reporter.query("." + storeName + "_future--producer_to_follower_consumer_latency_max_ms.DIVStatsCounter")
            .value(),
        v1ProducerToFollowerConsumerLatencyMs);

    // v1 becomes the current version and v2 starts pushing
    version.setStatus(VersionStatus.ONLINE);
    mockStore.setCurrentVersionWithoutCheck(1);
    Version version2 = new VersionImpl(storeName, 2);
    mockStore.addVersion(version2);

    stats.recordDuplicateMsg(storeName, 2);
    double v2ProducerBrokerLatencyMs = 802d;
    double v2BrokerConsumerLatencyMs = 202d;
    double v2ProducerConsumerLatencyMs = 1002d;
    stats.recordLatencies(
        storeName,
        2,
        consumerTimestampMs,
        v2ProducerBrokerLatencyMs,
        v2BrokerConsumerLatencyMs,
        v2ProducerConsumerLatencyMs);
    stats.handleStoreChanged(mockStore);

    // expect to see v1's stats on current reporter and v2's stats on future reporter
    Assert.assertEquals(reporter.query("." + storeName + "--future_version.VersionStat").value(), 2d);
    Assert.assertEquals(reporter.query("." + storeName + "--current_version.VersionStat").value(), 1d);
    Assert.assertEquals(reporter.query("." + storeName + "_future--duplicate_msg.DIVStatsCounter").value(), 1d);
    Assert.assertEquals(
        reporter.query("." + storeName + "_current--producer_to_broker_latency_avg_ms.DIVStatsCounter").value(),
        v1ProducerBrokerLatencyMs);
    Assert.assertEquals(
        reporter.query("." + storeName + "_current--producer_to_broker_latency_max_ms.DIVStatsCounter").value(),
        v1ProducerBrokerLatencyMs);
    Assert.assertEquals(
        reporter.query("." + storeName + "_future--broker_to_consumer_latency_avg_ms.DIVStatsCounter").value(),
        v2BrokerConsumerLatencyMs);
    Assert.assertEquals(
        reporter.query("." + storeName + "_future--broker_to_consumer_latency_max_ms.DIVStatsCounter").value(),
        v2BrokerConsumerLatencyMs);

    double v2ProducerToSourceBrokerLatencyMs = 812d;
    double v2SourceBrokerToLeaderConsumerLatencyMs = 212d;
    double v2ProducerToLeaderConsumerLatencyMs = 1012d;
    stats.recordLeaderLatencies(
        storeName,
        2,
        consumerTimestampMs,
        v2ProducerToSourceBrokerLatencyMs,
        v2SourceBrokerToLeaderConsumerLatencyMs,
        v2ProducerToLeaderConsumerLatencyMs);

    Assert.assertEquals(
        reporter.query("." + storeName + "_current--producer_to_source_broker_latency_avg_ms.DIVStatsCounter").value(),
        v1ProducerToSourceBrokerLatencyMs);
    Assert.assertEquals(
        reporter.query("." + storeName + "_current--producer_to_source_broker_latency_max_ms.DIVStatsCounter").value(),
        v1ProducerToSourceBrokerLatencyMs);
    Assert.assertEquals(
        reporter.query("." + storeName + "_future--source_broker_to_leader_consumer_latency_avg_ms.DIVStatsCounter")
            .value(),
        v2SourceBrokerToLeaderConsumerLatencyMs);
    Assert.assertEquals(
        reporter.query("." + storeName + "_future--source_broker_to_leader_consumer_latency_max_ms.DIVStatsCounter")
            .value(),
        v2SourceBrokerToLeaderConsumerLatencyMs);

    double v2ProducerToLocalBrokerLatencyMs = 822d;
    double v2LocalBrokerToFollowerConsumerLatencyMs = 222d;
    double v2ProducerToFollowerConsumerLatencyMs = 1022d;
    stats.recordFollowerLatencies(
        storeName,
        2,
        consumerTimestampMs,
        v2ProducerToLocalBrokerLatencyMs,
        v2LocalBrokerToFollowerConsumerLatencyMs,
        v2ProducerToFollowerConsumerLatencyMs);

    Assert.assertEquals(
        reporter.query("." + storeName + "_current--producer_to_local_broker_latency_avg_ms.DIVStatsCounter").value(),
        v1ProducerToLocalBrokerLatencyMs);
    Assert.assertEquals(
        reporter.query("." + storeName + "_current--producer_to_local_broker_latency_max_ms.DIVStatsCounter").value(),
        v1ProducerToLocalBrokerLatencyMs);
    Assert.assertEquals(
        reporter.query("." + storeName + "_future--local_broker_to_follower_consumer_latency_avg_ms.DIVStatsCounter")
            .value(),
        v2LocalBrokerToFollowerConsumerLatencyMs);
    Assert.assertEquals(
        reporter.query("." + storeName + "_future--local_broker_to_follower_consumer_latency_max_ms.DIVStatsCounter")
            .value(),
        v2LocalBrokerToFollowerConsumerLatencyMs);

    // v2 finishes pushing
    version2.setStatus(VersionStatus.ONLINE);
    stats.handleStoreChanged(mockStore);
    // since turning Version status to be online and becoming current version are two separated operations, expect to
    // see
    // v2's stats on backup reporter
    Assert.assertEquals(reporter.query("." + storeName + "_future--duplicate_msg.DIVStatsCounter").value(), 0d);

    // v2 becomes the current version
    mockStore.setCurrentVersionWithoutCheck(2);
    stats.handleStoreChanged(mockStore);

    // expect to see v2's stats on current reporter and v1's stats on backup reporter
    Assert.assertEquals(reporter.query("." + storeName + "_current--duplicate_msg.DIVStatsCounter").value(), 1d);
    Assert.assertEquals(
        reporter.query("." + storeName + "_current--broker_to_consumer_latency_avg_ms.DIVStatsCounter").value(),
        v2BrokerConsumerLatencyMs);
    Assert.assertEquals(
        reporter.query("." + storeName + "_current--broker_to_consumer_latency_max_ms.DIVStatsCounter").value(),
        v2BrokerConsumerLatencyMs);

    // v3 finishes pushing and the status becomes to be online
    Version version3 = new VersionImpl(storeName, 3);
    version3.setStatus(VersionStatus.ONLINE);
    mockStore.addVersion(version3);
    mockStore.deleteVersion(1);
    stats.handleStoreChanged(mockStore);
    stats.recordMissingMsg(storeName, 3);
  }

  @Test(dependsOnMethods = { "testStatsCanLoadAllStoresInTime" })
  public void testStatsCanDeleteStore() {
    String storeName = "store0";
    Assert.assertNotNull(metricsRepository.getMetric("." + storeName + "_future--success_msg.DIVStatsCounter"));
    stats.handleStoreDeleted(storeName);
    Assert.assertNull(metricsRepository.getMetric("." + storeName + "_future--success_msg.DIVStatsCounter"));
  }

  private Store createStore(String nameStore) {
    return new ZKStore(
        nameStore,
        "",
        10,
        PersistenceType.ROCKS_DB,
        RoutingStrategy.CONSISTENT_HASH,
        ReadStrategy.ANY_OF_ONLINE,
        OfflinePushStrategy.WAIT_ALL_REPLICAS,
        1);
  }
}
