package com.linkedin.venice.stats;

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
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import io.tehuti.metrics.MetricsRepository;
import java.util.ArrayList;
import java.util.List;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;
import static com.linkedin.venice.stats.StatsErrorCode.NULL_DIV_STATS;

public class AggVersionedDIVStatsTest {
  private static final int TEST_TIME = 10 *Time.MS_PER_SECOND;

  private AggVersionedDIVStats stats;
  protected MetricsRepository metricsRepository;
  protected MockTehutiReporter reporter;

  private ReadOnlyStoreRepository mockMetaRepository;
  private Store mockStore;
  List<Store> storeList;

  @BeforeTest
  public void setup() {
    metricsRepository = new MetricsRepository();
    this.reporter = new MockTehutiReporter();
    metricsRepository.addReporter(reporter);

    mockMetaRepository = mock(ReadOnlyStoreRepository.class);

    mockStore = createStore(TestUtils.getUniqueString("store"));
    storeList = new ArrayList<>();
    storeList.add(mockStore);

    stats = new AggVersionedDIVStats(metricsRepository, mockMetaRepository);
  }

  @Test(timeOut = TEST_TIME)
  public void  testStatsCanLoadAllStoresInTime() {
    //try to load 5000 stores
    for (int i = 0; i < 5000; i ++) {
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

    Assert.assertEquals(reporter.query("." + storeName + "_future--success_msg.DIVStatsCounter").value(), (double) NULL_DIV_STATS.code);
    Assert.assertEquals(reporter.query("." + storeName + "_current--duplicate_msg.DIVStatsCounter").value(), (double) NULL_DIV_STATS.code);
    Assert.assertEquals(reporter.query("." + storeName + "_backup--missing_msg.DIVStatsCounter").value(), (double) NULL_DIV_STATS.code);
    Assert.assertEquals(reporter.query("." + storeName + "_future--corrupted_msg.DIVStatsCounter").value(), (double) NULL_DIV_STATS.code);
  }

  @Test(dependsOnMethods = { "testStatsCanLoadAllStoresInTime" })
  public void testStatsCanAddStore() {
    Store newStore = createStore("store2");
    stats.handleStoreCreated(newStore);

    String storeName = newStore.getName();

    /**
     * Since this is a brand new store, it does not yet contain any versions, which means we should get
     * {@link NULL_DIV_STATS} on the version-specific stats...
     */

    Assert.assertEquals(reporter.query("." + storeName + "_current--current_idle_time.DIVStatsCounter").value(), (double) NULL_DIV_STATS.code);
    Assert.assertEquals(reporter.query("." + storeName + "_backup--overall_idle_time.DIVStatsCounter").value(), (double) NULL_DIV_STATS.code);
    Assert.assertEquals(reporter.query("." + storeName + "_total--corrupted_msg.DIVStatsCounter").value(), 0d);
    Assert.assertEquals(reporter.query("." + storeName + "_total--success_msg.DIVStatsCounter").value(), 0d);
  }

  @Test(dependsOnMethods = { "testStatsCanLoadAllStoresInTime" })
  public void testStatsCanUpdateVersionStatus() {
    String storeName = mockStore.getName();
    Assert.assertEquals(reporter.query("." + storeName + "--future_version.VersionStat").value(), 0d);

    //v1 starts pushing
    Version version = new VersionImpl(storeName, 1);
    mockStore.addVersion(version);
    stats.handleStoreChanged(mockStore);

    //expect to see v1's stats on future reporter
    Assert.assertEquals(reporter.query("." + storeName + "--future_version.VersionStat").value(), 1d);

    stats.recordCurrentIdleTime(storeName, 1);
    Assert.assertEquals(reporter.query("." + storeName + "_future--current_idle_time.DIVStatsCounter").value(), 1d);
    stats.recordProducerConsumerLatencyMs(storeName, 1, 1000d);
    Assert.assertEquals(reporter.query("." + storeName
        + "_future--producer_to_consumer_latency_avg_ms.DIVStatsCounter").value(), 1000d);
    Assert.assertEquals(reporter.query("." + storeName
        + "_future--producer_to_consumer_latency_min_ms.DIVStatsCounter").value(), 1000d);
    Assert.assertEquals(reporter.query("." + storeName
        + "_future--producer_to_consumer_latency_max_ms.DIVStatsCounter").value(), 1000d);
    stats.recordProducerLeaderConsumerLatencyMs(storeName, 1, 1000d);
    Assert.assertEquals(reporter.query("." + storeName
        + "_future--producer_to_leader_consumer_latency_avg_ms.DIVStatsCounter").value(), 1000d);
    Assert.assertEquals(reporter.query("." + storeName
        + "_future--producer_to_leader_consumer_latency_min_ms.DIVStatsCounter").value(), 1000d);
    Assert.assertEquals(reporter.query("." + storeName
        + "_future--producer_to_leader_consumer_latency_max_ms.DIVStatsCounter").value(), 1000d);
    stats.recordProducerFollowerConsumerLatencyMs(storeName, 1, 1000d);
    Assert.assertEquals(reporter.query("." + storeName
        + "_future--producer_to_follower_consumer_latency_avg_ms.DIVStatsCounter").value(), 1000d);
    Assert.assertEquals(reporter.query("." + storeName
        + "_future--producer_to_follower_consumer_latency_min_ms.DIVStatsCounter").value(), 1000d);
    Assert.assertEquals(reporter.query("." + storeName
        + "_future--producer_to_follower_consumer_latency_max_ms.DIVStatsCounter").value(), 1000d);

    //v1 becomes the current version and v2 starts pushing
    version.setStatus(VersionStatus.ONLINE);
    mockStore.setCurrentVersionWithoutCheck(1);
    Version version2 = new VersionImpl(storeName, 2);
    mockStore.addVersion(version2);


    stats.recordCurrentIdleTime(storeName, 1);
    stats.recordProducerBrokerLatencyMs(storeName, 1, 800d);
    stats.recordDuplicateMsg(storeName, 2);
    stats.recordBrokerConsumerLatencyMs(storeName, 2, 200d);
    stats.handleStoreChanged(mockStore);

    //expect to see v1's stats on current reporter and v2's stats on future reporter
    Assert.assertEquals(reporter.query("." + storeName + "--future_version.VersionStat").value(), 2d);
    Assert.assertEquals(reporter.query("." + storeName + "--current_version.VersionStat").value(), 1d);
    Assert.assertEquals(reporter.query("." + storeName + "_future--duplicate_msg.DIVStatsCounter").value(), 1d);
    Assert.assertEquals(reporter.query("." + storeName + "_current--current_idle_time.DIVStatsCounter").value(), 2d);
    Assert.assertEquals(reporter.query("." + storeName
        + "_current--producer_to_broker_latency_avg_ms.DIVStatsCounter").value(), 800d);
    Assert.assertEquals(reporter.query("." + storeName
        + "_current--producer_to_broker_latency_min_ms.DIVStatsCounter").value(), 800d);
    Assert.assertEquals(reporter.query("." + storeName
        + "_current--producer_to_broker_latency_max_ms.DIVStatsCounter").value(), 800d);
    Assert.assertEquals(reporter.query("." + storeName
        + "_future--broker_to_consumer_latency_avg_ms.DIVStatsCounter").value(), 200d);
    Assert.assertEquals(reporter.query("." + storeName
        + "_future--broker_to_consumer_latency_min_ms.DIVStatsCounter").value(), 200d);
    Assert.assertEquals(reporter.query("." + storeName
        + "_future--broker_to_consumer_latency_max_ms.DIVStatsCounter").value(), 200d);

    stats.recordProducerSourceBrokerLatencyMs(storeName, 1, 800d);
    stats.recordSourceBrokerLeaderConsumerLatencyMs(storeName, 2, 200d);
    Assert.assertEquals(reporter.query("." + storeName
        + "_current--producer_to_source_broker_latency_avg_ms.DIVStatsCounter").value(), 800d);
    Assert.assertEquals(reporter.query("." + storeName
        + "_current--producer_to_source_broker_latency_min_ms.DIVStatsCounter").value(), 800d);
    Assert.assertEquals(reporter.query("." + storeName
        + "_current--producer_to_source_broker_latency_max_ms.DIVStatsCounter").value(), 800d);
    Assert.assertEquals(reporter.query("." + storeName
        + "_future--source_broker_to_leader_consumer_latency_avg_ms.DIVStatsCounter").value(), 200d);
    Assert.assertEquals(reporter.query("." + storeName
        + "_future--source_broker_to_leader_consumer_latency_min_ms.DIVStatsCounter").value(), 200d);
    Assert.assertEquals(reporter.query("." + storeName
        + "_future--source_broker_to_leader_consumer_latency_max_ms.DIVStatsCounter").value(), 200d);

    stats.recordProducerLocalBrokerLatencyMs(storeName, 1, 800d);
    stats.recordLocalBrokerFollowerConsumerLatencyMs(storeName, 2, 200d);
    Assert.assertEquals(reporter.query("." + storeName
        + "_current--producer_to_local_broker_latency_avg_ms.DIVStatsCounter").value(), 800d);
    Assert.assertEquals(reporter.query("." + storeName
        + "_current--producer_to_local_broker_latency_min_ms.DIVStatsCounter").value(), 800d);
    Assert.assertEquals(reporter.query("." + storeName
        + "_current--producer_to_local_broker_latency_max_ms.DIVStatsCounter").value(), 800d);
    Assert.assertEquals(reporter.query("." + storeName
        + "_future--local_broker_to_follower_consumer_latency_avg_ms.DIVStatsCounter").value(), 200d);
    Assert.assertEquals(reporter.query("." + storeName
        + "_future--local_broker_to_follower_consumer_latency_min_ms.DIVStatsCounter").value(), 200d);
    Assert.assertEquals(reporter.query("." + storeName
        + "_future--local_broker_to_follower_consumer_latency_max_ms.DIVStatsCounter").value(), 200d);

    //v2 finishes pushing
    version2.setStatus(VersionStatus.ONLINE);
    stats.handleStoreChanged(mockStore);
    //since turning Version status to be online and becoming current version are two separated operations, expect to see
    //v2's stats on backup reporter
    Assert.assertEquals(reporter.query("." + storeName + "_current--current_idle_time.DIVStatsCounter").value(), 2d);
    Assert.assertEquals(reporter.query("." + storeName + "_future--duplicate_msg.DIVStatsCounter").value(), 0d);
    Assert.assertEquals(reporter.query("." + storeName + "_backup--duplicate_msg.DIVStatsCounter").value(), 1d);

    //v2 becomes the current version
    mockStore.setCurrentVersionWithoutCheck(2);
    stats.handleStoreChanged(mockStore);

    //expect to see v2's stats on current reporter and v1's stats on backup reporter
    Assert.assertEquals(reporter.query("." + storeName + "_current--duplicate_msg.DIVStatsCounter").value(), 1d);
    Assert.assertEquals(reporter.query("." + storeName + "_backup--current_idle_time.DIVStatsCounter").value(), 2d);
    Assert.assertEquals(reporter.query("." + storeName
        + "_current--broker_to_consumer_latency_avg_ms.DIVStatsCounter").value(), 200d);
    Assert.assertEquals(reporter.query("." + storeName
        + "_current--broker_to_consumer_latency_min_ms.DIVStatsCounter").value(), 200d);
    Assert.assertEquals(reporter.query("." + storeName
        + "_current--broker_to_consumer_latency_max_ms.DIVStatsCounter").value(), 200d);
    Assert.assertEquals(reporter.query("." + storeName
        + "_backup--producer_to_broker_latency_avg_ms.DIVStatsCounter").value(), 800d);
    Assert.assertEquals(reporter.query("." + storeName
        + "_backup--producer_to_broker_latency_min_ms.DIVStatsCounter").value(), 800d);
    Assert.assertEquals(reporter.query("." + storeName
        + "_backup--producer_to_broker_latency_max_ms.DIVStatsCounter").value(), 800d);


    //v3 finishes pushing and the status becomes to be online
    Version version3 = new VersionImpl(storeName, 3);
    version3.setStatus(VersionStatus.ONLINE);
    mockStore.addVersion(version3);
    mockStore.deleteVersion(1);
    stats.handleStoreChanged(mockStore);
    stats.recordMissingMsg(storeName, 3);

    //expect to see v1 stats being removed from reporters
    Assert.assertEquals(reporter.query("." + storeName + "_backup--missing_msg.DIVStatsCounter").value(), 1d);
  }


  private Store createStore(String nameStore) {
    return new ZKStore(nameStore, "", 10, PersistenceType.ROCKS_DB,
        RoutingStrategy.CONSISTENT_HASH, ReadStrategy.ANY_OF_ONLINE, OfflinePushStrategy.WAIT_ALL_REPLICAS, 1);
  }
}
