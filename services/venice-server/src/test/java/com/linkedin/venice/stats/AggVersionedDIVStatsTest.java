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
        reporter.query("." + storeName + "_future--success_msg.DIVStatsGauge").value(),
        (double) NULL_DIV_STATS.code);
    Assert.assertEquals(
        reporter.query("." + storeName + "_current--duplicate_msg.DIVStatsGauge").value(),
        (double) NULL_DIV_STATS.code);
    Assert.assertEquals(
        reporter.query("." + storeName + "_future--corrupted_msg.DIVStatsGauge").value(),
        (double) NULL_DIV_STATS.code);
  }

  @Test(dependsOnMethods = { "testStatsCanLoadAllStoresInTime" })
  public void testStatsCanCreateStore() {
    Store newStore = createStore("store2");
    stats.handleStoreCreated(newStore);

    String storeName = newStore.getName();

    Assert.assertEquals(reporter.query("." + storeName + "_total--corrupted_msg.DIVStatsGauge").value(), 0d);
    Assert.assertEquals(reporter.query("." + storeName + "_total--success_msg.DIVStatsGauge").value(), 0d);
  }

  @Test(dependsOnMethods = { "testStatsCanLoadAllStoresInTime" })
  public void testStatsCanDeleteStore() {
    String storeName = "store0";
    Assert.assertNotNull(metricsRepository.getMetric("." + storeName + "_future--success_msg.DIVStatsGauge"));
    stats.handleStoreDeleted(storeName);
    Assert.assertNull(metricsRepository.getMetric("." + storeName + "_future--success_msg.DIVStatsGauge"));
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
