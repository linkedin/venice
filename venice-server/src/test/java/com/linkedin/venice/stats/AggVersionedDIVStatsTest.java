package com.linkedin.venice.stats;

import com.linkedin.venice.meta.OfflinePushStrategy;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.ReadStrategy;
import com.linkedin.venice.meta.RoutingStrategy;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.tehuti.MockTehutiReporter;
import com.linkedin.venice.utils.TestUtils;
import io.tehuti.metrics.MetricsRepository;
import java.util.Arrays;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;


public class AggVersionedDIVStatsTest {
  private AggVersionedDIVStats stats;
  protected MetricsRepository metricsRepository;
  protected MockTehutiReporter reporter;

  private ReadOnlyStoreRepository mockMetaRepository;
  private Store mockStore;

  @BeforeTest
  public void setup() {
    metricsRepository = new MetricsRepository();
    this.reporter = new MockTehutiReporter();
    metricsRepository.addReporter(reporter);

    mockMetaRepository = mock(ReadOnlyStoreRepository.class);
    mockStore = createStore(TestUtils.getUniqueString("store"));
    Mockito.doReturn(Arrays.asList(mockStore)).when(mockMetaRepository).getAllStores();

    stats = new AggVersionedDIVStats(metricsRepository, mockMetaRepository);
    stats.loadAllStats();
  }

  @Test
  public void testStatsCanLoadStores() {
    String storeName = mockStore.getName();

    Assert.assertEquals(reporter.query("." + storeName + "_future--success_msg.DIVStatsCounter").value(), 0d);
    Assert.assertEquals(reporter.query("." + storeName + "_current--duplicate_msg.DIVStatsCounter").value(), 0d);
    Assert.assertEquals(reporter.query("." + storeName + "_backup--missing_msg.DIVStatsCounter").value(), 0d);
    Assert.assertEquals(reporter.query("." + storeName + "_future--corrupted_msg.DIVStatsCounter").value(), 0d);
  }

  @Test
  public void testStatsCanAddStore() {
    Store newStore = createStore("store2");
    stats.handleStoreCreated(newStore);

    String storeName = newStore.getName();

    Assert.assertEquals(reporter.query("." + storeName + "_current--current_idle_time.DIVStatsCounter").value(), 0d);
    Assert.assertEquals(reporter.query("." + storeName + "_backup--overall_idle_time.DIVStatsCounter").value(), 0d);
    Assert.assertEquals(reporter.query("." + storeName + "_total--corrupted_msg.DIVStatsCounter").value(), 0d);
    Assert.assertEquals(reporter.query("." + storeName + "_total--success_msg.DIVStatsCounter").value(), 0d);
  }

  @Test
  public void testStatsCanUpdateVersionStatus() {
    String storeName = mockStore.getName();
    Assert.assertEquals(reporter.query("." + storeName + "--future_version.VersionStat").value(), 0d);

    //v1 starts pushing
    Version version = new Version(storeName, 1);
    mockStore.addVersion(version);
    stats.handleStoreChanged(mockStore);

    //expect to see v1's stats on future reporter
    Assert.assertEquals(reporter.query("." + storeName + "--future_version.VersionStat").value(), 1d);

    stats.recordCurrentIdleTime(storeName, 1);
    Assert.assertEquals(reporter.query("." + storeName + "_future--current_idle_time.DIVStatsCounter").value(), 1d);

    //v1 becomes the current version and v2 starts pushing
    version.setStatus(VersionStatus.ONLINE);
    mockStore.setCurrentVersionWithoutCheck(1);
    Version version2 = new Version(storeName, 2);
    mockStore.addVersion(version2);


    stats.recordCurrentIdleTime(storeName, 1);
    stats.recordDuplicateMsg(storeName, 2);
    stats.handleStoreChanged(mockStore);

    //expect to see v1's stats on current reporter and v2's stats on future reporter
    Assert.assertEquals(reporter.query("." + storeName + "--future_version.VersionStat").value(), 2d);
    Assert.assertEquals(reporter.query("." + storeName + "--current_version.VersionStat").value(), 1d);
    Assert.assertEquals(reporter.query("." + storeName + "_future--duplicate_msg.DIVStatsCounter").value(), 1d);
    Assert.assertEquals(reporter.query("." + storeName + "_current--current_idle_time.DIVStatsCounter").value(), 2d);

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

    //v3 finishes pushing and the status becomes to be online
    Version version3 = new Version(storeName, 3);
    version3.setStatus(VersionStatus.ONLINE);
    mockStore.addVersion(version3);
    mockStore.deleteVersion(1);
    stats.handleStoreChanged(mockStore);
    stats.recordMissingMsg(storeName, 3);

    //expect to see v1 stats being removed from reporters
    Assert.assertEquals(reporter.query("." + storeName + "_backup--missing_msg.DIVStatsCounter").value(), 1d);
  }


  private Store createStore(String nameStore) {
    return new Store(nameStore, "", 10, PersistenceType.BDB,
        RoutingStrategy.CONSISTENT_HASH, ReadStrategy.ANY_OF_ONLINE, OfflinePushStrategy.WAIT_ALL_REPLICAS);
  }
}
