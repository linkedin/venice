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

import static org.apache.hadoop.yarn.webapp.hamlet.HamletSpec.Media.*;

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

    mockMetaRepository = Mockito.mock(ReadOnlyStoreRepository.class);
    mockStore = createStore(TestUtils.getUniqueString("store"));
    Mockito.doReturn(Arrays.asList(mockStore)).when(mockMetaRepository).getAllStores();

    stats = new AggVersionedDIVStats(metricsRepository, mockMetaRepository);
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

    //increase the version with VersionStatus "STARTED"
    Version version = new Version(storeName, 1);
    mockStore.addVersion(version);
    stats.handleStoreChanged(mockStore);

    Assert.assertEquals(reporter.query("." + storeName + "--future_version.VersionStat").value(), 1d);

    stats.recordCurrentIdleTime(storeName, 1);
    Assert.assertEquals(reporter.query("." + storeName + "_future--current_idle_time.DIVStatsCounter").value(), 1d);

    version.setStatus(VersionStatus.ONLINE);
    mockStore.setCurrentVersionWithoutCheck(1);
    Version newVersion = new Version(storeName, 2);
    mockStore.addVersion(newVersion);


    stats.recordCurrentIdleTime(storeName, 1);
    stats.recordDuplicateMsg(storeName, 2);
    stats.handleStoreChanged(mockStore);

    Assert.assertEquals(reporter.query("." + storeName + "--future_version.VersionStat").value(), 2d);
    Assert.assertEquals(reporter.query("." + storeName + "--current_version.VersionStat").value(), 1d);
    Assert.assertEquals(reporter.query("." + storeName + "_future--duplicate_msg.DIVStatsCounter").value(), 1d);
    Assert.assertEquals(reporter.query("." + storeName + "_current--current_idle_time.DIVStatsCounter").value(), 2d);

    newVersion.setStatus(VersionStatus.ONLINE);
    stats.handleStoreChanged(mockStore);
    Assert.assertEquals(reporter.query("." + storeName + "_current--current_idle_time.DIVStatsCounter").value(), 2d);
    Assert.assertEquals(reporter.query("." + storeName + "_future--duplicate_msg.DIVStatsCounter").value(), 0d);
    Assert.assertEquals(reporter.query("." + storeName + "_backup--duplicate_msg.DIVStatsCounter").value(), 1d);

    mockStore.setCurrentVersionWithoutCheck(2);
    stats.handleStoreChanged(mockStore);
    Assert.assertEquals(reporter.query("." + storeName + "_current--duplicate_msg.DIVStatsCounter").value(), 1d);
    Assert.assertEquals(reporter.query("." + storeName + "_backup--current_idle_time.DIVStatsCounter").value(), 2d);
  }


  private Store createStore(String nameStore) {
    return new Store(nameStore, "", 10, PersistenceType.BDB,
        RoutingStrategy.CONSISTENT_HASH, ReadStrategy.ANY_OF_ONLINE, OfflinePushStrategy.WAIT_ALL_REPLICAS);
  }
}
