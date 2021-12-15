package com.linkedin.davinci.stats;

import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.kafka.consumer.StoreIngestionTask;
import com.linkedin.venice.meta.OfflinePushStrategy;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.ReadStrategy;
import com.linkedin.venice.meta.RoutingStrategy;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionImpl;
import com.linkedin.venice.meta.ZKStore;
import com.linkedin.venice.tehuti.MockTehutiReporter;
import com.linkedin.venice.utils.Utils;

import io.tehuti.metrics.MetricsRepository;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;


public class AggVersionedStorageIngestionStatsTest {
  private AggVersionedStorageIngestionStats versionedIngestionStats;
  private MetricsRepository metricsRepository;
  private MockTehutiReporter reporter;

  private ReadOnlyStoreRepository mockStoreRepository;
  private VeniceServerConfig mockServerConfig;

  @BeforeTest
  public void setUp() {
    metricsRepository = new MetricsRepository();
    reporter = new MockTehutiReporter();
    metricsRepository.addReporter(reporter);

    mockStoreRepository = mock(ReadOnlyStoreRepository.class);
    mockServerConfig = mock(VeniceServerConfig.class);
    versionedIngestionStats = new AggVersionedStorageIngestionStats(metricsRepository, mockStoreRepository, mockServerConfig);
  }

  @Test
  public void testWithRegularStoreIngestion() {
    String storeName = Utils.getUniqueString("test-store");
    Store ingestionStore = getMockStore(storeName);
    ingestionStore.addVersion(new VersionImpl(storeName, 1, ""));
    ingestionStore.setCurrentVersion(1);
    doReturn(ingestionStore).when(mockStoreRepository).getStoreOrThrow(storeName);
    StoreIngestionTask mockStoreIngestionTask = mock(StoreIngestionTask.class);
    doReturn(ingestionStore).when(mockStoreIngestionTask).getIngestionStore();
    doReturn(true).when(mockStoreIngestionTask).isHybridMode();
    doReturn(100L).when(mockStoreIngestionTask).getRealTimeBufferOffsetLag();
    doReturn(0L).when(mockStoreIngestionTask).getOffsetLagThreshold();
    versionedIngestionStats.setIngestionTask(Version.composeKafkaTopic(storeName, 1), mockStoreIngestionTask);
    // Expected to see v1's records consumed on future reporter
    Assert.assertEquals(reporter.query("." + storeName + "_current--rt_topic_offset_lag.IngestionStatsGauge").value(),
        100d);
  }

  private Store getMockStore(String storeName) {
    return new ZKStore(storeName, "test", 0, PersistenceType.ROCKS_DB,
        RoutingStrategy.CONSISTENT_HASH, ReadStrategy.ANY_OF_ONLINE, OfflinePushStrategy.WAIT_ALL_REPLICAS, 1);
  }
}
