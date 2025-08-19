package com.linkedin.davinci.stats;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;

import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.kafka.consumer.StoreIngestionTask;
import com.linkedin.davinci.store.StorageEngine;
import com.linkedin.davinci.store.StorageEngineStats;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.stats.LongAdderRateGauge;
import com.linkedin.venice.tehuti.MockTehutiReporter;
import com.linkedin.venice.utils.TestMockTime;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import io.tehuti.TehutiException;
import io.tehuti.metrics.MetricsRepository;
import it.unimi.dsi.fastutil.ints.Int2ObjectMaps;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;


public class AggHostLevelIngestionStatsTest {
  private AggHostLevelIngestionStats aggStats;
  private HostLevelIngestionStats fooStats;
  private HostLevelIngestionStats barStats;
  private Map<String, StoreIngestionTask> sitMap;
  private StoreIngestionTask fooSIT;
  private StoreIngestionTask barSIT;
  private StorageEngine fooSE;
  private StorageEngine barSE;
  private StorageEngineStats fooSET;
  private StorageEngineStats barSET;
  private MetricsRepository metricsRepository;
  private MockTehutiReporter reporter;

  private static final String STORE_FOO = Utils.getUniqueString("store_foo");
  private static final String STORE_BAR = Utils.getUniqueString("store_bar");
  private static final long STORE_FOO_DISK_USAGE = 10;
  private static final long STORE_BAR_DISK_USAGE = 15;
  private static final long STORE_FOO_RMD_DISK_USAGE = 1;
  private static final long STORE_BAR_RMD_DISK_USAGE = 2;

  @BeforeTest
  public void setUp() {
    TestMockTime time = new TestMockTime();
    metricsRepository = new MetricsRepository(time);
    this.reporter = new MockTehutiReporter();
    metricsRepository.addReporter(reporter);
    VeniceServerConfig mockVeniceServerConfig = mock(VeniceServerConfig.class);
    doReturn(Int2ObjectMaps.emptyMap()).when(mockVeniceServerConfig).getKafkaClusterIdToAliasMap();
    fooSIT = mock(StoreIngestionTask.class);
    barSIT = mock(StoreIngestionTask.class);
    fooSE = mock(StorageEngine.class);
    barSE = mock(StorageEngine.class);
    fooSET = mock(StorageEngineStats.class);
    barSET = mock(StorageEngineStats.class);
    doReturn(fooSE).when(fooSIT).getStorageEngine();
    doReturn(barSE).when(barSIT).getStorageEngine();
    doReturn(fooSET).when(fooSE).getStats();
    doReturn(barSET).when(barSE).getStats();
    doReturn(STORE_FOO_DISK_USAGE).when(fooSET).getStoreSizeInBytes();
    doReturn(STORE_FOO_DISK_USAGE).when(fooSET).getCachedStoreSizeInBytes();
    doReturn(STORE_BAR_DISK_USAGE).when(barSET).getStoreSizeInBytes();
    doReturn(STORE_BAR_DISK_USAGE).when(barSET).getCachedStoreSizeInBytes();
    doReturn(STORE_FOO_RMD_DISK_USAGE).when(fooSET).getRMDSizeInBytes();
    doReturn(STORE_FOO_RMD_DISK_USAGE).when(fooSET).getCachedRMDSizeInBytes();
    doReturn(STORE_BAR_RMD_DISK_USAGE).when(barSET).getRMDSizeInBytes();
    doReturn(STORE_BAR_RMD_DISK_USAGE).when(barSET).getCachedRMDSizeInBytes();
    sitMap = new HashMap<>();
    sitMap.put(STORE_FOO, fooSIT);
    sitMap.put(STORE_BAR, barSIT);

    aggStats = new AggHostLevelIngestionStats(
        metricsRepository,
        mockVeniceServerConfig,
        sitMap,
        mock(ReadOnlyStoreRepository.class),
        true,
        time);
    fooStats = aggStats.getStoreStats(STORE_FOO);
    barStats = aggStats.getStoreStats(STORE_BAR);

    fooStats.recordStorageQuotaUsed(0.6);
    fooStats.recordStorageQuotaUsed(1);
    fooStats.recordTotalBytesReadFromKafkaAsUncompressedSize(100);
    barStats.recordTotalBytesReadFromKafkaAsUncompressedSize(200);
    fooStats.recordTotalRecordsConsumed();
    barStats.recordTotalRecordsConsumed();
    fooStats.recordTotalBytesConsumed(10);
    fooStats.recordTotalBytesConsumed(30);
    time.addMilliseconds(LongAdderRateGauge.RATE_GAUGE_CACHE_DURATION_IN_SECONDS * Time.MS_PER_SECOND);
  }

  @AfterTest
  public void cleanUp() {
    metricsRepository.close();
  }

  @Test
  public void testMetrics() {
    // The quota usage metric should show the latest value
    assertEquals(reporter.query("." + STORE_FOO + "--storage_quota_used.Gauge").value(), 1.0);
    assertEquals(
        reporter.query(".total--bytes_read_from_kafka_as_uncompressed_size.Rate").value(),
        300d / LongAdderRateGauge.RATE_GAUGE_CACHE_DURATION_IN_SECONDS);

    assertEquals(
        reporter.query(".total--records_consumed.Rate").value(),
        2d / LongAdderRateGauge.RATE_GAUGE_CACHE_DURATION_IN_SECONDS);
    assertThrows(TehutiException.class, () -> reporter.query("." + STORE_FOO + "--records_consumed.Rate"));
    assertThrows(TehutiException.class, () -> reporter.query("." + STORE_BAR + "--records_consumed.Rate"));

    assertEquals(
        reporter.query(".total--bytes_consumed.Rate").value(),
        40d / LongAdderRateGauge.RATE_GAUGE_CACHE_DURATION_IN_SECONDS);
    assertThrows(TehutiException.class, () -> reporter.query("." + STORE_FOO + "--bytes_consumed.Rate"));
    assertThrows(TehutiException.class, () -> reporter.query("." + STORE_BAR + "--bytes_consumed.Rate"));

    int fooSITgetStorageEngine = 0;
    int barSITgetStorageEngine = 0;
    int fooSEgetStats = 0;
    int barSEgetStats = 0;
    int fooSETgetRMDSizeInBytes = 0;
    int barSETgetRMDSizeInBytes = 0;
    int fooSETgetCachedRMDSizeInBytes = 0;
    int barSETgetCachedRMDSizeInBytes = 0;
    int fooSETgetStoreSizeInBytes = 0;
    int barSETgetStoreSizeInBytes = 0;
    int fooSETgetCachedStoreSizeInBytes = 0;
    int barSETgetCachedStoreSizeInBytes = 0;
    assertCallCounts(
        fooSITgetStorageEngine,
        barSITgetStorageEngine,
        fooSEgetStats,
        barSEgetStats,
        fooSETgetRMDSizeInBytes,
        barSETgetRMDSizeInBytes,
        fooSETgetCachedRMDSizeInBytes,
        barSETgetCachedRMDSizeInBytes,
        fooSETgetStoreSizeInBytes,
        barSETgetStoreSizeInBytes,
        fooSETgetCachedStoreSizeInBytes,
        barSETgetCachedStoreSizeInBytes);

    assertEquals(
        reporter.query("." + STORE_FOO + "--disk_usage_in_bytes.Gauge").value(),
        (double) STORE_FOO_DISK_USAGE);

    assertCallCounts(
        ++fooSITgetStorageEngine,
        barSITgetStorageEngine,
        ++fooSEgetStats,
        barSEgetStats,
        fooSETgetRMDSizeInBytes,
        barSETgetRMDSizeInBytes,
        fooSETgetCachedRMDSizeInBytes,
        barSETgetCachedRMDSizeInBytes,
        ++fooSETgetStoreSizeInBytes,
        barSETgetStoreSizeInBytes,
        fooSETgetCachedStoreSizeInBytes,
        barSETgetCachedStoreSizeInBytes);

    assertEquals(
        reporter.query(".total--disk_usage_in_bytes.Gauge").value(),
        (double) (STORE_FOO_DISK_USAGE + STORE_BAR_DISK_USAGE));

    assertCallCounts(
        ++fooSITgetStorageEngine,
        ++barSITgetStorageEngine,
        ++fooSEgetStats,
        ++barSEgetStats,
        fooSETgetRMDSizeInBytes,
        barSETgetRMDSizeInBytes,
        fooSETgetCachedRMDSizeInBytes,
        barSETgetCachedRMDSizeInBytes,
        fooSETgetStoreSizeInBytes,
        barSETgetStoreSizeInBytes,
        ++fooSETgetCachedStoreSizeInBytes,
        ++barSETgetCachedStoreSizeInBytes);

    assertEquals(
        reporter.query("." + STORE_BAR + "--rmd_disk_usage_in_bytes.Gauge").value(),
        (double) STORE_BAR_RMD_DISK_USAGE);

    assertCallCounts(
        fooSITgetStorageEngine,
        ++barSITgetStorageEngine,
        fooSEgetStats,
        ++barSEgetStats,
        fooSETgetRMDSizeInBytes,
        ++barSETgetRMDSizeInBytes,
        fooSETgetCachedRMDSizeInBytes,
        barSETgetCachedRMDSizeInBytes,
        fooSETgetStoreSizeInBytes,
        barSETgetStoreSizeInBytes,
        fooSETgetCachedStoreSizeInBytes,
        barSETgetCachedStoreSizeInBytes);

    assertEquals(
        reporter.query(".total--rmd_disk_usage_in_bytes.Gauge").value(),
        (double) (STORE_FOO_RMD_DISK_USAGE + STORE_BAR_RMD_DISK_USAGE));

    assertCallCounts(
        ++fooSITgetStorageEngine,
        ++barSITgetStorageEngine,
        ++fooSEgetStats,
        ++barSEgetStats,
        fooSETgetRMDSizeInBytes,
        barSETgetRMDSizeInBytes,
        ++fooSETgetCachedRMDSizeInBytes,
        ++barSETgetCachedRMDSizeInBytes,
        fooSETgetStoreSizeInBytes,
        barSETgetStoreSizeInBytes,
        fooSETgetCachedStoreSizeInBytes,
        barSETgetCachedStoreSizeInBytes);

    assertCallCounts(
        fooSITgetStorageEngine,
        barSITgetStorageEngine,
        fooSEgetStats,
        barSEgetStats,
        fooSETgetRMDSizeInBytes,
        barSETgetRMDSizeInBytes,
        fooSETgetCachedRMDSizeInBytes,
        barSETgetCachedRMDSizeInBytes,
        fooSETgetStoreSizeInBytes,
        barSETgetStoreSizeInBytes,
        fooSETgetCachedStoreSizeInBytes,
        barSETgetCachedStoreSizeInBytes);

    assertCallCounts(
        fooSITgetStorageEngine,
        barSITgetStorageEngine,
        fooSEgetStats,
        barSEgetStats,
        fooSETgetRMDSizeInBytes,
        barSETgetRMDSizeInBytes,
        fooSETgetCachedRMDSizeInBytes,
        barSETgetCachedRMDSizeInBytes,
        fooSETgetStoreSizeInBytes,
        barSETgetStoreSizeInBytes,
        fooSETgetCachedStoreSizeInBytes,
        barSETgetCachedStoreSizeInBytes);

    assertCallCounts(
        fooSITgetStorageEngine,
        barSITgetStorageEngine,
        fooSEgetStats,
        barSEgetStats,
        fooSETgetRMDSizeInBytes,
        barSETgetRMDSizeInBytes,
        fooSETgetCachedRMDSizeInBytes,
        barSETgetCachedRMDSizeInBytes,
        fooSETgetStoreSizeInBytes,
        barSETgetStoreSizeInBytes,
        fooSETgetCachedStoreSizeInBytes,
        barSETgetCachedStoreSizeInBytes);

    aggStats.handleStoreDeleted(STORE_BAR);
    assertNull(metricsRepository.getMetric("." + STORE_BAR + "--kafka_poll_result_num.Total"));
  }

  private void assertCallCounts(
      int fooSITgetStorageEngine,
      int barSITgetStorageEngine,
      int fooSEgetStats,
      int barSEgetStats,
      int fooSETgetRMDSizeInBytes,
      int barSETgetRMDSizeInBytes,
      int fooSETgetCachedRMDSizeInBytes,
      int barSETgetCachedRMDSizeInBytes,
      int fooSETgetStoreSizeInBytes,
      int barSETgetStoreSizeInBytes,
      int fooSETgetCachedStoreSizeInBytes,
      int barSETgetCachedStoreSizeInBytes) {
    verify(fooSIT, times(fooSITgetStorageEngine)).getStorageEngine();
    verify(barSIT, times(barSITgetStorageEngine)).getStorageEngine();
    verify(fooSE, times(fooSEgetStats)).getStats();
    verify(barSE, times(barSEgetStats)).getStats();
    verify(fooSET, times(fooSETgetRMDSizeInBytes)).getRMDSizeInBytes();
    verify(barSET, times(barSETgetRMDSizeInBytes)).getRMDSizeInBytes();
    verify(fooSET, times(fooSETgetCachedRMDSizeInBytes)).getCachedRMDSizeInBytes();
    verify(barSET, times(barSETgetCachedRMDSizeInBytes)).getCachedRMDSizeInBytes();
    verify(fooSET, times(fooSETgetStoreSizeInBytes)).getStoreSizeInBytes();
    verify(barSET, times(barSETgetStoreSizeInBytes)).getStoreSizeInBytes();
    verify(fooSET, times(fooSETgetCachedStoreSizeInBytes)).getCachedStoreSizeInBytes();
    verify(barSET, times(barSETgetCachedStoreSizeInBytes)).getCachedStoreSizeInBytes();
  }
}
