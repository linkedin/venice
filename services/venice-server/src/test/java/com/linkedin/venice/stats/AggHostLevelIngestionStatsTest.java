package com.linkedin.venice.stats;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.kafka.consumer.StoreIngestionTask;
import com.linkedin.davinci.stats.AggHostLevelIngestionStats;
import com.linkedin.davinci.stats.HostLevelIngestionStats;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.tehuti.MockTehutiReporter;
import com.linkedin.venice.utils.TestMockTime;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import io.tehuti.TehutiException;
import io.tehuti.metrics.MetricsRepository;
import it.unimi.dsi.fastutil.ints.Int2ObjectMaps;
import java.util.Collections;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;


public class AggHostLevelIngestionStatsTest {
  private AggHostLevelIngestionStats aggStats;
  private HostLevelIngestionStats fooStats;
  private HostLevelIngestionStats barStats;
  private MetricsRepository metricsRepository;
  private MockTehutiReporter reporter;

  private static final String STORE_FOO = Utils.getUniqueString("store_foo");
  private static final String STORE_BAR = Utils.getUniqueString("store_bar");

  @BeforeTest
  public void setUp() {
    TestMockTime time = new TestMockTime();
    metricsRepository = new MetricsRepository(time);
    this.reporter = new MockTehutiReporter();
    metricsRepository.addReporter(reporter);
    VeniceServerConfig mockVeniceServerConfig = Mockito.mock(VeniceServerConfig.class);
    doReturn(Int2ObjectMaps.emptyMap()).when(mockVeniceServerConfig).getKafkaClusterIdToAliasMap();
    aggStats = new AggHostLevelIngestionStats(
        metricsRepository,
        mockVeniceServerConfig,
        Collections.emptyMap(),
        mock(ReadOnlyStoreRepository.class),
        true,
        time);
    fooStats = aggStats.getStoreStats(STORE_FOO);
    barStats = aggStats.getStoreStats(STORE_BAR);

    StoreIngestionTask task = Mockito.mock(StoreIngestionTask.class);
    Mockito.doReturn(true).when(task).isRunning();

    fooStats.recordStorageQuotaUsed(0.6, time.getMilliseconds());
    fooStats.recordStorageQuotaUsed(1, time.getMilliseconds());
    fooStats.recordTotalBytesReadFromKafkaAsUncompressedSize(100);
    barStats.recordTotalBytesReadFromKafkaAsUncompressedSize(200);
    fooStats.recordDiskQuotaAllowed(100, time.getMilliseconds());
    fooStats.recordDiskQuotaAllowed(200, time.getMilliseconds());
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
    Assert.assertEquals(reporter.query("." + STORE_FOO + "--storage_quota_used.Gauge").value(), 1.0);
    Assert.assertEquals(
        reporter.query(".total--bytes_read_from_kafka_as_uncompressed_size.Rate").value(),
        300d / LongAdderRateGauge.RATE_GAUGE_CACHE_DURATION_IN_SECONDS);
    Assert.assertEquals(reporter.query("." + STORE_FOO + "--global_store_disk_quota_allowed.Gauge").value(), 200d);

    Assert.assertEquals(
        reporter.query(".total--records_consumed.Rate").value(),
        2d / LongAdderRateGauge.RATE_GAUGE_CACHE_DURATION_IN_SECONDS);
    Assert.assertThrows(TehutiException.class, () -> reporter.query("." + STORE_FOO + "--records_consumed.Rate"));
    Assert.assertThrows(TehutiException.class, () -> reporter.query("." + STORE_BAR + "--records_consumed.Rate"));

    Assert.assertEquals(
        reporter.query(".total--bytes_consumed.Rate").value(),
        40d / LongAdderRateGauge.RATE_GAUGE_CACHE_DURATION_IN_SECONDS);
    Assert.assertThrows(TehutiException.class, () -> reporter.query("." + STORE_FOO + "--bytes_consumed.Rate"));
    Assert.assertThrows(TehutiException.class, () -> reporter.query("." + STORE_BAR + "--bytes_consumed.Rate"));

    aggStats.handleStoreDeleted(STORE_FOO);
    Assert.assertNull(metricsRepository.getMetric("." + STORE_FOO + "--kafka_poll_result_num.Total"));
  }
}
