package com.linkedin.venice.stats;


import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.kafka.consumer.StoreIngestionTask;
import com.linkedin.davinci.stats.AggStoreIngestionStats;
import com.linkedin.venice.tehuti.MockTehutiReporter;
import com.linkedin.venice.utils.TestUtils;
import io.tehuti.metrics.MetricsRepository;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class AggStoreIngestionStatsTest {
  private AggStoreIngestionStats stats;
  private MetricsRepository metricsRepository;
  private MockTehutiReporter reporter;

  private static final String STORE_FOO = TestUtils.getUniqueString("store_foo");
  private static final String STORE_BAR = TestUtils.getUniqueString("store_bar");

  @BeforeTest
  public void setup() {
    metricsRepository = new MetricsRepository();
    this.reporter = new MockTehutiReporter();
    metricsRepository.addReporter(reporter);
    VeniceServerConfig mockVeniceServerConfig = Mockito.mock(VeniceServerConfig.class);
    stats = new AggStoreIngestionStats(metricsRepository, mockVeniceServerConfig);

    StoreIngestionTask task = Mockito.mock(StoreIngestionTask.class);
    Mockito.doReturn(true).when(task).isRunning();

    stats.recordPollResultNum(STORE_FOO, 1);
    stats.recordPollResultNum(STORE_BAR, 2);

    stats.recordStorageQuotaUsed(STORE_FOO, 0.6);
    stats.recordStorageQuotaUsed(STORE_FOO, 1);
    stats.recordTotalBytesReadFromKafkaAsUncompressedSize(100);
    stats.recordTotalBytesReadFromKafkaAsUncompressedSize(200);
    stats.recordDiskQuotaAllowed(STORE_FOO, 100);
    stats.recordDiskQuotaAllowed(STORE_FOO, 200);
  }

  @AfterTest
  public void cleanup() {
    metricsRepository.close();
  }

  @Test
  public void testMetrics() {
    Assert.assertEquals(reporter.query("." + STORE_FOO + "--kafka_poll_result_num.Total").value(), 1d);
    Assert.assertEquals(reporter.query("." + STORE_BAR + "--kafka_poll_result_num.Total").value(), 2d);
    Assert.assertEquals(reporter.query(".total--kafka_poll_result_num.Avg").value(), 1.5d);
    Assert.assertEquals(reporter.query("." + STORE_FOO + "--storage_quota_used.Avg").value(), 0.8);
    Assert.assertEquals(reporter.query(".total--bytes_read_from_kafka_as_uncompressed_size.Total").value(), 300d);
    Assert.assertEquals(reporter.query("." + STORE_FOO + "--global_store_disk_quota_allowed.Max").value(), 200d);
  }
}
