package com.linkedin.venice.stats;

import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.tehuti.MockTehutiReporter;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.stats.Percentiles;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;


public class AggServerHttpRequestStatsTest {
  private MetricsRepository metricsRepository;
  private MockTehutiReporter reporter;
  private MetricsRepository metricsRepositoryForKVProfiling;
  private MockTehutiReporter reporterForKVProfiling;
  private AggServerHttpRequestStats singleGetStats;
  private AggServerHttpRequestStats singleGetStatsWithKVProfiling;
  private AggServerHttpRequestStats batchGetStats;

  private static final String STORE_FOO = "store_foo";
  private static final String STORE_BAR = "store_bar";
  private static final String STORE_WITH_SMALL_VALUES = "store_with_small_values";
  private static final String STORE_WITH_LARGE_VALUES = "store_with_large_values";

  @BeforeTest
  public void setUp() {
    this.metricsRepository = new MetricsRepository();
    this.reporter = new MockTehutiReporter();
    this.metricsRepository.addReporter(reporter);

    this.metricsRepositoryForKVProfiling = new MetricsRepository();
    this.reporterForKVProfiling = new MockTehutiReporter();
    this.metricsRepositoryForKVProfiling.addReporter(reporterForKVProfiling);

    Assert.assertEquals(metricsRepository.metrics().size(), 0);
    Assert.assertEquals(metricsRepositoryForKVProfiling.metrics().size(), 0);

    this.singleGetStats = new AggServerHttpRequestStats(
        "test_cluster",
        metricsRepository,
        RequestType.SINGLE_GET,
        false,
        Mockito.mock(ReadOnlyStoreRepository.class),
        true,
        false);
    this.batchGetStats = new AggServerHttpRequestStats(
        "test_cluster",
        metricsRepository,
        RequestType.MULTI_GET,
        false,
        Mockito.mock(ReadOnlyStoreRepository.class),
        true,
        false);

    this.singleGetStatsWithKVProfiling = new AggServerHttpRequestStats(
        "test_cluster",
        metricsRepositoryForKVProfiling,
        RequestType.SINGLE_GET,
        true,
        Mockito.mock(ReadOnlyStoreRepository.class),
        true,
        false);
  }

  @AfterTest
  public void cleanUp() {
    metricsRepository.close();
    metricsRepositoryForKVProfiling.close();
    reporter.close();
    reporterForKVProfiling.close();
  }

  @Test
  public void testMetrics() {
    ServerHttpRequestStats singleGetServerStatsFoo = singleGetStats.getStoreStats(STORE_FOO);
    ServerHttpRequestStats singleGetServerStatsBar = singleGetStats.getStoreStats(STORE_BAR);
    ServerHttpRequestStats singleGetServerStatsWithKvProfilingFoo =
        singleGetStatsWithKVProfiling.getStoreStats(STORE_FOO);

    singleGetServerStatsFoo.recordSuccessRequest();
    singleGetServerStatsFoo.recordSuccessRequest();
    singleGetServerStatsFoo.recordErrorRequest();
    singleGetServerStatsBar.recordErrorRequest();

    singleGetServerStatsFoo.recordKeySizeInByte(100);
    singleGetServerStatsFoo.recordValueSizeInByte(1000);

    singleGetServerStatsWithKvProfilingFoo.recordKeySizeInByte(100);
    singleGetServerStatsWithKvProfilingFoo.recordValueSizeInByte(1000);

    Assert.assertTrue(
        reporter.query("." + STORE_FOO + "--success_request.OccurrenceRate").value() > 0,
        "success_request rate should be positive");
    Assert.assertTrue(
        reporter.query(".total--error_request.OccurrenceRate").value() > 0,
        "error_request rate should be positive");
    Assert.assertTrue(
        reporter.query(".total--success_request_ratio.RatioStat").value() > 0,
        "success_request_ratio should be positive");
    Assert.assertTrue(
        reporter.query(".store_foo--request_key_size.Avg").value() > 0,
        "Avg value for request key size should always be recorded");
    Assert.assertTrue(
        reporter.query(".store_foo--request_key_size.Max").value() > 0,
        "Max value for request key size should always be recorded");

    Assert.assertTrue(
        reporterForKVProfiling.query(".store_foo--request_key_size.Avg").value() > 0,
        "Avg value for request key size should always be recorded");
    Assert.assertTrue(
        reporterForKVProfiling.query(".store_foo--request_key_size.Max").value() > 0,
        "Max value for request key size should always be recorded");

    String[] fineGrainedPercentiles = new String[] { "0_01", "0_01", "0_1", "1", "2", "3", "4", "5", "10", "20", "30",
        "40", "50", "60", "70", "80", "90", "95", "99", "99_9" };
    for (String fineGrainedPercentile: fineGrainedPercentiles) {
      Assert.assertNull(
          metricsRepository.getMetric(".store_foo--request_key_size." + fineGrainedPercentile + "thPercentile"));
      Assert.assertNull(
          metricsRepository.getMetric(".store_foo--request_value_size." + fineGrainedPercentile + "thPercentile"));

      Assert.assertTrue(
          reporterForKVProfiling.query(".store_foo--request_key_size." + fineGrainedPercentile + "thPercentile")
              .value() > 0);
      Assert.assertTrue(
          reporterForKVProfiling.query(".store_foo--request_value_size." + fineGrainedPercentile + "thPercentile")
              .value() > 0);
    }

    singleGetStats.handleStoreDeleted(STORE_FOO);
    Assert.assertNull(metricsRepository.getMetric("." + STORE_FOO + "--success_request.OccurrenceRate"));
  }

  @Test
  public void testPercentileNamePattern() {
    String sensorName = "sensorName";
    String storeName = "storeName";
    Percentiles percentiles = TehutiUtils.getPercentileStatForNetworkLatency(sensorName, storeName);
    percentiles.stats().stream().map(namedMeasurable -> namedMeasurable.name()).forEach(System.out::println);
    String[] percentileStrings = new String[] { "50", "95", "99", "99_9" };

    for (int i = 0; i < percentileStrings.length; i++) {
      String expectedName = sensorName + "--" + storeName + "." + percentileStrings[i] + "thPercentile";
      Assert.assertTrue(
          percentiles.stats()
              .stream()
              .map(namedMeasurable -> namedMeasurable.name())
              .anyMatch(s -> s.equals(expectedName)),
          "The Percentiles don't contain the expected name! Missing percentile with name: " + expectedName);
    }
  }

  /**
   * Store FOO is a small value store, while store BAR is a large value store.
   */
  @Test
  public void testLargeValueMetrics() {
    ServerHttpRequestStats smallValueStats = singleGetStats.getStoreStats(STORE_WITH_SMALL_VALUES);
    ServerHttpRequestStats largeValueStats = singleGetStats.getStoreStats(STORE_WITH_LARGE_VALUES);
    ServerHttpRequestStats batchGetSmallStats = batchGetStats.getStoreStats(STORE_WITH_SMALL_VALUES);
    ServerHttpRequestStats batchGetLargeStats = batchGetStats.getStoreStats(STORE_WITH_LARGE_VALUES);

    smallValueStats.recordSuccessRequest();
    smallValueStats.recordMultiChunkLargeValueCount(0);
    largeValueStats.recordSuccessRequest();
    largeValueStats.recordMultiChunkLargeValueCount(1);

    // Sanity check
    Assert.assertTrue(
        reporter.query("." + STORE_WITH_SMALL_VALUES + "--success_request.OccurrenceRate").value() > 0,
        "success_request rate should be positive");
    Assert.assertTrue(
        reporter.query("." + STORE_WITH_LARGE_VALUES + "--success_request.OccurrenceRate").value() > 0,
        "success_request rate should be positive");

    // Main test
    Assert.assertEquals(
        (int) reporter.query("." + STORE_WITH_SMALL_VALUES + "--storage_engine_large_value_lookup.OccurrenceRate")
            .value(),
        0,
        "storage_engine_large_value_lookup rate should be zero");
    Assert.assertEquals(
        (int) reporter.query("." + STORE_WITH_SMALL_VALUES + "--storage_engine_large_value_lookup.Max").value(),
        0,
        "storage_engine_large_value_lookup rate should be zero");
    Assert.assertTrue(
        reporter.query("." + STORE_WITH_LARGE_VALUES + "--storage_engine_large_value_lookup.OccurrenceRate")
            .value() > 0,
        "storage_engine_large_value_lookup rate should be positive");
    Assert.assertEquals(
        (int) reporter.query("." + STORE_WITH_LARGE_VALUES + "--storage_engine_large_value_lookup.Max").value(),
        1,
        "storage_engine_large_value_lookup rate should be positive");

    batchGetSmallStats.recordSuccessRequest();
    batchGetSmallStats.recordMultiChunkLargeValueCount(0);
    batchGetLargeStats.recordSuccessRequest();
    batchGetLargeStats.recordMultiChunkLargeValueCount(5);
    batchGetLargeStats.recordSuccessRequest();
    batchGetLargeStats.recordMultiChunkLargeValueCount(15);

    // Sanity check
    Assert.assertTrue(
        reporter.query("." + STORE_WITH_SMALL_VALUES + "--multiget_success_request.OccurrenceRate").value() > 0,
        "success_request rate should be positive");
    Assert.assertTrue(
        reporter.query("." + STORE_WITH_LARGE_VALUES + "--multiget_success_request.OccurrenceRate").value() > 0,
        "success_request rate should be positive");
    // Main test
    Assert.assertTrue(
        (int) reporter
            .query("." + STORE_WITH_SMALL_VALUES + "--multiget_storage_engine_large_value_lookup.OccurrenceRate")
            .value() == 0,
        "storage_engine_large_value_lookup rate should be zero");
    Assert.assertEquals(
        (int) reporter.query("." + STORE_WITH_SMALL_VALUES + "--multiget_storage_engine_large_value_lookup.Max")
            .value(),
        0,
        "storage_engine_large_value_lookup rate should be zero");
    Assert.assertTrue(
        reporter.query("." + STORE_WITH_LARGE_VALUES + "--multiget_storage_engine_large_value_lookup.OccurrenceRate")
            .value() > 0,
        "storage_engine_large_value_lookup rate should be positive");
    Assert.assertTrue(
        reporter.query("." + STORE_WITH_LARGE_VALUES + "--multiget_storage_engine_large_value_lookup.Rate").value() > 0,
        "storage_engine_large_value_lookup rate should be positive");
    Assert.assertEquals(
        (int) reporter.query("." + STORE_WITH_LARGE_VALUES + "--multiget_storage_engine_large_value_lookup.Max")
            .value(),
        15,
        "storage_engine_large_value_lookup rate should be positive");
    Assert.assertEquals(
        (int) reporter.query("." + STORE_WITH_LARGE_VALUES + "--multiget_storage_engine_large_value_lookup.Avg")
            .value(),
        10,
        "storage_engine_large_value_lookup rate should be positive");
  }
}
