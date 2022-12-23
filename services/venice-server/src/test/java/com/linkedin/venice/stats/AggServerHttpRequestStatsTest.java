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
  protected MetricsRepository metricsRepository;
  protected MockTehutiReporter reporter;
  protected AggServerHttpRequestStats singleGetStats;
  protected AggServerHttpRequestStats batchGetStats;

  private static final String STORE_FOO = "store_foo";
  private static final String STORE_BAR = "store_bar";
  private static final String STORE_WITH_SMALL_VALUES = "store_with_small_values";
  private static final String STORE_WITH_LARGE_VALUES = "store_with_large_values";

  @BeforeTest
  public void setUp() {
    this.metricsRepository = new MetricsRepository();
    Assert.assertEquals(metricsRepository.metrics().size(), 0);
    this.reporter = new MockTehutiReporter();
    this.metricsRepository.addReporter(reporter);
    this.singleGetStats = new AggServerHttpRequestStats(
        metricsRepository,
        RequestType.SINGLE_GET,
        false,
        Mockito.mock(ReadOnlyStoreRepository.class),
        true);
    this.batchGetStats = new AggServerHttpRequestStats(
        metricsRepository,
        RequestType.MULTI_GET,
        false,
        Mockito.mock(ReadOnlyStoreRepository.class),
        true);
  }

  @AfterTest
  public void cleanUp() {
    metricsRepository.close();
    reporter.close();
  }

  @Test
  public void testMetrics() {

    singleGetStats.recordSuccessRequest(STORE_FOO);
    singleGetStats.recordSuccessRequest(STORE_BAR);
    singleGetStats.recordErrorRequest(STORE_FOO);
    singleGetStats.recordErrorRequest();

    Assert.assertTrue(
        reporter.query("." + STORE_FOO + "--success_request.OccurrenceRate").value() > 0,
        "success_request rate should be positive");
    Assert.assertTrue(
        reporter.query(".total--error_request.OccurrenceRate").value() > 0,
        "error_request rate should be positive");
    Assert.assertTrue(
        reporter.query(".total--success_request_ratio.RatioStat").value() > 0,
        "success_request_ratio should be positive");

    singleGetStats.handleStoreDeleted(STORE_FOO);
    Assert.assertNull(metricsRepository.getMetric("." + STORE_FOO + "--success_request.OccurrenceRate"));
  }

  @Test
  public void testPercentileNamePattern() {
    String sensorName = "sensorName";
    String storeName = "storeName";
    Percentiles percentiles = TehutiUtils.getPercentileStatForNetworkLatency(sensorName, storeName);
    percentiles.stats().stream().map(namedMeasurable -> namedMeasurable.name()).forEach(System.out::println);
    String[] percentileStrings = new String[] { "50", "77", "90", "95", "99", "99_9" };

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
    singleGetStats.recordSuccessRequest(STORE_WITH_SMALL_VALUES);
    singleGetStats.recordMultiChunkLargeValueCount(STORE_WITH_SMALL_VALUES, 0);
    singleGetStats.recordSuccessRequest(STORE_WITH_LARGE_VALUES);
    singleGetStats.recordMultiChunkLargeValueCount(STORE_WITH_LARGE_VALUES, 1);

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

    batchGetStats.recordSuccessRequest(STORE_WITH_SMALL_VALUES);
    batchGetStats.recordMultiChunkLargeValueCount(STORE_WITH_SMALL_VALUES, 0);
    batchGetStats.recordSuccessRequest(STORE_WITH_LARGE_VALUES);
    batchGetStats.recordMultiChunkLargeValueCount(STORE_WITH_LARGE_VALUES, 5);
    batchGetStats.recordSuccessRequest(STORE_WITH_LARGE_VALUES);
    batchGetStats.recordMultiChunkLargeValueCount(STORE_WITH_LARGE_VALUES, 15);

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
