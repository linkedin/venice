package com.linkedin.venice.stats;

import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.stats.dimensions.HttpResponseStatusCodeCategory;
import com.linkedin.venice.stats.dimensions.HttpResponseStatusEnum;
import com.linkedin.venice.stats.dimensions.VeniceResponseStatusCategory;
import com.linkedin.venice.tehuti.MockTehutiReporter;
import io.netty.handler.codec.http.HttpResponseStatus;
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
  private AggServerHttpRequestStats computeStats;

  private static final String STORE_FOO = "store_foo";
  private static final String STORE_BAR = "store_bar";
  private static final String STORE_WITH_SMALL_VALUES = "store_with_small_values";
  private static final String STORE_WITH_LARGE_VALUES = "store_with_large_values";

  private static final HttpResponseStatusEnum OK_STATUS = HttpResponseStatusEnum.OK;
  private static final HttpResponseStatusCodeCategory OK_CATEGORY = HttpResponseStatusCodeCategory.SUCCESS;
  private static final VeniceResponseStatusCategory OK_VENICE = VeniceResponseStatusCategory.SUCCESS;

  private static final HttpResponseStatusEnum ERROR_STATUS = HttpResponseStatusEnum.INTERNAL_SERVER_ERROR;
  private static final HttpResponseStatusCodeCategory ERROR_CATEGORY = HttpResponseStatusCodeCategory.SERVER_ERROR;
  private static final VeniceResponseStatusCategory ERROR_VENICE = VeniceResponseStatusCategory.FAIL;

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
        false,
        true);
    this.batchGetStats = new AggServerHttpRequestStats(
        "test_cluster",
        metricsRepository,
        RequestType.MULTI_GET,
        false,
        Mockito.mock(ReadOnlyStoreRepository.class),
        true,
        false,
        true);
    this.computeStats = new AggServerHttpRequestStats(
        "test_cluster",
        metricsRepository,
        RequestType.COMPUTE,
        false,
        Mockito.mock(ReadOnlyStoreRepository.class),
        true,
        false,
        true);

    this.singleGetStatsWithKVProfiling = new AggServerHttpRequestStats(
        "test_cluster",
        metricsRepositoryForKVProfiling,
        RequestType.SINGLE_GET,
        true,
        Mockito.mock(ReadOnlyStoreRepository.class),
        true,
        false,
        true);
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

    ServerHttpRequestStats computeServerStatsFoo = computeStats.getStoreStats(STORE_FOO);

    singleGetServerStatsFoo.recordSuccessRequest(OK_STATUS, OK_CATEGORY, OK_VENICE);
    singleGetServerStatsFoo.recordSuccessRequest(OK_STATUS, OK_CATEGORY, OK_VENICE);
    singleGetServerStatsFoo.recordErrorRequest(ERROR_STATUS, ERROR_CATEGORY, ERROR_VENICE);
    singleGetServerStatsBar.recordErrorRequest(ERROR_STATUS, ERROR_CATEGORY, ERROR_VENICE);

    singleGetServerStatsFoo.recordKeySizeInByte(100);
    singleGetServerStatsFoo.recordValueSizeInByte(OK_STATUS, OK_CATEGORY, OK_VENICE, 1000);

    singleGetServerStatsWithKvProfilingFoo.recordKeySizeInByte(100);
    singleGetServerStatsWithKvProfilingFoo.recordValueSizeInByte(OK_STATUS, OK_CATEGORY, OK_VENICE, 1000);

    computeServerStatsFoo.recordDotProductCount(10);
    computeServerStatsFoo.recordCosineSimilarityCount(10);
    computeServerStatsFoo.recordHadamardProductCount(10);
    computeServerStatsFoo.recordCountOperatorCount(10);
    computeServerStatsFoo.recordKeyNotFoundCount(5);

    Assert.assertTrue(
        reporter.query("." + STORE_FOO + "--success_request.OccurrenceRate").value() > 0,
        "success_request rate should be positive");
    Assert.assertTrue(
        reporter.query("." + STORE_FOO + "--compute_key_not_found.Rate").value() > 0,
        "compute_key_not_found rate should be positive");
    Assert.assertTrue(
        reporter.query(".total--compute_key_not_found.Rate").value() > 0,
        "total compute_key_not_found rate should be positive");
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

    Assert.assertTrue(
        reporter.query(".store_foo--compute_dot_product_count.Avg").value() > 0,
        "Avg value for compute dot product count should always be recorded");
    Assert.assertTrue(
        reporter.query(".store_foo--compute_cosine_similarity_count.Avg").value() > 0,
        "Avg value for compute cosine similarity count should always be recorded");
    Assert.assertTrue(
        reporter.query(".store_foo--compute_hadamard_product_count.Avg").value() > 0,
        "Avg value for compute hadamard product count should always be recorded");
    Assert.assertTrue(
        reporter.query(".store_foo--compute_count_operator_count.Avg").value() > 0,
        "Avg value for compute count operator count should always be recorded");

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

    smallValueStats.recordSuccessRequest(OK_STATUS, OK_CATEGORY, OK_VENICE);
    smallValueStats.recordMultiChunkLargeValueCount(0);
    largeValueStats.recordSuccessRequest(OK_STATUS, OK_CATEGORY, OK_VENICE);
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

    batchGetSmallStats.recordSuccessRequest(OK_STATUS, OK_CATEGORY, OK_VENICE);
    batchGetSmallStats.recordMultiChunkLargeValueCount(0);
    batchGetLargeStats.recordSuccessRequest(OK_STATUS, OK_CATEGORY, OK_VENICE);
    batchGetLargeStats.recordMultiChunkLargeValueCount(5);
    batchGetLargeStats.recordSuccessRequest(OK_STATUS, OK_CATEGORY, OK_VENICE);
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

  /**
   * Verifies per-store to total propagation across all metric types used by
   * {@code registerPerStoreAndTotal}: {@code MetricEntityStateThreeEnums} (success/error counts),
   * {@code MetricEntityStateBase} (request size, key not found, flush latency),
   * {@code MetricEntityStateOneEnum} (storage engine query time),
   * Tehuti-only sensors (early terminated, misrouted), compute op metrics, combined recording
   * methods, and the 3-arg {@code recordResponseSize}.
   *
   * Uses a dedicated store name to avoid interference from other tests that may delete stores.
   */
  @Test
  public void testPerStoreToTotalPropagation() {
    String store = "store_propagation";
    ServerHttpRequestStats singleGetPerStore = singleGetStats.getStoreStats(store);
    ServerHttpRequestStats batchGetPerStore = batchGetStats.getStoreStats(store);
    ServerHttpRequestStats computePerStore = computeStats.getStoreStats(store);

    // --- MetricEntityStateThreeEnums (success/error request counts) ---
    singleGetPerStore.recordSuccessRequest(OK_STATUS, OK_CATEGORY, OK_VENICE);
    singleGetPerStore.recordErrorRequest(ERROR_STATUS, ERROR_CATEGORY, ERROR_VENICE);

    assertPerStoreAndTotal(store, "success_request.OccurrenceRate");
    assertPerStoreAndTotal(store, "error_request.OccurrenceRate");

    // --- MetricEntityStateBase (request size, key not found, flush latency) ---
    batchGetPerStore.recordRequestSizeInBytes(512);
    batchGetPerStore.recordKeyNotFoundCount(3);
    batchGetPerStore.recordRequestKeyCount(10);

    assertPerStoreAndTotal(store, "multiget_request_size_in_bytes.Avg");
    assertPerStoreAndTotal(store, "multiget_key_not_found.Rate");
    assertPerStoreAndTotal(store, "multiget_request_key_count.Rate");

    // --- MetricEntityStateOneEnum (storage engine query time) ---
    singleGetPerStore.recordDatabaseLookupLatency(25.0, false);

    assertPerStoreAndTotal(store, "storage_engine_query_latency.Avg");

    // --- Tehuti-only sensors (early terminated, misrouted) ---
    singleGetPerStore.recordEarlyTerminatedEarlyRequest();
    singleGetPerStore.recordMisroutedStoreVersionRequest();

    assertPerStoreAndTotal(store, "early_terminated_request_count.OccurrenceRate");
    assertPerStoreAndTotal(store, "misrouted_store_version_request_count.OccurrenceRate");

    // --- Compute op metrics (MetricEntityStateOneEnum via createComputeOpMetric) ---
    computePerStore.recordDotProductCount(10);
    computePerStore.recordCosineSimilarityCount(20);
    computePerStore.recordHadamardProductCount(30);
    computePerStore.recordCountOperatorCount(40);

    assertPerStoreAndTotal(store, "compute_dot_product_count.Avg");
    assertPerStoreAndTotal(store, "compute_cosine_similarity_count.Avg");
    assertPerStoreAndTotal(store, "compute_hadamard_product_count.Avg");
    assertPerStoreAndTotal(store, "compute_count_operator_count.Avg");

    // --- Combined recording methods and 3-arg recordResponseSize ---
    // These resolve HttpResponseStatus dimensions internally and record to the same sensors
    // as the individual methods above, verifying the delegation works correctly.
    singleGetPerStore.recordSuccessRequestAndLatency(HttpResponseStatus.OK, OK_VENICE, 50.0, 1);
    singleGetPerStore.recordErrorRequestAndLatency(HttpResponseStatus.INTERNAL_SERVER_ERROR, ERROR_VENICE, 100.0, 1);
    singleGetPerStore.recordResponseSize(HttpResponseStatus.OK, OK_VENICE, 1024);

    assertPerStoreAndTotal(store, "response_size.50thPercentile");
  }

  /**
   * Verifies that recordings from different stores propagate to the same total sensor,
   * and the total reflects the aggregate across all stores.
   */
  @Test
  public void testMultipleStoresPropagateToSameTotal() {
    String storeA = "store_multi_a";
    String storeB = "store_multi_b";
    ServerHttpRequestStats statsA = singleGetStats.getStoreStats(storeA);
    ServerHttpRequestStats statsB = singleGetStats.getStoreStats(storeB);

    statsA.recordRequestSizeInBytes(100);
    statsB.recordRequestSizeInBytes(300);

    // Each per-store should have its own value
    Assert.assertEquals(
        (int) reporter.query("." + storeA + "--request_size_in_bytes.Max").value(),
        100,
        "storeA request_size Max should be 100");
    Assert.assertEquals(
        (int) reporter.query("." + storeB + "--request_size_in_bytes.Max").value(),
        300,
        "storeB request_size Max should be 300");

    // Total should see the max across both stores
    Assert.assertEquals(
        (int) reporter.query(".total--request_size_in_bytes.Max").value(),
        300,
        "Total request_size Max should be 300 (max across both stores)");
  }

  private void assertPerStoreAndTotal(String storeName, String metricSuffix) {
    Assert.assertTrue(
        reporter.query("." + storeName + "--" + metricSuffix).value() > 0,
        "Per-store " + metricSuffix + " should be positive");
    Assert.assertTrue(
        reporter.query(".total--" + metricSuffix).value() > 0,
        "Total " + metricSuffix + " should propagate from per-store recording");
  }
}
