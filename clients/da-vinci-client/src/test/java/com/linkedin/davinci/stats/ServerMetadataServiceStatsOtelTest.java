package com.linkedin.davinci.stats;

import static com.linkedin.davinci.stats.ServerMetadataServiceStats.UNKNOWN_STORE;
import static com.linkedin.davinci.stats.ServerMetricEntity.SERVER_METRIC_ENTITIES;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_RESPONSE_STATUS_CODE_CATEGORY;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.stats.VeniceMetricsConfig;
import com.linkedin.venice.stats.VeniceMetricsRepository;
import com.linkedin.venice.stats.dimensions.VeniceResponseStatusCategory;
import com.linkedin.venice.utils.OpenTelemetryDataTestUtils;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.tehuti.metrics.MetricsRepository;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class ServerMetadataServiceStatsOtelTest {
  private static final String TEST_METRIC_PREFIX = "server";
  private static final String TEST_CLUSTER_NAME = "test-cluster";
  private static final String TEST_STORE_NAME = "test-store";
  private static final String TEHUTI_INVOKE_METRIC = ".ServerMetadataStats--request_based_metadata_invoke_count.Rate";
  private static final String TEHUTI_FAILURE_METRIC = ".ServerMetadataStats--request_based_metadata_failure_count.Rate";
  private InMemoryMetricReader inMemoryMetricReader;
  private VeniceMetricsRepository metricsRepository;
  private ServerMetadataServiceStats stats;

  @BeforeMethod
  public void setUp() {
    this.inMemoryMetricReader = InMemoryMetricReader.create();
    this.metricsRepository = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricPrefix(TEST_METRIC_PREFIX)
            .setMetricEntities(SERVER_METRIC_ENTITIES)
            .setEmitOtelMetrics(true)
            .setOtelAdditionalMetricsReader(inMemoryMetricReader)
            .build());

    stats = new ServerMetadataServiceStats(metricsRepository, TEST_CLUSTER_NAME);
  }

  @AfterMethod
  public void tearDown() {
    if (metricsRepository != null) {
      metricsRepository.close();
    }
  }

  @Test
  public void testRecordFailureUsesStoreNameForNonNoStoreException() {
    stats.recordRequestBasedMetadataFailureCount(TEST_STORE_NAME, new VeniceException("some error"));

    // OTel counter recorded under the actual store name (not UNKNOWN_STORE)
    validateCounter(
        ServerMetadataOtelMetricEntity.METADATA_REQUEST_COUNT.getMetricName(),
        1,
        buildExpectedAttributes(TEST_STORE_NAME, VeniceResponseStatusCategory.FAIL));

    // Tehuti failure sensor also recorded (dual-recording), invoke sensor unaffected
    assertTrue(metricsRepository.getMetric(TEHUTI_FAILURE_METRIC).value() > 0);
    assertEquals(metricsRepository.getMetric(TEHUTI_INVOKE_METRIC).value(), 0d);
  }

  @Test
  public void testRecordSuccessIsOtelOnly() {
    stats.recordRequestBasedMetadataSuccessCount(TEST_STORE_NAME);

    // OTel counter recorded
    validateCounter(
        ServerMetadataOtelMetricEntity.METADATA_REQUEST_COUNT.getMetricName(),
        1,
        buildExpectedAttributes(TEST_STORE_NAME, VeniceResponseStatusCategory.SUCCESS));

    // Tehuti sensors NOT affected by success recording
    assertEquals(metricsRepository.getMetric(TEHUTI_INVOKE_METRIC).value(), 0d);
    assertEquals(metricsRepository.getMetric(TEHUTI_FAILURE_METRIC).value(), 0d);
  }

  @Test
  public void testSuccessAndFailureAreIndependent() {
    stats.recordRequestBasedMetadataSuccessCount(TEST_STORE_NAME);
    stats.recordRequestBasedMetadataSuccessCount(TEST_STORE_NAME);
    stats.recordRequestBasedMetadataFailureCount(TEST_STORE_NAME, null);

    validateCounter(
        ServerMetadataOtelMetricEntity.METADATA_REQUEST_COUNT.getMetricName(),
        2,
        buildExpectedAttributes(TEST_STORE_NAME, VeniceResponseStatusCategory.SUCCESS));

    validateCounter(
        ServerMetadataOtelMetricEntity.METADATA_REQUEST_COUNT.getMetricName(),
        1,
        buildExpectedAttributes(TEST_STORE_NAME, VeniceResponseStatusCategory.FAIL));

    // Tehuti: only failure sensor affected, invoke unaffected
    assertTrue(metricsRepository.getMetric(TEHUTI_FAILURE_METRIC).value() > 0);
    assertEquals(metricsRepository.getMetric(TEHUTI_INVOKE_METRIC).value(), 0d);
  }

  @Test
  public void testMultipleStores() {
    String storeA = "store-a";
    String storeB = "store-b";

    stats.recordRequestBasedMetadataSuccessCount(storeA);
    stats.recordRequestBasedMetadataSuccessCount(storeA);
    stats.recordRequestBasedMetadataSuccessCount(storeB);

    validateCounter(
        ServerMetadataOtelMetricEntity.METADATA_REQUEST_COUNT.getMetricName(),
        2,
        buildExpectedAttributes(storeA, VeniceResponseStatusCategory.SUCCESS));

    validateCounter(
        ServerMetadataOtelMetricEntity.METADATA_REQUEST_COUNT.getMetricName(),
        1,
        buildExpectedAttributes(storeB, VeniceResponseStatusCategory.SUCCESS));
  }

  @Test
  public void testNoNpeWhenOtelDisabled() {
    try (VeniceMetricsRepository disabledRepo = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricPrefix(TEST_METRIC_PREFIX).setEmitOtelMetrics(false).build())) {
      assertAllMethodsSafeWithRepo(disabledRepo);
    }
  }

  @Test
  public void testNoNpeWhenPlainMetricsRepository() {
    assertAllMethodsSafeWithRepo(new MetricsRepository());
  }

  @Test
  public void testUnknownStoreFailureUsesSentinel() {
    stats.recordRequestBasedMetadataFailureCount("arbitrary-store", new VeniceNoStoreException("arbitrary-store"));

    // OTel counter recorded under the sentinel store name
    validateCounter(
        ServerMetadataOtelMetricEntity.METADATA_REQUEST_COUNT.getMetricName(),
        1,
        buildExpectedAttributes(UNKNOWN_STORE, VeniceResponseStatusCategory.FAIL));

    // Tehuti failure sensor also recorded
    assertTrue(metricsRepository.getMetric(TEHUTI_FAILURE_METRIC).value() > 0);

    // Invoke sensor unaffected
    assertEquals(metricsRepository.getMetric(TEHUTI_INVOKE_METRIC).value(), 0d);
  }

  private void assertAllMethodsSafeWithRepo(MetricsRepository repo) {
    ServerMetadataServiceStats safeStats = new ServerMetadataServiceStats(repo, TEST_CLUSTER_NAME);
    safeStats.recordRequestBasedMetadataInvokeCount();
    safeStats.recordRequestBasedMetadataSuccessCount(TEST_STORE_NAME);
    safeStats.recordRequestBasedMetadataFailureCount(TEST_STORE_NAME, null);
    safeStats.recordRequestBasedMetadataFailureCount(TEST_STORE_NAME, new VeniceNoStoreException(TEST_STORE_NAME));
  }

  private Attributes buildExpectedAttributes(String storeName, VeniceResponseStatusCategory statusCategory) {
    return Attributes.builder()
        .put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), storeName)
        .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), TEST_CLUSTER_NAME)
        .put(VENICE_RESPONSE_STATUS_CODE_CATEGORY.getDimensionNameInDefaultFormat(), statusCategory.getDimensionValue())
        .build();
  }

  private void validateCounter(String metricName, long expectedValue, Attributes expectedAttributes) {
    OpenTelemetryDataTestUtils.validateLongPointDataFromCounter(
        inMemoryMetricReader,
        expectedValue,
        expectedAttributes,
        metricName,
        TEST_METRIC_PREFIX);
  }
}
