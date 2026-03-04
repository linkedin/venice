package com.linkedin.davinci.stats;

import static com.linkedin.davinci.stats.ServerMetricEntity.SERVER_METRIC_ENTITIES;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_RESPONSE_STATUS_CODE_CATEGORY;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.stats.VeniceMetricsConfig;
import com.linkedin.venice.stats.VeniceMetricsRepository;
import com.linkedin.venice.stats.dimensions.VeniceResponseStatusCategory;
import com.linkedin.venice.stats.metrics.MetricEntity;
import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import com.linkedin.venice.utils.OpenTelemetryDataTestUtils;
import com.linkedin.venice.utils.Utils;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.tehuti.metrics.MetricsRepository;
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

  @Test
  public void testRecordFailure() {
    stats.recordRequestBasedMetadataFailureCount(TEST_STORE_NAME);

    // OTel counter recorded
    validateCounter(
        ServerMetadataServiceStats.ServerMetadataOtelMetricEntity.METADATA_REQUEST_COUNT.getMetricName(),
        1,
        buildExpectedAttributes(TEST_STORE_NAME, VeniceResponseStatusCategory.FAIL));

    // Tehuti failure sensor also recorded (dual-recording)
    assertTrue(metricsRepository.getMetric(TEHUTI_FAILURE_METRIC).value() > 0);
  }

  @Test
  public void testRecordSuccessIsOtelOnly() {
    stats.recordRequestBasedMetadataSuccessCount(TEST_STORE_NAME);

    // OTel counter recorded
    validateCounter(
        ServerMetadataServiceStats.ServerMetadataOtelMetricEntity.METADATA_REQUEST_COUNT.getMetricName(),
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
    stats.recordRequestBasedMetadataFailureCount(TEST_STORE_NAME);

    validateCounter(
        ServerMetadataServiceStats.ServerMetadataOtelMetricEntity.METADATA_REQUEST_COUNT.getMetricName(),
        2,
        buildExpectedAttributes(TEST_STORE_NAME, VeniceResponseStatusCategory.SUCCESS));

    validateCounter(
        ServerMetadataServiceStats.ServerMetadataOtelMetricEntity.METADATA_REQUEST_COUNT.getMetricName(),
        1,
        buildExpectedAttributes(TEST_STORE_NAME, VeniceResponseStatusCategory.FAIL));
  }

  @Test
  public void testMultipleStores() {
    String storeA = "store-a";
    String storeB = "store-b";

    stats.recordRequestBasedMetadataSuccessCount(storeA);
    stats.recordRequestBasedMetadataSuccessCount(storeA);
    stats.recordRequestBasedMetadataSuccessCount(storeB);

    validateCounter(
        ServerMetadataServiceStats.ServerMetadataOtelMetricEntity.METADATA_REQUEST_COUNT.getMetricName(),
        2,
        buildExpectedAttributes(storeA, VeniceResponseStatusCategory.SUCCESS));

    validateCounter(
        ServerMetadataServiceStats.ServerMetadataOtelMetricEntity.METADATA_REQUEST_COUNT.getMetricName(),
        1,
        buildExpectedAttributes(storeB, VeniceResponseStatusCategory.SUCCESS));
  }

  @Test
  public void testNoNpeWhenOtelDisabled() {
    VeniceMetricsRepository disabledRepo = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricPrefix(TEST_METRIC_PREFIX).setEmitOtelMetrics(false).build());
    assertAllMethodsSafeWithRepo(disabledRepo);
  }

  @Test
  public void testNoNpeWhenPlainMetricsRepository() {
    assertAllMethodsSafeWithRepo(new MetricsRepository());
  }

  private void assertAllMethodsSafeWithRepo(MetricsRepository repo) {
    ServerMetadataServiceStats safeStats = new ServerMetadataServiceStats(repo, TEST_CLUSTER_NAME);
    safeStats.recordRequestBasedMetadataInvokeCount();
    safeStats.recordRequestBasedMetadataSuccessCount(TEST_STORE_NAME);
    safeStats.recordRequestBasedMetadataFailureCount(TEST_STORE_NAME);
  }

  @Test
  public void testOtelMetricEntityDefinitions() {
    ServerMetadataServiceStats.ServerMetadataOtelMetricEntity[] values =
        ServerMetadataServiceStats.ServerMetadataOtelMetricEntity.values();
    assertEquals(values.length, 1, "Expected exactly 1 ServerMetadataOtelMetricEntity");

    ServerMetadataServiceStats.ServerMetadataOtelMetricEntity metric =
        ServerMetadataServiceStats.ServerMetadataOtelMetricEntity.METADATA_REQUEST_COUNT;
    MetricEntity entity = metric.getMetricEntity();

    assertNotNull(entity);
    assertEquals(entity.getMetricName(), "metadata.request_count");
    assertEquals(entity.getMetricType(), MetricType.COUNTER);
    assertEquals(entity.getUnit(), MetricUnit.NUMBER);
    assertEquals(entity.getDescription(), "Request-based metadata invocation count by outcome");
    assertEquals(
        entity.getDimensionsList(),
        Utils.setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_RESPONSE_STATUS_CODE_CATEGORY));
    // Registration in SERVER_METRIC_ENTITIES is verified by ServerMetricEntityTest
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
