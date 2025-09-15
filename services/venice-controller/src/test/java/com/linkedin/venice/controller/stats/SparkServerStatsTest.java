package com.linkedin.venice.controller.stats;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.HTTP_REQUEST_URL;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;

import com.linkedin.venice.controller.AbstractTestVeniceParentHelixAdmin;
import com.linkedin.venice.stats.VeniceMetricsConfig;
import com.linkedin.venice.stats.VeniceMetricsRepository;
import com.linkedin.venice.stats.metrics.MetricEntity;
import com.linkedin.venice.utils.OpenTelemetryDataPointTestUtils;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import java.util.Arrays;
import java.util.Collection;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class SparkServerStatsTest extends AbstractTestVeniceParentHelixAdmin {
  private static final String TEST_METRIC_PREFIX = "spark_server";
  private static final String TEST_CLUSTER_NAME = AbstractTestVeniceParentHelixAdmin.clusterName;
  private InMemoryMetricReader inMemoryMetricReader;

  private SparkServerStats sparkServerStats;

  @BeforeMethod
  public void setUp() throws Exception {
    // add all the metrics that are used in the test
    Collection<MetricEntity> metricEntities = Arrays.asList(
        ControllerMetricEntity.SPARK_SERVER_REQUESTS_COUNT.getMetricEntity(),
        ControllerMetricEntity.SPARK_SERVER_FINISHED_REQUESTS_COUNT.getMetricEntity(),
        ControllerMetricEntity.SPARK_SERVER_SUCCESSFUL_REQUESTS_COUNT.getMetricEntity(),
        ControllerMetricEntity.SPARK_SERVER_FAILED_REQUESTS_COUNT.getMetricEntity(),
        ControllerMetricEntity.SPARK_SERVER_CURRENT_INFLIGHT_REQUESTS_COUNT.getMetricEntity(),
        ControllerMetricEntity.SPARK_SERVER_SUCCESSFUL_REQUESTS_LATENCY.getMetricEntity(),
        ControllerMetricEntity.SPARK_SERVER_FAILED_REQUESTS_LATENCY.getMetricEntity());

    // setup metric reader to validate metric emission
    this.inMemoryMetricReader = InMemoryMetricReader.create();
    VeniceMetricsRepository metricsRepository = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricPrefix(TEST_METRIC_PREFIX)
            .setMetricEntities(metricEntities)
            .setEmitOtelMetrics(true)
            .setOtelAdditionalMetricsReader(inMemoryMetricReader)
            .build());

    setupInternalMocks();

    this.sparkServerStats = new SparkServerStats(metricsRepository, TEST_CLUSTER_NAME);
  }

  @Test
  public void testRecordRequest() {
    String testPath = "/test_path";
    Attributes expectedAttributes = Attributes.builder()
        .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), TEST_CLUSTER_NAME)
        .put(HTTP_REQUEST_URL.getDimensionNameInDefaultFormat(), testPath)
        .build();

    // Record metric
    this.sparkServerStats.recordRequest(testPath);

    // test validation
    validateLongPointFromDataFromCounter(
        ControllerMetricEntity.SPARK_SERVER_REQUESTS_COUNT.getMetricName(),
        1,
        expectedAttributes);
    validateLongPointFromDataFromCounter(
        ControllerMetricEntity.SPARK_SERVER_CURRENT_INFLIGHT_REQUESTS_COUNT.getMetricName(),
        1,
        expectedAttributes);
  }

  @Test
  public void testRecordSuccessfulRequest() {
    String testPath = "/test_path";
    double testLatency = 10000;
    Attributes expectedAttributes = Attributes.builder()
        .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), TEST_CLUSTER_NAME)
        .put(HTTP_REQUEST_URL.getDimensionNameInDefaultFormat(), testPath)
        .build();

    // Record request
    this.sparkServerStats.recordRequest(testPath);

    // Test validation
    validateLongPointFromDataFromCounter(
        ControllerMetricEntity.SPARK_SERVER_REQUESTS_COUNT.getMetricName(),
        1,
        expectedAttributes);
    validateLongPointFromDataFromCounter(
        ControllerMetricEntity.SPARK_SERVER_CURRENT_INFLIGHT_REQUESTS_COUNT.getMetricName(),
        1,
        expectedAttributes);

    // Record success
    this.sparkServerStats.recordSuccessfulRequest(testPath, testLatency);

    // Test validation
    validateLongPointFromDataFromCounter(
        ControllerMetricEntity.SPARK_SERVER_SUCCESSFUL_REQUESTS_COUNT.getMetricName(),
        1,
        expectedAttributes);
    validateLongPointFromDataFromCounter(
        ControllerMetricEntity.SPARK_SERVER_CURRENT_INFLIGHT_REQUESTS_COUNT.getMetricName(),
        0,
        expectedAttributes);
  }

  @Test
  public void testRecordFailRequest() {
    String testPath = "/test_path";
    double testLatency = 10000;
    Attributes expectedAttributes = Attributes.builder()
        .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), TEST_CLUSTER_NAME)
        .put(HTTP_REQUEST_URL.getDimensionNameInDefaultFormat(), testPath)
        .build();

    // Record request
    this.sparkServerStats.recordRequest(testPath);

    // Test validation
    validateLongPointFromDataFromCounter(
        ControllerMetricEntity.SPARK_SERVER_REQUESTS_COUNT.getMetricName(),
        1,
        expectedAttributes);
    validateLongPointFromDataFromCounter(
        ControllerMetricEntity.SPARK_SERVER_CURRENT_INFLIGHT_REQUESTS_COUNT.getMetricName(),
        1,
        expectedAttributes);

    // Record success
    this.sparkServerStats.recordFailedRequest(testPath, testLatency);

    // Test validation
    validateLongPointFromDataFromCounter(
        ControllerMetricEntity.SPARK_SERVER_FAILED_REQUESTS_COUNT.getMetricName(),
        1,
        expectedAttributes);
    validateLongPointFromDataFromCounter(
        ControllerMetricEntity.SPARK_SERVER_CURRENT_INFLIGHT_REQUESTS_COUNT.getMetricName(),
        0,
        expectedAttributes);
  }

  private void validateLongPointFromDataFromCounter(
      String metricName,
      int expectedMetricValue,
      Attributes expectedAttributes) {
    OpenTelemetryDataPointTestUtils.validateLongPointDataFromCounter(
        inMemoryMetricReader,
        expectedMetricValue,
        expectedAttributes,
        metricName,
        TEST_METRIC_PREFIX);
  }
}
