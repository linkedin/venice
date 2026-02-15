package com.linkedin.venice.controller.stats;

import static com.linkedin.venice.controller.VeniceController.CONTROLLER_SERVICE_METRIC_ENTITIES;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.HTTP_RESPONSE_STATUS_CODE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.HTTP_RESPONSE_STATUS_CODE_CATEGORY;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CONTROLLER_ENDPOINT;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_RESPONSE_STATUS_CODE_CATEGORY;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.controller.AbstractTestVeniceParentHelixAdmin;
import com.linkedin.venice.controllerapi.ControllerRoute;
import com.linkedin.venice.stats.VeniceMetricsConfig;
import com.linkedin.venice.stats.VeniceMetricsRepository;
import com.linkedin.venice.stats.dimensions.HttpResponseStatusCodeCategory;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.dimensions.VeniceResponseStatusCategory;
import com.linkedin.venice.stats.metrics.MetricEntity;
import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.OpenTelemetryDataTestUtils;
import com.linkedin.venice.utils.Utils;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import spark.Request;
import spark.Response;


public class SparkServerStatsTest extends AbstractTestVeniceParentHelixAdmin {
  private static final String TEST_METRIC_PREFIX = "spark_server";
  private static final String TEST_CLUSTER_NAME = AbstractTestVeniceParentHelixAdmin.clusterName;
  private InMemoryMetricReader inMemoryMetricReader;

  private SparkServerStats sparkServerStats;
  private SparkServerStats sparkServerGenericClusterStats;

  @BeforeMethod
  public void setUp() throws Exception {
    // add all the metrics that are used in the test
    Collection<MetricEntity> metricEntities = Arrays.asList(
        SparkServerStats.SparkServerOtelMetricEntity.INFLIGHT_CALL_COUNT.getMetricEntity(),
        SparkServerStats.SparkServerOtelMetricEntity.CALL_COUNT.getMetricEntity(),
        SparkServerStats.SparkServerOtelMetricEntity.CALL_TIME.getMetricEntity());

    // setup metric reader to validate metric emission
    this.inMemoryMetricReader = InMemoryMetricReader.create();
    VeniceMetricsRepository metricsRepository = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricPrefix(TEST_METRIC_PREFIX)
            .setMetricEntities(metricEntities)
            .setEmitOtelMetrics(true)
            .setOtelAdditionalMetricsReader(inMemoryMetricReader)
            .build());

    setupInternalMocks();

    this.sparkServerStats = new SparkServerStats(metricsRepository, TEST_METRIC_PREFIX, TEST_CLUSTER_NAME);
    this.sparkServerGenericClusterStats = new SparkServerStats(
        metricsRepository,
        TEST_METRIC_PREFIX,
        SparkServerStats.NON_CLUSTER_SPECIFIC_STAT_CLUSTER_NAME);
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testRecordRequest(boolean genericCluster) {

    // Request
    String testPath = "/store";
    String testMethod = "GET";
    Request request = mock(Request.class);
    when(request.uri()).thenReturn(testPath);
    when(request.requestMethod()).thenReturn(testMethod);

    // Record metric
    SparkServerStats stats;
    if (genericCluster) {
      stats = sparkServerGenericClusterStats;
    } else {
      stats = sparkServerStats;
    }
    stats.recordRequest(request);

    // test validation
    String clusterName = genericCluster ? SparkServerStats.NON_CLUSTER_SPECIFIC_STAT_CLUSTER_NAME : TEST_CLUSTER_NAME;
    validateLongPointFromDataFromCounter(
        SparkServerStats.SparkServerOtelMetricEntity.INFLIGHT_CALL_COUNT.getMetricName(),
        1,
        Attributes.builder()
            .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), clusterName)
            .put(
                VENICE_CONTROLLER_ENDPOINT.getDimensionNameInDefaultFormat(),
                ControllerRoute.valueOfPath(testPath).toString().toLowerCase())
            .build());
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testRecordSuccessfulRequest(boolean genericCluster) {

    // Request
    String testPath = "/store";
    String testMethod = "GET";
    Request request = mock(Request.class);
    when(request.uri()).thenReturn(testPath);
    when(request.requestMethod()).thenReturn(testMethod);

    // Response
    int testResponseCode = 200;
    Response response = mock(Response.class);
    when(response.status()).thenReturn(testResponseCode);

    // Record metric
    SparkServerStats stats;
    if (genericCluster) {
      stats = sparkServerGenericClusterStats;
    } else {
      stats = sparkServerStats;
    }
    stats.recordRequest(request);

    // Test validation
    String clusterName = genericCluster ? SparkServerStats.NON_CLUSTER_SPECIFIC_STAT_CLUSTER_NAME : TEST_CLUSTER_NAME;
    validateLongPointFromDataFromCounter(
        SparkServerStats.SparkServerOtelMetricEntity.INFLIGHT_CALL_COUNT.getMetricName(),
        1,
        Attributes.builder()
            .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), clusterName)
            .put(
                VENICE_CONTROLLER_ENDPOINT.getDimensionNameInDefaultFormat(),
                ControllerRoute.valueOfPath(testPath).toString().toLowerCase())
            .build());

    // Record success
    int testCallTime = 1000;
    stats.recordSuccessfulRequest(request, response, testCallTime);

    // Test validation
    validateLongPointFromDataFromCounter(
        SparkServerStats.SparkServerOtelMetricEntity.CALL_COUNT.getMetricName(),
        1,
        Attributes.builder()
            .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), clusterName)
            .put(
                VENICE_CONTROLLER_ENDPOINT.getDimensionNameInDefaultFormat(),
                ControllerRoute.valueOfPath(testPath).toString().toLowerCase())
            .put(HTTP_RESPONSE_STATUS_CODE.getDimensionNameInDefaultFormat(), String.valueOf(testResponseCode))
            .put(
                HTTP_RESPONSE_STATUS_CODE_CATEGORY.getDimensionNameInDefaultFormat(),
                HttpResponseStatusCodeCategory.SUCCESS.getDimensionValue())
            .put(
                VENICE_RESPONSE_STATUS_CODE_CATEGORY.getDimensionNameInDefaultFormat(),
                VeniceResponseStatusCategory.SUCCESS.getDimensionValue())
            .build());
    validateLongPointFromDataFromCounter(
        SparkServerStats.SparkServerOtelMetricEntity.INFLIGHT_CALL_COUNT.getMetricName(),
        0,
        Attributes.builder()
            .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), clusterName)
            .put(
                VENICE_CONTROLLER_ENDPOINT.getDimensionNameInDefaultFormat(),
                ControllerRoute.valueOfPath(testPath).toString().toLowerCase())
            .build());
    validateExponentialHistogramPointData(
        SparkServerStats.SparkServerOtelMetricEntity.CALL_TIME.getMetricName(),
        testCallTime,
        testCallTime,
        1,
        testCallTime,
        Attributes.builder()
            .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), clusterName)
            .put(
                VENICE_CONTROLLER_ENDPOINT.getDimensionNameInDefaultFormat(),
                ControllerRoute.valueOfPath(testPath).toString().toLowerCase())
            .put(HTTP_RESPONSE_STATUS_CODE.getDimensionNameInDefaultFormat(), String.valueOf(testResponseCode))
            .put(
                HTTP_RESPONSE_STATUS_CODE_CATEGORY.getDimensionNameInDefaultFormat(),
                HttpResponseStatusCodeCategory.SUCCESS.getDimensionValue())
            .put(
                VENICE_RESPONSE_STATUS_CODE_CATEGORY.getDimensionNameInDefaultFormat(),
                VeniceResponseStatusCategory.SUCCESS.getDimensionValue())
            .build());
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testRecordFailRequest(boolean genericCluster) {
    // Request
    String testPath = "/store";
    String testMethod = "GET";
    Request request = mock(Request.class);
    when(request.uri()).thenReturn(testPath);
    when(request.requestMethod()).thenReturn(testMethod);

    // Response
    int testResponseCode = 500;
    Response response = mock(Response.class);
    when(response.status()).thenReturn(testResponseCode);

    // Record metric
    SparkServerStats stats;
    if (genericCluster) {
      stats = sparkServerGenericClusterStats;
    } else {
      stats = sparkServerStats;
    }
    stats.recordRequest(request);

    // Test validation
    String clusterName = genericCluster ? SparkServerStats.NON_CLUSTER_SPECIFIC_STAT_CLUSTER_NAME : TEST_CLUSTER_NAME;
    validateLongPointFromDataFromCounter(
        SparkServerStats.SparkServerOtelMetricEntity.INFLIGHT_CALL_COUNT.getMetricName(),
        1,
        Attributes.builder()
            .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), clusterName)
            .put(
                VENICE_CONTROLLER_ENDPOINT.getDimensionNameInDefaultFormat(),
                ControllerRoute.valueOfPath(testPath).toString().toLowerCase())
            .build());

    // Record success
    int testCallTime = 1000;
    stats.recordFailedRequest(request, response, testCallTime);

    // Test validation
    validateLongPointFromDataFromCounter(
        SparkServerStats.SparkServerOtelMetricEntity.CALL_COUNT.getMetricName(),
        1,
        Attributes.builder()
            .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), clusterName)
            .put(
                VENICE_CONTROLLER_ENDPOINT.getDimensionNameInDefaultFormat(),
                ControllerRoute.valueOfPath(testPath).toString().toLowerCase())
            .put(HTTP_RESPONSE_STATUS_CODE.getDimensionNameInDefaultFormat(), String.valueOf(testResponseCode))
            .put(
                HTTP_RESPONSE_STATUS_CODE_CATEGORY.getDimensionNameInDefaultFormat(),
                HttpResponseStatusCodeCategory.SERVER_ERROR.getDimensionValue())
            .put(
                VENICE_RESPONSE_STATUS_CODE_CATEGORY.getDimensionNameInDefaultFormat(),
                VeniceResponseStatusCategory.FAIL.getDimensionValue())
            .build());
    validateLongPointFromDataFromCounter(
        SparkServerStats.SparkServerOtelMetricEntity.INFLIGHT_CALL_COUNT.getMetricName(),
        0,
        Attributes.builder()
            .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), clusterName)
            .put(
                VENICE_CONTROLLER_ENDPOINT.getDimensionNameInDefaultFormat(),
                ControllerRoute.valueOfPath(testPath).toString().toLowerCase())
            .build());
    validateExponentialHistogramPointData(
        SparkServerStats.SparkServerOtelMetricEntity.CALL_TIME.getMetricName(),
        testCallTime,
        testCallTime,
        1,
        testCallTime,
        Attributes.builder()
            .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), clusterName)
            .put(
                VENICE_CONTROLLER_ENDPOINT.getDimensionNameInDefaultFormat(),
                ControllerRoute.valueOfPath(testPath).toString().toLowerCase())
            .put(HTTP_RESPONSE_STATUS_CODE.getDimensionNameInDefaultFormat(), String.valueOf(testResponseCode))
            .put(
                HTTP_RESPONSE_STATUS_CODE_CATEGORY.getDimensionNameInDefaultFormat(),
                HttpResponseStatusCodeCategory.SERVER_ERROR.getDimensionValue())
            .put(
                VENICE_RESPONSE_STATUS_CODE_CATEGORY.getDimensionNameInDefaultFormat(),
                VeniceResponseStatusCategory.FAIL.getDimensionValue())
            .build());
  }

  @Test
  public void testControllerTehutiMetricNameEnum() {
    Map<SparkServerStats.ControllerTehutiMetricNameEnum, String> expectedNames = new HashMap<>();
    expectedNames.put(SparkServerStats.ControllerTehutiMetricNameEnum.REQUEST, "request");
    expectedNames.put(SparkServerStats.ControllerTehutiMetricNameEnum.FINISHED_REQUEST, "finished_request");
    expectedNames
        .put(SparkServerStats.ControllerTehutiMetricNameEnum.CURRENT_IN_FLIGHT_REQUEST, "current_in_flight_request");
    expectedNames.put(SparkServerStats.ControllerTehutiMetricNameEnum.SUCCESSFUL_REQUEST, "successful_request");
    expectedNames.put(SparkServerStats.ControllerTehutiMetricNameEnum.FAILED_REQUEST, "failed_request");
    expectedNames
        .put(SparkServerStats.ControllerTehutiMetricNameEnum.SUCCESSFUL_REQUEST_LATENCY, "successful_request_latency");
    expectedNames.put(SparkServerStats.ControllerTehutiMetricNameEnum.FAILED_REQUEST_LATENCY, "failed_request_latency");

    assertEquals(
        SparkServerStats.ControllerTehutiMetricNameEnum.values().length,
        expectedNames.size(),
        "New ControllerTehutiMetricNameEnum values were added but not included in this test");

    for (SparkServerStats.ControllerTehutiMetricNameEnum enumValue: SparkServerStats.ControllerTehutiMetricNameEnum
        .values()) {
      String expectedName = expectedNames.get(enumValue);
      assertNotNull(expectedName, "No expected metric name for " + enumValue.name());
      assertEquals(enumValue.getMetricName(), expectedName, "Unexpected metric name for " + enumValue.name());
    }
  }

  @Test
  public void testSparkServerOtelMetricEntity() {
    Map<SparkServerStats.SparkServerOtelMetricEntity, MetricEntity> expectedMetrics = new HashMap<>();
    expectedMetrics.put(
        SparkServerStats.SparkServerOtelMetricEntity.INFLIGHT_CALL_COUNT,
        new MetricEntity(
            "inflight_call_count",
            MetricType.UP_DOWN_COUNTER,
            MetricUnit.NUMBER,
            "Count of all current inflight calls to controller spark server",
            Utils.setOf(
                VeniceMetricsDimensions.VENICE_CLUSTER_NAME,
                VeniceMetricsDimensions.VENICE_CONTROLLER_ENDPOINT)));
    expectedMetrics.put(
        SparkServerStats.SparkServerOtelMetricEntity.CALL_COUNT,
        new MetricEntity(
            "call_count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Count of all calls to controller spark server",
            Utils.setOf(
                VeniceMetricsDimensions.VENICE_CLUSTER_NAME,
                VeniceMetricsDimensions.VENICE_CONTROLLER_ENDPOINT,
                VeniceMetricsDimensions.HTTP_RESPONSE_STATUS_CODE,
                VeniceMetricsDimensions.HTTP_RESPONSE_STATUS_CODE_CATEGORY,
                VeniceMetricsDimensions.VENICE_RESPONSE_STATUS_CODE_CATEGORY)));
    expectedMetrics.put(
        SparkServerStats.SparkServerOtelMetricEntity.CALL_TIME,
        new MetricEntity(
            "call_time",
            MetricType.HISTOGRAM,
            MetricUnit.MILLISECOND,
            "Latency histogram of all successful calls to controller spark server",
            Utils.setOf(
                VeniceMetricsDimensions.VENICE_CLUSTER_NAME,
                VeniceMetricsDimensions.VENICE_CONTROLLER_ENDPOINT,
                VeniceMetricsDimensions.HTTP_RESPONSE_STATUS_CODE,
                VeniceMetricsDimensions.HTTP_RESPONSE_STATUS_CODE_CATEGORY,
                VeniceMetricsDimensions.VENICE_RESPONSE_STATUS_CODE_CATEGORY)));

    assertEquals(
        SparkServerStats.SparkServerOtelMetricEntity.values().length,
        expectedMetrics.size(),
        "New SparkServerOtelMetricEntity values were added but not included in this test");

    for (SparkServerStats.SparkServerOtelMetricEntity metric: SparkServerStats.SparkServerOtelMetricEntity.values()) {
      MetricEntity actual = metric.getMetricEntity();
      MetricEntity expected = expectedMetrics.get(metric);

      assertNotNull(expected, "No expected definition for " + metric.name());
      assertEquals(actual.getMetricName(), expected.getMetricName(), "Unexpected metric name for " + metric.name());
      assertEquals(actual.getMetricType(), expected.getMetricType(), "Unexpected metric type for " + metric.name());
      assertEquals(actual.getUnit(), expected.getUnit(), "Unexpected metric unit for " + metric.name());
      assertEquals(
          actual.getDescription(),
          expected.getDescription(),
          "Unexpected metric description for " + metric.name());
      assertEquals(
          actual.getDimensionsList(),
          expected.getDimensionsList(),
          "Unexpected metric dimensions for " + metric.name());
    }

    // Verify all SparkServerOtelMetricEntity entries are present in CONTROLLER_SERVICE_METRIC_ENTITIES
    for (MetricEntity expected: expectedMetrics.values()) {
      boolean found = false;
      for (MetricEntity actual: CONTROLLER_SERVICE_METRIC_ENTITIES) {
        if (Objects.equals(actual.getMetricName(), expected.getMetricName())
            && actual.getMetricType() == expected.getMetricType() && actual.getUnit() == expected.getUnit()
            && Objects.equals(actual.getDescription(), expected.getDescription())
            && Objects.equals(actual.getDimensionsList(), expected.getDimensionsList())) {
          found = true;
          break;
        }
      }
      assertTrue(found, "MetricEntity not found in CONTROLLER_SERVICE_METRIC_ENTITIES: " + expected.getMetricName());
    }
  }

  private void validateLongPointFromDataFromCounter(
      String metricName,
      int expectedMetricValue,
      Attributes expectedAttributes) {
    OpenTelemetryDataTestUtils.validateLongPointDataFromCounter(
        inMemoryMetricReader,
        expectedMetricValue,
        expectedAttributes,
        metricName,
        TEST_METRIC_PREFIX);
  }

  private void validateExponentialHistogramPointData(
      String metricName,
      double expectedMin,
      double expectedMax,
      long expectedCount,
      double expectedSum,
      Attributes expectedAttributes) {
    OpenTelemetryDataTestUtils.validateExponentialHistogramPointData(
        inMemoryMetricReader,
        expectedMin,
        expectedMax,
        expectedCount,
        expectedSum,
        expectedAttributes,
        metricName,
        TEST_METRIC_PREFIX);
  }
}
