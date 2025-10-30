package com.linkedin.venice.controller.stats;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.HTTP_RESPONSE_STATUS_CODE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.HTTP_RESPONSE_STATUS_CODE_CATEGORY;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CONTROLLER_ENDPOINT;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_RESPONSE_STATUS_CODE_CATEGORY;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.linkedin.venice.controller.AbstractTestVeniceParentHelixAdmin;
import com.linkedin.venice.controllerapi.ControllerRoute;
import com.linkedin.venice.stats.VeniceMetricsConfig;
import com.linkedin.venice.stats.VeniceMetricsRepository;
import com.linkedin.venice.stats.dimensions.HttpResponseStatusCodeCategory;
import com.linkedin.venice.stats.dimensions.VeniceResponseStatusCategory;
import com.linkedin.venice.stats.metrics.MetricEntity;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.OpenTelemetryDataTestUtils;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import java.util.Arrays;
import java.util.Collection;
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
        ControllerMetricEntity.INFLIGHT_CALL_COUNT.getMetricEntity(),
        ControllerMetricEntity.CALL_COUNT.getMetricEntity(),
        ControllerMetricEntity.CALL_TIME.getMetricEntity());

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
        ControllerMetricEntity.INFLIGHT_CALL_COUNT.getMetricName(),
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
        ControllerMetricEntity.INFLIGHT_CALL_COUNT.getMetricName(),
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
        ControllerMetricEntity.CALL_COUNT.getMetricName(),
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
        ControllerMetricEntity.INFLIGHT_CALL_COUNT.getMetricName(),
        0,
        Attributes.builder()
            .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), clusterName)
            .put(
                VENICE_CONTROLLER_ENDPOINT.getDimensionNameInDefaultFormat(),
                ControllerRoute.valueOfPath(testPath).toString().toLowerCase())
            .build());
    validateExponentialHistogramPointData(
        ControllerMetricEntity.CALL_TIME.getMetricName(),
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
        ControllerMetricEntity.INFLIGHT_CALL_COUNT.getMetricName(),
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
        ControllerMetricEntity.CALL_COUNT.getMetricName(),
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
        ControllerMetricEntity.INFLIGHT_CALL_COUNT.getMetricName(),
        0,
        Attributes.builder()
            .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), clusterName)
            .put(
                VENICE_CONTROLLER_ENDPOINT.getDimensionNameInDefaultFormat(),
                ControllerRoute.valueOfPath(testPath).toString().toLowerCase())
            .build());
    validateExponentialHistogramPointData(
        ControllerMetricEntity.CALL_TIME.getMetricName(),
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
