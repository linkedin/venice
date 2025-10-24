package com.linkedin.venice.fastclient.stats;

import static com.linkedin.venice.client.stats.BasicClientStats.CLIENT_METRIC_ENTITIES;
import static com.linkedin.venice.client.stats.ClientMetricEntity.ROUTE_CALL_COUNT;
import static com.linkedin.venice.stats.ClientType.FAST_CLIENT;
import static com.linkedin.venice.stats.VeniceMetricsRepository.getVeniceMetricsRepository;
import static com.linkedin.venice.stats.dimensions.HttpResponseStatusEnum.GONE;
import static com.linkedin.venice.stats.dimensions.HttpResponseStatusEnum.INTERNAL_SERVER_ERROR;
import static com.linkedin.venice.stats.dimensions.HttpResponseStatusEnum.OK;
import static com.linkedin.venice.stats.dimensions.HttpResponseStatusEnum.SERVICE_UNAVAILABLE;
import static com.linkedin.venice.stats.dimensions.HttpResponseStatusEnum.TOO_MANY_REQUESTS;
import static com.linkedin.venice.stats.dimensions.HttpResponseStatusEnum.UNKNOWN;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.HTTP_RESPONSE_STATUS_CODE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.HTTP_RESPONSE_STATUS_CODE_CATEGORY;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REQUEST_METHOD;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REQUEST_REJECTION_REASON;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_RESPONSE_STATUS_CODE_CATEGORY;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_ROUTE_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceResponseStatusCategory.FAIL;
import static com.linkedin.venice.stats.dimensions.VeniceResponseStatusCategory.SUCCESS;
import static com.linkedin.venice.utils.OpenTelemetryDataTestUtils.validateExponentialHistogramPointData;
import static com.linkedin.venice.utils.OpenTelemetryDataTestUtils.validateHistogramPointData;
import static com.linkedin.venice.utils.OpenTelemetryDataTestUtils.validateLongPointDataFromCounter;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.client.stats.ClientMetricEntity;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.stats.VeniceMetricsRepository;
import com.linkedin.venice.stats.dimensions.HttpResponseStatusCodeCategory;
import com.linkedin.venice.stats.dimensions.RejectionReason;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.tehuti.Metric;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class ClusterRouteStatsTest {
  private static final String STORE_NAME = "test_store";
  private static final String CLUSTER_NAME = "test_cluster";
  private static final String INSTANCE_URL = "http://localhost:8080";
  private static final RequestType REQUEST_TYPE = RequestType.SINGLE_GET;

  private VeniceMetricsRepository metricsRepository;
  private ClusterRouteStats clusterRouteStats;
  private InMemoryMetricReader inMemoryMetricReader;
  private String otelMetricPrefix;
  private URL instanceUrl = null;

  @BeforeClass
  public void setUp() throws MalformedURLException {
    instanceUrl = new URL(INSTANCE_URL);
    // Set up Venice metrics repository with both Tehuti and OpenTelemetry support
    inMemoryMetricReader = InMemoryMetricReader.create();
    metricsRepository = getVeniceMetricsRepository(FAST_CLIENT, CLIENT_METRIC_ENTITIES, true, inMemoryMetricReader);
    otelMetricPrefix = FAST_CLIENT.getMetricsPrefix();

    // Create ClusterRouteStats instance using the singleton pattern
    clusterRouteStats = ClusterRouteStats.getInstance(STORE_NAME);
  }

  @Test
  public void testHealthyRequestCountMetric() {
    // Get a route stats instance
    ClusterRouteStats.RouteStats routeStats =
        clusterRouteStats.getRouteStats(metricsRepository, CLUSTER_NAME, INSTANCE_URL, REQUEST_TYPE);
    assertNotNull(routeStats);

    // Verify that RouteStats was created successfully
    assertNotNull(routeStats, "RouteStats should be created successfully");

    // Record healthy requests with proper parameters
    double latency1 = 100.0;

    // Test that recording doesn't throw exceptions
    try {
      routeStats.recordHealthyRequest(latency1, OK, HttpResponseStatusCodeCategory.SUCCESS);
    } catch (Exception e) {
      throw new AssertionError("Recording healthy requests should not throw exceptions", e);
    }

    // Verify Tehuti metrics functionality.
    validateHealthyRequestTehutiMetrics();

    // Verify OpenTelemetry metrics functionality
    validateHealthyRequestOtelMetrics();
  }

  private void validateHealthyRequestTehutiMetrics() {
    Map<String, ? extends Metric> metrics = metricsRepository.metrics();

    assertTrue(
        metrics.size() > 0,
        "Metrics repository should contain some metrics after RouteStats creation and recording");

    String expectedHealthyMetricName =
        String.format(".%s_%s--healthy_request_count.OccurrenceRate", CLUSTER_NAME, instanceUrl.getHost());

    assertNotNull(
        metrics.get(expectedHealthyMetricName),
        String.format("Metric: %s should exist", expectedHealthyMetricName));

    verifyTehutiResponseWaitingTime(metrics);
  }

  private void validateHealthyRequestOtelMetrics() {
    Attributes expectedAttributes = Attributes.builder()
        .put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), STORE_NAME)
        .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), CLUSTER_NAME)
        .put(VENICE_REQUEST_METHOD.getDimensionNameInDefaultFormat(), REQUEST_TYPE.getDimensionValue())
        .put(VENICE_ROUTE_NAME.getDimensionNameInDefaultFormat(), instanceUrl.getHost())
        .put(HTTP_RESPONSE_STATUS_CODE.getDimensionNameInDefaultFormat(), OK.getDimensionValue())
        .put(
            HTTP_RESPONSE_STATUS_CODE_CATEGORY.getDimensionNameInDefaultFormat(),
            HttpResponseStatusCodeCategory.SUCCESS.getDimensionValue())
        .put(VENICE_RESPONSE_STATUS_CODE_CATEGORY.getDimensionNameInDefaultFormat(), SUCCESS.getDimensionValue())
        .build();

    // Validate healthy request count metric
    validateLongPointDataFromCounter(
        inMemoryMetricReader,
        1,
        expectedAttributes,
        ROUTE_CALL_COUNT.getMetricEntity().getMetricName(),
        otelMetricPrefix);

    validateExponentialHistogramPointData(
        inMemoryMetricReader,
        100.0,
        100.0,
        1,
        100.0,
        expectedAttributes,
        ClientMetricEntity.ROUTE_CALL_TIME.getMetricEntity().getMetricName(),
        otelMetricPrefix);
  }

  @Test
  public void testQuotaExceededRequestCountMetric() {
    // Get a route stats instance
    ClusterRouteStats.RouteStats routeStats =
        clusterRouteStats.getRouteStats(metricsRepository, CLUSTER_NAME, INSTANCE_URL, REQUEST_TYPE);
    assertNotNull(routeStats);

    // Verify that RouteStats was created successfully
    assertNotNull(routeStats, "RouteStats should be created successfully");

    // Record quota exceeded request with proper parameters
    double latency = 200.0;

    // Test that recording doesn't throw exceptions
    try {
      routeStats.recordQuotaExceededRequest(latency, TOO_MANY_REQUESTS, HttpResponseStatusCodeCategory.CLIENT_ERROR);
    } catch (Exception e) {
      throw new AssertionError("Recording quota exceeded requests should not throw exceptions", e);
    }

    // Verify Tehuti metrics functionality
    validateQuotaExceededRequestTehutiMetrics();

    // Verify OpenTelemetry metrics functionality
    validateQuotaExceededRequestOtelMetrics();
  }

  private void validateQuotaExceededRequestTehutiMetrics() {
    Map<String, ? extends Metric> metrics = metricsRepository.metrics();

    assertTrue(
        metrics.size() > 0,
        "Metrics repository should contain some metrics after RouteStats creation and recording");

    String expectedQuotaExceededMetricName =
        String.format(".%s_%s--quota_exceeded_request_count.OccurrenceRate", CLUSTER_NAME, instanceUrl.getHost());

    assertNotNull(
        metrics.get(expectedQuotaExceededMetricName),
        String.format("Metric: %s should exist", expectedQuotaExceededMetricName));

    verifyTehutiResponseWaitingTime(metrics);
  }

  private void validateQuotaExceededRequestOtelMetrics() {
    Attributes expectedAttributes = Attributes.builder()
        .put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), STORE_NAME)
        .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), CLUSTER_NAME)
        .put(VENICE_REQUEST_METHOD.getDimensionNameInDefaultFormat(), REQUEST_TYPE.getDimensionValue())
        .put(VENICE_ROUTE_NAME.getDimensionNameInDefaultFormat(), instanceUrl.getHost())
        .put(HTTP_RESPONSE_STATUS_CODE.getDimensionNameInDefaultFormat(), TOO_MANY_REQUESTS.getDimensionValue())
        .put(
            HTTP_RESPONSE_STATUS_CODE_CATEGORY.getDimensionNameInDefaultFormat(),
            HttpResponseStatusCodeCategory.CLIENT_ERROR.getDimensionValue())
        .put(VENICE_RESPONSE_STATUS_CODE_CATEGORY.getDimensionNameInDefaultFormat(), FAIL.getDimensionValue())
        .build();

    // Validate quota exceeded request count metric
    validateLongPointDataFromCounter(
        inMemoryMetricReader,
        1,
        expectedAttributes,
        ROUTE_CALL_COUNT.getMetricEntity().getMetricName(),
        otelMetricPrefix);

    // Validate response waiting time metric for quota exceeded requests
    validateExponentialHistogramPointData(
        inMemoryMetricReader,
        200.0,
        200.0,
        1,
        200.0,
        expectedAttributes,
        ClientMetricEntity.ROUTE_CALL_TIME.getMetricEntity().getMetricName(),
        otelMetricPrefix);
  }

  @Test
  public void testInternalServerErrorRequestCountMetric() {
    // Get a route stats instance
    ClusterRouteStats.RouteStats routeStats =
        clusterRouteStats.getRouteStats(metricsRepository, CLUSTER_NAME, INSTANCE_URL, REQUEST_TYPE);
    assertNotNull(routeStats);

    // Verify that RouteStats was created successfully
    assertNotNull(routeStats, "RouteStats should be created successfully");

    // Record internal server error request with proper parameters
    double latency = 300.0;

    // Test that recording doesn't throw exceptions
    try {
      routeStats.recordInternalServerErrorRequest(
          latency,
          INTERNAL_SERVER_ERROR,
          HttpResponseStatusCodeCategory.SERVER_ERROR);
    } catch (Exception e) {
      throw new AssertionError("Recording internal server error requests should not throw exceptions", e);
    }

    // Verify Tehuti metrics functionality
    validateInternalServerErrorRequestTehutiMetrics();

    // Verify OpenTelemetry metrics functionality
    validateInternalServerErrorRequestOtelMetrics();
  }

  private void validateInternalServerErrorRequestTehutiMetrics() {
    Map<String, ? extends Metric> metrics = metricsRepository.metrics();

    assertTrue(
        metrics.size() > 0,
        "Metrics repository should contain some metrics after RouteStats creation and recording");

    String expectedInternalServerErrorMetricName = String
        .format(".%s_%s--internal_server_error_request_count.OccurrenceRate", CLUSTER_NAME, instanceUrl.getHost());

    assertNotNull(
        metrics.get(expectedInternalServerErrorMetricName),
        String.format("Metric: %s should exist", expectedInternalServerErrorMetricName));

    verifyTehutiResponseWaitingTime(metrics);
  }

  private void validateInternalServerErrorRequestOtelMetrics() {
    Attributes expectedAttributes = Attributes.builder()
        .put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), STORE_NAME)
        .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), CLUSTER_NAME)
        .put(VENICE_REQUEST_METHOD.getDimensionNameInDefaultFormat(), REQUEST_TYPE.getDimensionValue())
        .put(VENICE_ROUTE_NAME.getDimensionNameInDefaultFormat(), instanceUrl.getHost())
        .put(HTTP_RESPONSE_STATUS_CODE.getDimensionNameInDefaultFormat(), INTERNAL_SERVER_ERROR.getDimensionValue())
        .put(
            HTTP_RESPONSE_STATUS_CODE_CATEGORY.getDimensionNameInDefaultFormat(),
            HttpResponseStatusCodeCategory.SERVER_ERROR.getDimensionValue())
        .put(VENICE_RESPONSE_STATUS_CODE_CATEGORY.getDimensionNameInDefaultFormat(), FAIL.getDimensionValue())
        .build();

    // Validate internal server error request count metric
    validateLongPointDataFromCounter(
        inMemoryMetricReader,
        1,
        expectedAttributes,
        ROUTE_CALL_COUNT.getMetricEntity().getMetricName(),
        otelMetricPrefix);

    // Validate response waiting time metric for internal server error requests
    validateExponentialHistogramPointData(
        inMemoryMetricReader,
        300.0,
        300.0,
        1,
        300.0,
        expectedAttributes,
        ClientMetricEntity.ROUTE_CALL_TIME.getMetricEntity().getMetricName(),
        otelMetricPrefix);
  }

  @Test
  public void testServiceUnavailableRequestCountMetric() {
    // Get a route stats instance
    ClusterRouteStats.RouteStats routeStats =
        clusterRouteStats.getRouteStats(metricsRepository, CLUSTER_NAME, INSTANCE_URL, REQUEST_TYPE);
    assertNotNull(routeStats);

    // Verify that RouteStats was created successfully
    assertNotNull(routeStats, "RouteStats should be created successfully");

    // Record service unavailable request with proper parameters
    double latency = 400.0;

    // Test that recording doesn't throw exceptions
    try {
      routeStats
          .recordServiceUnavailableRequest(latency, SERVICE_UNAVAILABLE, HttpResponseStatusCodeCategory.SERVER_ERROR);
    } catch (Exception e) {
      throw new AssertionError("Recording service unavailable requests should not throw exceptions", e);
    }

    // Verify Tehuti metrics functionality
    validateServiceUnavailableRequestTehutiMetrics();

    // Verify OpenTelemetry metrics functionality
    validateServiceUnavailableRequestOtelMetrics();
  }

  private void validateServiceUnavailableRequestTehutiMetrics() {
    Map<String, ? extends Metric> metrics = metricsRepository.metrics();

    assertTrue(
        metrics.size() > 0,
        "Metrics repository should contain some metrics after RouteStats creation and recording");

    String expectedServiceUnavailableMetricName =
        String.format(".%s_%s--service_unavailable_request_count.OccurrenceRate", CLUSTER_NAME, instanceUrl.getHost());

    assertNotNull(
        metrics.get(expectedServiceUnavailableMetricName),
        String.format("Metric: %s should exist", expectedServiceUnavailableMetricName));

    verifyTehutiResponseWaitingTime(metrics);
  }

  private void validateServiceUnavailableRequestOtelMetrics() {
    Attributes expectedAttributes = Attributes.builder()
        .put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), STORE_NAME)
        .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), CLUSTER_NAME)
        .put(VENICE_REQUEST_METHOD.getDimensionNameInDefaultFormat(), REQUEST_TYPE.getDimensionValue())
        .put(VENICE_ROUTE_NAME.getDimensionNameInDefaultFormat(), instanceUrl.getHost())
        .put(HTTP_RESPONSE_STATUS_CODE.getDimensionNameInDefaultFormat(), SERVICE_UNAVAILABLE.getDimensionValue())
        .put(
            HTTP_RESPONSE_STATUS_CODE_CATEGORY.getDimensionNameInDefaultFormat(),
            HttpResponseStatusCodeCategory.SERVER_ERROR.getDimensionValue())
        .put(VENICE_RESPONSE_STATUS_CODE_CATEGORY.getDimensionNameInDefaultFormat(), FAIL.getDimensionValue())
        .build();

    // Validate service unavailable request count metric
    validateLongPointDataFromCounter(
        inMemoryMetricReader,
        1,
        expectedAttributes,
        ROUTE_CALL_COUNT.getMetricEntity().getMetricName(),
        otelMetricPrefix);

    // Validate response waiting time metric for service unavailable requests
    validateExponentialHistogramPointData(
        inMemoryMetricReader,
        400.0,
        400.0,
        1,
        400.0,
        expectedAttributes,
        ClientMetricEntity.ROUTE_CALL_TIME.getMetricEntity().getMetricName(),
        otelMetricPrefix);
  }

  @Test
  public void testLeakedRequestCountMetric() {
    // Get a route stats instance
    ClusterRouteStats.RouteStats routeStats =
        clusterRouteStats.getRouteStats(metricsRepository, CLUSTER_NAME, INSTANCE_URL, REQUEST_TYPE);
    assertNotNull(routeStats);

    // Verify that RouteStats was created successfully
    assertNotNull(routeStats, "RouteStats should be created successfully");

    // Record leaked request with proper parameters
    double latency = 500.0;

    // Test that recording doesn't throw exceptions
    try {
      routeStats.recordLeakedRequest(latency, GONE, HttpResponseStatusCodeCategory.CLIENT_ERROR);
    } catch (Exception e) {
      throw new AssertionError("Recording leaked requests should not throw exceptions", e);
    }

    // Verify Tehuti metrics functionality
    validateLeakedRequestTehutiMetrics();

    // Verify OpenTelemetry metrics functionality
    validateLeakedRequestOtelMetrics();
  }

  private void validateLeakedRequestTehutiMetrics() {
    Map<String, ? extends Metric> metrics = metricsRepository.metrics();

    assertTrue(
        metrics.size() > 0,
        "Metrics repository should contain some metrics after RouteStats creation and recording");

    String expectedLeakedMetricName =
        String.format(".%s_%s--leaked_request_count.OccurrenceRate", CLUSTER_NAME, instanceUrl.getHost());

    assertNotNull(
        metrics.get(expectedLeakedMetricName),
        String.format("Metric: %s should exist", expectedLeakedMetricName));

    verifyTehutiResponseWaitingTime(metrics);
  }

  private void validateLeakedRequestOtelMetrics() {
    Attributes expectedAttributes = Attributes.builder()
        .put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), STORE_NAME)
        .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), CLUSTER_NAME)
        .put(VENICE_REQUEST_METHOD.getDimensionNameInDefaultFormat(), REQUEST_TYPE.getDimensionValue())
        .put(VENICE_ROUTE_NAME.getDimensionNameInDefaultFormat(), instanceUrl.getHost())
        .put(HTTP_RESPONSE_STATUS_CODE.getDimensionNameInDefaultFormat(), GONE.getDimensionValue())
        .put(
            HTTP_RESPONSE_STATUS_CODE_CATEGORY.getDimensionNameInDefaultFormat(),
            HttpResponseStatusCodeCategory.CLIENT_ERROR.getDimensionValue())
        .put(VENICE_RESPONSE_STATUS_CODE_CATEGORY.getDimensionNameInDefaultFormat(), FAIL.getDimensionValue())
        .build();

    // Validate leaked request count metric
    validateLongPointDataFromCounter(
        inMemoryMetricReader,
        1,
        expectedAttributes,
        ROUTE_CALL_COUNT.getMetricEntity().getMetricName(),
        otelMetricPrefix);

    // Validate response waiting time metric for leaked requests
    validateExponentialHistogramPointData(
        inMemoryMetricReader,
        500.0,
        500.0,
        1,
        500.0,
        expectedAttributes,
        ClientMetricEntity.ROUTE_CALL_TIME.getMetricEntity().getMetricName(),
        otelMetricPrefix);
  }

  @Test
  public void testOtherErrorRequestCountMetric() {
    // Get a route stats instance
    ClusterRouteStats.RouteStats routeStats =
        clusterRouteStats.getRouteStats(metricsRepository, CLUSTER_NAME, INSTANCE_URL, REQUEST_TYPE);
    assertNotNull(routeStats);

    // Verify that RouteStats was created successfully
    assertNotNull(routeStats, "RouteStats should be created successfully");

    // Record other error request with proper parameters
    double latency = 600.0;

    // Test that recording doesn't throw exceptions
    try {
      routeStats.recordOtherErrorRequest(latency);
    } catch (Exception e) {
      throw new AssertionError("Recording other error requests should not throw exceptions", e);
    }

    // Verify Tehuti metrics functionality
    validateOtherErrorRequestTehutiMetrics();

    // Verify OpenTelemetry metrics functionality
    validateOtherErrorRequestOtelMetrics();
  }

  private void validateOtherErrorRequestTehutiMetrics() {
    Map<String, ? extends Metric> metrics = metricsRepository.metrics();

    assertTrue(
        metrics.size() > 0,
        "Metrics repository should contain some metrics after RouteStats creation and recording");

    String expectedOtherErrorMetricName =
        String.format(".%s_%s--other_error_request_count.OccurrenceRate", CLUSTER_NAME, instanceUrl.getHost());

    assertNotNull(
        metrics.get(expectedOtherErrorMetricName),
        String.format("Metric: %s should exist", expectedOtherErrorMetricName));

    verifyTehutiResponseWaitingTime(metrics);
  }

  private void validateOtherErrorRequestOtelMetrics() {
    Attributes expectedAttributes = Attributes.builder()
        .put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), STORE_NAME)
        .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), CLUSTER_NAME)
        .put(VENICE_REQUEST_METHOD.getDimensionNameInDefaultFormat(), REQUEST_TYPE.getDimensionValue())
        .put(VENICE_ROUTE_NAME.getDimensionNameInDefaultFormat(), instanceUrl.getHost())
        .put(HTTP_RESPONSE_STATUS_CODE.getDimensionNameInDefaultFormat(), UNKNOWN.getDimensionValue())
        .put(
            HTTP_RESPONSE_STATUS_CODE_CATEGORY.getDimensionNameInDefaultFormat(),
            HttpResponseStatusCodeCategory.UNKNOWN.getDimensionValue())
        .put(VENICE_RESPONSE_STATUS_CODE_CATEGORY.getDimensionNameInDefaultFormat(), FAIL.getDimensionValue())
        .build();

    // Validate other error request count metric
    validateLongPointDataFromCounter(
        inMemoryMetricReader,
        1,
        expectedAttributes,
        ROUTE_CALL_COUNT.getMetricEntity().getMetricName(),
        otelMetricPrefix);

    // Validate response waiting time metric for other error requests
    validateExponentialHistogramPointData(
        inMemoryMetricReader,
        600.0,
        600.0,
        1,
        600.0,
        expectedAttributes,
        ClientMetricEntity.ROUTE_CALL_TIME.getMetricEntity().getMetricName(),
        otelMetricPrefix);
  }

  @Test
  public void testPendingRequestCountMetric() {
    // Get a route stats instance
    ClusterRouteStats.RouteStats routeStats =
        clusterRouteStats.getRouteStats(metricsRepository, CLUSTER_NAME, INSTANCE_URL, REQUEST_TYPE);
    assertNotNull(routeStats);

    // Verify that RouteStats was created successfully
    assertNotNull(routeStats, "RouteStats should be created successfully");

    // Record pending request count with different values
    int pendingCount1 = 5;
    int pendingCount2 = 10;
    int pendingCount3 = 3;

    // Test that recording doesn't throw exceptions
    try {
      routeStats.recordPendingRequestCount(pendingCount1);
      routeStats.recordPendingRequestCount(pendingCount2);
      routeStats.recordPendingRequestCount(pendingCount3);
    } catch (Exception e) {
      throw new AssertionError("Recording pending request count should not throw exceptions", e);
    }

    // Verify Tehuti metrics functionality
    validatePendingRequestCountTehutiMetrics();

    // Verify OpenTelemetry metrics functionality
    validatePendingRequestCountOtelMetrics(pendingCount1, pendingCount2, pendingCount3);
  }

  private void validatePendingRequestCountTehutiMetrics() {
    Map<String, ? extends Metric> metrics = metricsRepository.metrics();

    assertTrue(
        metrics.size() > 0,
        "Metrics repository should contain some metrics after RouteStats creation and recording");

    String expectedPendingCountAvgMetricName =
        String.format(".%s_%s--pending_request_count.Avg", CLUSTER_NAME, instanceUrl.getHost());
    String expectedPendingCountMaxMetricName =
        String.format(".%s_%s--pending_request_count.Max", CLUSTER_NAME, instanceUrl.getHost());

    assertNotNull(
        metrics.get(expectedPendingCountAvgMetricName),
        String.format("Metric: %s should exist", expectedPendingCountAvgMetricName));
    assertNotNull(
        metrics.get(expectedPendingCountMaxMetricName),
        String.format("Metric: %s should exist", expectedPendingCountMaxMetricName));

    // Verify that the metrics have reasonable values
    assertTrue(
        metrics.get(expectedPendingCountAvgMetricName).value() > 0,
        String.format("Metric: %s should have a value greater than 0", expectedPendingCountAvgMetricName));
    assertTrue(
        metrics.get(expectedPendingCountMaxMetricName).value() >= 10,
        String.format("Metric: %s should have a max value of at least 10", expectedPendingCountMaxMetricName));
  }

  private void validatePendingRequestCountOtelMetrics(int count1, int count2, int count3) {
    Attributes expectedAttributes = Attributes.builder()
        .put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), STORE_NAME)
        .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), CLUSTER_NAME)
        .put(VENICE_REQUEST_METHOD.getDimensionNameInDefaultFormat(), REQUEST_TYPE.getDimensionValue())
        .put(VENICE_ROUTE_NAME.getDimensionNameInDefaultFormat(), instanceUrl.getHost())
        .build();

    // Validate pending request count metric using MIN_MAX_COUNT_SUM_AGGREGATIONS
    // This should capture min, max, count, and sum values
    validateHistogramPointData(
        inMemoryMetricReader,
        count3, // min
        count2, // max
        3, // count
        count1 + count2 + count3, // sum
        expectedAttributes,
        ClientMetricEntity.ROUTE_REQUEST_PENDING_COUNT.getMetricEntity().getMetricName(),
        otelMetricPrefix);
  }

  @Test
  public void testRejectionRatioMetric() {
    // Get a route stats instance
    ClusterRouteStats.RouteStats routeStats =
        clusterRouteStats.getRouteStats(metricsRepository, CLUSTER_NAME, INSTANCE_URL, REQUEST_TYPE);
    assertNotNull(routeStats);

    // Verify that RouteStats was created successfully
    assertNotNull(routeStats, "RouteStats should be created successfully");

    // Record rejection ratio with different values and reasons
    double rejectionRatio1 = 0.15; // 15% rejection
    double rejectionRatio2 = 0.05; // 5% rejection

    // Test that recording doesn't throw exceptions
    try {
      routeStats.recordRejectionRatio(rejectionRatio1, RejectionReason.THROTTLED_BY_LOAD_CONTROLLER);
      routeStats.recordRejectionRatio(rejectionRatio2, RejectionReason.THROTTLED_BY_LOAD_CONTROLLER);
    } catch (Exception e) {
      throw new AssertionError("Recording rejection ratio should not throw exceptions", e);
    }

    // Verify Tehuti metrics functionality
    validateRejectionRatioTehutiMetrics();

    // Verify OpenTelemetry metrics functionality
    validateRejectionRatioOtelMetrics(rejectionRatio1, rejectionRatio2);
  }

  private void validateRejectionRatioTehutiMetrics() {
    Map<String, ? extends Metric> metrics = metricsRepository.metrics();

    assertTrue(
        metrics.size() > 0,
        "Metrics repository should contain some metrics after RouteStats creation and recording");

    String expectedRejectionRatioAvgMetricName =
        String.format(".%s_%s--rejection_ratio.Avg", CLUSTER_NAME, instanceUrl.getHost());
    String expectedRejectionRatioMaxMetricName =
        String.format(".%s_%s--rejection_ratio.Max", CLUSTER_NAME, instanceUrl.getHost());

    assertNotNull(
        metrics.get(expectedRejectionRatioAvgMetricName),
        String.format("Metric: %s should exist", expectedRejectionRatioAvgMetricName));
    assertNotNull(
        metrics.get(expectedRejectionRatioMaxMetricName),
        String.format("Metric: %s should exist", expectedRejectionRatioMaxMetricName));

    // Verify that the metrics have reasonable values
    assertTrue(
        metrics.get(expectedRejectionRatioAvgMetricName).value() > 0,
        String.format("Metric: %s should have a value greater than 0", expectedRejectionRatioAvgMetricName));
    assertTrue(
        metrics.get(expectedRejectionRatioMaxMetricName).value() >= 0.15,
        String.format("Metric: %s should have a max value of at least 0.15", expectedRejectionRatioMaxMetricName));
  }

  private void validateRejectionRatioOtelMetrics(double ratio1, double ratio2) {
    // Test with THROTTLED_BY_LOAD_CONTROLLER reason
    Attributes expectedAttributesThrottled = Attributes.builder()
        .put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), STORE_NAME)
        .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), CLUSTER_NAME)
        .put(VENICE_REQUEST_METHOD.getDimensionNameInDefaultFormat(), REQUEST_TYPE.getDimensionValue())
        .put(VENICE_ROUTE_NAME.getDimensionNameInDefaultFormat(), instanceUrl.getHost())
        .put(
            VENICE_REQUEST_REJECTION_REASON.getDimensionNameInDefaultFormat(),
            RejectionReason.THROTTLED_BY_LOAD_CONTROLLER.getDimensionValue())
        .build();

    validateHistogramPointData(
        inMemoryMetricReader,
        Math.min(ratio1, ratio2), // min
        Math.max(ratio1, ratio2), // max
        2, // count
        ratio1 + ratio2, // sum
        expectedAttributesThrottled,
        ClientMetricEntity.ROUTE_REQUEST_REJECTION_RATIO.getMetricEntity().getMetricName(),
        otelMetricPrefix);
  }

  private void verifyTehutiResponseWaitingTime(Map<String, ? extends Metric> metrics) {
    // Verify the ROUTE_CALL_TIME metric existence for internal server error requests
    String responseWaitingTime50thPercentile =
        String.format(".%s_%s--response_waiting_time.50thPercentile", CLUSTER_NAME, instanceUrl.getHost());
    String responseWaitingTime95thPercentile =
        String.format(".%s_%s--response_waiting_time.95thPercentile", CLUSTER_NAME, instanceUrl.getHost());
    String responseWaitingTime99thPercentile =
        String.format(".%s_%s--response_waiting_time.99thPercentile", CLUSTER_NAME, instanceUrl.getHost());

    assertTrue(
        metrics.get(responseWaitingTime50thPercentile).value() > 0,
        String.format("Metric: %s should have a value greater than 0", responseWaitingTime50thPercentile));
    assertTrue(
        metrics.get(responseWaitingTime95thPercentile).value() > 0,
        String.format("Metric: %s should have a value greater than 0", responseWaitingTime95thPercentile));
    assertTrue(
        metrics.get(responseWaitingTime99thPercentile).value() > 0,
        String.format("Metric: %s should have a value greater than 0", responseWaitingTime99thPercentile));
  }
}
