package com.linkedin.venice.stats.routing;

import static com.linkedin.venice.stats.ClientType.FAST_CLIENT;
import static com.linkedin.venice.stats.VeniceMetricsRepository.getVeniceMetricsRepository;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_HELIX_GROUP_ID;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static com.linkedin.venice.utils.OpenTelemetryDataTestUtils.validateHistogramPointData;
import static com.linkedin.venice.utils.OpenTelemetryDataTestUtils.validateLongPointDataFromCounter;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.client.stats.BasicClientStats;
import com.linkedin.venice.stats.VeniceMetricsRepository;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.tehuti.Metric;
import io.tehuti.metrics.MetricsRepository;
import java.util.Map;
import org.testng.annotations.Test;


public class HelixGroupStatsTest {
  @Test
  public void testMetrics() {
    MetricsRepository metricsRepository = new MetricsRepository();
    String storeName = "test_store";

    HelixGroupStats stats = new HelixGroupStats(metricsRepository, storeName);

    // No data points
    assertEquals(stats.getGroupResponseWaitingTimeAvg(0), -1d);

    int testGroupId = 1;
    stats.recordGroupNum(3);
    stats.recordGroupRequest(testGroupId);
    stats.recordGroupPendingRequest(testGroupId, 2);
    stats.recordGroupResponseWaitingTime(testGroupId, 10);

    assertTrue(stats.getGroupResponseWaitingTimeAvg(testGroupId) > 0);
  }

  @Test
  public void testHelixGroupCountMetric() {
    // Set up Venice metrics repository with both Tehuti and OpenTelemetry support
    InMemoryMetricReader inMemoryMetricReader = InMemoryMetricReader.create();
    VeniceMetricsRepository metricsRepository =
        getVeniceMetricsRepository(FAST_CLIENT, BasicClientStats.CLIENT_METRIC_ENTITIES, true, inMemoryMetricReader);
    String otelMetricPrefix = FAST_CLIENT.getMetricsPrefix();
    String storeName = "test_store";

    HelixGroupStats stats = new HelixGroupStats(metricsRepository, storeName);

    // Record group count
    stats.recordGroupNum(3);
    stats.recordGroupNum(5);
    stats.recordGroupNum(4);

    // Verify Tehuti metrics
    Map<String, ? extends Metric> metrics = metricsRepository.metrics();
    assertTrue(metrics.size() > 0, "Metrics repository should contain metrics after recording");

    String expectedTehutiMetricName = "." + storeName + "_HelixGroupStats--group_count.Avg";
    Metric tehutiMetric = metrics.get(expectedTehutiMetricName);
    assertNotNull(tehutiMetric, "Tehuti metric should exist: " + expectedTehutiMetricName);

    // Verify the average is correct: (3 + 5 + 4) / 3 = 4.0
    double avgValue = tehutiMetric.value();
    assertEquals(avgValue, 4.0, 0.01, "Average group count should be 4.0");

    // Verify OpenTelemetry metrics
    Attributes expectedAttributes =
        Attributes.builder().put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), storeName).build();

    // Validate histogram point data: min=3, max=5, count=3, sum=12
    validateHistogramPointData(
        inMemoryMetricReader,
        3.0, // min
        5.0, // max
        3, // count
        12.0, // sum (3 + 5 + 4)
        expectedAttributes,
        RoutingMetricEntity.HELIX_GROUP_COUNT.getMetricEntity().getMetricName(),
        otelMetricPrefix);
  }

  @Test
  public void testGroupRequestCountMetric() {
    // Set up Venice metrics repository with both Tehuti and OpenTelemetry support
    InMemoryMetricReader inMemoryMetricReader = InMemoryMetricReader.create();
    VeniceMetricsRepository metricsRepository =
        getVeniceMetricsRepository(FAST_CLIENT, BasicClientStats.CLIENT_METRIC_ENTITIES, true, inMemoryMetricReader);
    String otelMetricPrefix = FAST_CLIENT.getMetricsPrefix();
    String storeName = "test_store";

    HelixGroupStats stats = new HelixGroupStats(metricsRepository, storeName);

    // Record requests for different groups
    int groupId0 = 0;
    int groupId1 = 1;
    int groupId2 = 2;

    // Record 5 requests for group 0
    for (int i = 0; i < 5; i++) {
      stats.recordGroupRequest(groupId0);
    }

    // Record 3 requests for group 1
    for (int i = 0; i < 3; i++) {
      stats.recordGroupRequest(groupId1);
    }

    // Record 7 requests for group 2
    for (int i = 0; i < 7; i++) {
      stats.recordGroupRequest(groupId2);
    }

    // Verify Tehuti metrics for each group
    Map<String, ? extends Metric> metrics = metricsRepository.metrics();
    assertTrue(metrics.size() > 0, "Metrics repository should contain metrics after recording");

    // Verify group 0 metrics
    String expectedTehutiMetricNameGroup0 = "." + storeName + "_HelixGroupStats--group_0_request.OccurrenceRate";
    Metric tehutiMetricGroup0 = metrics.get(expectedTehutiMetricNameGroup0);
    assertNotNull(tehutiMetricGroup0, "Tehuti metric should exist for group 0: " + expectedTehutiMetricNameGroup0);
    assertTrue(tehutiMetricGroup0.value() > 0, "Group 0 request rate should be greater than 0");

    // Verify group 1 metrics
    String expectedTehutiMetricNameGroup1 = "." + storeName + "_HelixGroupStats--group_1_request.OccurrenceRate";
    Metric tehutiMetricGroup1 = metrics.get(expectedTehutiMetricNameGroup1);
    assertNotNull(tehutiMetricGroup1, "Tehuti metric should exist for group 1: " + expectedTehutiMetricNameGroup1);
    assertTrue(tehutiMetricGroup1.value() > 0, "Group 1 request rate should be greater than 0");

    // Verify group 2 metrics
    String expectedTehutiMetricNameGroup2 = "." + storeName + "_HelixGroupStats--group_2_request.OccurrenceRate";
    Metric tehutiMetricGroup2 = metrics.get(expectedTehutiMetricNameGroup2);
    assertNotNull(tehutiMetricGroup2, "Tehuti metric should exist for group 2: " + expectedTehutiMetricNameGroup2);
    assertTrue(tehutiMetricGroup2.value() > 0, "Group 2 request rate should be greater than 0");

    // Verify OpenTelemetry metrics for each group
    // Group 0: 5 requests
    Attributes expectedAttributesGroup0 = Attributes.builder()
        .put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), storeName)
        .put(VENICE_HELIX_GROUP_ID.getDimensionNameInDefaultFormat(), String.valueOf(groupId0))
        .build();

    validateLongPointDataFromCounter(
        inMemoryMetricReader,
        5, // count
        expectedAttributesGroup0,
        RoutingMetricEntity.HELIX_GROUP_CALL_COUNT.getMetricEntity().getMetricName(),
        otelMetricPrefix);

    // Group 1: 3 requests
    Attributes expectedAttributesGroup1 = Attributes.builder()
        .put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), storeName)
        .put(VENICE_HELIX_GROUP_ID.getDimensionNameInDefaultFormat(), String.valueOf(groupId1))
        .build();

    validateLongPointDataFromCounter(
        inMemoryMetricReader,
        3, // count
        expectedAttributesGroup1,
        RoutingMetricEntity.HELIX_GROUP_CALL_COUNT.getMetricEntity().getMetricName(),
        otelMetricPrefix);

    // Group 2: 7 requests
    Attributes expectedAttributesGroup2 = Attributes.builder()
        .put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), storeName)
        .put(VENICE_HELIX_GROUP_ID.getDimensionNameInDefaultFormat(), String.valueOf(groupId2))
        .build();

    validateLongPointDataFromCounter(
        inMemoryMetricReader,
        7, // count
        expectedAttributesGroup2,
        RoutingMetricEntity.HELIX_GROUP_CALL_COUNT.getMetricEntity().getMetricName(),
        otelMetricPrefix);
  }

  @Test
  public void testGroupPendingRequestMetric() {
    // Set up Venice metrics repository with both Tehuti and OpenTelemetry support
    InMemoryMetricReader inMemoryMetricReader = InMemoryMetricReader.create();
    VeniceMetricsRepository metricsRepository =
        getVeniceMetricsRepository(FAST_CLIENT, BasicClientStats.CLIENT_METRIC_ENTITIES, true, inMemoryMetricReader);
    String otelMetricPrefix = FAST_CLIENT.getMetricsPrefix();
    String storeName = "test_store";

    HelixGroupStats stats = new HelixGroupStats(metricsRepository, storeName);

    // Record pending requests for different groups with varying values
    int groupId0 = 0;
    int groupId1 = 1;
    int groupId2 = 2;

    // Record multiple pending request values for group 0
    stats.recordGroupPendingRequest(groupId0, 5);
    stats.recordGroupPendingRequest(groupId0, 10);
    stats.recordGroupPendingRequest(groupId0, 3);

    // Record multiple pending request values for group 1
    stats.recordGroupPendingRequest(groupId1, 8);
    stats.recordGroupPendingRequest(groupId1, 12);

    // Record multiple pending request values for group 2
    stats.recordGroupPendingRequest(groupId2, 2);
    stats.recordGroupPendingRequest(groupId2, 6);
    stats.recordGroupPendingRequest(groupId2, 4);
    stats.recordGroupPendingRequest(groupId2, 8);

    // Verify Tehuti metrics for each group
    Map<String, ? extends Metric> metrics = metricsRepository.metrics();
    assertTrue(metrics.size() > 0, "Metrics repository should contain metrics after recording");

    // Verify group 0 metrics - Average of (5, 10, 3) = 6.0
    String expectedTehutiMetricNameGroup0 = "." + storeName + "_HelixGroupStats--group_0_pending_request.Avg";
    Metric tehutiMetricGroup0 = metrics.get(expectedTehutiMetricNameGroup0);
    assertNotNull(tehutiMetricGroup0, "Tehuti metric should exist for group 0: " + expectedTehutiMetricNameGroup0);
    assertEquals(tehutiMetricGroup0.value(), 6.0, 0.01, "Group 0 average pending requests should be 6.0");

    // Verify group 1 metrics - Average of (8, 12) = 10.0
    String expectedTehutiMetricNameGroup1 = "." + storeName + "_HelixGroupStats--group_1_pending_request.Avg";
    Metric tehutiMetricGroup1 = metrics.get(expectedTehutiMetricNameGroup1);
    assertNotNull(tehutiMetricGroup1, "Tehuti metric should exist for group 1: " + expectedTehutiMetricNameGroup1);
    assertEquals(tehutiMetricGroup1.value(), 10.0, 0.01, "Group 1 average pending requests should be 10.0");

    // Verify group 2 metrics - Average of (2, 6, 4, 8) = 5.0
    String expectedTehutiMetricNameGroup2 = "." + storeName + "_HelixGroupStats--group_2_pending_request.Avg";
    Metric tehutiMetricGroup2 = metrics.get(expectedTehutiMetricNameGroup2);
    assertNotNull(tehutiMetricGroup2, "Tehuti metric should exist for group 2: " + expectedTehutiMetricNameGroup2);
    assertEquals(tehutiMetricGroup2.value(), 5.0, 0.01, "Group 2 average pending requests should be 5.0");

    // Verify OpenTelemetry metrics for each group
    // Group 0: min=3, max=10, count=3, sum=18
    Attributes expectedAttributesGroup0 = Attributes.builder()
        .put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), storeName)
        .put(VENICE_HELIX_GROUP_ID.getDimensionNameInDefaultFormat(), String.valueOf(groupId0))
        .build();

    validateHistogramPointData(
        inMemoryMetricReader,
        3.0, // min
        10.0, // max
        3, // count
        18.0, // sum (5 + 10 + 3)
        expectedAttributesGroup0,
        RoutingMetricEntity.HELIX_GROUP_REQUEST_PENDING_REQUESTS.getMetricEntity().getMetricName(),
        otelMetricPrefix);

    // Group 1: min=8, max=12, count=2, sum=20
    Attributes expectedAttributesGroup1 = Attributes.builder()
        .put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), storeName)
        .put(VENICE_HELIX_GROUP_ID.getDimensionNameInDefaultFormat(), String.valueOf(groupId1))
        .build();

    validateHistogramPointData(
        inMemoryMetricReader,
        8.0, // min
        12.0, // max
        2, // count
        20.0, // sum (8 + 12)
        expectedAttributesGroup1,
        RoutingMetricEntity.HELIX_GROUP_REQUEST_PENDING_REQUESTS.getMetricEntity().getMetricName(),
        otelMetricPrefix);

    // Group 2: min=2, max=8, count=4, sum=20
    Attributes expectedAttributesGroup2 = Attributes.builder()
        .put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), storeName)
        .put(VENICE_HELIX_GROUP_ID.getDimensionNameInDefaultFormat(), String.valueOf(groupId2))
        .build();

    validateHistogramPointData(
        inMemoryMetricReader,
        2.0, // min
        8.0, // max
        4, // count
        20.0, // sum (2 + 6 + 4 + 8)
        expectedAttributesGroup2,
        RoutingMetricEntity.HELIX_GROUP_REQUEST_PENDING_REQUESTS.getMetricEntity().getMetricName(),
        otelMetricPrefix);
  }

  @Test
  public void testGroupResponseWaitingTimeMetric() {
    // Set up Venice metrics repository with both Tehuti and OpenTelemetry support
    InMemoryMetricReader inMemoryMetricReader = InMemoryMetricReader.create();
    VeniceMetricsRepository metricsRepository =
        getVeniceMetricsRepository(FAST_CLIENT, BasicClientStats.CLIENT_METRIC_ENTITIES, true, inMemoryMetricReader);
    String otelMetricPrefix = FAST_CLIENT.getMetricsPrefix();
    String storeName = "test_store";

    HelixGroupStats stats = new HelixGroupStats(metricsRepository, storeName);

    // Record response waiting times for different groups
    int groupId0 = 0;
    int groupId1 = 1;
    int groupId2 = 2;

    // Record multiple response waiting times for group 0 (in milliseconds)
    stats.recordGroupResponseWaitingTime(groupId0, 50.0);
    stats.recordGroupResponseWaitingTime(groupId0, 100.0);
    stats.recordGroupResponseWaitingTime(groupId0, 75.0);

    // Record multiple response waiting times for group 1
    stats.recordGroupResponseWaitingTime(groupId1, 120.0);
    stats.recordGroupResponseWaitingTime(groupId1, 80.0);

    // Record multiple response waiting times for group 2
    stats.recordGroupResponseWaitingTime(groupId2, 30.0);
    stats.recordGroupResponseWaitingTime(groupId2, 90.0);
    stats.recordGroupResponseWaitingTime(groupId2, 60.0);
    stats.recordGroupResponseWaitingTime(groupId2, 40.0);

    // Verify Tehuti metrics for each group
    Map<String, ? extends Metric> metrics = metricsRepository.metrics();
    assertTrue(metrics.size() > 0, "Metrics repository should contain metrics after recording");

    // Verify group 0 metrics - Average of (50, 100, 75) = 75.0
    double avgGroup0 = stats.getGroupResponseWaitingTimeAvg(groupId0);
    assertEquals(avgGroup0, 75.0, 0.01, "Group 0 average response waiting time should be 75.0ms");

    // Verify group 1 metrics - Average of (120, 80) = 100.0
    double avgGroup1 = stats.getGroupResponseWaitingTimeAvg(groupId1);
    assertEquals(avgGroup1, 100.0, 0.01, "Group 1 average response waiting time should be 100.0ms");

    // Verify group 2 metrics - Average of (30, 90, 60, 40) = 55.0
    double avgGroup2 = stats.getGroupResponseWaitingTimeAvg(groupId2);
    assertEquals(avgGroup2, 55.0, 0.01, "Group 2 average response waiting time should be 55.0ms");

    // Verify Tehuti metric names exist
    String expectedTehutiMetricNameGroup0 = "." + storeName + "_HelixGroupStats--group_0_response_waiting_time.Avg";
    assertNotNull(
        metrics.get(expectedTehutiMetricNameGroup0),
        "Tehuti metric should exist for group 0: " + expectedTehutiMetricNameGroup0);

    String expectedTehutiMetricNameGroup1 = "." + storeName + "_HelixGroupStats--group_1_response_waiting_time.Avg";
    assertNotNull(
        metrics.get(expectedTehutiMetricNameGroup1),
        "Tehuti metric should exist for group 1: " + expectedTehutiMetricNameGroup1);

    String expectedTehutiMetricNameGroup2 = "." + storeName + "_HelixGroupStats--group_2_response_waiting_time.Avg";
    assertNotNull(
        metrics.get(expectedTehutiMetricNameGroup2),
        "Tehuti metric should exist for group 2: " + expectedTehutiMetricNameGroup2);

    // Verify OpenTelemetry metrics for each group
    // Group 0: min=50, max=100, count=3, sum=225
    Attributes expectedAttributesGroup0 = Attributes.builder()
        .put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), storeName)
        .put(VENICE_HELIX_GROUP_ID.getDimensionNameInDefaultFormat(), String.valueOf(groupId0))
        .build();

    validateHistogramPointData(
        inMemoryMetricReader,
        50.0, // min
        100.0, // max
        3, // count
        225.0, // sum (50 + 100 + 75)
        expectedAttributesGroup0,
        RoutingMetricEntity.HELIX_GROUP_CALL_TIME.getMetricEntity().getMetricName(),
        otelMetricPrefix);

    // Group 1: min=80, max=120, count=2, sum=200
    Attributes expectedAttributesGroup1 = Attributes.builder()
        .put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), storeName)
        .put(VENICE_HELIX_GROUP_ID.getDimensionNameInDefaultFormat(), String.valueOf(groupId1))
        .build();

    validateHistogramPointData(
        inMemoryMetricReader,
        80.0, // min
        120.0, // max
        2, // count
        200.0, // sum (120 + 80)
        expectedAttributesGroup1,
        RoutingMetricEntity.HELIX_GROUP_CALL_TIME.getMetricEntity().getMetricName(),
        otelMetricPrefix);

    // Group 2: min=30, max=90, count=4, sum=220
    Attributes expectedAttributesGroup2 = Attributes.builder()
        .put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), storeName)
        .put(VENICE_HELIX_GROUP_ID.getDimensionNameInDefaultFormat(), String.valueOf(groupId2))
        .build();

    validateHistogramPointData(
        inMemoryMetricReader,
        30.0, // min
        90.0, // max
        4, // count
        220.0, // sum (30 + 90 + 60 + 40)
        expectedAttributesGroup2,
        RoutingMetricEntity.HELIX_GROUP_CALL_TIME.getMetricEntity().getMetricName(),
        otelMetricPrefix);
  }
}
