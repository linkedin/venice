package com.linkedin.venice.router.stats;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.router.RouterServer;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.metrics.MetricEntity;
import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import com.linkedin.venice.utils.Utils;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.testng.annotations.Test;


public class RouterMetricEntityTest {
  @Test
  public void testRouterMetricEntities() {
    Map<RouterMetricEntity, MetricEntity> expectedMetrics = new HashMap<>();
    expectedMetrics.put(
        RouterMetricEntity.CALL_COUNT,
        new MetricEntity(
            "call_count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Count of all requests during response handling along with response codes",
            Utils.setOf(
                VeniceMetricsDimensions.VENICE_STORE_NAME,
                VeniceMetricsDimensions.VENICE_CLUSTER_NAME,
                VeniceMetricsDimensions.VENICE_REQUEST_METHOD,
                VeniceMetricsDimensions.HTTP_RESPONSE_STATUS_CODE,
                VeniceMetricsDimensions.HTTP_RESPONSE_STATUS_CODE_CATEGORY,
                VeniceMetricsDimensions.VENICE_RESPONSE_STATUS_CODE_CATEGORY)));
    expectedMetrics.put(
        RouterMetricEntity.CALL_TIME,
        new MetricEntity(
            "call_time",
            MetricType.HISTOGRAM,
            MetricUnit.MILLISECOND,
            "Latency based on all responses",
            Utils.setOf(
                VeniceMetricsDimensions.VENICE_STORE_NAME,
                VeniceMetricsDimensions.VENICE_CLUSTER_NAME,
                VeniceMetricsDimensions.VENICE_REQUEST_METHOD,
                VeniceMetricsDimensions.HTTP_RESPONSE_STATUS_CODE,
                VeniceMetricsDimensions.HTTP_RESPONSE_STATUS_CODE_CATEGORY,
                VeniceMetricsDimensions.VENICE_RESPONSE_STATUS_CODE_CATEGORY)));
    expectedMetrics.put(
        RouterMetricEntity.CALL_SIZE,
        new MetricEntity(
            "call_size",
            MetricType.HISTOGRAM,
            MetricUnit.BYTES,
            "Size of request and response in bytes",
            Utils.setOf(
                VeniceMetricsDimensions.VENICE_STORE_NAME,
                VeniceMetricsDimensions.VENICE_CLUSTER_NAME,
                VeniceMetricsDimensions.VENICE_REQUEST_METHOD,
                VeniceMetricsDimensions.VENICE_MESSAGE_TYPE)));
    expectedMetrics.put(
        RouterMetricEntity.KEY_SIZE,
        new MetricEntity(
            "key_size",
            MetricType.HISTOGRAM,
            MetricUnit.BYTES,
            "Size of keys in bytes",
            Utils.setOf(
                VeniceMetricsDimensions.VENICE_STORE_NAME,
                VeniceMetricsDimensions.VENICE_CLUSTER_NAME,
                VeniceMetricsDimensions.VENICE_REQUEST_METHOD)));
    expectedMetrics.put(
        RouterMetricEntity.KEY_COUNT,
        new MetricEntity(
            "key_count",
            MetricType.HISTOGRAM,
            MetricUnit.NUMBER,
            "Count of keys during response handling along with response codes",
            Utils.setOf(
                VeniceMetricsDimensions.VENICE_STORE_NAME,
                VeniceMetricsDimensions.VENICE_CLUSTER_NAME,
                VeniceMetricsDimensions.VENICE_REQUEST_METHOD,
                VeniceMetricsDimensions.HTTP_RESPONSE_STATUS_CODE,
                VeniceMetricsDimensions.HTTP_RESPONSE_STATUS_CODE_CATEGORY,
                VeniceMetricsDimensions.VENICE_RESPONSE_STATUS_CODE_CATEGORY)));
    expectedMetrics.put(
        RouterMetricEntity.RETRY_COUNT,
        new MetricEntity(
            "retry_count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Count of retries triggered",
            Utils.setOf(
                VeniceMetricsDimensions.VENICE_STORE_NAME,
                VeniceMetricsDimensions.VENICE_CLUSTER_NAME,
                VeniceMetricsDimensions.VENICE_REQUEST_METHOD,
                VeniceMetricsDimensions.VENICE_REQUEST_RETRY_TYPE)));
    expectedMetrics.put(
        RouterMetricEntity.ALLOWED_RETRY_COUNT,
        new MetricEntity(
            "allowed_retry_count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Count of allowed retry requests",
            Utils.setOf(
                VeniceMetricsDimensions.VENICE_STORE_NAME,
                VeniceMetricsDimensions.VENICE_CLUSTER_NAME,
                VeniceMetricsDimensions.VENICE_REQUEST_METHOD)));
    expectedMetrics.put(
        RouterMetricEntity.DISALLOWED_RETRY_COUNT,
        new MetricEntity(
            "disallowed_retry_count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Count of disallowed retry requests",
            Utils.setOf(
                VeniceMetricsDimensions.VENICE_STORE_NAME,
                VeniceMetricsDimensions.VENICE_CLUSTER_NAME,
                VeniceMetricsDimensions.VENICE_REQUEST_METHOD)));
    expectedMetrics.put(
        RouterMetricEntity.RETRY_DELAY,
        new MetricEntity(
            "retry_delay",
            MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS,
            MetricUnit.MILLISECOND,
            "Retry delay time",
            Utils.setOf(
                VeniceMetricsDimensions.VENICE_STORE_NAME,
                VeniceMetricsDimensions.VENICE_CLUSTER_NAME,
                VeniceMetricsDimensions.VENICE_REQUEST_METHOD)));
    expectedMetrics.put(
        RouterMetricEntity.ABORTED_RETRY_COUNT,
        new MetricEntity(
            "aborted_retry_count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Count of aborted retry requests",
            Utils.setOf(
                VeniceMetricsDimensions.VENICE_STORE_NAME,
                VeniceMetricsDimensions.VENICE_CLUSTER_NAME,
                VeniceMetricsDimensions.VENICE_REQUEST_METHOD,
                VeniceMetricsDimensions.VENICE_REQUEST_RETRY_ABORT_REASON)));

    for (RouterMetricEntity metric: RouterMetricEntity.values()) {
      MetricEntity actual = metric.getMetricEntity();
      MetricEntity expected = expectedMetrics.get(metric);

      assertNotNull(expected, "No expected definition for " + metric.name());
      assertNotNull(actual.getMetricName(), "Metric name should not be null for " + metric.name());
      assertEquals(actual.getMetricName(), expected.getMetricName(), "Unexpected metric name for " + metric.name());
      assertNotNull(actual.getMetricType(), "Metric type should not be null for " + metric.name());
      assertEquals(actual.getMetricType(), expected.getMetricType(), "Unexpected metric type for " + metric.name());
      assertNotNull(actual.getUnit(), "Metric unit should not be null for " + metric.name());
      assertEquals(actual.getUnit(), expected.getUnit(), "Unexpected metric unit for " + metric.name());
      assertNotNull(actual.getDescription(), "Metric description should not be null for " + metric.name());
      assertEquals(
          actual.getDescription(),
          expected.getDescription(),
          "Unexpected metric description for " + metric.name());
      assertNotNull(actual.getDimensionsList(), "Metric dimensions should not be null for " + metric.name());
      assertEquals(
          actual.getDimensionsList(),
          expected.getDimensionsList(),
          "Unexpected metric dimensions for " + metric.name());
    }

    // Convert expectedMetrics to a Collection for comparison
    Collection<MetricEntity> expectedMetricEntities = expectedMetrics.values();

    // Assert size
    assertEquals(
        RouterServer.ROUTER_SERVICE_METRIC_ENTITIES.size(),
        expectedMetricEntities.size(),
        "Unexpected size of ROUTER_SERVICE_METRIC_ENTITIES");

    // Assert contents
    for (MetricEntity actual: RouterServer.ROUTER_SERVICE_METRIC_ENTITIES) {
      boolean found = false;
      for (MetricEntity expected: expectedMetricEntities) {
        if (metricEntitiesEqual(actual, expected)) {
          found = true;
          break;
        }
      }
      assertTrue(found, "Unexpected MetricEntity found: " + actual.getMetricName());
    }
  }

  private boolean metricEntitiesEqual(MetricEntity actual, MetricEntity expected) {
    return Objects.equals(actual.getMetricName(), expected.getMetricName())
        && actual.getMetricType() == expected.getMetricType() && actual.getUnit() == expected.getUnit()
        && Objects.equals(actual.getDescription(), expected.getDescription())
        && Objects.equals(actual.getDimensionsList(), expected.getDimensionsList());
  }
}
