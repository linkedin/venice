package com.linkedin.venice.client.store;

import static com.linkedin.venice.client.stats.BasicClientStats.CLIENT_METRIC_ENTITIES;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.client.stats.BasicClientStats;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.metrics.MetricEntity;
import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import com.linkedin.venice.utils.Utils;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.testng.Assert;
import org.testng.annotations.Test;


public class ClientConfigTest {
  @Test
  public void testCloneConfig() {
    ClientConfig config = new ClientConfig("Test-store");
    ClientConfig clonedConfig = ClientConfig.cloneConfig(config);

    Assert.assertEquals(config, clonedConfig);
  }

  @Test
  public void testClientMetricEntities() {
    Map<BasicClientStats.BasicClientMetricEntity, MetricEntity> expectedMetrics = new HashMap<>();
    expectedMetrics.put(
        BasicClientStats.BasicClientMetricEntity.CALL_COUNT,
        new MetricEntity(
            "call_count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Count of all requests during response handling along with response codes",
            Utils.setOf(
                VeniceMetricsDimensions.VENICE_STORE_NAME,
                VeniceMetricsDimensions.VENICE_REQUEST_METHOD,
                VeniceMetricsDimensions.VENICE_CLIENT_TYPE,
                VeniceMetricsDimensions.HTTP_RESPONSE_STATUS_CODE,
                VeniceMetricsDimensions.HTTP_RESPONSE_STATUS_CODE_CATEGORY,
                VeniceMetricsDimensions.VENICE_RESPONSE_STATUS_CODE_CATEGORY)));
    expectedMetrics.put(
        BasicClientStats.BasicClientMetricEntity.CALL_TIME,
        new MetricEntity(
            "call_time",
            MetricType.HISTOGRAM,
            MetricUnit.MILLISECOND,
            "Latency based on all responses",
            Utils.setOf(
                VeniceMetricsDimensions.VENICE_STORE_NAME,
                VeniceMetricsDimensions.VENICE_REQUEST_METHOD,
                VeniceMetricsDimensions.VENICE_CLIENT_TYPE,
                VeniceMetricsDimensions.HTTP_RESPONSE_STATUS_CODE,
                VeniceMetricsDimensions.HTTP_RESPONSE_STATUS_CODE_CATEGORY,
                VeniceMetricsDimensions.VENICE_RESPONSE_STATUS_CODE_CATEGORY)));
    expectedMetrics.put(
        BasicClientStats.BasicClientMetricEntity.KEY_COUNT,
        new MetricEntity(
            "key_count",
            MetricType.HISTOGRAM,
            MetricUnit.NUMBER,
            "Count of keys during response handling along with response codes",
            Utils.setOf(
                VeniceMetricsDimensions.VENICE_STORE_NAME,
                VeniceMetricsDimensions.VENICE_REQUEST_METHOD,
                VeniceMetricsDimensions.VENICE_CLIENT_TYPE,
                VeniceMetricsDimensions.HTTP_RESPONSE_STATUS_CODE,
                VeniceMetricsDimensions.HTTP_RESPONSE_STATUS_CODE_CATEGORY,
                VeniceMetricsDimensions.VENICE_RESPONSE_STATUS_CODE_CATEGORY)));

    for (BasicClientStats.BasicClientMetricEntity metric: BasicClientStats.BasicClientMetricEntity.values()) {
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
        CLIENT_METRIC_ENTITIES.size(),
        expectedMetricEntities.size(),
        "Unexpected size of CLIENT_METRIC_ENTITIES");

    // Assert contents
    for (MetricEntity actual: CLIENT_METRIC_ENTITIES) {
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
