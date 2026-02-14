package com.linkedin.venice.controller.stats;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.controller.VeniceController;
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


public class ControllerMetricEntityTest {
  @Test
  public void testControllerMetricEntities() {
    Map<ControllerMetricEntity, MetricEntity> expectedMetrics = new HashMap<>();
    expectedMetrics.put(
        ControllerMetricEntity.INFLIGHT_CALL_COUNT,
        new MetricEntity(
            "inflight_call_count",
            MetricType.UP_DOWN_COUNTER,
            MetricUnit.NUMBER,
            "Count of all current inflight calls to controller spark server",
            Utils.setOf(
                VeniceMetricsDimensions.VENICE_CLUSTER_NAME,
                VeniceMetricsDimensions.VENICE_CONTROLLER_ENDPOINT)));
    expectedMetrics.put(
        ControllerMetricEntity.CALL_COUNT,
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
        ControllerMetricEntity.CALL_TIME,
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
    expectedMetrics.put(
        ControllerMetricEntity.STORE_REPUSH_CALL_COUNT,
        new MetricEntity(
            "store.repush.call_count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Count of all requests to repush a store",
            Utils.setOf(
                VeniceMetricsDimensions.VENICE_STORE_NAME,
                VeniceMetricsDimensions.VENICE_RESPONSE_STATUS_CODE_CATEGORY,
                VeniceMetricsDimensions.VENICE_CLUSTER_NAME,
                VeniceMetricsDimensions.STORE_REPUSH_TRIGGER_SOURCE)));
    expectedMetrics.put(
        ControllerMetricEntity.STORE_COMPACTION_NOMINATED_COUNT,
        new MetricEntity(
            "store.compaction.nominated_count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Count of stores nominated for scheduled compaction",
            Utils.setOf(VeniceMetricsDimensions.VENICE_STORE_NAME, VeniceMetricsDimensions.VENICE_CLUSTER_NAME)));
    expectedMetrics.put(
        ControllerMetricEntity.STORE_COMPACTION_ELIGIBLE_STATE,
        new MetricEntity(
            "store.compaction.eligible_state",
            MetricType.GAUGE,
            MetricUnit.NUMBER,
            "Track the state from the time a store is nominated for compaction to the time the repush is completed",
            Utils.setOf(VeniceMetricsDimensions.VENICE_STORE_NAME, VeniceMetricsDimensions.VENICE_CLUSTER_NAME)));
    expectedMetrics.put(
        ControllerMetricEntity.STORE_COMPACTION_TRIGGERED_COUNT,
        new MetricEntity(
            "store.compaction.triggered_count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Count of log compaction repush triggered for a store after it becomes eligible",
            Utils.setOf(
                VeniceMetricsDimensions.VENICE_STORE_NAME,
                VeniceMetricsDimensions.VENICE_RESPONSE_STATUS_CODE_CATEGORY,
                VeniceMetricsDimensions.VENICE_CLUSTER_NAME)));
    expectedMetrics.put(
        ControllerMetricEntity.PUSH_JOB_COUNT,
        new MetricEntity(
            "push_job.count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Push job completions, differentiated by push type and status",
            Utils.setOf(
                VeniceMetricsDimensions.VENICE_CLUSTER_NAME,
                VeniceMetricsDimensions.VENICE_STORE_NAME,
                VeniceMetricsDimensions.VENICE_PUSH_JOB_TYPE,
                VeniceMetricsDimensions.VENICE_PUSH_JOB_STATUS)));
    expectedMetrics.put(
        ControllerMetricEntity.TOPIC_CLEANUP_DELETABLE_COUNT,
        MetricEntity.createWithNoDimensions(
            "topic_cleanup_service.topic.deletable_count",
            MetricType.GAUGE,
            MetricUnit.NUMBER,
            "Count of topics currently eligible for deletion"));
    expectedMetrics.put(
        ControllerMetricEntity.TOPIC_CLEANUP_DELETED_COUNT,
        new MetricEntity(
            "topic_cleanup_service.topic.deleted_count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Count of topic deletion operations",
            Utils.setOf(VeniceMetricsDimensions.VENICE_RESPONSE_STATUS_CODE_CATEGORY)));

    for (ControllerMetricEntity metric: ControllerMetricEntity.values()) {
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

    Collection<MetricEntity> expectedMetricEntities = expectedMetrics.values();

    assertEquals(
        VeniceController.CONTROLLER_SERVICE_METRIC_ENTITIES.size(),
        expectedMetricEntities.size(),
        "Unexpected size of CONTROLLER_SERVICE_METRIC_ENTITIES");

    for (MetricEntity actual: VeniceController.CONTROLLER_SERVICE_METRIC_ENTITIES) {
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
