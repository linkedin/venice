package com.linkedin.davinci.stats;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.metrics.MetricEntity;
import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import com.linkedin.venice.utils.Utils;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;


public class ServerMetricEntityTest {
  @Test
  public void testServerMetricEntities() {
    Map<ServerMetricEntity, MetricEntity> expectedMetrics = new HashMap<>();
    expectedMetrics.put(
        ServerMetricEntity.INGESTION_HEARTBEAT_DELAY,
        new MetricEntity(
            "ingestion.replication.heartbeat.delay",
            MetricType.HISTOGRAM,
            MetricUnit.MILLISECOND,
            "Nearline ingestion replication lag",
            Utils.setOf(
                VeniceMetricsDimensions.VENICE_STORE_NAME,
                VeniceMetricsDimensions.VENICE_CLUSTER_NAME,
                VeniceMetricsDimensions.VENICE_REGION_NAME,
                VeniceMetricsDimensions.VENICE_VERSION_ROLE,
                VeniceMetricsDimensions.VENICE_REPLICA_TYPE,
                VeniceMetricsDimensions.VENICE_REPLICA_STATE)));

    expectedMetrics.put(
        ServerMetricEntity.INGESTION_RECORD_DELAY,
        new MetricEntity(
            "ingestion.replication.record.delay",
            MetricType.HISTOGRAM,
            MetricUnit.MILLISECOND,
            "Nearline ingestion record-level replication lag",
            Utils.setOf(
                VeniceMetricsDimensions.VENICE_STORE_NAME,
                VeniceMetricsDimensions.VENICE_CLUSTER_NAME,
                VeniceMetricsDimensions.VENICE_REGION_NAME,
                VeniceMetricsDimensions.VENICE_VERSION_ROLE,
                VeniceMetricsDimensions.VENICE_REPLICA_TYPE,
                VeniceMetricsDimensions.VENICE_REPLICA_STATE)));

    for (ServerMetricEntity metric: ServerMetricEntity.values()) {
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
  }

  @Test
  public void testGetMetricEntityNotNull() {
    // Verify that getMetricEntity() never returns null for any enum value
    for (ServerMetricEntity entity: ServerMetricEntity.values()) {
      assertNotNull(entity.getMetricEntity(), "getMetricEntity() should not return null for " + entity.name());
    }
  }
}
