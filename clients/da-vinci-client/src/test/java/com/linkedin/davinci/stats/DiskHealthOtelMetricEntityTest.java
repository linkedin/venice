package com.linkedin.davinci.stats;

import static com.linkedin.davinci.stats.DiskHealthOtelMetricEntity.DISK_HEALTH_STATUS;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.utils.Utils.setOf;

import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityTestFixture;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityTestFixture.MetricEntityExpectation;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;


public class DiskHealthOtelMetricEntityTest {
  @Test
  public void testMetricEntities() {
    new ModuleMetricEntityTestFixture<>(DiskHealthOtelMetricEntity.class, expectedDefinitions()).assertAll();
  }

  private static Map<DiskHealthOtelMetricEntity, MetricEntityExpectation> expectedDefinitions() {
    Map<DiskHealthOtelMetricEntity, MetricEntityExpectation> map = new HashMap<>();
    map.put(
        DISK_HEALTH_STATUS,
        new MetricEntityExpectation(
            "disk.health.status",
            MetricType.ASYNC_GAUGE,
            MetricUnit.NUMBER,
            "Disk health status: 1 if healthy, 0 if unhealthy",
            setOf(VENICE_CLUSTER_NAME)));
    return map;
  }
}
