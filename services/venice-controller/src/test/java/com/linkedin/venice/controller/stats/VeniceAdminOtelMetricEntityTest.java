package com.linkedin.venice.controller.stats;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_PUSH_JOB_TYPE;
import static com.linkedin.venice.utils.Utils.setOf;

import com.linkedin.venice.controller.stats.VeniceAdminStats.VeniceAdminOtelMetricEntity;
import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityTestFixture;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityTestFixture.MetricEntityExpectation;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;


public class VeniceAdminOtelMetricEntityTest {
  private static Map<VeniceAdminOtelMetricEntity, MetricEntityExpectation> expectedDefinitions() {
    Map<VeniceAdminOtelMetricEntity, MetricEntityExpectation> map = new HashMap<>();
    map.put(
        VeniceAdminOtelMetricEntity.ADMIN_TOPIC_UNEXPECTED_ABSENCE_COUNT,
        new MetricEntityExpectation(
            "admin.topic.unexpected_absence_count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Topics unexpectedly missing or truncated during push",
            setOf(VENICE_CLUSTER_NAME, VENICE_PUSH_JOB_TYPE)));
    map.put(
        VeniceAdminOtelMetricEntity.ADMIN_PUSH_STARTED_COUNT,
        new MetricEntityExpectation(
            "admin.push.started_count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Successful push starts from parent admin, differentiated by push type",
            setOf(VENICE_CLUSTER_NAME, VENICE_PUSH_JOB_TYPE)));
    map.put(
        VeniceAdminOtelMetricEntity.ADMIN_OPERATION_SERIALIZATION_FAILURE_COUNT,
        new MetricEntityExpectation(
            "admin.operation.serialization.failure_count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Failed admin operation serializations",
            setOf(VENICE_CLUSTER_NAME)));
    return map;
  }

  @Test
  public void testMetricEntities() {
    new ModuleMetricEntityTestFixture<>(VeniceAdminOtelMetricEntity.class, expectedDefinitions()).assertAll();
  }
}
