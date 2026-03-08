package com.linkedin.venice.controller.stats;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.HTTP_RESPONSE_STATUS_CODE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.HTTP_RESPONSE_STATUS_CODE_CATEGORY;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CONTROLLER_ENDPOINT;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_RESPONSE_STATUS_CODE_CATEGORY;
import static com.linkedin.venice.utils.Utils.setOf;

import com.linkedin.venice.controller.stats.SparkServerStats.SparkServerOtelMetricEntity;
import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityTestFixture;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityTestFixture.MetricEntityExpectation;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;


public class SparkServerOtelMetricEntityTest {
  private static Map<SparkServerOtelMetricEntity, MetricEntityExpectation> expectedDefinitions() {
    Map<SparkServerOtelMetricEntity, MetricEntityExpectation> map = new HashMap<>();
    map.put(
        SparkServerOtelMetricEntity.INFLIGHT_CALL_COUNT,
        new MetricEntityExpectation(
            "inflight_call_count",
            MetricType.UP_DOWN_COUNTER,
            MetricUnit.NUMBER,
            "Count of all current inflight calls to controller spark server",
            setOf(VENICE_CLUSTER_NAME, VENICE_CONTROLLER_ENDPOINT)));
    map.put(
        SparkServerOtelMetricEntity.CALL_COUNT,
        new MetricEntityExpectation(
            "call_count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Count of all calls to controller spark server",
            setOf(
                VENICE_CLUSTER_NAME,
                VENICE_CONTROLLER_ENDPOINT,
                HTTP_RESPONSE_STATUS_CODE,
                HTTP_RESPONSE_STATUS_CODE_CATEGORY,
                VENICE_RESPONSE_STATUS_CODE_CATEGORY)));
    map.put(
        SparkServerOtelMetricEntity.CALL_TIME,
        new MetricEntityExpectation(
            "call_time",
            MetricType.HISTOGRAM,
            MetricUnit.MILLISECOND,
            "Latency histogram of all successful calls to controller spark server",
            setOf(
                VENICE_CLUSTER_NAME,
                VENICE_CONTROLLER_ENDPOINT,
                HTTP_RESPONSE_STATUS_CODE,
                HTTP_RESPONSE_STATUS_CODE_CATEGORY,
                VENICE_RESPONSE_STATUS_CODE_CATEGORY)));
    return map;
  }

  @Test
  public void testMetricEntities() {
    new ModuleMetricEntityTestFixture<>(SparkServerOtelMetricEntity.class, expectedDefinitions()).assertAll();
  }
}
