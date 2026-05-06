package com.linkedin.davinci.stats;

import static com.linkedin.davinci.stats.ServerLoadOtelMetricEntity.REJECTION_RATIO;
import static com.linkedin.davinci.stats.ServerLoadOtelMetricEntity.REQUEST_COUNT;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_SERVER_LOAD_REQUEST_OUTCOME;
import static com.linkedin.venice.utils.Utils.setOf;

import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityTestFixture;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityTestFixture.MetricEntityExpectation;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;


public class ServerLoadOtelMetricEntityTest {
  @Test
  public void testMetricEntities() {
    new ModuleMetricEntityTestFixture<>(ServerLoadOtelMetricEntity.class, expectedDefinitions()).assertAll();
  }

  private static Map<ServerLoadOtelMetricEntity, MetricEntityExpectation> expectedDefinitions() {
    Map<ServerLoadOtelMetricEntity, MetricEntityExpectation> map = new HashMap<>();
    map.put(
        REQUEST_COUNT,
        new MetricEntityExpectation(
            "load_controller.request.count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Count of requests by load control outcome (accepted or rejected)",
            setOf(VENICE_CLUSTER_NAME, VENICE_SERVER_LOAD_REQUEST_OUTCOME)));
    map.put(
        REJECTION_RATIO,
        new MetricEntityExpectation(
            "load_controller.request.rejection_ratio",
            MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS,
            MetricUnit.RATIO,
            "Server load rejection ratio (fraction of requests being rejected)",
            setOf(VENICE_CLUSTER_NAME)));
    return map;
  }
}
