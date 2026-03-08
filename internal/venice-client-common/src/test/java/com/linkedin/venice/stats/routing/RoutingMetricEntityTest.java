package com.linkedin.venice.stats.routing;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_HELIX_GROUP_ID;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static com.linkedin.venice.utils.Utils.setOf;

import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityTestFixture;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityTestFixture.MetricEntityExpectation;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;


public class RoutingMetricEntityTest {
  private static Map<RoutingMetricEntity, MetricEntityExpectation> expectedDefinitions() {
    Map<RoutingMetricEntity, MetricEntityExpectation> map = new HashMap<>();
    map.put(
        RoutingMetricEntity.HELIX_GROUP_COUNT,
        new MetricEntityExpectation(
            "helix_group.count",
            MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS,
            MetricUnit.NUMBER,
            "Count of available Helix groups for routing",
            setOf(VENICE_STORE_NAME)));
    map.put(
        RoutingMetricEntity.HELIX_GROUP_CALL_COUNT,
        new MetricEntityExpectation(
            "helix_group.call_count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Count of requests routed to each Helix group",
            setOf(VENICE_STORE_NAME, VENICE_HELIX_GROUP_ID)));
    map.put(
        RoutingMetricEntity.HELIX_GROUP_REQUEST_PENDING_REQUESTS,
        new MetricEntityExpectation(
            "helix_group.request.pending_requests",
            MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS,
            MetricUnit.NUMBER,
            "Pending requests for each Helix group",
            setOf(VENICE_STORE_NAME, VENICE_HELIX_GROUP_ID)));
    map.put(
        RoutingMetricEntity.HELIX_GROUP_CALL_TIME,
        new MetricEntityExpectation(
            "helix_group.call_time",
            MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS,
            MetricUnit.MILLISECOND,
            "Waiting time for responses from each Helix group",
            setOf(VENICE_STORE_NAME, VENICE_HELIX_GROUP_ID)));
    return map;
  }

  @Test
  public void testMetricEntities() {
    new ModuleMetricEntityTestFixture<>(RoutingMetricEntity.class, expectedDefinitions()).assertAll();
  }
}
