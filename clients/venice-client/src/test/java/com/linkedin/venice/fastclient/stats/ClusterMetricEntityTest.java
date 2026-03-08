package com.linkedin.venice.fastclient.stats;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_INSTANCE_ERROR_TYPE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static com.linkedin.venice.utils.CollectionUtils.setOf;

import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityTestFixture;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityTestFixture.MetricEntityExpectation;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;


public class ClusterMetricEntityTest {
  @Test
  public void testMetricEntities() {
    new ModuleMetricEntityTestFixture<>(ClusterMetricEntity.class, expectedDefinitions()).assertAll();
  }

  private static Map<ClusterMetricEntity, MetricEntityExpectation> expectedDefinitions() {
    Map<ClusterMetricEntity, MetricEntityExpectation> map = new HashMap<>();
    map.put(
        ClusterMetricEntity.STORE_VERSION_UPDATE_FAILURE_COUNT,
        new MetricEntityExpectation(
            "store.version.update_failure_count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Count of version update failures for the store",
            Collections.singleton(VENICE_STORE_NAME)));
    map.put(
        ClusterMetricEntity.INSTANCE_ERROR_COUNT,
        new MetricEntityExpectation(
            "instance.error_count",
            MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS,
            MetricUnit.NUMBER,
            "Count of instance errors for the store",
            setOf(VENICE_STORE_NAME, VENICE_INSTANCE_ERROR_TYPE)));
    map.put(
        ClusterMetricEntity.STORE_VERSION_CURRENT,
        new MetricEntityExpectation(
            "store.version.current",
            MetricType.ASYNC_GAUGE,
            MetricUnit.NUMBER,
            "Current store version served by the client",
            Collections.singleton(VENICE_STORE_NAME)));
    return map;
  }
}
