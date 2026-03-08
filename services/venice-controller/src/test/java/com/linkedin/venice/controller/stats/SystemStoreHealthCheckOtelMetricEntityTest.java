package com.linkedin.venice.controller.stats;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_SYSTEM_STORE_TYPE;
import static com.linkedin.venice.utils.Utils.setOf;

import com.linkedin.venice.controller.stats.SystemStoreHealthCheckStats.SystemStoreHealthCheckOtelMetricEntity;
import com.linkedin.venice.stats.metrics.AbstractModuleMetricEntityTest;
import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import java.util.HashMap;
import java.util.Map;


public class SystemStoreHealthCheckOtelMetricEntityTest
    extends AbstractModuleMetricEntityTest<SystemStoreHealthCheckOtelMetricEntity> {
  public SystemStoreHealthCheckOtelMetricEntityTest() {
    super(SystemStoreHealthCheckOtelMetricEntity.class);
  }

  @Override
  protected Map<SystemStoreHealthCheckOtelMetricEntity, MetricEntityExpectation> expectedDefinitions() {
    Map<SystemStoreHealthCheckOtelMetricEntity, MetricEntityExpectation> map = new HashMap<>();
    map.put(
        SystemStoreHealthCheckOtelMetricEntity.SYSTEM_STORE_UNHEALTHY_COUNT,
        new MetricEntityExpectation(
            "system_store.health_check.unhealthy_count",
            MetricType.ASYNC_GAUGE,
            MetricUnit.NUMBER,
            "Unhealthy system stores, differentiated by system store type",
            setOf(VENICE_CLUSTER_NAME, VENICE_SYSTEM_STORE_TYPE)));
    map.put(
        SystemStoreHealthCheckOtelMetricEntity.SYSTEM_STORE_UNREPAIRABLE_COUNT,
        new MetricEntityExpectation(
            "system_store.health_check.unrepairable_count",
            MetricType.ASYNC_GAUGE,
            MetricUnit.NUMBER,
            "System stores that cannot be repaired",
            setOf(VENICE_CLUSTER_NAME)));
    return map;
  }
}
