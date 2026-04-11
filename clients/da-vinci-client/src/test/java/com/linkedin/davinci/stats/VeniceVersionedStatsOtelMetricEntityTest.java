package com.linkedin.davinci.stats;

import static com.linkedin.davinci.stats.VeniceVersionedStatsOtelMetricEntity.STORE_VERSION;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_VERSION_ROLE;
import static com.linkedin.venice.utils.Utils.setOf;

import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityTestFixture;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityTestFixture.MetricEntityExpectation;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;


public class VeniceVersionedStatsOtelMetricEntityTest {
  @Test
  public void testMetricEntities() {
    new ModuleMetricEntityTestFixture<>(VeniceVersionedStatsOtelMetricEntity.class, expectedDefinitions()).assertAll();
  }

  private static Map<VeniceVersionedStatsOtelMetricEntity, MetricEntityExpectation> expectedDefinitions() {
    Map<VeniceVersionedStatsOtelMetricEntity, MetricEntityExpectation> map = new HashMap<>();
    map.put(
        STORE_VERSION,
        new MetricEntityExpectation(
            "store.version",
            MetricType.ASYNC_GAUGE,
            MetricUnit.NUMBER,
            "Version number serving a given role (current or future) for a store",
            setOf(VENICE_CLUSTER_NAME, VENICE_STORE_NAME, VENICE_VERSION_ROLE)));
    return map;
  }
}
