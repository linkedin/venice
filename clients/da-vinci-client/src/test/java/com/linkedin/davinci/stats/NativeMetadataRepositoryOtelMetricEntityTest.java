package com.linkedin.davinci.stats;

import static com.linkedin.davinci.stats.NativeMetadataRepositoryOtelMetricEntity.METADATA_CACHE_STALENESS;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static com.linkedin.venice.utils.Utils.setOf;

import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityTestFixture;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityTestFixture.MetricEntityExpectation;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;


public class NativeMetadataRepositoryOtelMetricEntityTest {
  @Test
  public void testMetricEntities() {
    new ModuleMetricEntityTestFixture<>(NativeMetadataRepositoryOtelMetricEntity.class, expectedDefinitions())
        .assertAll();
  }

  private static Map<NativeMetadataRepositoryOtelMetricEntity, MetricEntityExpectation> expectedDefinitions() {
    Map<NativeMetadataRepositoryOtelMetricEntity, MetricEntityExpectation> map = new HashMap<>();
    map.put(
        METADATA_CACHE_STALENESS,
        new MetricEntityExpectation(
            "metadata.staleness_duration",
            MetricType.ASYNC_DOUBLE_GAUGE,
            MetricUnit.MILLISECOND,
            "Per-store metadata staleness in ms since the store metadata was last fetched from the meta system store",
            setOf(VENICE_CLUSTER_NAME, VENICE_STORE_NAME)));
    return map;
  }
}
