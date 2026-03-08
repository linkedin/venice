package com.linkedin.venice.controller.stats;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static com.linkedin.venice.utils.Utils.setOf;

import com.linkedin.venice.controller.stats.DisabledPartitionStats.DisabledPartitionOtelMetricEntity;
import com.linkedin.venice.stats.metrics.AbstractModuleMetricEntityTest;
import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import java.util.HashMap;
import java.util.Map;


public class DisabledPartitionOtelMetricEntityTest
    extends AbstractModuleMetricEntityTest<DisabledPartitionOtelMetricEntity> {
  public DisabledPartitionOtelMetricEntityTest() {
    super(DisabledPartitionOtelMetricEntity.class);
  }

  @Override
  protected Map<DisabledPartitionOtelMetricEntity, MetricEntityExpectation> expectedDefinitions() {
    Map<DisabledPartitionOtelMetricEntity, MetricEntityExpectation> map = new HashMap<>();
    map.put(
        DisabledPartitionOtelMetricEntity.DISABLED_PARTITION_COUNT,
        new MetricEntityExpectation(
            "partition.disabled_partition.count",
            MetricType.UP_DOWN_COUNTER,
            MetricUnit.NUMBER,
            "Partition replicas disabled in Helix due to ingestion errors",
            setOf(VENICE_CLUSTER_NAME, VENICE_STORE_NAME)));
    return map;
  }
}
