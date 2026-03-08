package com.linkedin.venice.controller.stats;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static com.linkedin.venice.utils.Utils.setOf;

import com.linkedin.venice.controller.stats.PartitionHealthStats.PartitionHealthOtelMetricEntity;
import com.linkedin.venice.stats.metrics.AbstractModuleMetricEntityTest;
import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import java.util.HashMap;
import java.util.Map;


public class PartitionHealthOtelMetricEntityTest
    extends AbstractModuleMetricEntityTest<PartitionHealthOtelMetricEntity> {
  public PartitionHealthOtelMetricEntityTest() {
    super(PartitionHealthOtelMetricEntity.class);
  }

  @Override
  protected Map<PartitionHealthOtelMetricEntity, MetricEntityExpectation> expectedDefinitions() {
    Map<PartitionHealthOtelMetricEntity, MetricEntityExpectation> map = new HashMap<>();
    map.put(
        PartitionHealthOtelMetricEntity.PARTITION_UNDER_REPLICATED_COUNT,
        new MetricEntityExpectation(
            "partition.under_replicated_count",
            MetricType.GAUGE,
            MetricUnit.NUMBER,
            "Partitions with fewer ready-to-serve replicas than the replication factor",
            setOf(VENICE_CLUSTER_NAME, VENICE_STORE_NAME)));
    return map;
  }
}
