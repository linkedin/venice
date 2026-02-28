package com.linkedin.venice.controller.stats;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static com.linkedin.venice.utils.Utils.setOf;

import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.metrics.MetricEntity;
import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityInterface;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Gauge;
import io.tehuti.metrics.stats.Max;
import java.util.Set;


/**
 * Resource level partition health stats.
 */
public class PartitionHealthStats extends AbstractVeniceStats {
  public static final String UNDER_REPLICATED_PARTITION_SENSOR = "underReplicatedPartition";

  private Sensor underReplicatedPartitionSensor;

  /**
   * Only for test usage.
   */
  public PartitionHealthStats(String resourceName) {
    super(null, resourceName);
  }

  public PartitionHealthStats(MetricsRepository metricsRepository, String name, String clusterName) {
    super(metricsRepository, name);
    synchronized (PartitionHealthStats.class) {
      Sensor existingMetric = metricsRepository.getSensor(getSensorFullName(UNDER_REPLICATED_PARTITION_SENSOR));
      if (existingMetric == null) {
        underReplicatedPartitionSensor = registerSensor(UNDER_REPLICATED_PARTITION_SENSOR, new Max(), new Gauge());
      } else {
        underReplicatedPartitionSensor = existingMetric;
      }
    }
  }

  public void recordUnderReplicatePartition(int num) {
    underReplicatedPartitionSensor.record(num);
  }

  public enum PartitionHealthOtelMetricEntity implements ModuleMetricEntityInterface {
    PARTITION_UNDER_REPLICATED_COUNT(
        "partition.under_replicated_count", MetricType.GAUGE, MetricUnit.NUMBER,
        "Partitions with fewer ready-to-serve replicas than the replication factor",
        setOf(VENICE_CLUSTER_NAME, VENICE_STORE_NAME)
    );

    private final MetricEntity metricEntity;

    PartitionHealthOtelMetricEntity(
        String metricName,
        MetricType metricType,
        MetricUnit unit,
        String description,
        Set<VeniceMetricsDimensions> dimensionsList) {
      this.metricEntity = new MetricEntity(metricName, metricType, unit, description, dimensionsList);
    }

    @Override
    public MetricEntity getMetricEntity() {
      return metricEntity;
    }
  }
}
