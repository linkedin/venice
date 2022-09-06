package com.linkedin.venice.controller.stats;

import com.linkedin.venice.stats.AbstractVeniceStats;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Gauge;
import io.tehuti.metrics.stats.Max;


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

  public PartitionHealthStats(MetricsRepository metricsRepository, String name) {
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
}
