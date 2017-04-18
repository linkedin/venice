package com.linkedin.venice.controller.stats;

import com.linkedin.venice.stats.AbstractVeniceStats;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Gauge;


/**
 * Resource level partition health stats.
 */
public class PartitionHealthStats extends AbstractVeniceStats {
  public static final String UNDER_REPLICATED_PARTITION_SENSOR = "underReplicatedPartition";

  private Sensor underReplicatedPartitionSensor;

  /**
   * Only for test usage.
   */
  public PartitionHealthStats(String resourceName){
    super(null, resourceName);
  }

  public PartitionHealthStats(MetricsRepository metricsRepository, String name) {
    super(metricsRepository, name);
    // TODO need a new stat type to combine with max and gauge, otherwise metric here could not represent the real
    // situation accurately.
    // InGraph would query this metric every min, so if the number changed dramatically during 1min period, we couldn't
    // know based on this metric.
    // If we use max stat here, as it has a time window internally, if last time we recorded a number prior to the start\
    // of current time window (No external view change happened in this time window), it will return 0.
    // So what we want it's max(max, gauge) here.
    if (metricsRepository.getSensor(getSensorFullName(UNDER_REPLICATED_PARTITION_SENSOR)) == null) {
      underReplicatedPartitionSensor = registerSensor(UNDER_REPLICATED_PARTITION_SENSOR, new Gauge());
    }
  }

  public void recordUnderReplicatePartition(int num) {
    underReplicatedPartitionSensor.record(num);
  }
}
