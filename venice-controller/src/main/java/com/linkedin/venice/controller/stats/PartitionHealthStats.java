package com.linkedin.venice.controller.stats;

import com.linkedin.venice.meta.Partition;
import com.linkedin.venice.meta.PartitionAssignment;
import com.linkedin.venice.meta.RoutingDataRepository;
import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.utils.Utils;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.stats.Count;
import io.tehuti.metrics.stats.OccurrenceRate;
import org.apache.log4j.Logger;
import io.tehuti.metrics.Sensor;


/**
 * Monitor the change of Helix's external view and warn in case that any partition is unhealthy. E.g. if the number of
 * replicas in a partition is smaller than the required replication factor, we would log a warn message and record to
 * our metrics.
 */
public class PartitionHealthStats extends AbstractVeniceStats implements RoutingDataRepository.RoutingDataChangedListener {
  private static final Logger logger = Logger.getLogger(PartitionHealthStats.class);

  public static final String DEFAULT_PARTITION_HEALTH_METRIC_NAME = "controller_partition_health";
  public static final String UNDER_REPLICATED_PARTITION_SENSOR = "underReplicatedPartition";

  private int requiredReplicaFactor;
  private Sensor underReplicatedPartitionSensor;

  /**
   * Only for test usage.
   */
  protected PartitionHealthStats(int requiredReplicaFactor) {
    super(null, null);
    this.requiredReplicaFactor = requiredReplicaFactor;
  }

  public PartitionHealthStats(MetricsRepository metricsRepository, String name,
      RoutingDataRepository routingDataRepository, int requriedReplicaFactor) {
    super(metricsRepository, name);
    this.requiredReplicaFactor = requriedReplicaFactor;
    // Monitor changes for all topics.
    routingDataRepository.subscribeRoutingDataChange(Utils.WILD_CHAR, this);
    underReplicatedPartitionSensor =
        registerSensor(UNDER_REPLICATED_PARTITION_SENSOR, new Count(), new OccurrenceRate());
  }

  @Override
  public void onRoutingDataChanged(PartitionAssignment partitionAssignment) {
    int underReplicaPartitions = 0;
    for (Partition partition : partitionAssignment.getAllPartitions()) {
      if (partition.getReadyToServeInstances().size() < requiredReplicaFactor) {
        underReplicaPartitions++;
      }
    }
    reportUnderReplicatedPartition(partitionAssignment.getTopic(), underReplicaPartitions);
  }

  @Override
  public void onRoutingDataDeleted(String kafkaTopic) {
    // Ignore this event.
  }

  protected void reportUnderReplicatedPartition(String version, int underReplicatedPartitions) {
    if (underReplicatedPartitions > 0) {
      logger.warn(
          "Version: " + version + " has " + underReplicatedPartitions + " partitions which are under replicated.");
      underReplicatedPartitionSensor.record(underReplicatedPartitions);
    }
  }
}
