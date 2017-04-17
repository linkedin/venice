package com.linkedin.venice.controller.stats;

import com.linkedin.venice.meta.Partition;
import com.linkedin.venice.meta.PartitionAssignment;
import com.linkedin.venice.meta.RoutingDataRepository;
import com.linkedin.venice.stats.AbstractVeniceAggStats;
import com.linkedin.venice.utils.Utils;
import io.tehuti.metrics.MetricsRepository;
import org.apache.log4j.Logger;


/**
 * Monitor the change of Helix's external view and warn in case that any partition is unhealthy. E.g. if the number of
 * replicas in a partition is smaller than the required replication factor, we would log a warn message and record to
 * our metrics.
 */
public class AggPartitionHealthStats extends AbstractVeniceAggStats<PartitionHealthStats> implements RoutingDataRepository.RoutingDataChangedListener {
  public static final String DEFAULT_PARTITION_HEALTH_METRIC_NAME = "controller_partition_health";

  private static final Logger logger = Logger.getLogger(AggPartitionHealthStats.class);

  private int requiredReplicaFactor;

  /**
   * Only for test usage.
   */
  protected AggPartitionHealthStats(int requiredReplicationFactor) {
    super(null, (metricRepo, resourceName) -> new PartitionHealthStats(resourceName));
    this.requiredReplicaFactor = requiredReplicationFactor;
  }

  public AggPartitionHealthStats(MetricsRepository metricsRepository, String name,
      RoutingDataRepository routingDataRepository, int requiredReplicationFactor) {
    super(metricsRepository, (metricsRepo, resourceName) -> new PartitionHealthStats(metricsRepo, resourceName));
    this.requiredReplicaFactor = requiredReplicationFactor;
    // Monitor changes for all topics.
    routingDataRepository.subscribeRoutingDataChange(Utils.WILDCARD_MATCH_ANY, this);
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
      totalStats.recordUnderReplicatePartition(underReplicatedPartitions);
      getStoreStats(version).recordUnderReplicatePartition(underReplicatedPartitions);
    }
  }
}
