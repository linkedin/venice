package com.linkedin.venice.controller.stats;

import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.meta.Partition;
import com.linkedin.venice.meta.PartitionAssignment;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.RoutingDataRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.pushmonitor.PushMonitor;
import com.linkedin.venice.pushmonitor.ReadOnlyPartitionStatus;
import com.linkedin.venice.stats.AbstractVeniceAggStats;
import com.linkedin.venice.utils.Utils;
import io.tehuti.metrics.MetricsRepository;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Monitor the change of Helix's external view and warn in case that any partition is unhealthy. E.g. if the number of
 * replicas in a partition is smaller than the required replication factor, we would log a warn message and record to
 * our metrics.
 */
public class AggPartitionHealthStats extends AbstractVeniceAggStats<PartitionHealthStats>
    implements RoutingDataRepository.RoutingDataChangedListener {
  private static final Logger LOGGER = LogManager.getLogger(AggPartitionHealthStats.class);

  private final ReadOnlyStoreRepository storeRepository;

  private final PushMonitor pushMonitor;

  /**
   * Only for test usage.
   */
  protected AggPartitionHealthStats(
      String clusterName,
      ReadOnlyStoreRepository storeRepository,
      PushMonitor pushMonitor) {
    super(clusterName, null, (metricRepo, resourceName) -> new PartitionHealthStats(resourceName));
    this.storeRepository = storeRepository;
    this.pushMonitor = pushMonitor;
  }

  public AggPartitionHealthStats(
      String clusterName,
      MetricsRepository metricsRepository,
      RoutingDataRepository routingDataRepository,
      ReadOnlyStoreRepository storeRepository,
      PushMonitor pushMonitor) {
    super(clusterName, metricsRepository, PartitionHealthStats::new);
    this.storeRepository = storeRepository;
    this.pushMonitor = pushMonitor;

    // Monitor changes for all topics.
    routingDataRepository.subscribeRoutingDataChange(Utils.WILDCARD_MATCH_ANY, this);
  }

  @Override
  public void onExternalViewChange(PartitionAssignment partitionAssignment) {
    int underReplicatedPartitions = 0;
    String storeName = Version.parseStoreFromKafkaTopicName(partitionAssignment.getTopic());
    int versionNumber = Version.parseVersionFromKafkaTopicName(partitionAssignment.getTopic());
    Store store = storeRepository.getStore(storeName);
    if (store == null) {
      throw new VeniceNoStoreException(storeName);
    }
    // We focus on versions which already completed bootstrap. On-going push has under replicated partition for sure,
    // but it would not affect our operations.
    if (!VersionStatus.isBootstrapCompleted(store.getVersionStatus(versionNumber))) {
      return;
    }
    for (Partition partition: partitionAssignment.getAllPartitions()) {
      if (pushMonitor.getReadyToServeInstances(partitionAssignment, partition.getId()).size() < store
          .getReplicationFactor()) {
        underReplicatedPartitions++;
      }
    }
    reportUnderReplicatedPartition(partitionAssignment.getTopic(), underReplicatedPartitions);
  }

  @Override
  public void onCustomizedViewChange(PartitionAssignment partitionAssignment) {
  }

  @Override
  public void onPartitionStatusChange(String topic, ReadOnlyPartitionStatus partitionStatus) {
    // Ignore this event
  }

  @Override
  public void onRoutingDataDeleted(String kafkaTopic) {
    // Ignore this event.
  }

  protected void reportUnderReplicatedPartition(String version, int underReplicatedPartitions) {
    if (underReplicatedPartitions > 0) {
      LOGGER.warn("Version: {} has {} partitions which are under replicated.", version, underReplicatedPartitions);
      totalStats.recordUnderReplicatePartition(underReplicatedPartitions);
      getStoreStats(version).recordUnderReplicatePartition(underReplicatedPartitions);
    }
  }
}
