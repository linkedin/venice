package com.linkedin.venice.controller.stats;

import static com.linkedin.venice.controller.stats.ControllerStatsDimensionUtils.dimensionMapBuilder;

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
import com.linkedin.venice.stats.OpenTelemetryMetricsSetup;
import com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.metrics.MetricEntityStateGeneric;
import com.linkedin.venice.utils.Utils;
import io.tehuti.metrics.MetricsRepository;
import java.util.Map;
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

  private final MetricEntityStateGeneric underReplicatedPartitionMetric;

  /**
   * Only for test usage.
   */
  protected AggPartitionHealthStats(
      String clusterName,
      ReadOnlyStoreRepository storeRepository,
      PushMonitor pushMonitor) {
    super(clusterName, null, (metricRepo, resourceName, cluster) -> new PartitionHealthStats(resourceName), true);
    this.storeRepository = storeRepository;
    this.pushMonitor = pushMonitor;
    this.underReplicatedPartitionMetric = null;
  }

  public AggPartitionHealthStats(
      String clusterName,
      MetricsRepository metricsRepository,
      RoutingDataRepository routingDataRepository,
      ReadOnlyStoreRepository storeRepository,
      PushMonitor pushMonitor) {
    super(clusterName, metricsRepository, PartitionHealthStats::new, true);
    this.storeRepository = storeRepository;
    this.pushMonitor = pushMonitor;

    OpenTelemetryMetricsSetup.OpenTelemetryMetricsSetupInfo otelData =
        OpenTelemetryMetricsSetup.builder(metricsRepository).setClusterName(clusterName).build();
    VeniceOpenTelemetryMetricsRepository otelRepository = otelData.getOtelRepository();
    Map<VeniceMetricsDimensions, String> baseDimensionsMap = otelData.getBaseDimensionsMap();

    underReplicatedPartitionMetric = MetricEntityStateGeneric.create(
        PartitionHealthStats.PartitionHealthOtelMetricEntity.PARTITION_UNDER_REPLICATED_COUNT.getMetricEntity(),
        otelRepository,
        baseDimensionsMap);

    // Monitor changes for all topics.
    routingDataRepository.subscribeRoutingDataChange(Utils.WILDCARD_MATCH_ANY, this);
  }

  @Override
  public void onExternalViewChange(PartitionAssignment partitionAssignment) {
    int underReplicatedPartitions = 0;
    String storeName = Version.parseStoreFromKafkaTopicName(partitionAssignment.getTopic());
    int versionNumber = Version.parseVersionFromKafkaTopicName(partitionAssignment.getTopic());
    try {
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
    } catch (Exception e) {
      LOGGER.error("Failed to process external view change for topic: {}", partitionAssignment.getTopic(), e);
    }
  }

  @Override
  public void onCustomizedViewChange(PartitionAssignment partitionAssignment) {
  }

  @Override
  public void onCustomizedViewAdded(PartitionAssignment partitionAssignment) {
    // Ignore this event
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
      if (underReplicatedPartitionMetric != null) {
        String storeName = Version.parseStoreFromKafkaTopicName(version);
        underReplicatedPartitionMetric
            .record(underReplicatedPartitions, dimensionMapBuilder().store(storeName).build());
      }
    }
  }
}
