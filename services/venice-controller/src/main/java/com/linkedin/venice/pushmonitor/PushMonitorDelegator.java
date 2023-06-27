package com.linkedin.venice.pushmonitor;

import com.linkedin.venice.controller.HelixAdminClient;
import com.linkedin.venice.controller.VeniceControllerConfig;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.helix.HelixCustomizedViewOfflinePushRepository;
import com.linkedin.venice.ingestion.control.RealTimeTopicSwitcher;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.OfflinePushStrategy;
import com.linkedin.venice.meta.PartitionAssignment;
import com.linkedin.venice.meta.ReadWriteStoreRepository;
import com.linkedin.venice.meta.RoutingDataRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreCleaner;
import com.linkedin.venice.meta.UncompletedPartition;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pushstatushelper.PushStatusStoreReader;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import com.linkedin.venice.utils.locks.AutoCloseableLock;
import com.linkedin.venice.utils.locks.ClusterLockManager;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class maintains the mapping of Kafka topic to each {@link PushMonitor} instance and delegates calls to the
 * correct instance.
 */
public class PushMonitorDelegator implements PushMonitor {
  private static final Logger LOGGER = LogManager.getLogger(PushMonitorDelegator.class);

  private final ReadWriteStoreRepository metadataRepository;
  private final String clusterName;
  private final ClusterLockManager clusterLockManager;

  private final PartitionStatusBasedPushMonitor partitionStatusBasedPushStatusMonitor;

  // Cache the relationship between kafka topic and push monitor here.
  private final Map<String, AbstractPushMonitor> topicToPushMonitorMap;

  public PushMonitorDelegator(
      String clusterName,
      RoutingDataRepository routingDataRepository,
      OfflinePushAccessor offlinePushAccessor,
      StoreCleaner storeCleaner,
      ReadWriteStoreRepository metadataRepository,
      AggPushHealthStats aggPushHealthStats,
      RealTimeTopicSwitcher leaderFollowerTopicReplicator,
      ClusterLockManager clusterLockManager,
      String aggregateRealTimeSourceKafkaUrl,
      List<String> activeActiveRealTimeSourceKafkaURLs,
      HelixAdminClient helixAdminClient,
      VeniceControllerConfig controllerConfig,
      PushStatusStoreReader pushStatusStoreReader) {
    this.clusterName = clusterName;
    this.metadataRepository = metadataRepository;

    this.partitionStatusBasedPushStatusMonitor = new PartitionStatusBasedPushMonitor(
        clusterName,
        offlinePushAccessor,
        storeCleaner,
        metadataRepository,
        routingDataRepository,
        aggPushHealthStats,
        leaderFollowerTopicReplicator,
        clusterLockManager,
        aggregateRealTimeSourceKafkaUrl,
        activeActiveRealTimeSourceKafkaURLs,
        helixAdminClient,
        controllerConfig,
        pushStatusStoreReader);
    this.clusterLockManager = clusterLockManager;

    this.topicToPushMonitorMap = new VeniceConcurrentHashMap<>();
  }

  private AbstractPushMonitor getPushMonitor(String kafkaTopic) {
    return topicToPushMonitorMap.computeIfAbsent(kafkaTopic, topicName -> {
      Store store = metadataRepository.getStore(Version.parseStoreFromKafkaTopicName(kafkaTopic));

      // WriteReadyStoreRepository is the source of truth. No need to refresh metadata repo here
      if (store == null) {
        throw new VeniceNoStoreException(
            Version.parseStoreFromKafkaTopicName(kafkaTopic),
            null,
            "Cannot find store metadata when tyring to allocate push status to push monitor."
                + "It's likely that the store has been deleted. topic: " + topicName);
      }
      return partitionStatusBasedPushStatusMonitor;
    });
  }

  @Override
  public void loadAllPushes() {
    LOGGER.info("Load all pushes started for cluster {}'s {}", clusterName, getClass().getSimpleName());
    try (AutoCloseableLock ignore = clusterLockManager.createClusterWriteLock()) {
      partitionStatusBasedPushStatusMonitor.loadAllPushes();
      LOGGER.info("Load all pushes finished for cluster {}'s {}", clusterName, getClass().getSimpleName());
    }
  }

  @Override
  public void startMonitorOfflinePush(
      String kafkaTopic,
      int numberOfPartition,
      int replicaFactor,
      OfflinePushStrategy strategy) {
    getPushMonitor(kafkaTopic).startMonitorOfflinePush(kafkaTopic, numberOfPartition, replicaFactor, strategy);
  }

  @Override
  public void stopMonitorOfflinePush(String kafkaTopic, boolean deletePushStatus, boolean isForcedDelete) {
    getPushMonitor(kafkaTopic).stopMonitorOfflinePush(kafkaTopic, deletePushStatus, isForcedDelete);
  }

  @Override
  public void stopAllMonitoring() {
    LOGGER.info("Stopping all monitoring for cluster {}'s {}", clusterName, getClass().getSimpleName());
    try (AutoCloseableLock ignore = clusterLockManager.createClusterWriteLock()) {
      partitionStatusBasedPushStatusMonitor.stopAllMonitoring();
      LOGGER.info("Successfully stopped all monitoring for cluster {}'s {}", clusterName, getClass().getSimpleName());
    } catch (Exception e) {
      LOGGER.error("Error when stopping all monitoring for cluster {}'s {}", clusterName, getClass().getSimpleName());
    }
  }

  @Override
  public void cleanupStoreStatus(String storeName) {
    partitionStatusBasedPushStatusMonitor.cleanupStoreStatus(storeName);
  }

  @Override
  public OfflinePushStatus getOfflinePushOrThrow(String topic) {
    return getPushMonitor(topic).getOfflinePushOrThrow(topic);
  }

  @Override
  public ExecutionStatusWithDetails getPushStatusAndDetails(String topic) {
    return getPushMonitor(topic).getPushStatusAndDetails(topic);
  }

  @Override
  public List<UncompletedPartition> getUncompletedPartitions(String topic) {
    return getPushMonitor(topic).getUncompletedPartitions(topic);
  }

  @Override
  public ExecutionStatusWithDetails getIncrementalPushStatusAndDetails(
      String kafkaTopic,
      String incrementalPushVersion,
      HelixCustomizedViewOfflinePushRepository customizedViewRepo) {
    return getPushMonitor(kafkaTopic)
        .getIncrementalPushStatusAndDetails(kafkaTopic, incrementalPushVersion, customizedViewRepo);
  }

  @Override
  public ExecutionStatusWithDetails getIncrementalPushStatusFromPushStatusStore(
      String kafkaTopic,
      String incrementalPushVersion,
      HelixCustomizedViewOfflinePushRepository customizedViewRepo,
      PushStatusStoreReader pushStatusStoreReader) {
    return getPushMonitor(kafkaTopic).getIncrementalPushStatusFromPushStatusStore(
        kafkaTopic,
        incrementalPushVersion,
        customizedViewRepo,
        pushStatusStoreReader);
  }

  @Override
  public Set<String> getOngoingIncrementalPushVersions(String kafkaTopic) {
    return getPushMonitor(kafkaTopic).getOngoingIncrementalPushVersions(kafkaTopic);
  }

  @Override
  public Set<String> getOngoingIncrementalPushVersions(String kafkaTopic, PushStatusStoreReader pushStatusStoreReader) {
    return getPushMonitor(kafkaTopic).getOngoingIncrementalPushVersions(kafkaTopic, pushStatusStoreReader);
  }

  @Override
  public List<String> getTopicsOfOngoingOfflinePushes() {
    return partitionStatusBasedPushStatusMonitor.getTopicsOfOngoingOfflinePushes();
  }

  @Override
  public void markOfflinePushAsError(String topic, String statusDetails) {
    getPushMonitor(topic).markOfflinePushAsError(topic, statusDetails);
  }

  @Override
  public void refreshAndUpdatePushStatus(
      String kafkaTopic,
      ExecutionStatus newStatus,
      Optional<String> newStatusDetails) {
    getPushMonitor(kafkaTopic).refreshAndUpdatePushStatus(kafkaTopic, newStatus, newStatusDetails);
  }

  @Override
  public void recordPushPreparationDuration(String topic, long offlinePushWaitTimeInSecond) {
    getPushMonitor(topic).recordPushPreparationDuration(topic, offlinePushWaitTimeInSecond);
  }

  public List<Instance> getReadyToServeInstances(PartitionAssignment partitionAssignment, int partitionId) {
    return getPushMonitor(partitionAssignment.getTopic()).getReadyToServeInstances(partitionAssignment, partitionId);
  }

  @Override
  public boolean isOfflinePushMonitorDaVinciPushStatusEnabled() {
    return partitionStatusBasedPushStatusMonitor.isOfflinePushMonitorDaVinciPushStatusEnabled();
  }
}
