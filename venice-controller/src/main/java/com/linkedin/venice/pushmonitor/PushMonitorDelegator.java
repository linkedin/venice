package com.linkedin.venice.pushmonitor;

import com.linkedin.venice.controller.MetadataStoreWriter;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.OfflinePushStrategy;
import com.linkedin.venice.meta.PartitionAssignment;
import com.linkedin.venice.meta.ReadWriteStoreRepository;
import com.linkedin.venice.meta.RoutingDataRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreCleaner;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.replication.TopicReplicator;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.tehuti.metrics.MetricsRepository;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.log4j.Logger;


/**
 * This is a wrapper on top of 2 push status monitors. It determines which
 * monitor shall be used when new pushes arrive. The selecting logic is
 * configurable in controller's configs.
 */
public class PushMonitorDelegator implements PushMonitor {
  private static final Logger logger = Logger.getLogger(PushMonitorDelegator.class);

  private final PushMonitorType pushMonitorType;
  private final ReadWriteStoreRepository metadataRepository;
  private final OfflinePushAccessor offlinePushAccessor;
  private final Object lock;
  private final String clusterName;

  private OfflinePushMonitor offlinePushMonitor;
  private PartitionStatusBasedPushMonitor partitionStatusBasedPushStatusMonitor;

  //Cache the relationship between kafka topic and push monitor here.
  private final Map<String, AbstractPushMonitor> topicToPushMonitorMap;

  public PushMonitorDelegator(PushMonitorType pushMonitorType, String clusterName,
      RoutingDataRepository routingDataRepository, OfflinePushAccessor offlinePushAccessor,
      StoreCleaner storeCleaner, ReadWriteStoreRepository metadataRepository,
      AggPushHealthStats aggPushHealthStats, boolean skipBufferReplayForHybrid,
      Optional<TopicReplicator> onlineOfflineTopicReplicator, Optional<TopicReplicator> leaderFollowerTopicReplicator,
      MetricsRepository metricsRepository, MetadataStoreWriter metadataStoreWriter) {

    this.clusterName = clusterName;
    this.pushMonitorType = pushMonitorType;
    this.metadataRepository = metadataRepository;
    this.offlinePushAccessor = offlinePushAccessor;
    this.lock = storeCleaner;

    this.offlinePushMonitor = new OfflinePushMonitor(clusterName, routingDataRepository, offlinePushAccessor,
        storeCleaner, metadataRepository, aggPushHealthStats, skipBufferReplayForHybrid, onlineOfflineTopicReplicator,
        metricsRepository, metadataStoreWriter);
    this.partitionStatusBasedPushStatusMonitor = new PartitionStatusBasedPushMonitor(clusterName, offlinePushAccessor,
        storeCleaner, metadataRepository, routingDataRepository, aggPushHealthStats, skipBufferReplayForHybrid,
        leaderFollowerTopicReplicator, metricsRepository, metadataStoreWriter);

    this.topicToPushMonitorMap = new VeniceConcurrentHashMap<>();
  }

  private AbstractPushMonitor getPushMonitor(String kafkaTopic) {
    return topicToPushMonitorMap.computeIfAbsent(kafkaTopic, topicName -> {
      Store store = metadataRepository.getStore(Version.parseStoreFromKafkaTopicName(kafkaTopic));

      //WriteReadyStoreRepository is the source of truth. No need to refresh metadata repo here
      if (store == null) {
        throw new VeniceNoStoreException(Version.parseStoreFromKafkaTopicName(kafkaTopic),
            Optional.of("Cannot find store metadata when tyring to allocate push status to push monitor."
                + "It's likely that the store has been deleted. topic: " + topicName));
      }

      //if the store is set to use L/F model, we would always use partition status based push status monitor
      Optional<Version> version = store.getVersion(Version.parseVersionFromKafkaTopicName(kafkaTopic));
      if (version.isPresent()) {
        if (version.get().isLeaderFollowerModelEnabled()) {
          return partitionStatusBasedPushStatusMonitor;
        }
      } else {
        logger.info("PushMonitorDelegator cannot get version metadata since the version isn't existing. "
            + "Kafka topic: " + kafkaTopic);

        //when version is not found, check the store metadata instead
        if (store.isLeaderFollowerModelEnabled()) {
          return partitionStatusBasedPushStatusMonitor;
        }
      }

      switch (pushMonitorType) {
        case WRITE_COMPUTE_STORE:
          return store.isWriteComputationEnabled() ? partitionStatusBasedPushStatusMonitor : offlinePushMonitor;
        case HYBRID_STORE:
          return store.isHybrid() ? partitionStatusBasedPushStatusMonitor : offlinePushMonitor;
        case PARTITION_STATUS_BASED:
          return partitionStatusBasedPushStatusMonitor;
        default:
          throw new VeniceException("Unknown push status monitor type.");
      }
    });
  }

  @Override
  public void loadAllPushes() {
    logger.info("Load all pushes started for cluster " + clusterName + "'s " + getClass().getSimpleName());
    lockAllPushMonitors();
    try {
      List<OfflinePushStatus> offlinePushMonitorStatuses = new ArrayList<>();
      List<OfflinePushStatus> partitionStatusBasedPushMonitorStatuses = new ArrayList<>();

      //This is for cleaning up legacy push statuses due to resource leaking. Ideally,
      //we won't need it anymore once resource leaking is fixed.
      List<OfflinePushStatus> legacyPushStatuses = new ArrayList<>();
      offlinePushAccessor.loadOfflinePushStatusesAndPartitionStatuses().forEach(status -> {
        try {
          if (getPushMonitor(status.getKafkaTopic()).equals(offlinePushMonitor)) {
            offlinePushMonitorStatuses.add(status);
          } else {
            partitionStatusBasedPushMonitorStatuses.add(status);
          }
        } catch (VeniceNoStoreException e) {
          logger.info("Found a legacy push status. topic: " + status.getKafkaTopic());
          legacyPushStatuses.add(status);
        }
      });

      offlinePushMonitor.loadAllPushes(offlinePushMonitorStatuses);
      partitionStatusBasedPushStatusMonitor.loadAllPushes(partitionStatusBasedPushMonitorStatuses);

      legacyPushStatuses.forEach(offlinePushAccessor::deleteOfflinePushStatusAndItsPartitionStatuses);
      logger.info("Load all pushes finished for cluster " + clusterName + "'s " + getClass().getSimpleName());
    } finally {
      unlockAllPushMonitors();
    }
  }

  private void lockAllPushMonitors() {
    offlinePushMonitor.acquirePushMonitorWriteLock();
    partitionStatusBasedPushStatusMonitor.acquirePushMonitorWriteLock();
  }

  private void unlockAllPushMonitors() {
    partitionStatusBasedPushStatusMonitor.unlockPushMonitorWriteLock();
    offlinePushMonitor.unlockPushMonitorWriteLock();
  }

  @Override
  public void startMonitorOfflinePush(String kafkaTopic, int numberOfPartition, int replicaFactor, OfflinePushStrategy strategy) {
    getPushMonitor(kafkaTopic).startMonitorOfflinePush(kafkaTopic, numberOfPartition, replicaFactor, strategy);
  }

  @Override
  public void stopMonitorOfflinePush(String kafkaTopic, boolean deletePushStatus) {
    getPushMonitor(kafkaTopic).stopMonitorOfflinePush(kafkaTopic, deletePushStatus);
  }

  @Override
  public void stopAllMonitoring() {
    logger.info("Stopping all monitoring for cluster " + clusterName + "'s " + getClass().getSimpleName());
    lockAllPushMonitors();
    try {
      partitionStatusBasedPushStatusMonitor.stopAllMonitoring();
      offlinePushMonitor.stopAllMonitoring();
      logger.info("Successfully stopped all monitoring for cluster " + clusterName + "'s " + getClass().getSimpleName());
    } catch (Exception e) {
      logger.error("Error when stopping all monitoring for cluster " + clusterName + "'s " + getClass().getSimpleName());
    } finally {
      unlockAllPushMonitors();
    }
  }

  @Override
  public void cleanupStoreStatus(String storeName) {
    offlinePushMonitor.cleanupStoreStatus(storeName);
    partitionStatusBasedPushStatusMonitor.cleanupStoreStatus(storeName);
  }

  @Override
  public OfflinePushStatus getOfflinePushOrThrow(String topic) {
    return getPushMonitor(topic).getOfflinePushOrThrow(topic);
  }

  @Override
  public Pair<ExecutionStatus, Optional<String>> getPushStatusAndDetails(String topic, Optional<String> incrementalPushVersion) {
    return getPushMonitor(topic).getPushStatusAndDetails(topic, incrementalPushVersion);
  }

  @Override
  public List<String> getTopicsOfOngoingOfflinePushes() {
    return Stream.concat(offlinePushMonitor.getTopicsOfOngoingOfflinePushes().stream(),
        partitionStatusBasedPushStatusMonitor.getTopicsOfOngoingOfflinePushes().stream()).collect(Collectors.toList());
  }

  @Override
  public Map<String, Long> getOfflinePushProgress(String topic) {
    return getPushMonitor(topic).getOfflinePushProgress(topic);
  }

  @Override
  public void markOfflinePushAsError(String topic, String statusDetails) {
    getPushMonitor(topic).markOfflinePushAsError(topic, statusDetails);
  }

  @Override
  public boolean wouldJobFail(String topic, PartitionAssignment partitionAssignmentAfterRemoving) {
    return getPushMonitor(topic).wouldJobFail(topic, partitionAssignmentAfterRemoving);
  }

  @Override
  public void refreshAndUpdatePushStatus(String kafkaTopic, ExecutionStatus newStatus, Optional<String> newStatusDetails) {
    getPushMonitor(kafkaTopic).refreshAndUpdatePushStatus(kafkaTopic, newStatus, newStatusDetails);
  }

  @Override
  public void recordPushPreparationDuration(String topic, long offlinePushWaitTimeInSecond) {
    getPushMonitor(topic).recordPushPreparationDuration(topic, offlinePushWaitTimeInSecond);
  }

  public List<Instance> getReadyToServeInstances(PartitionAssignment partitionAssignment, int partitionId) {
    return getPushMonitor(partitionAssignment.getTopic()).getReadyToServeInstances(partitionAssignment, partitionId);
  }
}
