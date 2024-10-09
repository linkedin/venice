package com.linkedin.venice.pushmonitor;

import static com.linkedin.venice.meta.Store.NON_EXISTING_VERSION;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.COMPLETED;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.END_OF_INCREMENTAL_PUSH_RECEIVED;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.ERROR;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.NOT_CREATED;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.START_OF_INCREMENTAL_PUSH_RECEIVED;
import static com.linkedin.venice.pushmonitor.OfflinePushStatus.HELIX_ASSIGNMENT_COMPLETED;
import static com.linkedin.venice.pushmonitor.OfflinePushStatus.HELIX_RESOURCE_NOT_CREATED;

import com.linkedin.venice.controller.HelixAdminClient;
import com.linkedin.venice.controller.VeniceControllerClusterConfig;
import com.linkedin.venice.controller.stats.DisabledPartitionStats;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.helix.HelixCustomizedViewOfflinePushRepository;
import com.linkedin.venice.helix.ResourceAssignment;
import com.linkedin.venice.ingestion.control.RealTimeTopicSwitcher;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.OfflinePushStrategy;
import com.linkedin.venice.meta.PartitionAssignment;
import com.linkedin.venice.meta.ReadWriteStoreRepository;
import com.linkedin.venice.meta.RoutingDataRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreCleaner;
import com.linkedin.venice.meta.UncompletedPartition;
import com.linkedin.venice.meta.UncompletedReplica;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.pushstatushelper.PushStatusStoreReader;
import com.linkedin.venice.throttle.EventThrottler;
import com.linkedin.venice.utils.HelixUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import com.linkedin.venice.utils.locks.AutoCloseableLock;
import com.linkedin.venice.utils.locks.ClusterLockManager;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * AbstractPushMonitor is a high level abstraction that manages {@link OfflinePushStatus}.
 * Depending on the implementation, it collects info from different paths and updates push
 * status accordingly.
 *
 * At present, push status has 1 initial state {@link ExecutionStatus#STARTED} and 2 end states
 * {@link ExecutionStatus#COMPLETED} and {@link ExecutionStatus#ERROR}.
 * State mutation is unidirectional and once it reaches to either end state, we stop mutating it.
 * Check {@link OfflinePushStatus} for more details.
 */

public abstract class AbstractPushMonitor
    implements PushMonitor, PartitionStatusListener, RoutingDataRepository.RoutingDataChangedListener {
  public static final int MAX_PUSH_TO_KEEP = 5;

  private static final Logger LOGGER = LogManager.getLogger(AbstractPushMonitor.class);

  private final OfflinePushAccessor offlinePushAccessor;
  private final String clusterName;
  private final ReadWriteStoreRepository metadataRepository;
  private final RoutingDataRepository routingDataRepository;
  private final StoreCleaner storeCleaner;
  private final AggPushHealthStats aggPushHealthStats;
  private final Map<String, OfflinePushStatus> topicToPushMap = new VeniceConcurrentHashMap<>();
  private RealTimeTopicSwitcher realTimeTopicSwitcher;
  private final ClusterLockManager clusterLockManager;
  private final String aggregateRealTimeSourceKafkaUrl;
  private final List<String> activeActiveRealTimeSourceKafkaURLs;
  private final HelixAdminClient helixAdminClient;
  private final EventThrottler helixClientThrottler;
  private final boolean disableErrorLeaderReplica;
  private final long offlineJobResourceAssignmentWaitTimeInMilliseconds;

  private final PushStatusCollector pushStatusCollector;
  private final boolean isOfflinePushMonitorDaVinciPushStatusEnabled;

  private final DisabledPartitionStats disabledPartitionStats;

  public AbstractPushMonitor(
      String clusterName,
      OfflinePushAccessor offlinePushAccessor,
      StoreCleaner storeCleaner,
      ReadWriteStoreRepository metadataRepository,
      RoutingDataRepository routingDataRepository,
      AggPushHealthStats aggPushHealthStats,
      RealTimeTopicSwitcher realTimeTopicSwitcher,
      ClusterLockManager clusterLockManager,
      String aggregateRealTimeSourceKafkaUrl,
      List<String> activeActiveRealTimeSourceKafkaURLs,
      HelixAdminClient helixAdminClient,
      VeniceControllerClusterConfig controllerConfig,
      PushStatusStoreReader pushStatusStoreReader,
      DisabledPartitionStats disabledPartitionStats) {
    this.clusterName = clusterName;
    this.offlinePushAccessor = offlinePushAccessor;
    this.storeCleaner = storeCleaner;
    this.metadataRepository = metadataRepository;
    this.routingDataRepository = routingDataRepository;
    this.aggPushHealthStats = aggPushHealthStats;
    this.realTimeTopicSwitcher = realTimeTopicSwitcher;
    this.clusterLockManager = clusterLockManager;
    this.aggregateRealTimeSourceKafkaUrl = aggregateRealTimeSourceKafkaUrl;
    this.activeActiveRealTimeSourceKafkaURLs = activeActiveRealTimeSourceKafkaURLs;
    this.helixAdminClient = helixAdminClient;
    this.disabledPartitionStats = disabledPartitionStats;

    this.disableErrorLeaderReplica = controllerConfig.isErrorLeaderReplicaFailOverEnabled();
    this.helixClientThrottler =
        new EventThrottler(10, "push_monitor_helix_client_throttler", false, EventThrottler.BLOCK_STRATEGY);
    this.offlineJobResourceAssignmentWaitTimeInMilliseconds = controllerConfig.getOffLineJobWaitTimeInMilliseconds();
    this.pushStatusCollector = new PushStatusCollector(
        metadataRepository,
        pushStatusStoreReader,
        (topic) -> handleCompletedPush(topic),
        (topic, details) -> handleErrorPush(topic, details),
        controllerConfig.isDaVinciPushStatusScanEnabled(),
        controllerConfig.getDaVinciPushStatusScanIntervalInSeconds(),
        controllerConfig.getDaVinciPushStatusScanThreadNumber(),
        controllerConfig.getDaVinciPushStatusScanNoReportRetryMaxAttempt(),
        controllerConfig.getDaVinciPushStatusScanMaxOfflineInstanceCount(),
        controllerConfig.getDaVinciPushStatusScanMaxOfflineInstanceRatio(),
        controllerConfig.useDaVinciSpecificExecutionStatusForError());
    this.isOfflinePushMonitorDaVinciPushStatusEnabled = controllerConfig.isDaVinciPushStatusEnabled();
    pushStatusCollector.start();
  }

  @Override
  public void loadAllPushes() {
    try (AutoCloseableLock ignore = clusterLockManager.createClusterWriteLock()) {
      List<OfflinePushStatus> offlinePushStatuses = offlinePushAccessor.loadOfflinePushStatusesAndPartitionStatuses();
      loadAllPushes(offlinePushStatuses);
    }
  }

  private void loadAllPushes(List<OfflinePushStatus> offlinePushStatusList) {
    pushStatusCollector.start();
    try (AutoCloseableLock ignore = clusterLockManager.createClusterWriteLock()) {
      LOGGER.info("Load all pushes started for cluster {}'s {}", clusterName, getClass().getSimpleName());
      // Subscribe to changes first
      List<OfflinePushStatus> refreshedOfflinePushStatusList = new ArrayList<>();
      for (OfflinePushStatus offlinePushStatus: offlinePushStatusList) {
        try {
          routingDataRepository.subscribeRoutingDataChange(offlinePushStatus.getKafkaTopic(), this);

          /**
           * Now that we're subscribed, update the view of this data.  We refresh this data after subscribing to be sure
           * that we're going to get ALL the change events and not lose any in between reading the data and subscribing
           * to changes in the data.
           */
          refreshedOfflinePushStatusList
              .add(offlinePushAccessor.getOfflinePushStatusAndItsPartitionStatuses(offlinePushStatus.getKafkaTopic()));
        } catch (Exception e) {
          LOGGER.error("Could not load offline push for {}", offlinePushStatus.getKafkaTopic(), e);
        }

      }
      offlinePushStatusList = refreshedOfflinePushStatusList;

      for (OfflinePushStatus offlinePushStatus: offlinePushStatusList) {
        try {
          topicToPushMap.put(offlinePushStatus.getKafkaTopic(), offlinePushStatus);
          getOfflinePushAccessor().subscribePartitionStatusChange(offlinePushStatus, this);

          // Check the status for running pushes. In case controller missed some notification during the failover, we
          // need to update it based on current routing data.
          if (!offlinePushStatus.getCurrentStatus().isTerminal()) {
            String topic = offlinePushStatus.getKafkaTopic();
            if (routingDataRepository.containsKafkaTopic(topic)) {
              pushStatusCollector.subscribeTopic(topic, offlinePushStatus.getNumberOfPartition());
              ExecutionStatusWithDetails statusWithDetails =
                  checkPushStatus(offlinePushStatus, routingDataRepository.getPartitionAssignments(topic), null);
              if (statusWithDetails.getStatus().isTerminal()) {
                handleTerminalOfflinePushUpdate(offlinePushStatus, statusWithDetails);
              } else {
                checkWhetherToStartBufferReplayForHybrid(offlinePushStatus);
              }
            } else {
              // In any case, we found the offline push status is STARTED, but the related version could not be found.
              // We only log it as cleaning up here was found to prematurely delete push jobs during controller failover
              LOGGER.info("Found legacy offline push: {}", offlinePushStatus.getKafkaTopic());
            }
          }
        } catch (Exception e) {
          LOGGER.error("Could not load offline push for {}", offlinePushStatus.getKafkaTopic(), e);
        }
      }

      // scan the map to see if there are any old error push statues that can be retired
      Map<String, List<Integer>> storeToVersionNumsMap = new HashMap<>();
      topicToPushMap.keySet()
          .forEach(
              topic -> storeToVersionNumsMap
                  .computeIfAbsent(Version.parseStoreFromKafkaTopicName(topic), storeName -> new ArrayList<>())
                  .add(Version.parseVersionFromKafkaTopicName(topic)));

      storeToVersionNumsMap.forEach(this::retireOldErrorPushes);

      // Update the last successful push duration time for each store.
      storeToVersionNumsMap.keySet().forEach(storeName -> {
        Integer currentVersion = getStoreCurrentVersion(storeName);
        if (currentVersion != null) {
          OfflinePushStatus currentVersionPushStatus =
              topicToPushMap.get(Version.composeKafkaTopic(storeName, currentVersion));
          if (currentVersionPushStatus != null) {
            long durationSecs = currentVersionPushStatus.getSuccessfulPushDurationInSecs();
            if (durationSecs >= 0) {
              aggPushHealthStats.recordSuccessfulPushGauge(storeName, durationSecs);
            }
          }
        }
      });

      LOGGER.info("Load all pushes finished for cluster {}'s {}", clusterName, getClass().getSimpleName());
    }
  }

  @Override
  public void startMonitorOfflinePush(
      String kafkaTopic,
      int numberOfPartition,
      int replicaFactor,
      OfflinePushStrategy strategy) {
    String storeName = Version.parseStoreFromKafkaTopicName(kafkaTopic);
    try (AutoCloseableLock ignore = clusterLockManager.createStoreWriteLock(storeName)) {
      if (topicToPushMap.containsKey(kafkaTopic)) {
        ExecutionStatus existingStatus = getPushStatus(kafkaTopic);
        if (existingStatus.isError()) {
          LOGGER.info(
              "The previous push status for topic: {} is 'ERROR',"
                  + " and the new push will clean up the previous 'ERROR' push status",
              kafkaTopic);
          cleanupPushStatus(getOfflinePush(kafkaTopic), true);
        } else {
          throw new VeniceException(
              "Push status has already been created for topic:" + kafkaTopic + " in cluster:" + clusterName);
        }
      }

      OfflinePushStatus pushStatus = new OfflinePushStatus(kafkaTopic, numberOfPartition, replicaFactor, strategy);
      offlinePushAccessor.createOfflinePushStatusAndItsPartitionStatuses(pushStatus);
      topicToPushMap.put(kafkaTopic, pushStatus);
      offlinePushAccessor.subscribePartitionStatusChange(pushStatus, this);
      routingDataRepository.subscribeRoutingDataChange(kafkaTopic, this);
      pushStatusCollector.subscribeTopic(kafkaTopic, numberOfPartition);
      LOGGER.info("Started monitoring push on topic:{}", kafkaTopic);
    }
  }

  @Override
  public void stopMonitorOfflinePush(String kafkaTopic, boolean deletePushStatus, boolean isForcedDelete) {
    LOGGER.info("Stopping monitoring push on topic:{}", kafkaTopic);
    String storeName = Version.parseStoreFromKafkaTopicName(kafkaTopic);
    try (AutoCloseableLock ignore = clusterLockManager.createStoreWriteLock(storeName)) {
      if (!topicToPushMap.containsKey(kafkaTopic)) {
        LOGGER.warn("Push status does not exist for topic:{} in cluster:{}", kafkaTopic, clusterName);
        return;
      }
      OfflinePushStatus pushStatus = getOfflinePush(kafkaTopic);
      offlinePushAccessor.unsubscribePartitionsStatusChange(pushStatus, this);
      routingDataRepository.unSubscribeRoutingDataChange(kafkaTopic, this);
      if (pushStatus.getCurrentStatus().isError() && !isForcedDelete) {
        retireOldErrorPushes(storeName);
      } else {
        cleanupPushStatus(pushStatus, deletePushStatus);
      }
      pushStatusCollector.unsubscribeTopic(kafkaTopic);
      LOGGER.info("Stopped monitoring push on topic: {}", kafkaTopic);
    }
  }

  @Override
  public void stopAllMonitoring() {
    LOGGER.info("Stopping monitoring push for all topics.");
    try (AutoCloseableLock ignore = clusterLockManager.createClusterWriteLock()) {
      for (Map.Entry<String, OfflinePushStatus> entry: topicToPushMap.entrySet()) {
        String kafkaTopic = entry.getKey();
        stopMonitorOfflinePush(kafkaTopic, false, false);
      }
      LOGGER.info("Successfully stopped monitoring push for all topics.");
      pushStatusCollector.clear();
    } catch (Exception e) {
      LOGGER.error("Error when stopping monitoring push for all topics", e);
    }
  }

  @Override
  public void cleanupStoreStatus(String storeName) {
    try (AutoCloseableLock ignore = clusterLockManager.createStoreWriteLock(storeName)) {
      List<String> topicList = topicToPushMap.keySet()
          .stream()
          .filter(topic -> Version.parseStoreFromKafkaTopicName(topic).equals(storeName))
          .collect(Collectors.toList());

      topicList.forEach(topic -> cleanupPushStatus(getOfflinePush(topic), true));
    }
  }

  @Override
  public OfflinePushStatus getOfflinePushOrThrow(String topic) {
    if (topicToPushMap.containsKey(topic)) {
      return topicToPushMap.get(topic);
    } else {
      throw new VeniceException("Can not find offline push status for topic:" + topic);
    }
  }

  protected OfflinePushStatus getOfflinePush(String topic) {
    return topicToPushMap.get(topic);
  }

  public ExecutionStatus getPushStatus(String topic) {
    return getPushStatusAndDetails(topic).getStatus();
  }

  @Override
  public ExecutionStatusWithDetails getIncrementalPushStatusAndDetails(
      String kafkaTopic,
      String incrementalPushVersion,
      HelixCustomizedViewOfflinePushRepository customizedViewRepo) {
    OfflinePushStatus pushStatus = getOfflinePush(kafkaTopic);
    if (pushStatus == null) {
      return new ExecutionStatusWithDetails(NOT_CREATED, "Offline job hasn't been created yet.");
    }
    Map<Integer, Map<CharSequence, Integer>> pushStatusMap = pushStatus.getIncrementalPushStatus(
        getRoutingDataRepository().getPartitionAssignments(kafkaTopic),
        incrementalPushVersion);
    Map<Integer, Integer> completedReplicas =
        customizedViewRepo.getCompletedStatusReplicas(kafkaTopic, pushStatus.getNumberOfPartition());
    ExecutionStatus incrementalPushStatus = checkIncrementalPushStatus(
        pushStatusMap,
        completedReplicas,
        kafkaTopic,
        incrementalPushVersion,
        pushStatus.getNumberOfPartition(),
        pushStatus.getReplicationFactor());
    return new ExecutionStatusWithDetails(incrementalPushStatus);
  }

  @Override
  public ExecutionStatusWithDetails getIncrementalPushStatusFromPushStatusStore(
      String kafkaTopic,
      String incrementalPushVersion,
      HelixCustomizedViewOfflinePushRepository customizedViewRepo,
      PushStatusStoreReader pushStatusStoreReader) {
    OfflinePushStatus pushStatus = getOfflinePush(kafkaTopic);
    if (pushStatus == null) {
      return new ExecutionStatusWithDetails(NOT_CREATED, "Offline job hasn't been created yet.");
    }
    return getIncrementalPushStatusFromPushStatusStore(
        kafkaTopic,
        incrementalPushVersion,
        customizedViewRepo,
        pushStatusStoreReader,
        pushStatus.getNumberOfPartition(),
        pushStatus.getReplicationFactor());
  }

  ExecutionStatusWithDetails getIncrementalPushStatusFromPushStatusStore(
      String kafkaTopic,
      String incrementalPushVersion,
      HelixCustomizedViewOfflinePushRepository customizedViewRepo,
      PushStatusStoreReader pushStatusStoreReader,
      int numberOfPartitions,
      int replicationFactor) {
    LOGGER.debug(
        "Querying incremental push status from PS3 for storeVersion: {}, incrementalPushVersion: {}",
        kafkaTopic,
        incrementalPushVersion);
    String storeName = Version.parseStoreFromKafkaTopicName(kafkaTopic);
    int storeVersion = Version.parseVersionFromVersionTopicName(kafkaTopic);
    Map<Integer, Map<CharSequence, Integer>> pushStatusMap =
        pushStatusStoreReader.getPartitionStatuses(storeName, storeVersion, incrementalPushVersion, numberOfPartitions);
    Map<Integer, Integer> completedReplicas =
        customizedViewRepo.getCompletedStatusReplicas(kafkaTopic, numberOfPartitions);
    return new ExecutionStatusWithDetails(
        checkIncrementalPushStatus(
            pushStatusMap,
            completedReplicas,
            kafkaTopic,
            incrementalPushVersion,
            numberOfPartitions,
            replicationFactor));
  }

  private ExecutionStatus checkIncrementalPushStatus(
      Map<Integer, Map<CharSequence, Integer>> pushStatusMap,
      Map<Integer, Integer> completedReplicasInPartition,
      String kafkaTopic,
      String incrementalPushVersion,
      int partitionCount,
      int replicationFactor) {

    class IncPushPartitionStates {
      private static final int UNKNOWN = -1;
      private int partitionId;
      private int minRequiredReplicationFactor;
      private int numOfReplicasWithEoip;

      private IncPushPartitionStates(int partitionId, int minRequiredReplicationFactor, int numOfReplicasWithEoip) {
        this.partitionId = partitionId;
        this.minRequiredReplicationFactor = minRequiredReplicationFactor;
        this.numOfReplicasWithEoip = numOfReplicasWithEoip;
      }

      private IncPushPartitionStates(int partitionId) {
        this(partitionId, UNKNOWN, UNKNOWN);
      }

      @Override
      public String toString() {
        return String.format(
            "(%s, %s, %s)",
            partitionId,
            minRequiredReplicationFactor == UNKNOWN ? "U" : minRequiredReplicationFactor,
            numOfReplicasWithEoip == UNKNOWN ? "U" : numOfReplicasWithEoip);
      }
    }

    // when push status map is null or empty means that given incremental push hasn't been created/started yet
    if (pushStatusMap == null || pushStatusMap.isEmpty()) {
      return NOT_CREATED;
    }
    int numberOfPartitionsWithEnoughEoipReceivedReplicas = 0;
    boolean isIncrementalPushStatusAvailableForAtLeastOneReplica = false;

    List<IncPushPartitionStates> unFinishedPartitions = new LinkedList<>();
    for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
      Map<CharSequence, Integer> replicaStatusMap = pushStatusMap.get(partitionId);
      // inc push status of replicas of this partition is not available yet
      if (replicaStatusMap == null || replicaStatusMap.isEmpty()) {
        unFinishedPartitions.add(new IncPushPartitionStates(partitionId));
        continue;
      }

      int numberOfReplicasWithEoipStatus = 0;
      for (Map.Entry<CharSequence, Integer> replicaStatus: replicaStatusMap.entrySet()) {
        if (!ExecutionStatus.isIncrementalPushStatus(replicaStatus.getValue())) {
          return ERROR;
        }
        isIncrementalPushStatusAvailableForAtLeastOneReplica = true;
        if (replicaStatus.getValue() == END_OF_INCREMENTAL_PUSH_RECEIVED.getValue()) {
          numberOfReplicasWithEoipStatus++;
        }
      }

      // To consider a push job completed, EOIP status should be reported by all replicas with COMPLETED status
      // in customized view and number of such replicas cannot be less than (replicationFactor - 1).
      int minRequiredReplicationFactor =
          Math.max(1, Math.max(replicationFactor - 1, completedReplicasInPartition.getOrDefault(partitionId, 0)));
      if (numberOfReplicasWithEoipStatus >= minRequiredReplicationFactor) {
        numberOfPartitionsWithEnoughEoipReceivedReplicas++;
      } else {
        unFinishedPartitions
            .add(new IncPushPartitionStates(partitionId, minRequiredReplicationFactor, numberOfReplicasWithEoipStatus));
      }
    }
    if (numberOfPartitionsWithEnoughEoipReceivedReplicas == partitionCount) {
      return END_OF_INCREMENTAL_PUSH_RECEIVED;
    }
    LOGGER.info(
        "{} out of {} partitions are sufficiently replicated, kafkaTopic: {}, incrementalPushVersion: {}, unfinished partitions (partitionId, minRequired, No. of EOIP replicas): {}, size: {}",
        numberOfPartitionsWithEnoughEoipReceivedReplicas,
        partitionCount,
        kafkaTopic,
        incrementalPushVersion,
        unFinishedPartitions,
        unFinishedPartitions.size());

    // to report SOIP at least one replica should have seen either SOIP or EOIP
    if (isIncrementalPushStatusAvailableForAtLeastOneReplica) {
      return START_OF_INCREMENTAL_PUSH_RECEIVED;
    }
    return NOT_CREATED;
  }

  @Override
  public Set<String> getOngoingIncrementalPushVersions(String kafkaTopic) {
    OfflinePushStatus pushStatus = getOfflinePush(kafkaTopic);
    String latestIncrementalPushVersion = null;
    if (pushStatus != null) {
      latestIncrementalPushVersion =
          pushStatus.getLatestIncrementalPushVersion(getRoutingDataRepository().getPartitionAssignments(kafkaTopic));
    }
    if (latestIncrementalPushVersion == null || latestIncrementalPushVersion.isEmpty()) {
      return Collections.emptySet();
    }
    return Collections.singleton(latestIncrementalPushVersion);
  }

  /**
   * Get ongoing incremental push versions from the push status store
   * @param kafkaTopic kafka topic belonging to a store version for which we are fetching ongoing inc-pushe versions
   * @param pushStatusStoreReader - push status system store reader
   * @return set of (supposedly) ongoing incremental pushes
   */
  @Override
  public Set<String> getOngoingIncrementalPushVersions(String kafkaTopic, PushStatusStoreReader pushStatusStoreReader) {
    String storeName = Version.parseStoreFromKafkaTopicName(kafkaTopic);
    int storeVersion = Version.parseVersionFromVersionTopicName(kafkaTopic);
    return pushStatusStoreReader.getSupposedlyOngoingIncrementalPushVersions(storeName, storeVersion)
        .keySet()
        .stream()
        .map(CharSequence::toString)
        .collect(Collectors.toSet());
  }

  @Override
  public ExecutionStatusWithDetails getPushStatusAndDetails(String topic) {
    OfflinePushStatus pushStatus = getOfflinePush(topic);
    if (pushStatus == null) {
      return new ExecutionStatusWithDetails(NOT_CREATED, "Offline job hasn't been created yet.");
    }
    ExecutionStatus currentPushStatus = pushStatus.getCurrentStatus();
    if (currentPushStatus.equals(ExecutionStatus.NOT_STARTED) || currentPushStatus.equals(ExecutionStatus.STARTED)) {
      // Check whether resource assignment has completed; if yes, continue; if no:
      // 1. if the push duration hasn't passed the resource assignment wait timeout, log the current status
      // 2. if the duration has passed the wait timeout, terminate the push
      final ResourceAssignment resourceAssignment = routingDataRepository.getResourceAssignment();
      final OfflinePushStrategy strategy = pushStatus.getStrategy();
      final int replicationFactor = pushStatus.getReplicationFactor();
      final PushStatusDecider statusDecider = strategy.getPushStatusDecider();
      Optional<String> notReadyReason =
          statusDecider.hasEnoughNodesToStartPush(topic, replicationFactor, resourceAssignment, Optional.empty());
      if (notReadyReason.isPresent()) {
        final long elapsedTimeInSec = getDurationInSec(pushStatus);
        if (elapsedTimeInSec < TimeUnit.MILLISECONDS.toSeconds(offlineJobResourceAssignmentWaitTimeInMilliseconds)) {
          LOGGER.info(
              "After waiting for " + elapsedTimeInSec + " seconds, resource assignment for: " + topic
                  + " is still not complete, strategy=" + strategy.toString() + ", replicationFactor="
                  + replicationFactor + ", reason=" + notReadyReason.get());
        } else {
          // early termination
          // Time out, after waiting maxWaitTimeMs, there are not enough nodes assigned.
          recordPushPreparationDuration(topic, elapsedTimeInSec);
          String errorMsg = "After waiting for " + elapsedTimeInSec + " seconds, resource assignment for: " + topic
              + " timed out, strategy=" + strategy.toString() + ", replicationFactor=" + replicationFactor + ", reason="
              + notReadyReason.get();
          ExecutionStatusWithDetails executionStatusWithDetails = new ExecutionStatusWithDetails(ERROR, errorMsg);
          handleTerminalOfflinePushUpdate(pushStatus, executionStatusWithDetails);
          return executionStatusWithDetails;
        }
      } else {
        // Update the status details if this is the first time finding out Helix assignment completes
        Optional<String> statusDetails = pushStatus.getOptionalStatusDetails();
        if (statusDetails.isPresent() && Objects.equals(statusDetails.get(), HELIX_RESOURCE_NOT_CREATED)) {
          OfflinePushStatus newStatus =
              refreshAndUpdatePushStatus(topic, ExecutionStatus.STARTED, Optional.of(HELIX_ASSIGNMENT_COMPLETED));
          if (newStatus != null) {
            pushStatus = newStatus;
          }
          recordPushPreparationDuration(topic, getDurationInSec(pushStatus));
        }
      }
    }
    return new ExecutionStatusWithDetails(
        currentPushStatus,
        pushStatus.getStatusDetails(),
        true,
        pushStatus.getStatusUpdateTimestamp());
  }

  @Override
  public List<String> getTopicsOfOngoingOfflinePushes() {
    List<String> result = new ArrayList<>();
    result.addAll(
        topicToPushMap.values()
            .stream()
            .filter(status -> !status.getCurrentStatus().isTerminal())
            .map(OfflinePushStatus::getKafkaTopic)
            .collect(Collectors.toList()));
    return result;
  }

  @Override
  public List<OfflinePushStatus> getOfflinePushStatusForStore(String storeName) {
    List<OfflinePushStatus> result = new ArrayList<>();
    result.addAll(
        topicToPushMap.values()
            .stream()
            .filter(status -> Version.parseStoreFromKafkaTopicName(status.getKafkaTopic()).equals(storeName))
            .collect(Collectors.toList()));
    return result;
  }

  @Override
  public List<UncompletedPartition> getUncompletedPartitions(String topic) {
    OfflinePushStatus pushStatus = getOfflinePush(topic);
    if (pushStatus == null || pushStatus.getCurrentStatus().equals(COMPLETED)) {
      return Collections.emptyList();
    }
    PushStatusDecider decider = pushStatus.getStrategy().getPushStatusDecider();
    List<UncompletedPartition> uncompletedPartitions = new ArrayList<>();
    for (PartitionStatus partitionStatus: pushStatus.getPartitionStatuses()) {
      int completedReplicaInPartition = 0;
      List<UncompletedReplica> uncompletedReplicas = new ArrayList<>();
      for (ReplicaStatus replicaStatus: partitionStatus.getReplicaStatuses()) {
        ExecutionStatus currentStatus = PushStatusDecider.getReplicaCurrentStatus(replicaStatus.getStatusHistory());
        if (currentStatus.equals(COMPLETED)) {
          completedReplicaInPartition++;
        } else {
          UncompletedReplica uncompletedReplica = new UncompletedReplica(
              replicaStatus.getInstanceId(),
              currentStatus,
              replicaStatus.getCurrentProgress(),
              replicaStatus.getIncrementalPushVersion());
          uncompletedReplicas.add(uncompletedReplica);
        }
      }
      if (!decider.hasEnoughReplicasForOnePartition(completedReplicaInPartition, pushStatus.getReplicationFactor())) {
        uncompletedPartitions.add(new UncompletedPartition(partitionStatus.getPartitionId(), uncompletedReplicas));
      }
    }
    return uncompletedPartitions;
  }

  @Override
  public void markOfflinePushAsError(String topic, String statusDetails) {
    OfflinePushStatus status = getOfflinePush(topic);
    if (status == null) {
      LOGGER.warn(
          "Could not find offline push status for topic: {}" + ". Ignore the request of marking status as ERROR.",
          topic);
      return;
    }

    handleTerminalOfflinePushUpdate(status, new ExecutionStatusWithDetails(ERROR, statusDetails));
  }

  /**
   * this is to clear legacy push statuses
   */
  private void cleanupPushStatus(OfflinePushStatus offlinePushStatus, boolean deletePushStatus) {
    String storeName = Version.parseStoreFromKafkaTopicName(offlinePushStatus.getKafkaTopic());
    try (AutoCloseableLock ignore = clusterLockManager.createStoreWriteLock(storeName)) {
      topicToPushMap.remove(offlinePushStatus.getKafkaTopic());
      if (deletePushStatus) {
        offlinePushAccessor.deleteOfflinePushStatusAndItsPartitionStatuses(offlinePushStatus.getKafkaTopic());
      }
    } catch (Exception e) {
      LOGGER.warn("Could not delete legacy push status: {}", offlinePushStatus.getKafkaTopic(), e);
    }
  }

  protected void retireOldErrorPushes(String storeName) {
    List<Integer> versionNums = topicToPushMap.keySet()
        .stream()
        .filter(topic -> Version.parseStoreFromKafkaTopicName(topic).equals(storeName))
        .map(Version::parseVersionFromKafkaTopicName)
        .collect(Collectors.toList());

    retireOldErrorPushes(storeName, versionNums);
  }

  /**
   * We'd like to keep at most {@link #MAX_PUSH_TO_KEEP} push status for debugging purpose.
   * If it's a successful push, it will be cleaned up when the version is retired. If it's
   * error push, we'll leave it until the store reaches the push status limit.
   */
  private void retireOldErrorPushes(String storeName, List<Integer> versionNums) {
    List<OfflinePushStatus> errorPushStatusList = versionNums.stream()
        .sorted()
        .map(version -> getOfflinePush(Version.composeKafkaTopic(storeName, version)))
        .filter(offlinePushStatus -> offlinePushStatus.getCurrentStatus().isError())
        .collect(Collectors.toList());

    for (OfflinePushStatus errorPushStatus: errorPushStatusList) {
      if (versionNums.size() <= MAX_PUSH_TO_KEEP) {
        break;
      }

      int errorVersion = Version.parseVersionFromKafkaTopicName(errorPushStatus.getKafkaTopic());
      // Make sure we do boxing; List.remove(primitive int) treats the primitive int as index
      versionNums.remove(Integer.valueOf(errorVersion));

      cleanupPushStatus(errorPushStatus, true);
    }
  }

  protected abstract ExecutionStatusWithDetails checkPushStatus(
      OfflinePushStatus pushStatus,
      PartitionAssignment partitionAssignment,
      DisableReplicaCallback callback);

  public abstract List<Instance> getReadyToServeInstances(PartitionAssignment partitionAssignment, int partitionId);

  public OfflinePushStatus refreshAndUpdatePushStatus(
      String kafkaTopic,
      ExecutionStatus newStatus,
      Optional<String> newStatusDetails) {
    final OfflinePushStatus refreshedPushStatus = getOfflinePushOrThrow(kafkaTopic);
    if (refreshedPushStatus.validatePushStatusTransition(newStatus)) {
      return updatePushStatus(refreshedPushStatus, newStatus, newStatusDetails);
    } else {
      LOGGER.info(
          "refreshedPushStatus does not allow transitioning to {}, because it is currently in: {} status. Will skip "
              + "updating the status.",
          newStatus,
          refreshedPushStatus.getCurrentStatus());
      return null;
    }
  }

  /**
   * Direct calls to updatePushStatus should be made carefully. e.g. calling with {@link ExecutionStatus}.ERROR or
   * other terminal status update should be made through handleOfflinePushUpdate. That method will then invoke
   * handleErrorPush and perform relevant operations to handle the ERROR status update properly.
   *
   * @return the new {@link OfflinePushStatus} if it was updated, or null if the update was skipped.
   */
  protected OfflinePushStatus updatePushStatus(
      OfflinePushStatus expectedCurrPushStatus,
      ExecutionStatus newExecutionStatus,
      Optional<String> newExecutionStatusDetails) {
    final String kafkaTopic = expectedCurrPushStatus.getKafkaTopic();
    String storeName = Version.parseStoreFromKafkaTopicName(kafkaTopic);
    try (AutoCloseableLock ignore = clusterLockManager.createStoreWriteLock(storeName)) {
      final OfflinePushStatus actualCurrPushStatus = getOfflinePushOrThrow(kafkaTopic);
      if (!Objects.equals(actualCurrPushStatus, expectedCurrPushStatus)) {
        LOGGER.warn(
            "For topic {}, the actual current push status is different from the expected current push status."
                + " [actual current status = {}], [expected push status = {}]",
            kafkaTopic,
            actualCurrPushStatus,
            expectedCurrPushStatus);
      }
      if (!actualCurrPushStatus.validatePushStatusTransition(newExecutionStatus)) {
        LOGGER.warn(
            "Skip updating push execution status for topic {} due to invalid transition from {} to {}",
            kafkaTopic,
            actualCurrPushStatus.getCurrentStatus(),
            newExecutionStatus);
        return null;
      }

      OfflinePushStatus clonedPushStatus = expectedCurrPushStatus.clonePushStatus();
      clonedPushStatus.updateStatus(newExecutionStatus, newExecutionStatusDetails);
      // Update remote storage
      offlinePushAccessor.updateOfflinePushStatus(clonedPushStatus);
      // Update local copy
      topicToPushMap.put(kafkaTopic, clonedPushStatus);
      return clonedPushStatus;
    }
  }

  protected long getDurationInSec(OfflinePushStatus pushStatus) {
    long start = pushStatus.getStartTimeSec();
    return System.currentTimeMillis() / Time.MS_PER_SECOND - start;
  }

  protected OfflinePushAccessor getOfflinePushAccessor() {
    return offlinePushAccessor;
  }

  protected ReadWriteStoreRepository getReadWriteStoreRepository() {
    return metadataRepository;
  }

  protected RoutingDataRepository getRoutingDataRepository() {
    return routingDataRepository;
  }

  @Override
  public void onPartitionStatusChange(String topic, ReadOnlyPartitionStatus partitionStatus) {
    String storeName = Version.parseStoreFromKafkaTopicName(topic);
    try (AutoCloseableLock ignore = clusterLockManager.createStoreWriteLock(storeName)) {
      OfflinePushStatus pushStatus = getOfflinePush(topic);
      if (pushStatus == null) {
        LOGGER.error("Can not find Offline push for topic:{}, ignore the partition status change notification.", topic);
        return;
      }

      // On controller side, partition status is read only. It could be only updated by storage node.
      pushStatus = pushStatus.clonePushStatus();
      pushStatus.setPartitionStatus(partitionStatus);
      this.topicToPushMap.put(pushStatus.getKafkaTopic(), pushStatus);

      onPartitionStatusChange(pushStatus);
    }
  }

  protected void onPartitionStatusChange(OfflinePushStatus offlinePushStatus) {
    checkWhetherToStartBufferReplayForHybrid(offlinePushStatus);
  }

  protected DisableReplicaCallback getDisableReplicaCallback(String kafkaTopic) {
    if (!disableErrorLeaderReplica) {
      return null;
    }
    return new DisableReplicaCallback() {
      private final Map<String, Set<Integer>> disabledReplicaMap = new HashMap<>();

      @Override
      public void disableReplica(String instance, int partitionId) {
        LOGGER.warn(
            "Disabling errored out leader replica of {} partition: {} on host {}",
            kafkaTopic,
            partitionId,
            instance);
        helixAdminClient.enablePartition(
            false,
            clusterName,
            instance,
            kafkaTopic,
            Collections.singletonList(HelixUtils.getPartitionName(kafkaTopic, partitionId)));
        disabledReplicaMap.computeIfAbsent(instance, k -> new HashSet<>()).add(partitionId);
        disabledPartitionStats.recordDisabledPartition();
      }

      @Override
      public boolean isReplicaDisabled(String instance, int partitionId) {
        Set<Integer> disabledPartitions = disabledReplicaMap.computeIfAbsent(instance, k -> {
          helixClientThrottler.maybeThrottle(1);
          Map<String, List<String>> helixMap = helixAdminClient.getDisabledPartitionsMap(clusterName, instance);
          List<String> disablePartitionList = helixMap.get(kafkaTopic);
          return disablePartitionList != null
              ? disablePartitionList.stream().map(HelixUtils::getPartitionId).collect(Collectors.toSet())
              : null;
        });
        return disabledPartitions != null && disabledPartitions.contains(partitionId);
      }
    };
  }

  @Override
  public void onExternalViewChange(PartitionAssignment partitionAssignment) {
    LOGGER.info("Received the routing data changed notification for topic: {}", partitionAssignment.getTopic());
    String storeName = Version.parseStoreFromKafkaTopicName(partitionAssignment.getTopic());

    try (AutoCloseableLock ignore = clusterLockManager.createStoreWriteLock(storeName)) {
      String kafkaTopic = partitionAssignment.getTopic();
      OfflinePushStatus pushStatus = getOfflinePush(kafkaTopic);

      if (pushStatus != null) {
        ExecutionStatus previousStatus = pushStatus.getCurrentStatus();
        if (previousStatus.equals(ExecutionStatus.COMPLETED) || previousStatus.isError()) {
          LOGGER.warn("Skip updating push status: {} since it is already in: {}", kafkaTopic, previousStatus);
          return;
        }

        ExecutionStatusWithDetails statusWithDetails =
            checkPushStatus(pushStatus, partitionAssignment, getDisableReplicaCallback(kafkaTopic));
        if (!statusWithDetails.getStatus().equals(pushStatus.getCurrentStatus())) {
          if (statusWithDetails.getStatus().isTerminal()) {
            LOGGER.info(
                "Offline push status will be changed to {} for topic: {} from status: {}",
                statusWithDetails.getStatus(),
                kafkaTopic,
                pushStatus.getCurrentStatus());
            handleTerminalOfflinePushUpdate(pushStatus, statusWithDetails);
          } else if (statusWithDetails.getStatus().equals(ExecutionStatus.END_OF_PUSH_RECEIVED)) {
            // For all partitions, at least one replica has received the EOP. Check if it's time to start buffer replay.
            checkWhetherToStartBufferReplayForHybrid(pushStatus);
          }
        }
      } else {
        LOGGER.info(
            "Can not find a running offline push for topic:{}, ignore the routing data changed notification.",
            partitionAssignment.getTopic());
      }
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
  public void onRoutingDataDeleted(String kafkaTopic) {
    // Beside the external view, we also care about the ideal state here. If the resource was deleted from the
    // externalview by mistake,
    // as long as the resource exists in the ideal state, helix will recover it automatically, thus push will keep
    // working.
    if (routingDataRepository.doesResourcesExistInIdealState(kafkaTopic)) {
      LOGGER.warn("Resource is remaining in the ideal state. Ignore the deletion in the external view.");
      return;
    }
    OfflinePushStatus pushStatus;
    pushStatus = getOfflinePush(kafkaTopic);
    if (pushStatus != null && pushStatus.getCurrentStatus().equals(ExecutionStatus.STARTED)) {
      String statusDetails = "Helix resource for Topic:" + kafkaTopic + " is deleted, stopping the running push.";
      LOGGER.info(statusDetails);
      handleTerminalOfflinePushUpdate(pushStatus, new ExecutionStatusWithDetails(ERROR, statusDetails));
    }
  }

  protected void checkWhetherToStartBufferReplayForHybrid(OfflinePushStatus offlinePushStatus) {
    // As the outer method already locked on this instance, so this method is thread-safe.
    String storeName = Version.parseStoreFromKafkaTopicName(offlinePushStatus.getKafkaTopic());
    Store store = getReadWriteStoreRepository().getStore(storeName);
    if (store == null) {
      LOGGER
          .info("Got a null store from metadataRepository for store name: '{}'. Will attempt a refresh().", storeName);
      store = getReadWriteStoreRepository().refreshOneStore(storeName);
      if (store == null) {
        throw new IllegalStateException(
            "checkHybridPushStatus could not find a store named '" + storeName
                + "' in the metadataRepository, even after refresh()!");
      } else {
        LOGGER.info("metadataRepository.refresh() allowed us to retrieve store: '{}'!", storeName);
      }
    }

    if (store.isHybrid()) {
      Version version = store.getVersion(Version.parseVersionFromKafkaTopicName(offlinePushStatus.getKafkaTopic()));
      boolean isDataRecovery = version != null && version.getDataRecoveryVersionConfig() != null;
      if (offlinePushStatus.isReadyToStartBufferReplay(isDataRecovery)) {
        LOGGER.info("{} is ready to start buffer replay.", offlinePushStatus.getKafkaTopic());
        RealTimeTopicSwitcher realTimeTopicSwitcher = getRealTimeTopicSwitcher();
        try {
          String newStatusDetails;
          realTimeTopicSwitcher.switchToRealTimeTopic(
              Version.composeRealTimeTopic(storeName),
              offlinePushStatus.getKafkaTopic(),
              store,
              aggregateRealTimeSourceKafkaUrl,
              activeActiveRealTimeSourceKafkaURLs);
          newStatusDetails = "kicked off buffer replay";
          updatePushStatus(offlinePushStatus, ExecutionStatus.END_OF_PUSH_RECEIVED, Optional.of(newStatusDetails));
          LOGGER.info("Successfully {} for offlinePushStatus: {}", newStatusDetails, offlinePushStatus);
        } catch (Exception e) {
          // TODO: Figure out a better error handling...
          String newStatusDetails = "Failed to kick off the buffer replay";
          handleTerminalOfflinePushUpdate(offlinePushStatus, new ExecutionStatusWithDetails(ERROR, newStatusDetails));
          LOGGER.error("{} for offlinePushStatus: {}", newStatusDetails, offlinePushStatus, e);
        }
      } else if (!offlinePushStatus.getCurrentStatus().isTerminal()) {
        LOGGER.info(
            "{} is not ready to start buffer replay. Current state: {}",
            offlinePushStatus.getKafkaTopic(),
            offlinePushStatus.getCurrentStatus().toString());
      }
    }
  }

  /**
   * This method will unsubscribe external view changes and is intended to be called when the statues are terminable.
   */
  protected void handleTerminalOfflinePushUpdate(
      OfflinePushStatus pushStatus,
      ExecutionStatusWithDetails statusWithDetails) {
    ExecutionStatus status = statusWithDetails.getStatus();
    LOGGER.info(
        "Found a offline pushes could be terminated: {} status: {}",
        pushStatus.getKafkaTopic(),
        statusWithDetails.getStatus());
    if (status.equals(ExecutionStatus.COMPLETED)) {
      pushStatusCollector.handleServerPushStatusUpdate(pushStatus.getKafkaTopic(), COMPLETED, null);
    } else if (status.isError()) {
      String statusDetailsString = "STATUS DETAILS ABSENT.";
      if (statusWithDetails.getDetails() == null) {
        LOGGER.error(
            "Status details should be provided in order to terminateOfflinePush, but they are missing.",
            new VeniceException("Exception not thrown, for stacktrace logging purposes."));
      } else {
        statusDetailsString = statusWithDetails.getDetails();
      }
      pushStatusCollector.handleServerPushStatusUpdate(pushStatus.getKafkaTopic(), status, statusDetailsString);
    }
  }

  protected void handleCompletedPush(String topic) {
    routingDataRepository.unSubscribeRoutingDataChange(topic, this);
    OfflinePushStatus pushStatus = getOfflinePush(topic);
    if (pushStatus == null) {
      LOGGER.warn("Could not find OfflinePushStatus for topic: {}, will skip push completion handling", topic);
      return;
    }
    LOGGER.info(
        "Updating offline push status, push for: {} old status: {}, new status: {}",
        topic,
        pushStatus.getCurrentStatus(),
        ExecutionStatus.COMPLETED);

    long durationSecs = getDurationInSec(pushStatus);
    pushStatus.setSuccessfulPushDurationInSecs(durationSecs);
    String storeName = Version.parseStoreFromKafkaTopicName(topic);
    int versionNumber = Version.parseVersionFromKafkaTopicName(topic);
    updateStoreVersionStatus(storeName, versionNumber, VersionStatus.ONLINE);
    // Updating the version's overall push status must be the last step due to the current way we load and check for
    // pushes that might have completed during controller restart or leadership handover. If the overall push status is
    // updated first but failed to complete the version swap for whatever reason it will never get a second chance.
    updatePushStatus(pushStatus, ExecutionStatus.COMPLETED, Optional.empty());
    aggPushHealthStats.recordSuccessfulPush(storeName, durationSecs);
    aggPushHealthStats.recordSuccessfulPushGauge(storeName, durationSecs);
    // If we met some error to retire the old version, we should not throw the exception out to fail this operation,
    // because it will be collected once a new push is completed for this store.
    try {
      storeCleaner.topicCleanupWhenPushComplete(clusterName, storeName, versionNumber);
    } catch (Exception e) {
      LOGGER.warn(
          "Couldn't perform topic cleanup when push job completed for topic: {} in cluster: {}",
          topic,
          clusterName,
          e);
    }
    try {
      Store store = metadataRepository.getStore(storeName);
      /** Do not delete previous versions as for repush previous current version should be deleted instead
       * such deletions are handled in @see StoreBackupVersionCleanupService
       */
      if (store.getVersionOrThrow(versionNumber).getRepushSourceVersion() <= NON_EXISTING_VERSION) {
        storeCleaner.retireOldStoreVersions(clusterName, storeName, false, -1);
      }
    } catch (Exception e) {
      LOGGER.warn("Could not retire the old versions for store: {} in cluster: {}", storeName, clusterName, e);
    }
    LOGGER.info("Offline push for topic: {} is completed.", pushStatus.getKafkaTopic());
  }

  protected void handleErrorPush(String topic, ExecutionStatusWithDetails executionStatusWithDetails) {
    ExecutionStatus executionStatus = executionStatusWithDetails.getStatus();
    String statusDetails = executionStatusWithDetails.getDetails();
    routingDataRepository.unSubscribeRoutingDataChange(topic, this);
    OfflinePushStatus pushStatus = getOfflinePush(topic);
    if (pushStatus == null) {
      LOGGER.warn("Could not find OfflinePushStatus for topic: {}, will skip push error handling", topic);
      return;
    }
    LOGGER.info(
        "Updating offline push status, push for: {} is now {}, new status: {}, statusDetails: {}",
        topic,
        pushStatus.getCurrentStatus(),
        executionStatus,
        statusDetails);
    updatePushStatus(pushStatus, executionStatus, Optional.of(statusDetails));
    String storeName = Version.parseStoreFromKafkaTopicName(topic);
    int versionNumber = Version.parseVersionFromKafkaTopicName(topic);
    try {
      updateStoreVersionStatus(storeName, versionNumber, VersionStatus.ERROR);
      aggPushHealthStats.recordFailedPush(storeName, getDurationInSec(pushStatus));
      // If we met some error to delete error version, we should not throw the exception out to fail this operation,
      // because it will be collected once a new push is completed for this store.
      storeCleaner.deleteOneStoreVersion(clusterName, storeName, versionNumber);
    } catch (Exception e) {
      LOGGER.warn(
          "Could not delete error version: {} for store: {} in cluster: {}",
          versionNumber,
          storeName,
          clusterName,
          e);
    }
    LOGGER.info("Offline push for topic: {} fails.", pushStatus.getKafkaTopic());
  }

  private void updateStoreVersionStatus(String storeName, int versionNumber, VersionStatus status) {
    VersionStatus newStatus = status;
    try (AutoCloseableLock ignore = clusterLockManager.createStoreWriteLock(storeName)) {
      Store store = metadataRepository.getStore(storeName);
      if (store == null) {
        throw new VeniceNoStoreException(storeName);
      }

      if (!store.isEnableWrites() && status.equals(VersionStatus.ONLINE)) {
        newStatus = VersionStatus.PUSHED;
      }

      /**
       * The offline push job for this version has been killed by {@link com.linkedin.venice.controller.Admin#killOfflinePush}.
       * Don't set the status to ONLINE or swap current version.
       */
      Version version = store.getVersion(versionNumber);
      if (version != null && VersionStatus.isVersionKilled(version.getStatus())) {
        if (newStatus == VersionStatus.ONLINE) {
          /**
           * When a version is first killed and then completed, don't continue to update overall push status to complete
           * or retire old versions. Throw an exception to stop executing the follow-up logic in its caller
           * {@link AbstractPushMonitor#handleCompletedPush}.
           */
          throw new VeniceException(
              String.format(
                  "store: %s version: %d has been killed by killOfflinePush. Abort version status update and current version swapping",
                  storeName,
                  versionNumber));
        }

        if (VersionStatus.isVersionErrored(newStatus)) {
          /**
           * When a version is first killed and then errored, don't need to update its version status to ERROR.
           * Return to its caller to continue following clean-up work in {@link AbstractPushMonitor#handleErrorPush}.
           */
          return;
        }
      }

      store.updateVersionStatus(versionNumber, newStatus);
      LOGGER.info("Updated store: {} version: {} to status: {}", store.getName(), versionNumber, newStatus.toString());
      if (newStatus.equals(VersionStatus.ONLINE)) {
        if (versionNumber > store.getCurrentVersion()) {
          // Here we'll check if version swap is deferred. If so, we don't perform the setCurrentVersion. We'll continue
          // on and wait for an admin command to mark the version to 'current' OR just let the next push cycle it out.
          version = store.getVersion(versionNumber);
          if (version == null) {
            // This shouldn't be possible, but putting a check here just in case things go pear shaped
            throw new VeniceException(
                String.format(
                    "No version present for store %s version %d!  Aborting version swap!",
                    storeName,
                    versionNumber));
          }
          if (version.isVersionSwapDeferred()) {
            LOGGER.info(
                "Version swap is deferred for store {} on version {}. Skipping version swap.",
                store.getName(),
                versionNumber);
          } else {
            int previousVersion = store.getCurrentVersion();
            store.setCurrentVersion(versionNumber);
            realTimeTopicSwitcher.transmitVersionSwapMessage(store, previousVersion, versionNumber);
          }
        } else {
          LOGGER.info(
              "Current version for store {}: {} is newer than the given version: {}. "
                  + "The current version will not be changed.",
              store.getName(),
              store.getCurrentVersion(),
              versionNumber);
        }
      }
      metadataRepository.updateStore(store);
    }
  }

  private Integer getStoreCurrentVersion(String storeName) {
    Store store = metadataRepository.getStore(storeName);
    if (store == null) {
      return null;
    }
    return store.getCurrentVersion();
  }

  @Override
  public void recordPushPreparationDuration(String topic, long offlinePushWaitTimeInSecond) {
    String storeName = Version.parseStoreFromKafkaTopicName(topic);
    aggPushHealthStats.recordPushPrepartionDuration(storeName, offlinePushWaitTimeInSecond);
  }

  /**
   * For testing only; in order to override the topicReplicator with mocked Replicator.
   */
  public void setRealTimeTopicSwitcher(RealTimeTopicSwitcher realTimeTopicSwitcher) {
    this.realTimeTopicSwitcher = realTimeTopicSwitcher;
  }

  public RealTimeTopicSwitcher getRealTimeTopicSwitcher() {
    return realTimeTopicSwitcher;
  }

  @Override
  public boolean isOfflinePushMonitorDaVinciPushStatusEnabled() {
    return isOfflinePushMonitorDaVinciPushStatusEnabled;
  }
}
