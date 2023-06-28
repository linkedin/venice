package com.linkedin.venice.pushmonitor;

import static com.linkedin.venice.pushmonitor.ExecutionStatus.ARCHIVED;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.COMPLETED;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.DATA_RECOVERY_COMPLETED;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.END_OF_PUSH_RECEIVED;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.ERROR;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.STARTED;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.START_OF_INCREMENTAL_PUSH_RECEIVED;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.OfflinePushStrategy;
import com.linkedin.venice.meta.Partition;
import com.linkedin.venice.meta.PartitionAssignment;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Class stores all the statuses and history of one offline push.
 */
public class OfflinePushStatus {
  private static final Logger LOGGER = LogManager.getLogger(OfflinePushStatus.class);
  private final String kafkaTopic;
  private final int numberOfPartition;
  private final int replicationFactor;
  private final OfflinePushStrategy strategy;

  private ExecutionStatus currentStatus;
  /**
   * The initial status details will be overridden later, when the Helix resource is created.
   */
  private Optional<String> statusDetails = Optional.of(HELIX_RESOURCE_NOT_CREATED);
  private List<StatusSnapshot> statusHistory;
  private String incrementalPushVersion = "";
  // Key is Partition Id (0 to n-1); value is the corresponding partition status.
  private Map<Integer, PartitionStatus> partitionIdToStatus;

  private Map<String, String> pushProperties;

  private int successfulPushDurationInSecs = -1;

  public static final String HELIX_RESOURCE_NOT_CREATED = "Helix Resource not created.";
  public static final String HELIX_ASSIGNMENT_COMPLETED = "Helix assignment complete";

  public OfflinePushStatus(
      String kafkaTopic,
      int numberOfPartition,
      int replicationFactor,
      OfflinePushStrategy strategy) {
    this.kafkaTopic = kafkaTopic;
    this.numberOfPartition = numberOfPartition;
    this.replicationFactor = replicationFactor;
    this.strategy = strategy;
    this.pushProperties = new HashMap<>();
    this.currentStatus = STARTED; // Initial push status
    this.statusHistory = new ArrayList<>();
    addHistoricStatus(currentStatus, incrementalPushVersion);
    this.partitionIdToStatus = new VeniceConcurrentHashMap<>(numberOfPartition);
    for (int i = 0; i < numberOfPartition; i++) {
      ReadOnlyPartitionStatus partitionStatus = new ReadOnlyPartitionStatus(i, Collections.emptyList());
      partitionIdToStatus.put(i, partitionStatus);
    }
  }

  public void updateStatus(ExecutionStatus newStatus) {
    updateStatus(newStatus, Optional.empty());
  }

  public void updateStatus(ExecutionStatus newStatus, Optional<String> newStatusDetails) {
    if (validatePushStatusTransition(newStatus)) {
      this.currentStatus = newStatus;
      this.statusDetails = newStatusDetails;
      addHistoricStatus(newStatus, incrementalPushVersion);
    } else {
      if (this.currentStatus.equals(newStatus)) {
        // State change is redundant. Just log the event, no need to throw a whole trace.
        LOGGER.warn(
            "Redundant push state status received for state {}.  New state details: {}",
            newStatus,
            newStatusDetails.orElse("not specified!!"));
      } else {
        throw new VeniceException(
            "Can not transit status from: " + currentStatus + " to " + newStatus + " for topic " + kafkaTopic
                + ", newStatusDetails: " + newStatusDetails + ", statusHistory: " + statusHistory);
      }
    }
  }

  protected long getStartTimeSec() {
    String timeString = statusHistory.get(0).getTime();
    LocalDateTime time = LocalDateTime.parse(timeString);
    return time.atZone(ZoneId.systemDefault()).toEpochSecond();
  }

  /**
   * Judge whether current status could be transferred to the new status.
   * <p>
   * Push's state machine:
   * <ul>
   *   <li>NOT_STARTED->STARTED</li>
   *   <li>NOT_STARTED->ERROR</li>
   *   <li>STARTED->STARTED</li>
   *   <li>STARTED->COMPLETED</li>
   *   <li>STARTED->ERROR</li>
   *   <li>STARTED->END_OF_PUSH_RECEIVED</li>
   *   <li>END_OF_PUSH_RECEIVED->COMPLETED</li>
   *   <li>END_OF_PUSH_RECEIVED->ERROR</li>
   *   <li>COMPLETED->ARCHIVED</li>
   *   <li>ERROR->ARCHIVED</li>
   * </ul>
   * @param newStatus
   * @return
   */
  public boolean validatePushStatusTransition(ExecutionStatus newStatus) {
    boolean isValid;
    switch (currentStatus) {
      case NOT_CREATED:
        isValid = Utils.verifyTransition(newStatus, STARTED, ERROR);
        break;
      case STARTED:
        isValid = Utils.verifyTransition(newStatus, STARTED, ERROR, COMPLETED, END_OF_PUSH_RECEIVED);
        break;
      case ERROR:
      case COMPLETED:
        isValid = Utils.verifyTransition(newStatus, ARCHIVED);
        break;
      case END_OF_PUSH_RECEIVED:
        isValid = Utils.verifyTransition(newStatus, COMPLETED, ERROR);
        break;
      default:
        isValid = false;
    }
    return isValid;
  }

  public void setPartitionStatus(PartitionStatus partitionStatus) {
    setPartitionStatus(partitionStatus, true);
  }

  public void setPartitionStatus(PartitionStatus partitionStatus, boolean updateDetails) {
    if (partitionStatus.getPartitionId() < 0 || partitionStatus.getPartitionId() >= numberOfPartition) {
      throw new IllegalArgumentException(
          "Received an invalid partition:" + partitionStatus.getPartitionId() + " for topic:" + kafkaTopic);
    }
    if (partitionStatus instanceof ReadOnlyPartitionStatus) {
      partitionIdToStatus.put(partitionStatus.getPartitionId(), partitionStatus);
    } else {
      partitionIdToStatus
          .put(partitionStatus.getPartitionId(), ReadOnlyPartitionStatus.fromPartitionStatus(partitionStatus));
    }
    if (updateDetails) {
      updateStatusDetails();
    }
  }

  private void updateStatusDetails() {
    PushStatusDecider decider = strategy.getPushStatusDecider();
    Set<Integer> incompletePartitions = new HashSet<>();
    int finishedPartitions = 0;
    for (PartitionStatus partitionStatus: getPartitionStatuses()) {
      int finishedReplicaInPartition = 0;
      for (ReplicaStatus replicaStatus: partitionStatus.getReplicaStatuses()) {
        if (replicaStatus.getCurrentStatus().isTerminal()) {
          finishedReplicaInPartition++;
        }
      }
      if (decider.hasEnoughReplicasForOnePartition(finishedReplicaInPartition, replicationFactor)) {
        finishedPartitions++;
      } else {
        incompletePartitions.add(partitionStatus.getPartitionId());
      }
    }
    if (finishedPartitions > 0) {
      String message = finishedPartitions + "/" + numberOfPartition + " partitions completed.";
      if (incompletePartitions.size() > 0 && incompletePartitions.size() <= 5) {
        message += ". Following partitions still not complete " + incompletePartitions;
      }
      setStatusDetails(message);
    }
  }

  /**
   * Returns map of partitionId -> list of status history for all working replicas of that partition
   */
  public Map<Integer, Map<CharSequence, Integer>> getIncrementalPushStatus(
      PartitionAssignment partitionAssignment,
      String incrementalPushVersion) {
    Map<Integer, Map<CharSequence, Integer>> incPushStatusMap = new HashMap<>(numberOfPartition);
    for (PartitionStatus partitionStatus: getPartitionStatuses()) {
      Map<CharSequence, Integer> partitionPushStatus = new HashMap<>();
      Partition partition = partitionAssignment.getPartition(partitionStatus.getPartitionId());
      Set<String> workingInstances =
          partition.getWorkingInstances().stream().map(Instance::getNodeId).collect(Collectors.toSet());
      for (ReplicaStatus replicaStatus: partitionStatus.getReplicaStatuses()) {
        // skip replicaStatus if it does not belong to any working instance
        if (!workingInstances.contains(replicaStatus.getInstanceId())) {
          continue;
        }
        for (StatusSnapshot snapshot: replicaStatus.getStatusHistory()) {
          // skip snapshot if it does not belong to the given incremental push version
          if (!incrementalPushVersion.equals(snapshot.getIncrementalPushVersion())) {
            continue;
          }
          String instanceId = replicaStatus.getInstanceId();
          Integer instanceStatus = partitionPushStatus.get(instanceId);
          // if the same instance has both SOIP and EOIP status in history, then keep only EOIP
          if (instanceStatus == null || instanceStatus == START_OF_INCREMENTAL_PUSH_RECEIVED.getValue()) {
            partitionPushStatus.put(instanceId, snapshot.getStatus().getValue());
          }
        }
      }
      incPushStatusMap.put(partitionStatus.getPartitionId(), partitionPushStatus);
    }
    return incPushStatusMap;
  }

  /**
   * Returns map of partitionId -> list of status history for all working replicas of a partition
   */
  private Map<Integer, List<StatusSnapshot>> getReplicaHistory(PartitionAssignment partitionAssignment) {
    Map<Integer, List<StatusSnapshot>> partitionAggregatedPushStatus = new HashMap<>(numberOfPartition);
    for (PartitionStatus partitionStatus: getPartitionStatuses()) {
      List<StatusSnapshot> statusHistoryForAllReplicasOfSamePartition = new LinkedList<>();
      Partition partition = partitionAssignment.getPartition(partitionStatus.getPartitionId());
      Set<String> workingInstances =
          partition.getWorkingInstances().stream().map(Instance::getNodeId).collect(Collectors.toSet());
      partitionStatus.getReplicaStatuses()
          .stream()
          .filter(replicaStatus -> workingInstances.contains(replicaStatus.getInstanceId()))
          .map(ReplicaStatus::getStatusHistory)
          .reduce(statusHistoryForAllReplicasOfSamePartition, (resultList, newStatusHistory) -> {
            resultList.addAll(newStatusHistory);
            return resultList;
          });
      partitionAggregatedPushStatus.put(partitionStatus.getPartitionId(), statusHistoryForAllReplicasOfSamePartition);
    }
    return partitionAggregatedPushStatus;
  }

  public String getLatestIncrementalPushVersion(PartitionAssignment partitionAssignment) {
    String latestIncrementalPushVersion = null;
    for (List<StatusSnapshot> replicaHistory: getReplicaHistory(partitionAssignment).values()) {
      for (StatusSnapshot statusSnapshot: replicaHistory) {
        String incPushVersion = statusSnapshot.getIncrementalPushVersion();
        if (!StringUtils.isEmpty(incPushVersion)) {
          if (latestIncrementalPushVersion == null) {
            latestIncrementalPushVersion = incPushVersion;
          } else {
            if (StatusSnapshot.getIncrementalPushJobTimeInMs(incPushVersion) > StatusSnapshot
                .getIncrementalPushJobTimeInMs(latestIncrementalPushVersion)) {
              latestIncrementalPushVersion = incPushVersion;
            }
          }
        }
      }
    }
    return latestIncrementalPushVersion;
  }

  public String getKafkaTopic() {
    return kafkaTopic;
  }

  public int getNumberOfPartition() {
    return numberOfPartition;
  }

  public int getReplicationFactor() {
    return replicationFactor;
  }

  public OfflinePushStrategy getStrategy() {
    return strategy;
  }

  public ExecutionStatus getCurrentStatus() {
    return currentStatus;
  }

  public void setCurrentStatus(ExecutionStatus currentStatus) {
    this.currentStatus = currentStatus;
  }

  @JsonIgnore
  public Optional<String> getOptionalStatusDetails() {
    return statusDetails;
  }

  /** Necessary for the JSON serde */
  public String getStatusDetails() {
    return statusDetails.orElse(null);
  }

  /** Necessary for the JSON serde */
  public void setStatusDetails(String statusDetails) {
    this.statusDetails = Optional.ofNullable(statusDetails);
  }

  public List<StatusSnapshot> getStatusHistory() {
    return statusHistory;
  }

  public void setStatusHistory(List<StatusSnapshot> statusHistory) {
    this.statusHistory = statusHistory;
  }

  // Only used by accessor while loading data from Zookeeper.
  public void setPartitionStatuses(List<PartitionStatus> partitionStatuses) {
    this.partitionIdToStatus.clear();
    for (PartitionStatus partitionStatus: partitionStatuses) {
      if (partitionStatus instanceof ReadOnlyPartitionStatus) {
        this.partitionIdToStatus.put(partitionStatus.getPartitionId(), partitionStatus);
      } else {
        this.partitionIdToStatus
            .put(partitionStatus.getPartitionId(), ReadOnlyPartitionStatus.fromPartitionStatus(partitionStatus));
      }
    }
    updateStatusDetails();
  }

  public String getIncrementalPushVersion() {
    return incrementalPushVersion;
  }

  public void setIncrementalPushVersion(String incrementalPushVersion) {
    this.incrementalPushVersion = incrementalPushVersion;
  }

  public long getSuccessfulPushDurationInSecs() {
    return successfulPushDurationInSecs;
  }

  public void setSuccessfulPushDurationInSecs(long successfulPushDurationInSecs) {
    this.successfulPushDurationInSecs = (int) successfulPushDurationInSecs;
  }

  public OfflinePushStatus clonePushStatus() {
    OfflinePushStatus clonePushStatus =
        new OfflinePushStatus(kafkaTopic, numberOfPartition, replicationFactor, strategy);
    clonePushStatus.setCurrentStatus(currentStatus);
    clonePushStatus.setStatusDetails(statusDetails.orElse(null));
    // Status history is append-only. So here we don't need to deep copy each object in this list. Simply copy the list
    // itself is able to avoid affecting the object while updating the cloned one.
    clonePushStatus.setStatusHistory(new ArrayList<>(statusHistory));
    // As same as status history, there is no way update properties inside Partition status object. So only
    // copy list is enough here.
    clonePushStatus.setPartitionStatuses(new ArrayList<>(partitionIdToStatus.values()));
    clonePushStatus.setPushProperties(new HashMap<>(pushProperties));
    clonePushStatus.setIncrementalPushVersion(incrementalPushVersion);
    clonePushStatus.setSuccessfulPushDurationInSecs((successfulPushDurationInSecs));
    return clonePushStatus;
  }

  /**
   * @return a map which's id is replica id and value is the offset that replica already consumed.
   */
  @JsonIgnore
  public Map<String, Long> getProgress() {
    Map<String, Long> progress = new HashMap<>();
    for (PartitionStatus partitionStatus: getPartitionStatuses()) {
      // Don't count progress of error tasks
      partitionStatus.getReplicaStatuses()
          .stream()
          // Don't count progress of error tasks
          .filter(replicaStatus -> !replicaStatus.getCurrentStatus().equals(ERROR))
          .forEach(replicaStatus -> {
            String replicaId =
                ReplicaStatus.getReplicaId(kafkaTopic, partitionStatus.getPartitionId(), replicaStatus.getInstanceId());
            progress.put(replicaId, replicaStatus.getCurrentProgress());
          });
    }
    return progress;
  }

  @JsonIgnore
  public Collection<PartitionStatus> getPartitionStatuses() {
    return Collections.unmodifiableCollection(partitionIdToStatus.values());
  }

  public PartitionStatus getPartitionStatus(int partitionId) {
    return partitionIdToStatus.get(partitionId);
  }

  private void addHistoricStatus(ExecutionStatus status, String incrementalPushVersion) {
    StatusSnapshot snapshot = new StatusSnapshot(status, LocalDateTime.now().toString());
    if (!StringUtils.isEmpty(incrementalPushVersion)) {
      snapshot.setIncrementalPushVersion(incrementalPushVersion);
    }
    statusHistory.add(snapshot);
  }

  /**
   * Checks whether at least one replica of each partition has returned {@link ExecutionStatus#END_OF_PUSH_RECEIVED}
   *
   * This is intended for {@link OfflinePushStatus} instances which belong to Hybrid Stores, though there
   * should be no negative side-effects if called on an instance tied to a non-hybrid store, as the logic
   * should consistently return false in that case.
   *
   * @return true if at least one replica of each partition has consumed an EOP control message, false otherwise
   */
  public boolean isReadyToStartBufferReplay(boolean isDataRecovery) {
    // Only allow the push in STARTED status to start buffer replay. It could avoid:
    // 1. Send duplicated start buffer replay message.
    // 2. Send start buffer replay message when a push had already been terminated.
    if (!getCurrentStatus().equals(STARTED)) {
      return false;
    }
    boolean isReady = true;
    ExecutionStatus requiredStatus = isDataRecovery ? DATA_RECOVERY_COMPLETED : END_OF_PUSH_RECEIVED;
    for (PartitionStatus partitionStatus: getPartitionStatuses()) {
      boolean proceedToNextPartition = false;
      for (ReplicaStatus replicaStatus: partitionStatus.getReplicaStatuses()) {
        if (replicaStatus.getCurrentStatus() == requiredStatus) {
          proceedToNextPartition = true;
          break;
        } else {
          // If the previous status contains required status then the partition is also ready to start buffer replay.
          // We don't have to worry about duplicate start buffer replay message scenario here because it's already
          // handled by the check on the overall status equals to STARTED.
          for (StatusSnapshot snapshot: replicaStatus.getStatusHistory()) {
            if (snapshot.getStatus() == requiredStatus) {
              proceedToNextPartition = true;
              break;
            }
          }
        }
      }
      if (!proceedToNextPartition) {
        isReady = false;
        break;
      }
    }
    return isReady;
  }

  public Map<String, String> getPushProperties() {
    return pushProperties;
  }

  public void setPushProperties(Map<String, String> pushProperties) {
    this.pushProperties = pushProperties;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    OfflinePushStatus that = (OfflinePushStatus) o;

    if (numberOfPartition != that.numberOfPartition) {
      return false;
    }
    if (replicationFactor != that.replicationFactor) {
      return false;
    }
    if (!kafkaTopic.equals(that.kafkaTopic)) {
      return false;
    }
    if (strategy != that.strategy) {
      return false;
    }
    if (currentStatus != that.currentStatus) {
      return false;
    }
    if (!statusDetails.equals(that.statusDetails)) {
      return false;
    }
    if (!statusHistory.equals(that.statusHistory)) {
      return false;
    }
    if (!pushProperties.equals(that.pushProperties)) {
      return false;
    }
    if (!incrementalPushVersion.equals(that.incrementalPushVersion)) {
      return false;
    }

    if (successfulPushDurationInSecs != that.successfulPushDurationInSecs) {
      return false;
    }
    return partitionIdToStatus.equals(that.partitionIdToStatus);
  }

  @Override
  public int hashCode() {
    int result = kafkaTopic.hashCode();
    result = 31 * result + numberOfPartition;
    result = 31 * result + replicationFactor;
    result = 31 * result + strategy.hashCode();
    result = 31 * result + currentStatus.hashCode();
    result = 31 * result + statusDetails.hashCode();
    result = 31 * result + statusHistory.hashCode();
    result = 31 * result + partitionIdToStatus.hashCode();
    result = 31 * result + pushProperties.hashCode();
    result = 31 * result + incrementalPushVersion.hashCode();
    result = 31 * result + (int) successfulPushDurationInSecs;
    return result;
  }

  @Override
  public String toString() {
    return "OfflinePushStatus{" + "kafkaTopic='" + kafkaTopic + '\'' + ", numberOfPartition=" + numberOfPartition
        + ", replicationFactor=" + replicationFactor + ", strategy=" + strategy + ", currentStatus=" + currentStatus
        + ", statusDetails=" + statusDetails + ", incrementalPushVersion=" + incrementalPushVersion
        + ", lastSuccessfulPushDurationSecs=" + successfulPushDurationInSecs + '}';
  }
}
