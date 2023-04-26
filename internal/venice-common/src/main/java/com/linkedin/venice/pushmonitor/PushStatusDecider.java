package com.linkedin.venice.pushmonitor;

import static com.linkedin.venice.pushmonitor.ExecutionStatus.COMPLETED;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.END_OF_PUSH_RECEIVED;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.ERROR;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.NOT_CREATED;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.STARTED;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.isDeterminedStatus;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.HelixState;
import com.linkedin.venice.helix.ResourceAssignment;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.OfflinePushStrategy;
import com.linkedin.venice.meta.Partition;
import com.linkedin.venice.meta.PartitionAssignment;
import com.linkedin.venice.systemstore.schemas.StoreReplicaStatus;
import com.linkedin.venice.utils.Pair;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Decide the offline push status by checking all of replicas statuses under different offline push strategies.
 */
public abstract class PushStatusDecider {
  private final Logger logger = LogManager.getLogger(PushStatusDecider.class);
  private static final String REASON_NOT_IN_EV = "not yet in EXTERNALVIEW";
  private static final String REASON_NOT_ENOUGH_PARTITIONS_IN_EV = "not enough partitions in EXTERNALVIEW";
  private static final String REASON_UNDER_REPLICATED = "does not have enough replicas";

  private static Map<OfflinePushStrategy, PushStatusDecider> decidersMap = new HashMap<>();

  static {
    decidersMap.put(OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION, new WaitNMinusOnePushStatusDecider());
    decidersMap.put(OfflinePushStrategy.WAIT_ALL_REPLICAS, new WaitAllPushStatusDecider());
  }

  /**
   * Check the push status based on the current routing data which includes all of replicas statuses.
   */
  public ExecutionStatus checkPushStatus(OfflinePushStatus pushStatus, PartitionAssignment partitionAssignment) {
    return checkPushStatusAndDetails(pushStatus, partitionAssignment).getFirst();
  }

  public Pair<ExecutionStatus, Optional<String>> checkPushStatusAndDetails(
      OfflinePushStatus pushStatus,
      PartitionAssignment partitionAssignment) {
    boolean isAllPartitionCompleted = true;
    // If there are not enough partitions assigned, push could not be completed.
    // Continue to decide whether push is failed or not.
    if (partitionAssignment.isMissingAssignedPartitions()) {
      logger.warn("There are no enough partitions assigned to resource: {}", pushStatus.getKafkaTopic());
      isAllPartitionCompleted = false;
    }
    for (Partition partition: partitionAssignment.getAllPartitions()) {
      int replicationFactor = pushStatus.getReplicationFactor();
      int errorReplicasCount = partition.getErrorInstances().size();
      int completedReplicasCount = partition.getReadyToServeInstances().size();
      int assignedReplicasCount = partition.getAllInstances().size();
      logger.debug(
          "Checking Push status for offline push for topic: {} Partition: {} has {} assigned replicas including {} error replicas, {} completed replicas.",
          pushStatus.getKafkaTopic(),
          partition.getId(),
          assignedReplicasCount,
          errorReplicasCount,
          completedReplicasCount);

      // Is push failed due to there is enough number of error replicas.
      if (!hasEnoughReplicasForOnePartition(replicationFactor - errorReplicasCount, replicationFactor)) {
        String statusDetails = "Too many ERROR replicas " + errorReplicasCount + "/" + replicationFactor
            + " in partition " + partition.getId() + " under strategy:" + getStrategy();
        logger.warn("Push for topic: {} should fail because of {}", pushStatus.getKafkaTopic(), statusDetails);
        return new Pair<>(ERROR, Optional.of(statusDetails));
      }

      // Is the partition has enough number of completed replicas.
      if (!hasEnoughReplicasForOnePartition(completedReplicasCount, replicationFactor)) {
        logger.debug(
            "Push for topic: {} can not terminated because partition: {} does not have enough COMPLETED replicas. Completed replicas: {} replicationFactor: {} under strategy: {}.",
            pushStatus.getKafkaTopic(),
            partition.getId(),
            completedReplicasCount,
            replicationFactor,
            getStrategy());
        isAllPartitionCompleted = false;
      }
    }
    if (isAllPartitionCompleted) {
      return new Pair<>(COMPLETED, Optional.empty());
    } else {
      return new Pair<>(STARTED, Optional.empty());
    }
  }

  /**
   * Check the current status based on {@link PartitionStatus}
   */
  public Pair<ExecutionStatus, Optional<String>> checkPushStatusAndDetailsByPartitionsStatus(
      OfflinePushStatus pushStatus,
      PartitionAssignment partitionAssignment,
      DisableReplicaCallback callback) {
    // Sanity check
    if (partitionAssignment == null || partitionAssignment.isMissingAssignedPartitions()) {
      logger.warn("partitionAssignment not ready: {}", partitionAssignment);
      return new Pair<>(NOT_CREATED, Optional.empty());
    }

    boolean isAllPartitionCompleted = true;
    boolean isAllPartitionEndOfPushReceived = true;

    if (pushStatus.getPartitionStatuses().size() != pushStatus.getNumberOfPartition()) {
      isAllPartitionCompleted = false;
      isAllPartitionEndOfPushReceived = false;
    } else {
      for (PartitionStatus partitionStatus: pushStatus.getPartitionStatuses()) {
        int partitionId = partitionStatus.getPartitionId();
        Partition partition = partitionAssignment.getPartition(partitionId);
        if (partition == null) {
          // Defensive coding. Should never happen if the sanity check above works.
          throw new IllegalStateException("partition " + partitionId + " is null.");
        }
        ExecutionStatus executionStatus = getPartitionStatus(
            partitionStatus,
            pushStatus.getReplicationFactor(),
            partition.getInstanceToStateMap(),
            callback);

        if (executionStatus == ERROR) {
          return new Pair<>(
              executionStatus,
              Optional.of(
                  "too many ERROR replicas in partition: " + partitionStatus.getPartitionId()
                      + " for offlinePushStrategy: " + getStrategy().name()));
        }

        if (!executionStatus.equals(COMPLETED)) {
          isAllPartitionCompleted = false;
        }

        if (!executionStatus.equals(END_OF_PUSH_RECEIVED) && !executionStatus.equals(COMPLETED)) {
          isAllPartitionEndOfPushReceived = false;
        }
      }
    }

    if (isAllPartitionCompleted) {
      return new Pair<>(COMPLETED, Optional.empty());
    }
    if (isAllPartitionEndOfPushReceived) {
      return new Pair<>(END_OF_PUSH_RECEIVED, Optional.empty());
    } else {
      return new Pair<>(STARTED, Optional.empty());
    }
  }

  public static List<Instance> getReadyToServeInstances(
      PartitionStatus partitionStatus,
      PartitionAssignment partitionAssignment,
      int partitionId) {
    return partitionAssignment.getPartition(partitionId)
        .getAllInstances()
        .values()
        .stream()
        .flatMap(List::stream)
        .filter(
            instance -> PushStatusDecider
                .getReplicaCurrentStatus(partitionStatus.getReplicaHistoricStatusList(instance.getNodeId()))
                .equals(ExecutionStatus.COMPLETED))
        .collect(Collectors.toList());
  }

  /**
   * Replicas from L/F and Online/Offline model will be considered ready to serve if their status is {@link ExecutionStatus.COMPLETED}.
   * More information is needed if you'd like to change/support other behaviors such as routing to the leader replica.
   * @param replicaStatusMap
   * @return List of ready to serve instance ids
   */
  public static List<String> getReadyToServeInstances(Map<CharSequence, StoreReplicaStatus> replicaStatusMap) {
    return replicaStatusMap.entrySet()
        .stream()
        .filter(e -> e.getValue().status == COMPLETED.value)
        .map(e -> e.getKey().toString())
        .collect(Collectors.toList());
  }

  /**
   * @return status details: if empty, push has enough replicas in every partition, otherwise, message contains details
   */
  public Optional<String> hasEnoughNodesToStartPush(
      String kafkaTopic,
      int replicationFactor,
      ResourceAssignment resourceAssignment,
      Optional<String> previousReason) {
    if (!resourceAssignment.containsResource(kafkaTopic)) {
      String reason = REASON_NOT_IN_EV;
      String message =
          "Routing data repository has not created assignment for resource: " + kafkaTopic + "(" + reason + ")";
      logConditionally(reason, previousReason, message);
      return Optional.of(reason);
    }

    PartitionAssignment partitionAssignment = resourceAssignment.getPartitionAssignment(kafkaTopic);
    if (partitionAssignment.isMissingAssignedPartitions()) {
      String reason = REASON_NOT_ENOUGH_PARTITIONS_IN_EV + partitionAssignment.getAssignedNumberOfPartitions() + "/"
          + partitionAssignment.getExpectedNumberOfPartitions();
      String message = "There are " + reason + " assigned to resource: " + kafkaTopic;
      logConditionally(reason, previousReason, message);
      return Optional.of(reason);
    }

    StringBuilder underReplicatedPartitionString = new StringBuilder();
    for (Partition partition: partitionAssignment.getAllPartitions()) {
      if (!this.hasEnoughReplicasForOnePartition(partition.getWorkingInstances().size(), replicationFactor)) {
        underReplicatedPartitionString.append(" ").append(partition.getId());
      }
    }

    if (underReplicatedPartitionString.length() == 0) {
      return Optional.empty();
    }

    String reason = "Partitions: " + underReplicatedPartitionString.toString() + " " + REASON_UNDER_REPLICATED;
    String message = reason + " for resource: " + kafkaTopic;
    logConditionally(reason, previousReason, message);
    return Optional.of(reason);
  }

  private void logConditionally(String newReason, Optional<String> previousReason, String message) {
    if (!previousReason.isPresent()) {
      logger.info(message);
      return;
    }
    if (!previousReason.get().equals(newReason)) {
      logger.info(message);
    }
  }

  public abstract OfflinePushStrategy getStrategy();

  protected abstract boolean hasEnoughReplicasForOnePartition(int actual, int expected);

  protected abstract int getNumberOfToleratedErrors();

  protected ExecutionStatus getPartitionStatus(
      PartitionStatus partitionStatus,
      int replicationFactor,
      Map<Instance, String> instanceToStateMap,
      DisableReplicaCallback callback) {
    return getPartitionStatus(
        partitionStatus,
        replicationFactor,
        instanceToStateMap,
        getNumberOfToleratedErrors(),
        callback);
  }

  protected ExecutionStatus getPartitionStatus(
      PartitionStatus partitionStatus,
      int replicationFactor,
      Map<Instance, String> instanceToStateMap,
      int numberOfToleratedErrors,
      DisableReplicaCallback callback) {
    Map<ExecutionStatus, Integer> executionStatusMap = new HashMap<>();

    // when resources are running under L/F model, leader is usually taking more critical work and
    // are more important than followers. Therefore, we strictly require leaders to be completed before
    // partitions can be completed. Vice versa, partitions will be in error state if leader is in error
    // state.
    boolean isLeaderCompleted = true;
    int previouslyDisabledErrorReplica = 0;
    for (Map.Entry<Instance, String> entry: instanceToStateMap.entrySet()) {
      ExecutionStatus currentStatus =
          getReplicaCurrentStatus(partitionStatus.getReplicaHistoricStatusList(entry.getKey().getNodeId()));
      if (entry.getValue().equals(HelixState.LEADER_STATE)) {
        if (!currentStatus.equals(COMPLETED)) {
          isLeaderCompleted = false;
        }
        if (currentStatus.equals(ERROR) && callback != null
            && !callback.isReplicaDisabled(entry.getKey().getNodeId(), partitionStatus.getPartitionId())) {
          callback.disableReplica(entry.getKey().getNodeId(), partitionStatus.getPartitionId());
        }
      } else if (entry.getValue().equals(HelixState.OFFLINE_STATE)) {
        // If the replica is in offline state, check if its due to previously disabled replica or not.
        if (callback != null
            && callback.isReplicaDisabled(entry.getKey().getNodeId(), partitionStatus.getPartitionId())) {
          previouslyDisabledErrorReplica++;
          continue; // Dont add disabled replica to status map
        }
      }
      executionStatusMap.merge(currentStatus, 1, Integer::sum);
    }

    if (executionStatusMap.containsKey(COMPLETED)
        && executionStatusMap.get(COMPLETED) >= (replicationFactor - numberOfToleratedErrors) && isLeaderCompleted) {
      return COMPLETED;
    }

    if (executionStatusMap.containsKey(ERROR) && (executionStatusMap.get(ERROR)
        + previouslyDisabledErrorReplica > instanceToStateMap.size() - replicationFactor + numberOfToleratedErrors)) {
      return ERROR;
    }

    /**
     * Report EOP if at least one replica has consumed an EOP control message
     */
    if (executionStatusMap.containsKey(END_OF_PUSH_RECEIVED) && executionStatusMap.get(END_OF_PUSH_RECEIVED) > 0) {
      return END_OF_PUSH_RECEIVED;
    }

    return STARTED;
  }

  /**
   * The helper function is used by both controller and router (leader/follower stores); please be cautious when modifying
   * it because it would affect both components.
   */
  public static ExecutionStatus getReplicaCurrentStatus(List<StatusSnapshot> historicStatusList) {
    List<ExecutionStatus> statusList =
        historicStatusList.stream().map(statusSnapshot -> statusSnapshot.getStatus()).collect(Collectors.toList());
    // prep to traverse the list from latest status.
    Collections.reverse(statusList);
    ExecutionStatus status = STARTED;
    for (ExecutionStatus executionStatus: statusList) {
      if (isDeterminedStatus(executionStatus)) {
        status = executionStatus;
        break;
      }
    }

    return status;
  }

  public static PushStatusDecider getDecider(OfflinePushStrategy strategy) {
    if (!decidersMap.containsKey(strategy)) {
      throw new VeniceException("Unknown offline push strategy:" + strategy);
    } else {
      return decidersMap.get(strategy);
    }
  }

  protected static void updateDecider(OfflinePushStrategy strategy, PushStatusDecider decider) {
    decidersMap.put(strategy, decider);
  }
}
