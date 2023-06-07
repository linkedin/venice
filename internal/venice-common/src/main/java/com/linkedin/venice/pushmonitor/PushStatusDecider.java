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
import java.util.EnumMap;
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
            partition.getInstanceToHelixStateMap(),
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
        .getAllInstancesSet()
        .stream()
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
      Map<Instance, HelixState> instanceToStateMap,
      DisableReplicaCallback callback) {
    int numberOfToleratedErrors = getNumberOfToleratedErrors();
    Map<ExecutionStatus, Integer> executionStatusMap = new EnumMap<>(ExecutionStatus.class);

    // when resources are running under L/F model, leader is usually taking more critical work and
    // are more important than followers. Therefore, we strictly require leaders to be completed before
    // partitions can be completed. Vice versa, partitions will be in error state if leader is in error
    // state.
    boolean isLeaderCompleted = true;
    int previouslyDisabledErrorReplica = 0;
    for (Map.Entry<Instance, HelixState> entry: instanceToStateMap.entrySet()) {
      ExecutionStatus currentStatus =
          getReplicaCurrentStatus(partitionStatus.getReplicaHistoricStatusList(entry.getKey().getNodeId()));
      if (entry.getValue() == HelixState.LEADER) {
        if (!currentStatus.equals(COMPLETED)) {
          isLeaderCompleted = false;
        }
        if (currentStatus.equals(ERROR) && callback != null
            && !callback.isReplicaDisabled(entry.getKey().getNodeId(), partitionStatus.getPartitionId())) {
          callback.disableReplica(entry.getKey().getNodeId(), partitionStatus.getPartitionId());
        }
      } else if (entry.getValue() == HelixState.OFFLINE) {
        // If the replica is in offline state, check if its due to previously disabled replica or not.
        if (callback != null
            && callback.isReplicaDisabled(entry.getKey().getNodeId(), partitionStatus.getPartitionId())) {
          previouslyDisabledErrorReplica++;
          continue; // Dont add disabled replica to status map
        }
      }
      executionStatusMap.merge(currentStatus, 1, Integer::sum);
    }

    Integer statusCount = executionStatusMap.get(COMPLETED);
    if (statusCount != null && statusCount >= (replicationFactor - numberOfToleratedErrors) && isLeaderCompleted) {
      return COMPLETED;
    }

    statusCount = executionStatusMap.get(ERROR);
    if (statusCount != null && (statusCount + previouslyDisabledErrorReplica > instanceToStateMap.size()
        - replicationFactor + numberOfToleratedErrors)) {
      return ERROR;
    }

    /**
     * Report EOP if at least one replica has consumed an EOP control message
     */
    statusCount = executionStatusMap.get(END_OF_PUSH_RECEIVED);
    if (statusCount != null && statusCount > 0) {
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
