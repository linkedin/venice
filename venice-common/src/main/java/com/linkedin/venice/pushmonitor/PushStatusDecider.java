package com.linkedin.venice.pushmonitor;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.HelixState;
import com.linkedin.venice.helix.ResourceAssignment;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.OfflinePushStrategy;
import com.linkedin.venice.meta.Partition;
import com.linkedin.venice.meta.PartitionAssignment;
import com.linkedin.venice.utils.Pair;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.log4j.Logger;

import static com.linkedin.venice.pushmonitor.ExecutionStatus.*;


/**
 * Decide the offline push status by checking all of replicas statuses under different offline push strategies.
 */
public abstract class PushStatusDecider {
  private final Logger logger = Logger.getLogger(PushStatusDecider.class);

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

  public Pair<ExecutionStatus, Optional<String>> checkPushStatusAndDetails(OfflinePushStatus pushStatus, PartitionAssignment partitionAssignment) {
    boolean isAllPartitionCompleted = true;
    // If there are not enough partitions assigned, push could not be completed.
    // Continue to decide whether push is failed or not.
    if (partitionAssignment.isMissingAssignedPartitions()) {
      logger.warn("There no not enough partitions assigned to resource: " + pushStatus.getKafkaTopic());
      isAllPartitionCompleted = false;
    }
    for (Partition partition : partitionAssignment.getAllPartitions()) {
      int replicationFactor = pushStatus.getReplicationFactor();
      int errorReplicasCount = partition.getErrorInstances().size();
      int completedReplicasCount = partition.getReadyToServeInstances().size();
      int assignedReplicasCount = partition.getAllInstances().size();
      if (logger.isDebugEnabled()) {
        logger.debug("Checking Push status for offline push for topic:" + pushStatus.getKafkaTopic() + "Partition:"
            + partition.getId() + " has " + assignedReplicasCount + " assigned replicas including " + errorReplicasCount
            + " error replicas, " + completedReplicasCount + " completed replicas.");
      }

      // Is push failed due to there is enough number of error replicas.
      if (!hasEnoughReplicasForOnePartition(replicationFactor - errorReplicasCount, replicationFactor)) {
        String statusDetails = "too many ERROR replicas (" + errorReplicasCount + "/" + replicationFactor
            + ") in partition " + partition.getId() + " for OfflinePushStrategy:" + getStrategy();
        logger.warn("Push for topic:" + pushStatus.getKafkaTopic() + " should fail because of " + statusDetails);
        return new Pair<>(ERROR, Optional.of(statusDetails));
      }

      // Is the partition has enough number of completed replicas.
      if (!hasEnoughReplicasForOnePartition(completedReplicasCount, replicationFactor)) {
        if (logger.isDebugEnabled()) {
          logger.debug("Push for topic:" + pushStatus.getKafkaTopic() + " can not terminated because the partition:"
              + partition.getId() + " does not have enough COMPLETED replicas. Completed replicas:"
              + completedReplicasCount + " replicationFactor:" + replicationFactor + " under strategy:"
              + getStrategy());
        }
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
  public Pair<ExecutionStatus, Optional<String>> checkPushStatusAndDetailsByPartitionsStatus(OfflinePushStatus pushStatus,
      PartitionAssignment partitionAssignment) {
    // Sanity check
    if (partitionAssignment == null || partitionAssignment.isMissingAssignedPartitions()) {
      logger.warn("partitionAssignment not ready: " + partitionAssignment);
      return new Pair<>(NOT_CREATED, Optional.empty());
    }

    boolean isAllPartitionCompleted = true;

    for (PartitionStatus partitionStatus : pushStatus.getPartitionStatuses()) {
      int partitionId = partitionStatus.getPartitionId();
      Partition partition = partitionAssignment.getPartition(partitionId);
      if (null == partition) {
        // Defensive coding. Should never happen if the sanity check above works.
        throw new IllegalStateException("partition " + partitionId + " is null.");
      }
      ExecutionStatus executionStatus = getPartitionStatus(partitionStatus, pushStatus.getReplicationFactor(),
          partition.getInstanceToStateMap());

      if (executionStatus == ERROR) {
        return new Pair<>(executionStatus, Optional.of("too many ERROR replicas in partition: " + partitionStatus.getPartitionId()
            + " for offlinePushStrategy: " + getStrategy().name()));
      }

      if (!executionStatus.equals(COMPLETED)) {
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
   * @return status details: if empty, push has enough replicas in every partition, otherwise, message contains details
   */
  public Optional<String> hasEnoughNodesToStartPush(String kafkaTopic, int replicationFactor, ResourceAssignment resourceAssignment) {
    if (!resourceAssignment.containsResource(kafkaTopic)) {
      String reason = "not yet in EXTERNALVIEW";
      logger.info("Routing data repository has not created assignment for resource: " + kafkaTopic + "(" + reason + ")");
      return Optional.of(reason);
    }

    PartitionAssignment partitionAssignment = resourceAssignment.getPartitionAssignment(kafkaTopic);
    if (partitionAssignment.isMissingAssignedPartitions()) {
      String reason = "not enough partitions in EXTERNALVIEW";
      logger.info("There are " + reason + " assigned to resource: " + kafkaTopic);
      return Optional.of(reason);
    }

    ArrayList<Integer> underReplicatedPartition = new ArrayList<>();
    for (Partition partition : partitionAssignment.getAllPartitions()) {
      if (!this.hasEnoughReplicasForOnePartition(partition.getWorkingInstances().size(), replicationFactor)) {
        underReplicatedPartition.add(partition.getId());
        logger.info("Partition: " + partition.getId() + " does not have enough replica for resource: " + kafkaTopic);
      }
    }

    if (underReplicatedPartition.isEmpty()) {
      return Optional.empty();
    }

    String reason = underReplicatedPartition.size() + " partitions under-replicated in EXTERNALVIEW";
    logger.info(reason + " for resource '" + kafkaTopic + "': " + underReplicatedPartition.toString());
    return Optional.of(reason);
  }

  public abstract OfflinePushStrategy getStrategy();

  protected abstract boolean hasEnoughReplicasForOnePartition(int actual, int expected);

  protected abstract int getNumberOfToleratedErrors();

  protected ExecutionStatus getPartitionStatus(PartitionStatus partitionStatus, int replicationFactor, Map<Instance, String> instanceToStateMap) {
    return getPartitionStatus(partitionStatus, replicationFactor, instanceToStateMap, getNumberOfToleratedErrors());
  }

  protected ExecutionStatus getPartitionStatus(PartitionStatus partitionStatus, int replicationFactor,
      Map<Instance, String> instanceToStateMap, int numberOfToleratedErrors) {
    Map<ExecutionStatus, Integer> executionStatusMap = new HashMap<>();

    //when resources are running under L/F model, leader is usually taking more critical work and
    //are more important than followers. Therefore, we strictly require leaders to be completed before
    //partitions can be completed. Vice versa, partitions will be in error state if leader is in error
    //state.
    boolean isLeaderCompleted = true;
    boolean isLeaderInError = false;

    for (Map.Entry<Instance, String> entry : instanceToStateMap.entrySet()) {
      ExecutionStatus currentStatus =
          getReplicaCurrentStatus(partitionStatus.getReplicaHistoricStatusList(entry.getKey().getNodeId()));
      if (entry.getValue().equals(HelixState.LEADER_STATE)) {
        if (!currentStatus.equals(COMPLETED)) {
          isLeaderCompleted = false;
        }

        if (currentStatus.equals(ERROR)) {
          isLeaderInError = true;
        }

      }

      executionStatusMap.merge(currentStatus, 1, Integer::sum);
    }

    if (executionStatusMap.containsKey(COMPLETED) &&
        executionStatusMap.get(COMPLETED) >= replicationFactor - numberOfToleratedErrors &&
        isLeaderCompleted) {
      return COMPLETED;
    }

    if (isLeaderInError) {
      return ERROR;
    }

    if (executionStatusMap.containsKey(ERROR) &&
        executionStatusMap.get(ERROR) > instanceToStateMap.size() - replicationFactor + numberOfToleratedErrors) {
      return ERROR;
    }

    return STARTED;
  }

  public static ExecutionStatus getReplicaCurrentStatus(List<StatusSnapshot> historicStatusList) {
    List<ExecutionStatus> statusList = historicStatusList.stream()
        .map(statusSnapshot -> statusSnapshot.getStatus())
        .sorted(Collections.reverseOrder())
        .collect(Collectors.toList());

    ExecutionStatus status = STARTED;
    for (ExecutionStatus executionStatus : statusList) {
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

  protected static void updateDecider(OfflinePushStrategy strategy, PushStatusDecider decider){
    decidersMap.put(strategy, decider);
  }
}
