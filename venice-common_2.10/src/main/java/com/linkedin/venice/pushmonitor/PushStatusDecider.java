package com.linkedin.venice.pushmonitor;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.ResourceAssignment;
import com.linkedin.venice.meta.OfflinePushStrategy;
import com.linkedin.venice.meta.Partition;
import com.linkedin.venice.meta.PartitionAssignment;
import com.linkedin.venice.meta.RoutingDataRepository;
import com.linkedin.venice.utils.Pair;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.StringJoiner;
import org.apache.log4j.Logger;


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
    if (!partitionAssignment.hasEnoughAssignedPartitions()) {
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
        return new Pair<>(ExecutionStatus.ERROR, Optional.of(statusDetails));
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
      return new Pair<>(ExecutionStatus.COMPLETED, Optional.empty());
    } else {
      return new Pair<>(ExecutionStatus.STARTED, Optional.empty());
    }
  }

  /**
   * @return status details: if empty, push has enough replicas in every partition, otherwise, message contains details
   */
  public Optional<String> hasEnoughNodesToStartPush(OfflinePushStatus offlinePushStatus, ResourceAssignment resourceAssignment) {
    if (!resourceAssignment.containsResource(offlinePushStatus.getKafkaTopic())) {
      String reason = "not yet in EXTERNALVIEW";
      logger.info("Routing data repository has not created assignment for resource: " + offlinePushStatus.getKafkaTopic()
              + "(" + reason + ")");
      return Optional.of(reason);
    }
    PartitionAssignment partitionAssignment =
        resourceAssignment.getPartitionAssignment(offlinePushStatus.getKafkaTopic());
    if (!partitionAssignment.hasEnoughAssignedPartitions()) {
      String reason = "not enough partitions in EXTERNALVIEW";
      logger.info("There are " + reason + " assigned to resource: " + offlinePushStatus.getKafkaTopic());
      return Optional.of(reason);
    }
    ArrayList<Integer> underReplicatedPartition = new ArrayList<>();
    for (Partition partition : partitionAssignment.getAllPartitions()) {
      if (!this.hasEnoughReplicasForOnePartition(partition.getBootstrapAndReadyToServeInstances().size(),
          offlinePushStatus.getReplicationFactor())) {
        underReplicatedPartition.add(partition.getId());
        logger.info("Partition: " + partition.getId() + " does not have enough replica for resource: " + offlinePushStatus.getKafkaTopic());
      }
    }
    if (underReplicatedPartition.isEmpty()) {
      return Optional.empty();
    } else {
      String reason = underReplicatedPartition.size() + " partitions under-replicated in EXTERNALVIEW";
      logger.info(reason + " for resource '" + offlinePushStatus.getKafkaTopic() + "': " + underReplicatedPartition.toString());
      return Optional.of(reason);
    }
  }

  public abstract OfflinePushStrategy getStrategy();

  protected abstract boolean hasEnoughReplicasForOnePartition(int actual, int expected);

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
