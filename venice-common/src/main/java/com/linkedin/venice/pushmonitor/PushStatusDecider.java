package com.linkedin.venice.pushmonitor;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.job.ExecutionStatus;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.OfflinePushStrategy;
import com.linkedin.venice.meta.Partition;
import com.linkedin.venice.meta.PartitionAssignment;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
    boolean isAllPartitionCompleted = true;
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
        logger.warn(
            "Push for topic:" + pushStatus.getKafkaTopic() + " should fail due to too many error replicas. Partition:"
                + partition.getId() + " strategy:" + getStrategy() + " replicationFactor:" + replicationFactor
                + " errorReplicas:" + errorReplicasCount);
        return ExecutionStatus.ERROR;
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
    return isAllPartitionCompleted ? ExecutionStatus.COMPLETED : ExecutionStatus.STARTED;
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
