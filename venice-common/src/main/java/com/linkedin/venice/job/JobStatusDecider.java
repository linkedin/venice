package com.linkedin.venice.job;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.OfflinePushStrategy;
import com.linkedin.venice.meta.Partition;
import com.linkedin.venice.meta.PartitionAssignment;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;


/**
 * Decide the job status by checking all of partitions and tasks status in different offline push strategies.
 */
public abstract class JobStatusDecider {
  private final Logger logger = Logger.getLogger(JobStatusDecider.class);
  private static Map<OfflinePushStrategy, JobStatusDecider> decidersMap= new HashMap<>();

  static {
    decidersMap.put(OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION, new WaitNMinusOneJobStatusDeicder());
    decidersMap.put(OfflinePushStrategy.WAIT_ALL_REPLICAS, new WaitAllJobStatsDecider());
  }

  /**
   * Check given partitions and instances to see whether there are enough task executors to start a new job.
   *
   * @param job                 Job need to be started.
   * @param partitionAssignment partition assignment for resource
   *
   * @return true if the job could be started. false if at least one of partition does not have enough instances.
   */
  public boolean hasEnoughTaskExecutorsToStart(Job job, PartitionAssignment partitionAssignment) {
    int actualNumberOfPartition = partitionAssignment.getAssignedNumberOfPartitions();
    if (!hasEnoughPartitions(actualNumberOfPartition, job)) {
      return false;
    }

    for (Partition partition : partitionAssignment.getAllPartitions()) {
      if (!hasEnoughReplicasForOnePartition(partition.getBootstrapAndReadyToServeInstances().size(), job.getReplicationFactor())) {
        //Some of nodes are failed OR some of replicas haven't been allocated.
        logger.info("Number of replicas:" + partition.getBootstrapAndReadyToServeInstances().size() + " in partition:"
            + partition.getId() + " is smaller than the required replication factor:" + job.getReplicationFactor()
            + ", Job:" + job.getJobId() + " Strategy:" + getStrategy());
        return false;
      }
    }
    return true;
  }

  /**
   * Check given partitions and instances to see whether there are minimum task executors to keep job running.
   *
   *
   * @param job                 a running job.
   * @param partitionAssignment partition assignment for resource
   *
   * @return true if the job could continue running. false if at least one of partition dose NOT have minimum instances.
   */
  public boolean hasMinimumTaskExecutorsToKeepRunning(Job job, PartitionAssignment partitionAssignment) {
    int actualNumberOfPartition = partitionAssignment.getAssignedNumberOfPartitions();
    if (!hasEnoughPartitions(actualNumberOfPartition, job)) {
      return false;
    }

    for (Partition partition : partitionAssignment.getAllPartitions()) {
      /*
      * Note: The side effect of using minimum replicas to judge whether job is failed is job could never be terminated.
      * For example, replica factor=3, strategy=wait n-1, minimum replica=1. Two of replicas become ERROR, so job is neither
      * successful nor failed. Helix will not re-balance the ERROR replicas until we do the manual operation. So job keeps
      * running and never be terminated.
      * In order to avoid this type of deadlock, if number of error replicas is larger than n-strategy, we fail the job.
      * For the example above, there are 2 error replicas, which is larger than n-strategy(3-(3-1)=1), job should be failed.
       */
      if (!hasEnoughReplicasForOnePartition(job.getReplicationFactor() - partition.getErrorInstances().size(),
          job.getReplicationFactor())) {
        logger.info("Number of error replicas:" + partition.getErrorInstances().size()
            + " is larger than n-strategy in partition:" + partition.getId() + " in job:" + job.getJobId()
            + ", strategy:" + getStrategy());
        return false;
      }
      if (!hasMinimumReplicasForOnePartition(partition.getBootstrapAndReadyToServeInstances().size(), job)) {
        logger.info("Number of replicas:" + partition.getBootstrapAndReadyToServeInstances().size() + " in partition:"
            + partition.getId() + " is smaller than minimum replicas:" + job.getMinimumReplicas() + " in job:"
            + job.getJobId());
        return false;
      }
    }
    return true;
  }

  /**
   * Check all of current tasks status to judge whether job is completed or error or still running.
   * <p>
   * Please note this method is only the query method which will not change the status of this job.
   *
   * @return Calculated job status.
   */
  public ExecutionStatus checkJobStatus(Job job) {
    //Only check the status of running job.
    if (!job.getStatus().equals(ExecutionStatus.STARTED)) {
      throw new VeniceException("Job:" + job.getJobId() + " is not running.");
    }
    boolean isAllCompleted = true;
    for (int partitionId = 0; partitionId < job.getNumberOfPartition(); partitionId++) {
      List<Task> tasksInPartition = job.tasksInPartition(partitionId);
      int errorTaskCount = 0;
      int completeTaskCount = 0;
      int totalTaskCount = tasksInPartition.size();
      for (Task task : tasksInPartition) {
        if (task.getStatus().equals(ExecutionStatus.ERROR)) {
          errorTaskCount++;
        } else if (task.getStatus().equals(ExecutionStatus.COMPLETED)) {
          completeTaskCount++;
        }
      }
      if (logger.isDebugEnabled()) {
        logger.debug(errorTaskCount + " error tasks and " + completeTaskCount + " completed tasks are founded for job:"
            + job.getJobId() + " topic:" + job.getKafkaTopic());
      }
      if (!hasEnoughReplicasForOnePartition(job.getReplicationFactor() - errorTaskCount, job.getReplicationFactor())) {
        logger.info("Number or error task:" + errorTaskCount + " in partition:" + partitionId
            + " is larger than n-strategy, job:" + job.getJobId() + ", strategy:" + getStrategy());
        return ExecutionStatus.ERROR;
      }
      if (!hasMinimumReplicasForOnePartition(totalTaskCount - errorTaskCount, job)) {
        logger.info("Number of non-error tasks:" + (totalTaskCount - errorTaskCount) + " in partition:"
            + partitionId + " is smaller than minimum replicas:" + job.getMinimumReplicas() + ", Job:" + job.getJobId()
            + " Strategy:" + getStrategy());
        return ExecutionStatus.ERROR;
      } else if (!hasEnoughReplicasForOnePartition(completeTaskCount, job.getReplicationFactor())) {
        if (logger.isDebugEnabled()) {
          logger.debug("Number of completed tasks:" + completeTaskCount + " in partition:" + partitionId
              + " is smaller than required. Replication factor:" + job.getReplicationFactor() + ", Strategy:"
              + getStrategy());
        }
        isAllCompleted = false;
      }
    }
    //All of tasks status are Completed.
    return isAllCompleted ? ExecutionStatus.COMPLETED : ExecutionStatus.STARTED;
  }

  /**
   * Compare the actual number of replicas and required number to decide whether the partition has enough replicas. Abstract method which should be implemented in sub-classes.
   *
   * @param actual actual nubmer of replicas
   * @param expected required number of replicas
   *
   * @return
   */
  protected abstract boolean hasEnoughReplicasForOnePartition(int actual, int expected);

  protected static boolean hasMinimumReplicasForOnePartition(int actual, Job job) {
    return actual >= job.getMinimumReplicas();
  }

  private boolean hasEnoughPartitions(int actual, Job job) {
    if (actual != job.getNumberOfPartition()) {
      logger.info("Number of partitions:" + actual + " is different from the number required when job was created:" +
          job.getNumberOfPartition() + " Job:" + job.getJobId());
      return false;
    } else {
      return true;
    }
  }



  public abstract OfflinePushStrategy getStrategy();

  public static JobStatusDecider getDecider(OfflinePushStrategy strategy) {
    if (!decidersMap.containsKey(strategy)) {
      throw new VeniceException("No known JobStatusDecider for offline push strategy:" + strategy);
    } else {
      return decidersMap.get(strategy);
    }
  }
}
