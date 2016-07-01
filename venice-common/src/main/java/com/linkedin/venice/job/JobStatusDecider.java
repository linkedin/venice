package com.linkedin.venice.job;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.OfflinePushStrategy;
import com.linkedin.venice.meta.Partition;
import java.util.Collection;
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
   * Check given partition and instances to see whether job has enough task executors.
   *
   * @param job
   * @param partitions collection of partitions
   *
   * @return
   */
  public boolean hasEnoughTaskExecutors(Job job, Collection<Partition> partitions) {
    if (partitions.size() != job.getNumberOfPartition()) {
      logger.info(
          "Number of partitions:" + partitions.size() + " is different from the number required when job was created:" +
              job.getNumberOfPartition());
      return false;
    }

    for (Partition partition : partitions) {
      if (partition.getId() < 0 || partition.getId() >= partitions.size()) {
        throw new VeniceException(
            "Invalid partition Id:" + partition.getId() + ". Valid partition id should be in range: [0, " + (
                partitions.size() - 1) + "]");
      }
      if (!hasEnoughReplicasForOnePartition(partition.getInstances().size(), job.getReplicationFactor())) {
        //Some of nodes are failed.
        logger.info("Number of replicas:" + partition.getInstances().size() + " in partition:" + partition.getId()
            + "is smaller from the required. Replication factor:" + job.getReplicationFactor() + ", Strategy:" + getStrategy());
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
        logger.info(
            "Number of non-error tasks:" + (job.getReplicationFactor() - errorTaskCount) + " in partition:" + partitionId
                + " is smaller than required. Replication factor:" + job.getReplicationFactor() + ", Strategy:"
                + getStrategy());
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

  public abstract OfflinePushStrategy getStrategy();

  public static JobStatusDecider getDecider(OfflinePushStrategy strategy) {
    if (!decidersMap.containsKey(strategy)) {
      throw new VeniceException("No known JobStatusDecider for offline push strategy:" + strategy);
    } else {
      return decidersMap.get(strategy);
    }
  }
}
