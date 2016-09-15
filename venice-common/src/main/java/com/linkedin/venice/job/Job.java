package com.linkedin.venice.job;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Partition;
import com.linkedin.venice.meta.PartitionAssignment;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.codehaus.jackson.annotate.JsonIgnore;


/**
 * Job is a approach to let cluster process off-line push, stream writing or data migration. It's composed by a
 * collection of task which will be executed by instances in cluster.
 */
public abstract class Job {

  private final long jobId;
  private final String kafkaTopic;
  private final int numberOfPartition;
  private final int replicationFactor;
  private final int minimumReplicas;

  private ExecutionStatus status;

  public static final int DEFAULT_MIN_REPLCIAS = 1;

  public Job(long jobId, String kafkaTopic, int numberOfPartition, int replicationFactor, int minimumReplicas) {
    this.jobId = jobId;
    if (numberOfPartition < 1) {
      throw new VeniceException("Partition count should be larger than 0. Given value is:" + numberOfPartition);
    }
    this.numberOfPartition = numberOfPartition;
    if (replicationFactor < 1) {
      throw new VeniceException("Replication factor should larger than 0. Given value is:" + replicationFactor);
    }
    this.replicationFactor = replicationFactor;
    if (minimumReplicas < 1 || minimumReplicas > replicationFactor) {
      throw new VeniceException(
          "Minimum replicas should be larger than 0 and smaller than Replication Factor:" + replicationFactor
              + ". Given value is: " + minimumReplicas);
    }
    this.minimumReplicas = minimumReplicas;
    this.kafkaTopic = kafkaTopic;
    this.status = ExecutionStatus.NEW;
  }

  public Job(long jobId, String kafkaTopic, int numberOfPartition, int replicationFactor) {
    this(jobId, kafkaTopic, numberOfPartition, replicationFactor , DEFAULT_MIN_REPLCIAS);
  }


    public long getJobId() {
    return jobId;
  }

  public int getNumberOfPartition() {
    return numberOfPartition;
  }

  public int getReplicationFactor() {
    return replicationFactor;
  }

  public int getMinimumReplicas() {
    return minimumReplicas;
  }

  public ExecutionStatus getStatus() {
    return status;
  }

  @JsonIgnore
  public Map<String, Long> getProgress() {
    Map<String, Long> progress = new HashMap<>();
    for (int partition = 0; partition < getNumberOfPartition(); partition++) {
      for (Task task : tasksInPartition(partition)) {
        if (!task.getStatus().equals(ExecutionStatus.ERROR)) { //Don't count progress of error tasks
          progress.put(task.getTaskId(), task.getProgress());
        }
      }
    }
    return progress;
  }

  public void setStatus(ExecutionStatus status) {
    this.status = status;
  }

  public String getKafkaTopic() {
    return kafkaTopic;
  }

  public boolean isTerminated() {
    if (status.equals(ExecutionStatus.COMPLETED) || status.equals(ExecutionStatus.ERROR)) {
      return true;
    }
    return false;
  }

  public abstract void updateTaskStatus(Task task);

  public abstract ExecutionStatus getTaskStatus(int partitionId, String taskId);

  public abstract long getTaskProgress(int partitionId, String taskId);

  public abstract Task getTask(int partitionId, String taskId);

  public abstract void deleteTask(Task task);

  public abstract void addTask(Task task);

  public abstract List<Task> tasksInPartition(int partitionId);

  public abstract String generateTaskId(int paritionId, String instanceId);

  public abstract Set<Integer> updateExecutingTasks(PartitionAssignment partitionAssignment);

  /**
   * When a new status needs to be assigned to this job. Verify it at first to see whether this new status is valid or
   * not based on current job status.
   *
   * @param status
   *
   * @throws com.linkedin.venice.exceptions.VeniceException If the given status is invalid to update.
   */
  public abstract void validateJobStatusTransition(ExecutionStatus status);

  public abstract Job cloneJob();
}
