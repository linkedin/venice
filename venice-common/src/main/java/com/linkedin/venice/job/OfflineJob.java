package com.linkedin.venice.job;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.OfflinePushStrategy;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * Job used to describe the off-line push.
 */
public class OfflineJob extends Job {
  private Map<Integer, Map<String, Task>> partitionToTasksMap;
  private OfflinePushStrategy strategy;

  public OfflineJob(long jobId, String kafkaTopic, int numberOfPartition, int replicaFactor) {
    this(jobId, kafkaTopic, numberOfPartition, replicaFactor, OfflinePushStrategy.WAIT_ALL_REPLICAS);
  }

  public OfflineJob(long jobId, String kafkaTopic, int numberOfPartition, int replicaFactor,
      OfflinePushStrategy strategy) {
    super(jobId, kafkaTopic, numberOfPartition, replicaFactor);
    this.strategy = strategy;
    this.partitionToTasksMap = new HashMap<>();
    for (int i = 0; i < this.getNumberOfPartition(); i++) {
      this.partitionToTasksMap.put(i, new HashMap<>());
    }
  }

  //TODO Sync up nodes map in real-time to handle the cases like node failed, new node is assigned etc.
  public ExecutionStatus checkJobStatus() {
    //Only check the status of running job.
    if (!this.getStatus().equals(ExecutionStatus.STARTED)) {
      throw new VeniceException("Job:" + getJobId() + " is not running.");
    }
    if (strategy.equals(OfflinePushStrategy.WAIT_ALL_REPLICAS)) {
      boolean isAllCompleted = true;
      for (Map<String, Task> taskMap : partitionToTasksMap.values()) {
        if (taskMap.size() != this.getReplicaFactor()) {
          //Some of tasks did not report "Started" status.
          isAllCompleted = false;
        }
        for (Task task : taskMap.values()) {
          if (task.getStatus().equals(ExecutionStatus.ERROR)) {
            //Right now we don't have any retry. If one of task is failed, the whole job is failed.
            return ExecutionStatus.ERROR;
          } else if (!task.getStatus().equals(ExecutionStatus.COMPLETED)) {
            isAllCompleted = false;
          }
        }
      }
      //All of tasks status are Completed.
      return isAllCompleted ?ExecutionStatus.COMPLETED:ExecutionStatus.STARTED;
    }
    //TODO support more off-line push strategies.
    throw new VeniceException("Strategy:" + strategy + " is not supported.");
  }

  /**
   * Update the status of taks.
   *
   * @param task
   */
  @Override
  public void updateTaskStatus(Task task) {
    verifyTaskStatus(task);
    Map<String, Task> taskMap = partitionToTasksMap.get(task.getPartitionId());
    Task newTask = new Task(task.getTaskId(), task.getPartitionId(), task.getInstanceId(), task.getStatus());
    newTask.setProgress(task.getProgress());
    taskMap.put(newTask.getTaskId(), newTask);
  }

  /**
   * Verify whether the task's status is correct to update.
   * <p>
   * The transitions of state machine:
   * <p>
   * <ul> <li>NEW->STARTED</li> <li>STARTED->PROGRESS</li><li>STARTED->COMPLETE</li><li>STARTED->ERROR</li><li>PROGRESS->COMPLETED</li><li>PROGRESS->ERROR</li>
   * </ul>
   *
   * @param task
   */
  public void verifyTaskStatus(Task task) {
    Map<String, Task> taskMap = partitionToTasksMap.get(task.getPartitionId());
    if (taskMap == null) {
      throw new VeniceException("Partition:" + task.getPartitionId() + " dose not exist.");
    }
    Task oldTask = taskMap.get(task.getTaskId());
    if (task.getStatus().equals(ExecutionStatus.STARTED)) {
      if (oldTask != null) {
        throw new VeniceException("Task:" + task.getTaskId() + "is already started");
      }
    } else {
      if (oldTask == null) {
        throw new VeniceException("Task:" + task.getTaskId() + " dose not exist.");
      }
      if (oldTask.getStatus().equals(ExecutionStatus.COMPLETED) || oldTask.getStatus().equals(ExecutionStatus.ERROR)) {
        throw new VeniceException("Can not update a terminated task.");
      }
    }
  }

  @Override
  public ExecutionStatus getTaskStatus(int partitionId, String taskId) {
    Map<String, Task> taskMap = partitionToTasksMap.get(partitionId);
    if (taskMap == null) {
      throw new VeniceException("Partition:" + partitionId + " dose not exist.");
    }
    Task task = taskMap.get(taskId);
    if (task == null) {
      throw new VeniceException("Task:" + taskId + " dose not exist.");
    }
    return task.getStatus();
  }

  public void setTask(Task task) {
    partitionToTasksMap.get(task.getPartitionId()).put(task.getTaskId(), task);
  }

  public void deleteTask(Task task) {
    partitionToTasksMap.get(task.getPartitionId()).remove(task.getTaskId());
  }

  public Task getTask(int partitionId, String taskId) {
    return partitionToTasksMap.get(partitionId).get(taskId);
  }

  public List<Task> tasksInPartition(int partitionId) {
    return new ArrayList<>(partitionToTasksMap.get(partitionId).values());
  }
}
