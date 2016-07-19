package com.linkedin.venice.job;

import com.linkedin.venice.exceptions.VeniceException;
import static com.linkedin.venice.job.ExecutionStatus.*;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.OfflinePushStrategy;
import com.linkedin.venice.meta.Partition;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * Job used to describe the off-line push.
 */
public class OfflineJob extends Job {
  /**
   * The map holds the pair of partition id -> tasks map. Task map holds the pair of taskId -> Task.
   */
  private Map<Integer, Map<String, Task>> partitionToTasksMap;
  private OfflinePushStrategy strategy;

  public OfflineJob(long jobId, String kafkaTopic, int numberOfPartition, int replicaFactor) {
    this(jobId, kafkaTopic, numberOfPartition, replicaFactor, OfflinePushStrategy.WAIT_ALL_REPLICAS);
  }

  public OfflineJob(long jobId, String kafkaTopic, int numberOfPartition, int replicaFactor,
      OfflinePushStrategy strategy) {
    super(jobId, kafkaTopic, numberOfPartition, replicaFactor);
    this.strategy = strategy;
    partitionToTasksMap = new HashMap<>();
    for (int i = 0; i < this.getNumberOfPartition(); i++) {
      this.partitionToTasksMap.put(i, new HashMap<>());
    }
  }

  /**
   * When job is running, change partitions information if nodes which running tasks are changed. Job do not care about
   * the number of partitions and replicas for this new partition mapping. They should be guaranteed by invoker. Eg job
   * manager.
   *
   * @param partitions
   */
  public Set<Integer> updateExecutingTasks(Map<Integer, Partition> partitions) {
    HashSet<Integer> changedPartitions = new HashSet<>();
    for (Integer partitionId : partitions.keySet()) {
      if (partitionId < 0 || partitionId >= getNumberOfPartition()) {
        throw new VeniceException(
            "Invalid partition Id:" + partitionId + ". Valid partition id should be in range: [0, " + (
                getNumberOfPartition() - 1) + "]");
      }
      Map<String, Task> oldTaskMap = partitionToTasksMap.get(partitionId);
      Partition partition = partitions.get(partitionId);
      HashSet<String> removeTaskIds = new HashSet<>(oldTaskMap.keySet());
      for (Instance instance : partition.getBootstrapAndReadyToServeInstances()) {
        String taskId = generateTaskId(partitionId, instance.getNodeId());
        if (!oldTaskMap.containsKey(taskId)) {
          // New replica
          oldTaskMap.put(taskId, new Task(taskId, partitionId, instance.getNodeId()));
          changedPartitions.add(partitionId);
        } else {
          // Existing replica
          removeTaskIds.remove(taskId);
        }
      }
      //Remove non-existing replicas.
      if (!removeTaskIds.isEmpty()) {
        removeTaskIds.forEach(oldTaskMap::remove);
        changedPartitions.add(partitionId);
      }
    }
    return changedPartitions;
  }

  /**
   * The transition of Job state machine:
   * <p>
   * <ul>
   *   <li>NEW->STARTED</li>
   *   <li>STARTED->COMPLETED</li>
   *   <li>STARTED->ERROR</li>
   *   <li>COMPLETED->ARCHIVED</li>
   *   <li>ERROR->ARCHIVED</li>
   * </ul>
   */
  @Override
  public void validateJobStatusTransition(ExecutionStatus newStatus) {
    boolean isValid = true;
    ExecutionStatus currentStatus = getStatus();
    switch (currentStatus) {
      case NEW:
        isValid = verifyTransition(newStatus, STARTED, ERROR);
        break;
      case STARTED:
        isValid = verifyTransition(newStatus, COMPLETED, ERROR);
        break;
      case COMPLETED:
        //Same as ERROR.
      case ERROR:
        isValid = verifyTransition(newStatus, ARCHIVED);
        break;
      case ARCHIVED:
        isValid = false;
        break;
      default:
        throw new VeniceException("Invalid job status:" + currentStatus.toString());
    }
    if (!isValid) {
      throw new VeniceException(
          "Job is " + currentStatus.toString() + " can not be update to status:" + newStatus.toString());
    }
  }

  @Override
  public synchronized Job cloneJob() {
    OfflineJob clonedJob = new OfflineJob(getJobId(),getKafkaTopic(),getNumberOfPartition(), getReplicationFactor(),strategy);
    clonedJob.setStatus(getStatus());
    for(Integer partitionId:partitionToTasksMap.keySet()){
      Map<String, Task> taskMap = partitionToTasksMap.get(partitionId);
      Map<String, Task> clonedTaskMap = clonedJob.partitionToTasksMap.get(partitionId);
      for(Task task:taskMap.values()){
        Task clonedTask = new Task(task.getTaskId(),task.getPartitionId(),task.getInstanceId());
        clonedTask.setStatus(task.getStatus());
        clonedTaskMap.put(clonedTask.getTaskId(),clonedTask);
      }
    }
    return clonedJob;
  }

  /**
   * Update the status of task.
   *
   * @param task
   */
  @Override
  public void updateTaskStatus(Task task) {
    verifyTaskStatusTransition(task);
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
   * <ul>
   *   <li>NEW->STARTED</li>
   *   <li>NEW->ERROR</li>
   *   <li>STARTED->PROGRESS</li>
   *   <li>STARTED->COMPLETED</li>
   *   <li>STARTED->ERROR</li>
   *   <li>PROGRESS->COMPLETED</li>
   *   <li>PROGRESS->ERROR</li>
   * </ul>
   *
   * @param task
   */
  public void verifyTaskStatusTransition(Task task) {
    Map<String, Task> taskMap = partitionToTasksMap.get(task.getPartitionId());
    if (taskMap == null) {
      throw new VeniceException("Partition:" + task.getPartitionId() + " dose not exist.");
    }
    Task oldTask = taskMap.get(task.getTaskId());
    if (oldTask == null) {
      throw new VeniceException("Can not find task:" + task.getTaskId());
    }
    boolean isValid = true;
    switch (oldTask.getStatus()) {
      case NEW:
        isValid = verifyTransition(task.getStatus(), STARTED, ERROR);
        break;
      case STARTED:
        isValid = verifyTransition(task.getStatus(), PROGRESS, COMPLETED, ERROR);
        break;
      case PROGRESS:
        isValid = verifyTransition(task.getStatus(), COMPLETED, ERROR);
        break;
      case COMPLETED:
        // Same as ERROR.
      case ERROR:
        isValid = false;
        break;
      default:
        throw new VeniceException("Invalid task status:" + oldTask.getStatus().toString());
    }
    if (!isValid) {
      throw new VeniceException(
          "Task is " + oldTask.getStatus().toString() + " can not be update to status:" + task.getStatus().toString());
    }
  }

  private boolean verifyTransition(ExecutionStatus newStatus, ExecutionStatus... allowed) {
    return Arrays.asList(allowed).contains(newStatus);
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

  public void addTask(Task task) {
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

  public String generateTaskId(int partition, String instanceId) {
    return getJobId() + "_" + partition + "_" + instanceId;
  }
}
