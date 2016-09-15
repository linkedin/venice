package com.linkedin.venice.job;

/**
 * Venice Task which is executed in one instance.
 */
public class Task {
  private final String taskId;
  private final int partitionId;
  private final String instanceId;
  private ExecutionStatus status;

  private long progress = 0; /* Number of offsets consumed */

  public Task(String taskId, int partitionId, String instanceId) {
    this.taskId = taskId;
    this.partitionId = partitionId;
    this.instanceId = instanceId;
    status = ExecutionStatus.NEW;
  }

  public Task(String taskId, int partitionId, String instanceId, ExecutionStatus status) {
    this(taskId, partitionId, instanceId);
    this.status = status;
  }

  public String getTaskId() {
    return taskId;
  }

  public int getPartitionId() {
    return partitionId;
  }

  public String getInstanceId() {
    return instanceId;
  }

  public ExecutionStatus getStatus() {
    return status;
  }

  protected void setStatus(ExecutionStatus status) {
    this.status = status;
  }

  public long getProgress() {
    return progress;
  }

  public void setProgress(long progress) {
    this.progress = progress;
  }
}
