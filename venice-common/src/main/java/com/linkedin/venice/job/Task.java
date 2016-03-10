package com.linkedin.venice.job;

/**
 * Venice Task which is executed in one instance.
 */
public class Task {
  private final String taskId;
  private final int partitionId;
  private final String instanceId;
  private JobAndTaskStatus status = JobAndTaskStatus.UNKNOW;
  //Decribe the progress of task's execution.Initial value is 0 then Complete value = 1
  private float progress = 0;

  public Task(String taskId, int partitionId, String instanceId) {
    this.taskId = taskId;
    this.partitionId = partitionId;
    this.instanceId = instanceId;
  }

  public Task(String taskId, int partitionId, String instanceId, JobAndTaskStatus status) {
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

  public JobAndTaskStatus getStatus() {
    return status;
  }

  protected void setStatus(JobAndTaskStatus status) {
    this.status = status;
  }

  public float getProgress() {
    return progress;
  }

  protected void setProgress(float progress) {
    this.progress = progress;
  }
}
