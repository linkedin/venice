package com.linkedin.venice.proxyjob.datawriter.utils;

import com.linkedin.venice.hadoop.task.datawriter.DataWriterTaskTracker;
import com.linkedin.venice.jobs.ComputeJob;
import com.linkedin.venice.proxyjob.datawriter.task.SerializingDataWriterTaskTracker;
import java.io.Serializable;


public class ProxyJobStatus implements Serializable {
  private final SerializingDataWriterTaskTracker taskTracker;
  private final ComputeJob.Status status;
  private final Throwable failureReason;

  public ProxyJobStatus(DataWriterTaskTracker taskTracker, ComputeJob.Status status, Throwable failureReason) {
    this.taskTracker = SerializingDataWriterTaskTracker.fromDataWriterTaskTracker(taskTracker);
    this.status = status;
    this.failureReason = failureReason;
  }

  public DataWriterTaskTracker getTaskTracker() {
    return taskTracker;
  }

  public ComputeJob.Status getStatus() {
    return status;
  }

  public Throwable getFailureReason() {
    return failureReason;
  }
}
