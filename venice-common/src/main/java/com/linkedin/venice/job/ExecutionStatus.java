package com.linkedin.venice.job;

/**
 * Status of executing off-line push. The status will be used by both Job and Task.
 *
 * Adding new status will break the Controller to Storage Node communication.
 * This is used as part of StoreStatusMessage. When adding states,
 * backward compat needs to be fixed, after things are in production at least.
 */
public enum ExecutionStatus {
  //Job doesn't yet exist
  NOT_CREATED,
  //Job/Task just be created.
  NEW,
  //Job is started and start consuming data from Kafka
  STARTED,
  //The progress of processing the data. Should only be used for Task.
  PROGRESS,
  //For task, data is read and put into storage engine. For Job, all of tasks are completed.
  COMPLETED,
  //Met error when processing the data.
  ERROR,
  //Job is terminated and be removed from repository. Should be archived to historic data storage. Only be used for Job
  ARCHIVED;

  public boolean isTerminal() {
    return this == ERROR || this == COMPLETED || this == ARCHIVED;
  }
}
