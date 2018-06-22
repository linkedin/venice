package com.linkedin.venice.pushmonitor;

/**
 * Status of executing off-line push. The status will be used by both Job and Task.
 *
 * Adding new status will break the Controller to Storage Node communication.
 * This is used as part of StoreStatusMessage. When adding states,
 * backward compat needs to be fixed, after things are in production at least.
 *
 * TODO: Break this up in JobExecutionStatus and TaskExecutionStatus. It's pretty confusing to mix them ): ...
 */
public enum ExecutionStatus {
  /** Job doesn't yet exist */
  NOT_CREATED(true, false, false, false),

  /**
   * Legacy status.
   *
   * Now only used as the initial value for an {@link ExecutionStatus} in VeniceParentHelixAdmin, but should
   * never actually be returned to the push job, under normal circumstances...
   */
  NEW(true, true, false, false),

  /** Job/Task is started and start consuming data from Kafka */
  STARTED(true, true, false, false),

  /** Task is processing data. */
  PROGRESS(false, true, false, false),

  /** Tasks belonging to a Hybrid Store emits this instead of {@link ExecutionStatus#COMPLETED} when it consumes a EOP message */
  END_OF_PUSH_RECEIVED(true, true, true, false),

  /** Tasks belonging to a Hybrid Store emits this instead when it consumes a SOBR message */
  START_OF_BUFFER_REPLAY_RECEIVED(false, true, true, false),

  /** An incremental push job/task is started*/
  START_OF_INCREMENTAL_PUSH_RECEIVED(true, true, false, false),

  /** An incremental push job/task is completed*/
  END_OF_INCREMENTAL_PUSH_RECEIVED(true, true, false, true),

  /**
   * For task, data is read and put into storage engine.
   * For Job, all of tasks are completed.
   */
  COMPLETED(true, true, false, true),

  /** Job/task met error when processing the data. */
  ERROR(true, true, false, true),

  /**
   * Job is terminated and be removed from repository. Should be archived to historic data storage.
   * Only be used for Job
   */
  ARCHIVED(true, false, false, true),

  /** Job status is unknown when checking, and it could be caused by network issue */
  UNKNOWN(true, false, false, false);

  final boolean isJobStatus;
  final boolean isTaskStatus;
  final boolean isUsedByHybridStoresOnly;
  final boolean isTerminal;

  ExecutionStatus(boolean isJobStatus, boolean isTaskStatus, boolean isUsedByHybridStoresOnly, boolean isTerminal) {
    this.isJobStatus = isJobStatus;
    this.isTaskStatus = isTaskStatus;
    this.isUsedByHybridStoresOnly = isUsedByHybridStoresOnly;
    this.isTerminal = isTerminal;
  }

  public boolean isJobStatus() {
    return this.isJobStatus;
  }

  public boolean isTaskStatus() {
    return this.isTaskStatus;
  }

  public boolean isUsedByHybridStoresOnly() {
    return this.isUsedByHybridStoresOnly;
  }

  /**
   * @return true for {@link #COMPLETED}, {@link #ERROR} and {@link #ARCHIVED}, false otherwise.
   */
  public boolean isTerminal() {
    return this.isTerminal;
  }
}
