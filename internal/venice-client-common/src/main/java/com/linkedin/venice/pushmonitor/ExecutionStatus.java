package com.linkedin.venice.pushmonitor;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;


/**
 * Status of executing off-line push. The status will be used by both Job and Task.
 *
 * Adding new status will break the Controller to Storage Node communication.
 * This is used as part of StoreStatusMessage. When adding states,
 * backward compat needs to be fixed, after things are in production at least.
 *
 * Whenever a new status is introduced, we should follow the correct deployment
 * order:
 * The consumers of the execution status should be deployed with the latest code
 * before anyone reports the new status; the consumers include controllers, routers,
 * servers at this moment; in future, there could also be Samza users that depend
 * on ExecutionStatus.
 *
 * TODO: Break this up in JobExecutionStatus and TaskExecutionStatus. It's pretty confusing to mix them ): ...
 */
public enum ExecutionStatus {
  /** Job doesn't yet exist */
  NOT_CREATED(true, false, false, false, 0),

  /**
   * Legacy status.
   *
   * Now only used as the initial value for an {@link ExecutionStatus} in VeniceParentHelixAdmin, but should
   * never actually be returned to the push job, under normal circumstances...
   */
  NEW(true, true, false, false, 1),

  /** Job/Task is started and start consuming data from Kafka */
  STARTED(true, true, false, false, 2),

  /** Task is processing data. */
  PROGRESS(false, true, false, false, 3),

  /** Tasks belonging to a Hybrid Store emits this instead of {@link ExecutionStatus#COMPLETED} when it consumes a EOP message */
  END_OF_PUSH_RECEIVED(true, true, true, false, 4),

  /** Tasks belonging to a Hybrid Store emits this instead when it consumes a SOBR message */
  @Deprecated
  START_OF_BUFFER_REPLAY_RECEIVED(false, true, true, false, 5),

  /** Tasks belonging to a Hybrid Store emits this instead when it consumes a TS message */
  TOPIC_SWITCH_RECEIVED(false, true, true, false, 6),

  /** An incremental push job/task is started*/
  START_OF_INCREMENTAL_PUSH_RECEIVED(true, true, false, false, 7),

  /** An incremental push job/task is completed*/
  END_OF_INCREMENTAL_PUSH_RECEIVED(true, true, false, true, 8),

  /* Task is dropped by the storage node */
  DROPPED(false, true, false, false, 9),

  /**
   * For task, data is read and put into storage engine.
   * For Job, all of tasks are completed.
   */
  COMPLETED(true, true, false, true, 10),

  /** a non-fatal error task meets when processing the data. Often happens after EOP is received. **/
  WARNING(false, true, false, true, 11),

  /** Job/task met error when processing the data. */
  ERROR(true, true, false, true, 12),

  CATCH_UP_BASE_TOPIC_OFFSET_LAG(false, true, true, false, 13),

  /**
   * Job is terminated and be removed from repository. Should be archived to historic data storage.
   * Only be used for Job
   * TODO: remove ARCHIVED as it's not been used anymore
   */
  ARCHIVED(true, false, false, true, 14),

  /** Job status is unknown when checking, and it could be caused by network issue */
  UNKNOWN(true, false, false, false, 15),

  /** Job/Task is created but ingestion haven't started yet */
  NOT_STARTED(false, true, false, false, 16),

  DATA_RECOVERY_COMPLETED(false, true, false, false, 17);

  final boolean isJobStatus;
  final boolean isTaskStatus;
  final boolean isUsedByHybridStoresOnly;
  final boolean isTerminal;

  final int value;

  ExecutionStatus(
      boolean isJobStatus,
      boolean isTaskStatus,
      boolean isUsedByHybridStoresOnly,
      boolean isTerminal,
      int value) {
    this.isJobStatus = isJobStatus;
    this.isTaskStatus = isTaskStatus;
    this.isUsedByHybridStoresOnly = isUsedByHybridStoresOnly;
    this.isTerminal = isTerminal;
    this.value = value;
  }

  /**
   * Some of the statuses are like watermark. These statuses are used in {@link PushMonitor} and
   * {@link com.linkedin.venice.router.api.VeniceVersionFinder} to determine whether a job is finished
   * and whether a host is ready to serve read requests.
   */
  public static boolean isDeterminedStatus(ExecutionStatus status) {
    return status == STARTED || status == COMPLETED || status == ERROR || status == DROPPED
        || status == END_OF_PUSH_RECEIVED;
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

  public static boolean isIncrementalPushStatus(ExecutionStatus status) {
    return status == START_OF_INCREMENTAL_PUSH_RECEIVED || status == END_OF_INCREMENTAL_PUSH_RECEIVED;
  }

  public static boolean isIncrementalPushStatus(int statusVal) {
    return statusVal == START_OF_INCREMENTAL_PUSH_RECEIVED.getValue()
        || statusVal == END_OF_INCREMENTAL_PUSH_RECEIVED.getValue();
  }

  public int getValue() {
    return value;
  }

  /**
   * Get ExecutionStatus from integer ordinal value in avro.
   */
  private static final Map<Integer, ExecutionStatus> idMapping = new HashMap<>();
  static {
    Arrays.stream(values()).forEach(s -> idMapping.put(s.value, s));
  }

  public static ExecutionStatus fromInt(int v) {
    ExecutionStatus status = idMapping.get(v);
    if (status == null) {
      return ExecutionStatus.UNKNOWN;
    }
    return status;
  }
}
