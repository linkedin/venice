package com.linkedin.davinci.notifier;

import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.pushstatushelper.PushStatusStoreWriter;
import com.linkedin.venice.utils.DaemonThreadFactory;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;


/**
 * This is a scheduler for sending batching push status in DaVinci.
 */
public class DaVinciPushStatusUpdateTask {
  private final Version version;
  private final PushStatusStoreWriter pushStatusStoreWriter;
  private boolean batchPushStartSignalSent;
  private boolean batchPushEndSignalSent;
  private final long daVinciPushStatusCheckIntervalInMs;
  private final Supplier<Boolean> areAllPartitionFuturesCompletedSuccessfully;
  private final Map<Integer, ExecutionStatus> partitionStatus = new VeniceConcurrentHashMap<>();
  // Executor for scheduling tasks
  private final ScheduledExecutorService scheduler =
      Executors.newScheduledThreadPool(1, new DaemonThreadFactory("davinci-push-status-update-task-scheduler"));

  public DaVinciPushStatusUpdateTask(
      Version version,
      long daVinciPushStatusCheckIntervalInMs,
      PushStatusStoreWriter pushStatusStoreWriter,
      Supplier<Boolean> areAllPartitionFuturesCompletedSuccessfully) {
    this.version = version;
    this.batchPushStartSignalSent = false;
    this.batchPushEndSignalSent = false;
    this.daVinciPushStatusCheckIntervalInMs = daVinciPushStatusCheckIntervalInMs;
    this.pushStatusStoreWriter = pushStatusStoreWriter;
    this.areAllPartitionFuturesCompletedSuccessfully = areAllPartitionFuturesCompletedSuccessfully;
  }

  public boolean isBatchPushStartSignalSent() {
    return batchPushStartSignalSent;
  }

  public boolean isBatchPushEndSignalSent() {
    return batchPushEndSignalSent;
  }

  public void batchPushStartSignalSent() {
    this.batchPushStartSignalSent = true;
  }

  public void batchPushEndSignalSent() {
    this.batchPushEndSignalSent = true;
  }

  public void updatePartitionStatus(int partition, ExecutionStatus status) {
    partitionStatus.put(partition, status);
  }

  private void maybeSendBatchingStatus() {
    // First, check if the batch push start signal has been sent, if not, send out STARTED status for this version
    // immediately, regardless if there is any partition subscription happened (this is also needed for delayed version
    // swap for DaVinci during target colo pushes)
    // If STARTED is sent, and if COMPLETED is not sent, check if all partitions are in COMPLETED status. If so, send
    // the COMPLETED status.
    if (!isBatchPushStartSignalSent()) {
      pushStatusStoreWriter.writeVersionLevelPushStatus(
          version.getStoreName(),
          version.getNumber(),
          ExecutionStatus.STARTED,
          getTrackedPartitions());
      batchPushStartSignalSent();
    } else if (!isBatchPushEndSignalSent() && areAllPartitionsOnSameTerminalStatus(ExecutionStatus.COMPLETED)
        && areAllPartitionFuturesCompletedSuccessfully.get()) {
      pushStatusStoreWriter.writeVersionLevelPushStatus(
          version.getStoreName(),
          version.getNumber(),
          ExecutionStatus.COMPLETED,
          getTrackedPartitions());
      batchPushEndSignalSent();
      // Shutdown the scheduler after sending the final status
      shutdown();
    }
  }

  /**
   * Get the partition id set that is being tracked
   */
  public Set<Integer> getTrackedPartitions() {
    return partitionStatus.keySet();
  }

  public boolean areAllPartitionsOnSameTerminalStatus(ExecutionStatus status) {
    if (partitionStatus.isEmpty()) {
      return false;
    }
    return partitionStatus.values().stream().allMatch(status::equals);
  }

  public void start() {
    // Schedule a task to check current status of all hosted partitions periodically
    scheduler.scheduleAtFixedRate(() -> {
      maybeSendBatchingStatus();
    }, 0, daVinciPushStatusCheckIntervalInMs, TimeUnit.MILLISECONDS);
  }

  // Shutdown the scheduler gracefully
  public void shutdown() {
    scheduler.shutdown();
  }
}
