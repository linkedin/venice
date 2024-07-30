package com.linkedin.davinci.notifier;

import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.pushstatushelper.PushStatusStoreWriter;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This is a delayed scheduler for sending batching push status in DaVinci.
 */
public class PushStatusDelayedUpdateTask {
  private static final Logger LOGGER = LogManager.getLogger(PushStatusDelayedUpdateTask.class);
  private Version version;
  private boolean batchPushStartSignalSent;
  private boolean batchPushEndSignalSent;
  private final Map<Integer, ExecutionStatus> partitionStatus = new VeniceConcurrentHashMap<>();
  // Executor for scheduling tasks
  private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
  // Reference to the currently scheduled future task
  private ScheduledFuture<Void> scheduledTask;

  public PushStatusDelayedUpdateTask(Version version) {
    this.version = version;
    this.batchPushStartSignalSent = false;
    this.batchPushEndSignalSent = false;
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

  public void updatePartitionStatusAndMaybeSendBatchingStatus(
      int partition,
      ExecutionStatus status,
      PushStatusStoreWriter pushStatusStoreWriter) {
    partitionStatus.put(partition, status);
    if (status == ExecutionStatus.COMPLETED) {
      if (!isBatchPushEndSignalSent() && areAllPartitionsOnSameTerminalStatus(status)) {
        // Create a delayed task which wakes up after 30 seconds and check whether all partitions are still on
        // COMPLETED status. If so, send the COMPLETE status to the push status store.
        // If there is already a task scheduled, cancel it and schedule a new one.
        scheduleDelayedTaskToWriteBatchingStatus(status, () -> {
          pushStatusStoreWriter
              .writeVersionLevelPushStatus(version.getStoreName(), version.getNumber(), status, getTrackedPartitions());
        });
      }
      // Otherwise, don't send any update
    } else {
      // STARTED status
      if (!isBatchPushStartSignalSent()) {
        pushStatusStoreWriter
            .writeVersionLevelPushStatus(version.getStoreName(), version.getNumber(), status, getTrackedPartitions());
        batchPushStartSignalSent();
      } // Otherwise, don't send any update
    }
  }

  private synchronized void scheduleDelayedTaskToWriteBatchingStatus(ExecutionStatus status, Runnable updateStatus) {
    // If there is already a scheduled task, cancel it
    if (scheduledTask != null && !scheduledTask.isDone()) {
      scheduledTask.cancel(false);
    }

    // Schedule the new task with a delay of 30 seconds
    scheduledTask = scheduler.schedule(() -> {
      // Double-check the condition to send the COMPLETE event
      if (!isBatchPushEndSignalSent() && areAllPartitionsOnSameTerminalStatus(status)) {
        updateStatus.run();
        batchPushEndSignalSent();
      } else {
        LOGGER.info(
            "A new partition subscription has been added. Not sending the batch push status yet for {}",
            version.kafkaTopicName());
      }
      return null;
    }, 30, TimeUnit.SECONDS);
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

  // Shutdown the scheduler gracefully
  public void shutdown() {
    scheduler.shutdown();
    try {
      if (!scheduler.awaitTermination(30, TimeUnit.SECONDS)) {
        scheduler.shutdownNow();
      }
    } catch (InterruptedException ex) {
      scheduler.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }
}
