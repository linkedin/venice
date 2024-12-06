package com.linkedin.davinci.notifier;

import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.pushstatushelper.PushStatusStoreWriter;
import com.linkedin.venice.utils.DaemonThreadFactory;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This is a scheduler for sending batching push status in DaVinci.
 */
public class DaVinciPushStatusUpdateTask {
  private static final Logger LOGGER = LogManager.getLogger(DaVinciPushStatusUpdateTask.class);
  private final Version version;
  private final PushStatusStoreWriter pushStatusStoreWriter;
  private boolean batchPushStartSignalSent;
  private boolean batchPushEndSignalSent;
  private final long daVinciPushStatusCheckIntervalInMs;
  private final Supplier<Boolean> areAllPartitionFuturesCompletedSuccessfully;
  private final Map<Integer, ExecutionStatus> batchPushPartitionStatus = new VeniceConcurrentHashMap<>();
  private final Map<String, IncrementalPushStatus> incrementalPushVersionToStatus = new VeniceConcurrentHashMap<>();
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

  public void updatePartitionStatus(int partition, ExecutionStatus status, Optional<String> incrementalPushVersion) {
    if (incrementalPushVersion.isPresent()) {
      updateIncrementalPushStatus(incrementalPushVersion.get(), partition, status);
    } else {
      batchPushPartitionStatus.put(partition, status);
    }
  }

  private void updateIncrementalPushStatus(String incrementalPushVersion, int partition, ExecutionStatus status) {
    IncrementalPushStatus incrementalPushStatus = incrementalPushVersionToStatus
        .computeIfAbsent(incrementalPushVersion, k -> new IncrementalPushStatus(incrementalPushVersion));

    if (incrementalPushStatus.endSignalSent) {
      LOGGER.warn(
          "Received status update for store version {} partition {} with status {} for incremental push version {} after terminal signal is sent; ignoring it",
          version.kafkaTopicName(),
          partition,
          status,
          incrementalPushVersion);
      return;
    }
    incrementalPushStatus.partitionStatus.put(partition, status);
  }

  private void maybeSendBatchingStatus() {
    handleBatchPushStatus();
    handleIncrementalPushStatus();
  }

  private void handleBatchPushStatus() {
    if (!isBatchPushStartSignalSent()) {
      sendPushStatus(ExecutionStatus.STARTED, Optional.empty());
      batchPushStartSignalSent();
    } else if (!isBatchPushEndSignalSent()
        && areAllPartitionsOnSameTerminalStatus(ExecutionStatus.COMPLETED, Optional.empty())
        && areAllPartitionFuturesCompletedSuccessfully.get()) {
      sendPushStatus(ExecutionStatus.COMPLETED, Optional.empty());
      batchPushEndSignalSent();
      batchPushPartitionStatus.clear();
    } else if (!isBatchPushEndSignalSent() && isAnyPartitionOnErrorStatus(Optional.empty())) {
      sendPushStatus(ExecutionStatus.ERROR, Optional.empty());
      batchPushEndSignalSent();
      batchPushPartitionStatus.clear();
    }
  }

  private void handleIncrementalPushStatus() {
    incrementalPushVersionToStatus.forEach((incrementalPushVersion, incrementalPushStatus) -> {
      if (!incrementalPushStatus.startSignalSent) {
        sendPushStatus(ExecutionStatus.START_OF_INCREMENTAL_PUSH_RECEIVED, Optional.of(incrementalPushVersion));
        incrementalPushStatus.startSignalSent = true;
      } else if (!incrementalPushStatus.endSignalSent && areAllPartitionsOnSameTerminalStatus(
          ExecutionStatus.END_OF_INCREMENTAL_PUSH_RECEIVED,
          Optional.of(incrementalPushVersion))) {
        sendPushStatus(ExecutionStatus.END_OF_INCREMENTAL_PUSH_RECEIVED, Optional.of(incrementalPushVersion));
        incrementalPushStatus.endSignalSent = true;
        // Clean up the status map for incremental push versions in case there are too many of them and cause OOM
        incrementalPushStatus.partitionStatus.clear();
      }
    });
  }

  private void sendPushStatus(ExecutionStatus status, Optional<String> incrementalPushVersion) {
    pushStatusStoreWriter.writeVersionLevelPushStatus(
        version.getStoreName(),
        version.getNumber(),
        status,
        getTrackedPartitions(incrementalPushVersion),
        incrementalPushVersion);
  }

  public Set<Integer> getTrackedPartitions(Optional<String> incrementalPushVersion) {
    if (incrementalPushVersion.isPresent()) {
      IncrementalPushStatus incrementalPushStatus = incrementalPushVersionToStatus.get(incrementalPushVersion.get());
      return (incrementalPushStatus == null) ? Collections.emptySet() : incrementalPushStatus.partitionStatus.keySet();
    } else {
      return batchPushPartitionStatus.keySet();
    }
  }

  public boolean areAllPartitionsOnSameTerminalStatus(ExecutionStatus status, Optional<String> incrementalPushVersion) {
    return arePartitionsOnTargetStatus(status, incrementalPushVersion, true);
  }

  public boolean isAnyPartitionOnErrorStatus(Optional<String> incrementalPushVersion) {
    return arePartitionsOnTargetStatus(ExecutionStatus.ERROR, incrementalPushVersion, false);
  }

  private boolean arePartitionsOnTargetStatus(
      ExecutionStatus status,
      Optional<String> incrementalPushVersion,
      boolean allMatch) {
    Map<Integer, ExecutionStatus> partitionStatus = getPartitionStatus(incrementalPushVersion);
    if (partitionStatus.isEmpty()) {
      return false;
    }
    return allMatch
        ? partitionStatus.values().stream().allMatch(status::equals)
        : partitionStatus.values().stream().anyMatch(status::equals);
  }

  private Map<Integer, ExecutionStatus> getPartitionStatus(Optional<String> incrementalPushVersion) {
    if (incrementalPushVersion.isPresent()) {
      IncrementalPushStatus incrementalPushStatus = incrementalPushVersionToStatus.get(incrementalPushVersion.get());
      if (incrementalPushStatus == null) {
        return Collections.EMPTY_MAP;
      }
      return incrementalPushStatus.partitionStatus;
    } else {
      return batchPushPartitionStatus;
    }
  }

  public void start() {
    scheduler.scheduleAtFixedRate(
        this::maybeSendBatchingStatus,
        0,
        daVinciPushStatusCheckIntervalInMs,
        TimeUnit.MILLISECONDS);
  }

  public void shutdown() {
    scheduler.shutdown();
  }

  private static class IncrementalPushStatus {
    private final String incrementalPushVersion;
    private boolean startSignalSent;
    private boolean endSignalSent;
    private Map<Integer, ExecutionStatus> partitionStatus;

    public IncrementalPushStatus(String incrementalPushVersion) {
      this.incrementalPushVersion = incrementalPushVersion;
      this.partitionStatus = new ConcurrentHashMap<>();
      this.startSignalSent = false;
      this.endSignalSent = false;
    }

    @Override
    public String toString() {
      return "IncrementalPushStatus{" + "incrementalPushVersion='" + incrementalPushVersion + '\''
          + ", startSignalSent=" + startSignalSent + ", endSignalSent=" + endSignalSent + ", partitionStatus="
          + partitionStatus + '}';
    }
  }
}
