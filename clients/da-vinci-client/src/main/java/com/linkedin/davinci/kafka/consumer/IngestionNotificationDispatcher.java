package com.linkedin.davinci.kafka.consumer;

import com.linkedin.davinci.notifier.VeniceNotifier;
import com.linkedin.venice.exceptions.VeniceIngestionTaskKilledException;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.pushmonitor.HybridStoreQuotaStatus;
import com.linkedin.venice.utils.LatencyUtils;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Class to wrap all the interactions with {@link com.linkedin.davinci.notifier.VeniceNotifier}
 */
class IngestionNotificationDispatcher {
  public static long PROGRESS_REPORT_INTERVAL = TimeUnit.MILLISECONDS.convert(10, TimeUnit.MINUTES);
  public static long QUOTA_REPORT_INTERVAL = TimeUnit.MILLISECONDS.convert(10, TimeUnit.MINUTES);

  private final Logger LOGGER;
  private final Queue<VeniceNotifier> notifiers;
  private final String topic;
  private final BooleanSupplier isCurrentVersion;

  private long lastProgressReportTime = 0;
  // Contains the last reported Notification record for each partition.
  private final Map<Integer, NotificationRecord> lastQuotaStatusReported = new HashMap<>();

  public IngestionNotificationDispatcher(
      Queue<VeniceNotifier> notifiers,
      String topic,
      BooleanSupplier isCurrentVersion) {
    this.LOGGER =
        LogManager.getLogger(IngestionNotificationDispatcher.class.getSimpleName() + " for [ Topic: " + topic + " ] ");
    this.notifiers = notifiers;
    this.topic = topic;
    this.isCurrentVersion = isCurrentVersion;
  }

  @FunctionalInterface
  interface NotifierFunction {
    void apply(VeniceNotifier notifier);
  }

  @FunctionalInterface
  interface PreNotificationCheck {
    /**
     * @return true if notifications should be fired, or false otherwise.
     */
    boolean apply();
  }

  private void report(
      PartitionConsumptionState pcs,
      String reportType,
      NotifierFunction function,
      PreNotificationCheck preCheck) {
    if (pcs == null) {
      LOGGER.info("Partition has been unsubscribed, no need to report {}", reportType);
      return;
    }
    if (!preCheck.apply()) {
      return;
    }

    for (VeniceNotifier notifier: notifiers) {
      try {
        function.apply(notifier);
      } catch (Exception ex) {
        LOGGER.error("Error reporting status to notifier {}", notifier.getClass(), ex);
      }
    }
    LOGGER.info("Reported {} to {} notifiers for PartitionConsumptionState: {}", reportType, notifiers.size(), pcs);
  }

  void report(
      PartitionConsumptionState pcs,
      ExecutionStatus reportType,
      NotifierFunction function,
      PreNotificationCheck preCheck) {
    if (!reportType.isTaskStatus()) {
      // Should never happen, but whatever...
      throw new IllegalArgumentException(
          "The " + IngestionNotificationDispatcher.class.getSimpleName() + " can only be used to report task status.");
    }
    report(pcs, reportType.name(), function, preCheck);
  }

  void report(PartitionConsumptionState pcs, ExecutionStatus reportType, NotifierFunction function) {
    report(pcs, reportType.name(), function, () -> true);
  }

  void report(
      PartitionConsumptionState pcs,
      HybridStoreQuotaStatus reportType,
      NotifierFunction function,
      PreNotificationCheck preCheck) {
    report(pcs, reportType.name(), function, preCheck);
  }

  void reportStarted(PartitionConsumptionState pcs) {
    report(pcs, ExecutionStatus.STARTED, notifier -> notifier.started(topic, pcs.getPartition()));
  }

  void reportRestarted(PartitionConsumptionState pcs) {
    report(
        pcs,
        ExecutionStatus.STARTED,
        notifier -> notifier.restarted(topic, pcs.getPartition(), pcs.getLatestProcessedLocalVersionTopicOffset()));
  }

  void reportCatchUpVersionTopicOffsetLag(PartitionConsumptionState pcs) {
    report(pcs, ExecutionStatus.CATCH_UP_BASE_TOPIC_OFFSET_LAG, notifier -> {
      notifier.catchUpVersionTopicOffsetLag(topic, pcs.getPartition());
      pcs.releaseLatch();
    });
  }

  void reportCompleted(PartitionConsumptionState pcs) {
    reportCompleted(pcs, false);
  }

  /**
   * @param forceCompletion a flag that forces the completion announcement even there are still
   *                        offset lags. It's only used in
   *                        {@link LeaderFollowerStoreIngestionTask#reportIfCatchUpVersionTopicOffset(PartitionConsumptionState)}
   *                        when we need a leader to continue replication. Check out the method above
   *                        for more details.
   */
  void reportCompleted(PartitionConsumptionState pcs, boolean forceCompletion) {
    report(pcs, ExecutionStatus.COMPLETED, notifier -> {
      notifier.completed(
          topic,
          pcs.getPartition(),
          pcs.getLatestProcessedLocalVersionTopicOffset(),
          pcs.getLeaderFollowerState().toString());
      pcs.releaseLatch();
      pcs.completionReported();
    }, () -> {
      if (pcs.isErrorReported()) {
        // Notifiers will not be sent a completion notification, they should only
        // receive the previously-sent error notification.
        LOGGER.error(
            "Processing completed WITH ERRORS for Replica: {}, Last Offset: {}",
            pcs.getReplicaId(),
            pcs.getLatestProcessedLocalVersionTopicOffset());
        return false;
      }
      if (!forceCompletion && !pcs.isComplete()) {
        LOGGER.error(
            "Unexpected! Received a request to report completion "
                + "but the PartitionConsumptionState says it is incomplete: {}",
            pcs);
        return false;
      }
      if (pcs.isCompletionReported()) {
        LOGGER.info("Received a request to report completion, but it has already been reported. Skipping.");
        return false;
      }
      return true;
    });
  }

  void reportQuotaNotViolated(PartitionConsumptionState pcs) {
    report(
        pcs,
        HybridStoreQuotaStatus.QUOTA_NOT_VIOLATED,
        notifier -> notifier
            .quotaNotViolated(topic, pcs.getPartition(), pcs.getLatestProcessedLocalVersionTopicOffset()),
        () -> checkQuotaStatusReported(pcs.getPartition(), HybridStoreQuotaStatus.QUOTA_NOT_VIOLATED));
  }

  void reportQuotaViolated(PartitionConsumptionState pcs) {
    report(
        pcs,
        HybridStoreQuotaStatus.QUOTA_VIOLATED,
        notifier -> notifier.quotaViolated(topic, pcs.getPartition(), pcs.getLatestProcessedLocalVersionTopicOffset()),
        () -> checkQuotaStatusReported(pcs.getPartition(), HybridStoreQuotaStatus.QUOTA_VIOLATED));
  }

  private boolean checkQuotaStatusReported(int partitionId, HybridStoreQuotaStatus status) {
    if (!lastQuotaStatusReported.containsKey(partitionId)) {
      lastQuotaStatusReported.put(partitionId, new NotificationRecord(System.currentTimeMillis(), status));
      return true;
    }

    NotificationRecord lastRecord = lastQuotaStatusReported.get(partitionId);
    if (lastRecord.getStatus() != status
        || LatencyUtils.getElapsedTimeFromMsToMs(lastRecord.getTimeStampInMs()) >= QUOTA_REPORT_INTERVAL) {
      lastQuotaStatusReported.put(partitionId, new NotificationRecord(System.currentTimeMillis(), status));
      return true;
    }
    return false;
  }

  void reportProgress(PartitionConsumptionState pcs) {
    report(
        pcs,
        ExecutionStatus.PROGRESS,
        notifier -> notifier.progress(topic, pcs.getPartition(), pcs.getLatestProcessedLocalVersionTopicOffset()),
        () -> {

          // Progress reporting happens too frequently for each Kafka Pull,
          // Report progress only if configured intervals have elapsed.
          // This has a drawback if there are messages but the interval has not elapsed
          // they will not be reported. But if there are no messages after that
          // for a long time, no progress will be reported. That is OK for now.
          long timeElapsed = System.currentTimeMillis() - lastProgressReportTime;
          if (timeElapsed < PROGRESS_REPORT_INTERVAL) {
            return false;
          }

          if (!isCurrentVersion.getAsBoolean() && // The currently-active version should always report progress.
          (!pcs.isStarted() || pcs.isEndOfPushReceived() || pcs.isErrorReported())) {
            LOGGER.debug(
                "Can not report progress for topic '{}', because it has not been started or has already been terminated. "
                    + "partitionConsumptionState: {}",
                topic,
                pcs);
            return false;
          }

          lastProgressReportTime = System.currentTimeMillis();
          return true;
        });
  }

  void reportEndOfPushReceived(PartitionConsumptionState pcs) {
    report(
        pcs,
        ExecutionStatus.END_OF_PUSH_RECEIVED,
        notifier -> notifier
            .endOfPushReceived(topic, pcs.getPartition(), pcs.getLatestProcessedLocalVersionTopicOffset()));
  }

  void reportStartOfIncrementalPushReceived(PartitionConsumptionState pcs, String version) {
    report(
        pcs,
        ExecutionStatus.START_OF_INCREMENTAL_PUSH_RECEIVED,
        notifier -> notifier.startOfIncrementalPushReceived(
            topic,
            pcs.getPartition(),
            pcs.getLatestProcessedLocalVersionTopicOffset(),
            version));
  }

  void reportEndOfIncrementalPushReceived(PartitionConsumptionState pcs, String version) {
    report(
        pcs,
        ExecutionStatus.END_OF_INCREMENTAL_PUSH_RECEIVED,
        notifier -> notifier.endOfIncrementalPushReceived(
            topic,
            pcs.getPartition(),
            pcs.getLatestProcessedLocalVersionTopicOffset(),
            version));
  }

  void reportBatchEndOfIncrementalPushStatus(PartitionConsumptionState pcs) {
    report(
        pcs,
        ExecutionStatus.END_OF_INCREMENTAL_PUSH_RECEIVED,
        notifier -> notifier.batchEndOfIncrementalPushReceived(
            topic,
            pcs.getPartition(),
            pcs.getLatestProcessedLocalVersionTopicOffset(),
            pcs.getPendingReportIncPushVersionList()));
  }

  void reportTopicSwitchReceived(PartitionConsumptionState pcs) {
    report(
        pcs,
        ExecutionStatus.TOPIC_SWITCH_RECEIVED,
        notifier -> notifier
            .topicSwitchReceived(topic, pcs.getPartition(), pcs.getLatestProcessedLocalVersionTopicOffset()));
  }

  void reportDataRecoveryCompleted(PartitionConsumptionState pcs) {
    report(
        pcs,
        ExecutionStatus.DATA_RECOVERY_COMPLETED,
        notifier -> notifier
            .dataRecoveryCompleted(topic, pcs.getPartition(), pcs.getOffsetRecord().getLocalVersionTopicOffset(), ""));
  }

  void reportError(int partition, String message, Exception consumerEx) {
    for (VeniceNotifier notifier: notifiers) {
      try {
        notifier.error(topic, partition, message, consumerEx);
      } catch (Exception ex) {
        LOGGER.error("Error reporting status to notifier {}", notifier.getClass(), ex);
      }
    }

    LOGGER.info("Reported {} to {} notifiers for partition: {}", ExecutionStatus.ERROR, notifiers.size(), partition);
  }

  void reportError(Collection<PartitionConsumptionState> pcsList, String message, Exception consumerEx) {
    for (PartitionConsumptionState pcs: pcsList) {
      report(pcs, ExecutionStatus.ERROR, notifier -> {
        notifier.error(topic, pcs.getPartition(), message, consumerEx);
        pcs.errorReported();
      }, () -> {
        StringBuilder logMessage = new StringBuilder();
        logMessage.append("Replica: ").append(pcs.getReplicaId()).append(" has already been ");
        boolean report = true;

        if (pcs.isComplete()) {
          logMessage.append("marked as completed so an error will not be reported.");
          report = false;
        }
        if (pcs.isErrorReported()) {
          logMessage.append("reported as an error before so it will not be reported again.");
          report = false;
        }

        if (!report) {
          LOGGER.info(logMessage.toString());
        }
        return report;
      });
    }
  }

  /**
   * Report the consumption is stopped by the kill signal. As kill and error are orthogonal features, so separate it
   * from report error.
   */
  void reportKilled(Collection<PartitionConsumptionState> pcsList, VeniceIngestionTaskKilledException ke) {
    for (PartitionConsumptionState pcs: pcsList) {
      report(pcs, ExecutionStatus.ERROR, notifier -> {
        notifier.error(topic, pcs.getPartition(), ke.getMessage(), ke);
        pcs.errorReported();
      }, () -> {
        if (pcs.isErrorReported()) {
          LOGGER.warn("Replica: {} has been reported as error before.", pcs.getReplicaId());
          return false;
        }
        // Once a replica is completed, there is no need to kill the state transition.
        if (pcs.isCompletionReported()) {
          LOGGER.warn(
              "Replica: {} has been marked as completed, so an error will not be reported...",
              pcs.getReplicaId());
          return false;
        }
        return true;
      });
    }
  }

  void reportStopped(PartitionConsumptionState pcs) {
    report(pcs, "STOPPED", notifier -> {
      notifier.stopped(topic, pcs.getPartition(), pcs.getLatestProcessedLocalVersionTopicOffset());
    }, () -> true);
  }

  private static class NotificationRecord {
    private final long timeStampInMs;
    private final HybridStoreQuotaStatus status;

    public NotificationRecord(long timeStampInMs, HybridStoreQuotaStatus status) {
      this.timeStampInMs = timeStampInMs;
      this.status = status;
    }

    public HybridStoreQuotaStatus getStatus() {
      return status;
    }

    public long getTimeStampInMs() {
      return timeStampInMs;
    }
  }
}
