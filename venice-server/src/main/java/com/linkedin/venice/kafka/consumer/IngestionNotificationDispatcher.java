package com.linkedin.venice.kafka.consumer;

import com.linkedin.venice.notifier.VeniceNotifier;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import org.apache.log4j.Logger;

import java.util.Collection;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

/**
 * Class to wrap all of the interaction with {@link com.linkedin.venice.notifier.VeniceNotifier}
 */
class IngestionNotificationDispatcher {
  public static long PROGRESS_REPORT_INTERVAL = TimeUnit.MILLISECONDS.convert(10, TimeUnit.MINUTES);

  private final Logger logger;
  private final Queue<VeniceNotifier> notifiers;
  private final String topic;
  private final BooleanSupplier isCurrentVersion;

  private long lastProgressReportTime = 0;

  public IngestionNotificationDispatcher(Queue<VeniceNotifier> notifiers, String topic, BooleanSupplier isCurrentVersion) {
    this.logger = Logger.getLogger(IngestionNotificationDispatcher.class.getSimpleName() + " for [ Topic: " + topic + " ] ");
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

  private void report(PartitionConsumptionState pcs, ExecutionStatus reportType, NotifierFunction function, PreNotificationCheck preCheck) {
    if (!reportType.isTaskStatus()) {
      // Should never happen, but whatever...
      throw new IllegalArgumentException("The " + IngestionNotificationDispatcher.class.getSimpleName() +
          " can only be used to report task status.");
    }
    if (null == pcs) {
      logger.info("Partition " + pcs.getPartition() + " has been unsubscribed, no need to report " + reportType);
      return;
    }

    if (!preCheck.apply()) {
      return;
    }

    for(VeniceNotifier notifier : notifiers) {
      try {
        function.apply(notifier);
      } catch(Exception ex) {
        logger.error("Error reporting status to notifier " + notifier.getClass() , ex);
      }
    }

    logger.info("Reported " + reportType + " to " + notifiers.size() + " notifiers for PartitionConsumptionState: " + pcs);
  }

  void report(PartitionConsumptionState pcs, ExecutionStatus reportType, NotifierFunction function) {
    report(pcs, reportType, function, () -> true);
  }

  void reportStarted(PartitionConsumptionState pcs) {
    report(pcs, ExecutionStatus.STARTED, notifier -> notifier.started(topic, pcs.getPartition()));
  }

  void reportRestarted(PartitionConsumptionState pcs) {
    report(pcs, ExecutionStatus.STARTED, notifier -> notifier.restarted(topic, pcs.getPartition(), pcs.getOffsetRecord().getOffset()));
  }

  void reportCompleted(PartitionConsumptionState pcs) {
    report(pcs, ExecutionStatus.COMPLETED,
        notifier -> {
          notifier.completed(topic, pcs.getPartition(), pcs.getOffsetRecord().getOffset());
          pcs.completionReported();
        }, () -> {
          if (pcs.isErrorReported()) {
            // Notifiers will not be sent a completion notification, they should only
            // receive the previously-sent error notification.
            logger.error("Processing completed WITH ERRORS for Partition: " + pcs.getPartition() +
                ", Last Offset: " + pcs.getOffsetRecord().getOffset());
            return false;
          }
          if (!pcs.isComplete()) {
            logger.error("Unexpected! Received a request to report completion " +
                "but the PartitionConsumptionState says it is incomplete: " + pcs);
            return false;
          }
          if (pcs.isCompletionReported()) {
            logger.info("Received a request to report completion, but it has already been reported. Skipping.");
            return false;
          }
          return true;
        }
    );
  }

  void reportProgress(PartitionConsumptionState pcs) {
    report(pcs, ExecutionStatus.PROGRESS,
        notifier -> notifier.progress(topic, pcs.getPartition(), pcs.getOffsetRecord().getOffset()),
        () -> {
          logger.info("pcs: " + pcs);
          if (!isCurrentVersion.getAsBoolean() && // The currently-active version should always report progress.
              (!pcs.isStarted() ||
                  pcs.isEndOfPushReceived() ||
                  pcs.isErrorReported())) {
            logger.warn("Can not report progress for Topic:" + topic +
                ", Partition:" + pcs.getPartition() +
                ", offset:" + pcs.getOffsetRecord().getOffset() +
                ", because it has not been started or already been terminated." +
                " partitionConsumptionState: " + pcs.toString());
            return false;
          }

          // Progress reporting happens too frequently for each Kafka Pull,
          // Report progress only if configured intervals have elapsed.
          // This has a drawback if there are messages but the interval has not elapsed
          // they will not be reported. But if there are no messages after that
          // for a long time, no progress will be reported. That is OK for now.
          long timeElapsed = System.currentTimeMillis() - lastProgressReportTime;
          if(timeElapsed < PROGRESS_REPORT_INTERVAL) {
            return false;
          }

          lastProgressReportTime = System.currentTimeMillis();
          return true;
        }
    );
  }

  void reportEndOfPushReceived(PartitionConsumptionState pcs) {
    report(pcs, ExecutionStatus.END_OF_PUSH_RECEIVED,
        notifier -> notifier.endOfPushReceived(topic, pcs.getPartition(), pcs.getOffsetRecord().getOffset()));
  }

  void reportStartOfBufferReplayReceived(PartitionConsumptionState pcs) {
    report(pcs, ExecutionStatus.START_OF_BUFFER_REPLAY_RECEIVED,
        notifier -> notifier.startOfBufferReplayReceived(topic, pcs.getPartition(), pcs.getOffsetRecord().getOffset()));
  }

  void reportError(Collection<PartitionConsumptionState> pcsList, String message, Exception consumerEx) {
    for(PartitionConsumptionState pcs: pcsList) {
      report(pcs, ExecutionStatus.ERROR,
          notifier -> {
            notifier.error(topic, pcs.getPartition(), message, consumerEx);
            pcs.errorReported();
          }, () -> {
            if (pcs.isEndOfPushReceived()) {
              logger.warn("Partition:" + pcs.getPartition() + " has been marked as completed, so an error will not be reported...");
              return false;
            }
            if (pcs.isErrorReported()) {
              logger.warn("Partition:" + pcs.getPartition() + " has been reported as error before.");
              return false;
            }
            return true;
          }
      );
    }
  }

}