package com.linkedin.venice.kafka.consumer;

import com.linkedin.venice.exceptions.VeniceIngestionTaskKilledException;
import com.linkedin.venice.notifier.VeniceNotifier;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.pushmonitor.HybridStoreQuotaStatus;
import com.linkedin.venice.utils.RedundantExceptionFilter;
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
  private final RedundantExceptionFilter filter = RedundantExceptionFilter.getDailyRedundantExceptioFilter();

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

  private void report(PartitionConsumptionState pcs, String reportType, NotifierFunction function, PreNotificationCheck preCheck) {
    if (null == pcs) {
      // Problem: calling pcs.getPartition() with pcs null.
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

  void report(PartitionConsumptionState pcs, ExecutionStatus reportType, NotifierFunction function, PreNotificationCheck preCheck) {
    if (!reportType.isTaskStatus()) {
      // Should never happen, but whatever...
      throw new IllegalArgumentException("The " + IngestionNotificationDispatcher.class.getSimpleName() +
          " can only be used to report task status.");
    }
    report(pcs, reportType.name(), function, preCheck);
  }

  void report(PartitionConsumptionState pcs, ExecutionStatus reportType, NotifierFunction function) {
    report(pcs, reportType.name(), function, () -> true);
  }

  void report(PartitionConsumptionState pcs, HybridStoreQuotaStatus reportType, NotifierFunction function) {
    report(pcs, reportType.name(), function, () -> true);
  }

  void reportStarted(PartitionConsumptionState pcs) {
    report(pcs, ExecutionStatus.STARTED, notifier -> notifier.started(topic, pcs.getPartition()));
  }


  void reportRestarted(PartitionConsumptionState pcs) {
    report(pcs, ExecutionStatus.STARTED, notifier -> notifier.restarted(topic, pcs.getPartition(), pcs.getOffsetRecord().getOffset()));
  }

  void reportCatchUpBaseTopicOffsetLag(PartitionConsumptionState pcs) {
    report(pcs, ExecutionStatus.CATCH_UP_BASE_TOPIC_OFFSET_LAG,
        notifier -> {
          notifier.catchUpBaseTopicOffsetLag(topic, pcs.getPartition());
          pcs.releaseLatch();
        });
  }

  void reportCompleted(PartitionConsumptionState pcs) {
    report(pcs, ExecutionStatus.COMPLETED,
        notifier -> {
          notifier.completed(topic, pcs.getPartition(), pcs.getOffsetRecord().getOffset());
          pcs.releaseLatch();
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

  void reportQuotaNotViolated(PartitionConsumptionState pcs) {
    report(pcs, HybridStoreQuotaStatus.QUOTA_NOT_VIOLATED,
        notifier -> notifier.quotaNotViolated(topic, pcs.getPartition(), pcs.getOffsetRecord().getOffset()));
  }

  void reportQuotaViolated(PartitionConsumptionState pcs) {
    report(pcs, HybridStoreQuotaStatus.QUOTA_VIOLATED,
        notifier -> notifier.quotaViolated(topic, pcs.getPartition(), pcs.getOffsetRecord().getOffset())
    );
  }

  void reportProgress(PartitionConsumptionState pcs) {
    report(pcs, ExecutionStatus.PROGRESS,
        notifier -> notifier.progress(topic, pcs.getPartition(), pcs.getOffsetRecord().getOffset()),
        () -> {

          // Progress reporting happens too frequently for each Kafka Pull,
          // Report progress only if configured intervals have elapsed.
          // This has a drawback if there are messages but the interval has not elapsed
          // they will not be reported. But if there are no messages after that
          // for a long time, no progress will be reported. That is OK for now.
          long timeElapsed = System.currentTimeMillis() - lastProgressReportTime;
          if(timeElapsed < PROGRESS_REPORT_INTERVAL) {
            return false;
          }

          if (!isCurrentVersion.getAsBoolean() && // The currently-active version should always report progress.
              (!pcs.isStarted() ||
                  pcs.isEndOfPushReceived() ||
                  pcs.isErrorReported())) {
            if (logger.isDebugEnabled()) {
              logger.debug("Can not report progress for topic '" + topic +
                  "', because it has not been started or has already been terminated. partitionConsumptionState: " +
                  pcs.toString());
            }
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

  void reportStartOfIncrementalPushReceived(PartitionConsumptionState pcs, String version) {
    report(pcs, ExecutionStatus.START_OF_INCREMENTAL_PUSH_RECEIVED,
        notifier ->
            notifier.startOfIncrementalPushReceived(topic, pcs.getPartition(), pcs.getOffsetRecord().getOffset(), version));
  }

  void reportEndOfIncrementalPushRecived(PartitionConsumptionState pcs, String version) {
    report(pcs, ExecutionStatus.END_OF_INCREMENTAL_PUSH_RECEIVED,
        notifier ->
            notifier.endOfIncrementalPushReceived(topic, pcs.getPartition(), pcs.getOffsetRecord().getOffset(), version));
  }

  void reportStartOfBufferReplayReceived(PartitionConsumptionState pcs) {
    report(pcs, ExecutionStatus.START_OF_BUFFER_REPLAY_RECEIVED,
        notifier -> notifier.startOfBufferReplayReceived(topic, pcs.getPartition(), pcs.getOffsetRecord().getOffset()));
  }

  void reportTopicSwitchReceived(PartitionConsumptionState pcs) {
    report(pcs, ExecutionStatus.TOPIC_SWITCH_RECEIVED,
        notifier -> notifier.topicSwitchReceived(topic, pcs.getPartition(), pcs.getOffsetRecord().getOffset()));
  }

  void reportError(int partition, String message, Exception consumerEx) {
    for (VeniceNotifier notifier : notifiers) {
      try {
        notifier.error(topic, partition, message, consumerEx);
      } catch (Exception ex) {
        logger.error("Error reporting status to notifier " + notifier.getClass() , ex);
      }
    }

    logger.info("Reported " + ExecutionStatus.ERROR + " to " + notifiers.size() + " notifiers for partition: " + partition);
  }

  void reportError(Collection<PartitionConsumptionState> pcsList, String message, Exception consumerEx) {
    for(PartitionConsumptionState pcs: pcsList) {
      report(pcs, ExecutionStatus.ERROR,
          notifier -> {
            notifier.error(topic, pcs.getPartition(), message, consumerEx);
            pcs.errorReported();
          }, () -> {
            String logMessage = "Partition: " + pcs.getPartition() + " has already been ";
            boolean report = true;

            if (pcs.isComplete()) {
              logMessage += "marked as completed so an error will not be reported.";
              report = false;
            }
            if (pcs.isErrorReported()) {
              logMessage += "reported as an error before so it will not be reported again.";
              report = false;
            }

            if (!report) {
              if (filter.isRedundantException(message)) {
                logger.warn(logMessage + " The full stacktrace for this error message has already been printed earlier,"
                    + " so it will not be re-printed again. Current error message: " + message);
              } else {
                logger.warn(logMessage + " Full stacktrace below: ", consumerEx);
              }
            }
            return report;
          }
      );
    }
  }

  /**
   * Report the consumption is stopped by the kill signal. As kill and error are orthogonal features, so separate it
   * from report error.
   */
  void reportKilled(Collection<PartitionConsumptionState> pcsList, VeniceIngestionTaskKilledException ke) {
    for (PartitionConsumptionState pcs : pcsList) {
      report(pcs, ExecutionStatus.ERROR, notifier -> {
        notifier.error(topic, pcs.getPartition(), ke.getMessage(), ke);
        pcs.errorReported();
      }, () -> {
        if (pcs.isErrorReported()) {
          logger.warn("Partition:" + pcs.getPartition() + " has been reported as error before.");
          return false;
        }
        // Once a replica is completed, there is not need to kill the state transition.
        if(pcs.isCompletionReported()){
          logger.warn("Partition:" + pcs.getPartition()
              + " has been marked as completed, so an error will not be reported...");
          return false;
        }
        return true;
      });
    }
  }
}