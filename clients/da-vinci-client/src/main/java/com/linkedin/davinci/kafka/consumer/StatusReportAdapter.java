package com.linkedin.davinci.kafka.consumer;

import static com.linkedin.venice.pushmonitor.SubPartitionStatus.CATCH_UP_BASE_TOPIC_OFFSET_LAG;
import static com.linkedin.venice.pushmonitor.SubPartitionStatus.COMPLETED;
import static com.linkedin.venice.pushmonitor.SubPartitionStatus.DATA_RECOVERY_COMPLETED;
import static com.linkedin.venice.pushmonitor.SubPartitionStatus.END_OF_INCREMENTAL_PUSH_RECEIVED;
import static com.linkedin.venice.pushmonitor.SubPartitionStatus.END_OF_PUSH_RECEIVED;
import static com.linkedin.venice.pushmonitor.SubPartitionStatus.PROGRESS;
import static com.linkedin.venice.pushmonitor.SubPartitionStatus.RESTARTED;
import static com.linkedin.venice.pushmonitor.SubPartitionStatus.STARTED;
import static com.linkedin.venice.pushmonitor.SubPartitionStatus.START_OF_INCREMENTAL_PUSH_RECEIVED;
import static com.linkedin.venice.pushmonitor.SubPartitionStatus.TOPIC_SWITCH_RECEIVED;

import com.linkedin.venice.exceptions.VeniceIngestionTaskKilledException;
import com.linkedin.venice.pushmonitor.SubPartitionStatus;
import java.util.Collection;
import java.util.Optional;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class forwards status report requests to {@link IngestionNotificationDispatcher}.
 */
public class StatusReportAdapter {
  private static final Logger LOGGER = LogManager.getLogger(StatusReportAdapter.class);
  private final IngestionNotificationDispatcher dispatcher;

  public StatusReportAdapter(IngestionNotificationDispatcher notificationDispatcher) {
    this.dispatcher = notificationDispatcher;
  }

  // This method is called when PartitionConsumptionState are not initialized
  public void reportError(int errorPartitionId, String message, Exception consumerEx) {
    dispatcher.reportError(errorPartitionId, message, consumerEx);
  }

  public void reportError(Collection<PartitionConsumptionState> pcsList, String message, Exception consumerEx) {
    dispatcher.reportError(pcsList, message, consumerEx);
  }

  public void reportKilled(Collection<PartitionConsumptionState> pcsList, VeniceIngestionTaskKilledException ke) {
    dispatcher.reportKilled(pcsList, ke);
  }

  public void reportQuotaViolated(PartitionConsumptionState pcs) {
    dispatcher.reportQuotaViolated(pcs);
  }

  public void reportQuotaNotViolated(PartitionConsumptionState pcs) {
    dispatcher.reportQuotaNotViolated(pcs);
  }

  public void reportStopped(PartitionConsumptionState pcs) {
    dispatcher.reportStopped(pcs);
  }

  public void reportDataRecoveryCompleted(PartitionConsumptionState pcs) {
    report(pcs, DATA_RECOVERY_COMPLETED, () -> dispatcher.reportDataRecoveryCompleted(pcs));
  }

  public void reportStarted(PartitionConsumptionState pcs) {
    report(pcs, STARTED, () -> dispatcher.reportStarted(pcs));
  }

  public void reportRestarted(PartitionConsumptionState pcs) {
    report(pcs, RESTARTED, () -> dispatcher.reportRestarted(pcs));
  }

  public void reportEndOfPushReceived(PartitionConsumptionState pcs) {
    report(pcs, END_OF_PUSH_RECEIVED, () -> dispatcher.reportEndOfPushReceived(pcs));
  }

  public void reportProgress(PartitionConsumptionState pcs) {
    report(pcs, PROGRESS, () -> dispatcher.reportProgress(pcs));
  }

  public void reportTopicSwitchReceived(PartitionConsumptionState pcs) {
    report(pcs, TOPIC_SWITCH_RECEIVED, () -> dispatcher.reportTopicSwitchReceived(pcs));
  }

  public void reportCatchUpVersionTopicOffsetLag(PartitionConsumptionState pcs) {
    report(pcs, CATCH_UP_BASE_TOPIC_OFFSET_LAG, () -> dispatcher.reportCatchUpVersionTopicOffsetLag(pcs));
  }

  public void reportCompleted(PartitionConsumptionState pcs) {
    reportCompleted(pcs, false);
  }

  public void reportCompleted(PartitionConsumptionState pcs, boolean forceCompletion) {
    report(pcs, COMPLETED, () -> dispatcher.reportCompleted(pcs, forceCompletion));
  }

  public void reportStartOfIncrementalPushReceived(PartitionConsumptionState pcs, String version) {
    report(
        pcs,
        START_OF_INCREMENTAL_PUSH_RECEIVED,
        Optional.of(version),
        () -> dispatcher.reportStartOfIncrementalPushReceived(pcs, version));
  }

  public void reportEndOfIncrementalPushReceived(PartitionConsumptionState pcs, String version) {
    report(
        pcs,
        END_OF_INCREMENTAL_PUSH_RECEIVED,
        Optional.of(version),
        () -> dispatcher.reportEndOfIncrementalPushReceived(pcs, version));
  }

  private void report(PartitionConsumptionState pcs, SubPartitionStatus subPartitionStatus, Runnable report) {
    report(pcs, subPartitionStatus, Optional.empty(), report);
  }

  private void report(
      PartitionConsumptionState pcs,
      SubPartitionStatus status,
      Optional<String> version,
      Runnable report) {
    // The version aware status name makes multiple incremental pushes work properly.
    String versionAwareStatus = status.name() + (version.map(s -> "-" + s).orElse(""));
    if (!status.equals(PROGRESS)) {
      LOGGER.info("Status: {} reported from partition: {}", versionAwareStatus, pcs.getPartition());
    }
    report.run();
    if (status.equals(COMPLETED)) {
      pcs.completionReported();
    }
  }
}
