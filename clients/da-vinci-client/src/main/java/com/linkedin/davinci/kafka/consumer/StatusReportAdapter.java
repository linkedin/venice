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
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class forwards status report requests to {@link IngestionNotificationDispatcher} at USER partition level.
 * It will record all sub-partitions status reporting and report only once for each user-partition when all the
 * sub-partitions have reported the status.
 */
public class StatusReportAdapter {
  private static final Logger LOGGER = LogManager.getLogger(StatusReportAdapter.class);
  private final AmplificationFactorAdapter amplificationFactorAdapter;
  private final IngestionNotificationDispatcher dispatcher;
  private final Map<Integer, PartitionReportStatus> partitionReportStatus = new VeniceConcurrentHashMap<>();

  public StatusReportAdapter(
      IngestionNotificationDispatcher notificationDispatcher,
      AmplificationFactorAdapter amplificationFactorAdapter) {
    this.dispatcher = notificationDispatcher;
    this.amplificationFactorAdapter = amplificationFactorAdapter;
  }

  /**
   * This method is expected to be invoked when {@link StoreIngestionTask} is subscribing to a user partition.
   * Here it performs initialization by creating a new {@link PartitionReportStatus} so old status will not be reused.
   */
  public void initializePartitionReportStatus(int userPartition) {
    partitionReportStatus.put(userPartition, new PartitionReportStatus(userPartition));
  }

  // This method is called when PartitionConsumptionState are not initialized
  public void reportError(int errorPartitionId, String message, Exception consumerEx) {
    dispatcher.reportError(errorPartitionId, message, consumerEx);
  }

  public void reportError(Collection<PartitionConsumptionState> pcsList, String message, Exception consumerEx) {
    dispatcher.reportError(amplificationFactorAdapter.getLeaderPcsList(pcsList), message, consumerEx);
  }

  public void reportKilled(Collection<PartitionConsumptionState> pcsList, VeniceIngestionTaskKilledException ke) {
    dispatcher.reportKilled(amplificationFactorAdapter.getLeaderPcsList(pcsList), ke);
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
    int userPartition = pcs.getUserPartition();
    partitionReportStatus.get(userPartition).recordSubPartitionStatus(pcs, status, version, report);
  }

  /**
   * This class contains all subPartition status for a specific user partition. It will record each partition's
   * status in subPartition level and report once a status is ready to report for this partition.
   */
  class PartitionReportStatus {
    // This data structure indicates how many subPartitions have recorded this ingestion status.
    private final Map<String, AtomicInteger> statusRecordCounterMap = new VeniceConcurrentHashMap<>();
    // This data structure stores the details of the status recording for each subPartitions.
    private final Map<String, List<AtomicBoolean>> statusRecordDetailsMap = new VeniceConcurrentHashMap<>();
    // This data structure indicates whether a specific status for this user partition has been reported.
    private final Map<String, AtomicBoolean> statusReportIndicatorMap = new VeniceConcurrentHashMap<>();
    private final int userPartition;

    public PartitionReportStatus(int userPartition) {
      this.userPartition = userPartition;
    }

    /**
     * Record status for a subPartition. Once all subPartitions have recorded this status, it will execute the provided
     * report logic to deliver the status at user partition level.
     */
    public void recordSubPartitionStatus(
        PartitionConsumptionState partitionConsumptionState,
        SubPartitionStatus status,
        Optional<String> version,
        Runnable report) {
      // Do NOT log PROGRESS status to avoid log spamming issue.
      boolean logStatus = !status.equals(SubPartitionStatus.PROGRESS);
      int amplificationFactor = amplificationFactorAdapter.getAmplificationFactor();
      int subPartitionId = partitionConsumptionState.getPartition();
      int subPartitionIndex = subPartitionId - userPartition * amplificationFactor;
      // The version aware subPartition status name makes multiple incremental pushes work properly.
      String versionAwareStatus = status.name() + (version.map(s -> "-" + s).orElse(""));
      statusRecordCounterMap.putIfAbsent(versionAwareStatus, new AtomicInteger(0));
      statusReportIndicatorMap.putIfAbsent(versionAwareStatus, new AtomicBoolean(false));
      statusRecordDetailsMap.computeIfAbsent(versionAwareStatus, v -> {
        List<AtomicBoolean> list = new ArrayList<>();
        for (int i = 0; i < amplificationFactor; i++) {
          list.add(new AtomicBoolean(false));
        }
        return list;
      });

      AtomicInteger counter = statusRecordCounterMap.get(versionAwareStatus);
      int updatedCount;
      /*
       * For inc push to RT policy, the control msg is only consumed by leader subPartition. We need to record the
       * detailed subPartition status for every subPartition of the same user partition so that the below report logic
       * can be triggered.
       */
      if (status.equals(START_OF_INCREMENTAL_PUSH_RECEIVED) || status.equals(END_OF_INCREMENTAL_PUSH_RECEIVED)) {
        amplificationFactorAdapter
            .executePartitionConsumptionState(userPartition, pcs -> pcs.recordSubPartitionStatus(versionAwareStatus));
        updatedCount = counter.addAndGet(amplificationFactor);
      } else {
        /**
         * Both the drainer thread and SIT thread can hit this path via ready-to-serve check. Adding this additional
         * safeguard to avoid race condition.
         */
        boolean updateResult =
            statusRecordDetailsMap.get(versionAwareStatus).get(subPartitionIndex).compareAndSet(false, true);
        if (updateResult) {
          if (logStatus) {
            LOGGER.info(
                "{} reported from subPartition: {}, status report details: {}.",
                versionAwareStatus,
                subPartitionId,
                statusRecordDetailsMap.get(versionAwareStatus));
          }
          partitionConsumptionState.recordSubPartitionStatus(versionAwareStatus);
          updatedCount = counter.incrementAndGet();
        } else {
          updatedCount = counter.get();
        }
      }
      if (updatedCount == amplificationFactor) {
        maybeReportStatus(partitionConsumptionState.getReplicaId(), status, versionAwareStatus, report, logStatus);
      }
    }

    private void maybeReportStatus(
        String replicaId,
        SubPartitionStatus status,
        String versionAwareStatus,
        Runnable report,
        boolean logStatus) {
      // This is a safeguard to make sure we only report exactly once for each status.
      if (statusReportIndicatorMap.get(versionAwareStatus).compareAndSet(false, true)) {
        if (logStatus) {
          LOGGER.info(
              "Reporting status {} for user partition: {}. ReplicaId: {}",
              versionAwareStatus,
              userPartition,
              replicaId);
        }
        report.run();
        if (status.equals(COMPLETED)) {
          amplificationFactorAdapter
              .executePartitionConsumptionState(userPartition, PartitionConsumptionState::completionReported);
        }
      }
    }
  }
}
