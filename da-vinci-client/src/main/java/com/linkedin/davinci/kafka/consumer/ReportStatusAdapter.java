package com.linkedin.davinci.kafka.consumer;

import com.linkedin.venice.exceptions.VeniceIngestionTaskKilledException;
import com.linkedin.venice.meta.IncrementalPushPolicy;
import com.linkedin.venice.pushmonitor.SubPartitionStatus;
import com.linkedin.venice.utils.PartitionUtils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * ReportStatusAdapter forwards status report requests to notificationDispatcher at USER-partition level. It will record
 * all sub-partitions status reporting and report only once for user-partition when all the preconditions are met.
 */
class ReportStatusAdapter {
  private static final Logger logger = LogManager.getLogger();

  private final int amplificationFactor;
  private final IncrementalPushPolicy incrementalPushPolicy;
  private final ConcurrentMap<Integer, PartitionConsumptionState> partitionConsumptionStateMap;
  private final IngestionNotificationDispatcher notificationDispatcher;
  /*
   * A user partition to status cleanup flag map, indicating whether we need to clean up partition report status when
   * a sub-partition of the specified user partition is subscribed.
   */
  private final Map<Integer, AtomicBoolean> statusCleanupFlagMap = new VeniceConcurrentHashMap<>();
  /*
   * A user partition to a map from status to boolean. The internal map inidicates whether a certain ingestion status
   * has been reported or not for the given user partition. This map is used to make sure each user partition will
   * only report once for each status.
   */
  private final Map<Integer, Map<String, AtomicBoolean>> statusReportMap = new VeniceConcurrentHashMap<>();
  /*
   * A user partition to completable future map. COMPLETED reporting will be blocked here if corresponding future
   * exists and is not done.
   */
  private final Map<Integer, CompletableFuture<Void>> completionReportLatchMap = new VeniceConcurrentHashMap<>();

  public ReportStatusAdapter(
      IngestionNotificationDispatcher notificationDispatcher,
      int amplificationFactor,
      IncrementalPushPolicy incrementalPushPolicy,
      ConcurrentMap<Integer, PartitionConsumptionState> partitionConsumptionStateMap
  ) {
    this.notificationDispatcher = notificationDispatcher;
    this.amplificationFactor = amplificationFactor;
    this.partitionConsumptionStateMap = partitionConsumptionStateMap;
    this.incrementalPushPolicy = incrementalPushPolicy;
  }

  public void reportStarted(PartitionConsumptionState partitionConsumptionState) {
    report(partitionConsumptionState, SubPartitionStatus.STARTED,
        () -> notificationDispatcher.reportStarted(partitionConsumptionState));
  }

  public void reportRestarted(PartitionConsumptionState partitionConsumptionState) {
    report(partitionConsumptionState, SubPartitionStatus.RESTARTED,
        () -> notificationDispatcher.reportRestarted(partitionConsumptionState));
  }

  public void reportEndOfPushReceived(PartitionConsumptionState partitionConsumptionState) {
    report(partitionConsumptionState, SubPartitionStatus.END_OF_PUSH_RECEIVED,
        () -> notificationDispatcher.reportEndOfPushReceived(partitionConsumptionState));
  }

  public void reportStartOfBufferReplayReceived(PartitionConsumptionState partitionConsumptionState) {
    report(partitionConsumptionState, SubPartitionStatus.START_OF_BUFFER_REPLAY_RECEIVED,
        () -> notificationDispatcher.reportStartOfBufferReplayReceived(partitionConsumptionState));
  }

  public void reportStartOfIncrementalPushReceived(PartitionConsumptionState partitionConsumptionState, String version) {
    report(partitionConsumptionState, SubPartitionStatus.START_OF_INCREMENTAL_PUSH_RECEIVED, Optional.of(version),
        () -> notificationDispatcher.reportStartOfIncrementalPushReceived(partitionConsumptionState, version));
  }

  public void reportEndOfIncrementalPushReceived(PartitionConsumptionState partitionConsumptionState, String version) {
    report(partitionConsumptionState, SubPartitionStatus.END_OF_INCREMENTAL_PUSH_RECEIVED, Optional.of(version),
        () -> notificationDispatcher.reportEndOfIncrementalPushReceived(partitionConsumptionState, version));
  }

  public void reportProgress(PartitionConsumptionState partitionConsumptionState) {
    report(partitionConsumptionState, SubPartitionStatus.PROGRESS,
        () -> notificationDispatcher.reportProgress(partitionConsumptionState));
  }

  public void reportTopicSwitchReceived(PartitionConsumptionState partitionConsumptionState) {
    report(partitionConsumptionState, SubPartitionStatus.TOPIC_SWITCH_RECEIVED,
        () -> notificationDispatcher.reportTopicSwitchReceived(partitionConsumptionState));
  }

  public void reportCatchUpBaseTopicOffsetLag(PartitionConsumptionState partitionConsumptionState) {
    report(partitionConsumptionState, SubPartitionStatus.CATCH_UP_BASE_TOPIC_OFFSET_LAG,
        () -> notificationDispatcher.reportCatchUpBaseTopicOffsetLag(partitionConsumptionState));
  }

  public void reportCompleted(PartitionConsumptionState partitionConsumptionState) {
    reportCompleted(partitionConsumptionState, false);
  }

  public void reportCompleted(PartitionConsumptionState partitionConsumptionState, boolean forceCompletion) {
    if (amplificationFactor == 1) {
      partitionConsumptionState.lagHasCaughtUp();
      notificationDispatcher.reportCompleted(partitionConsumptionState, forceCompletion);
      return;
    }
    int userPartition = partitionConsumptionState.getUserPartition();
    // Initialize status reporting map.
    statusReportMap.get(userPartition).putIfAbsent(SubPartitionStatus.COMPLETED.toString(), new AtomicBoolean(false));
    for (int subPartitionId : PartitionUtils.getSubPartitions(userPartition, amplificationFactor)) {
      if (!partitionConsumptionStateMap.containsKey(subPartitionId)
          /*
           * In processEndOfPush, endOfPushReceived is marked as true first then END_OF_PUSH_RECEIVED get reported.
           * add a check in completion report to prevent a race condition that END_OF_PUSH_RECEIVED comes after completion reported.
           */
          || !partitionConsumptionStateMap.get(subPartitionId).hasSubPartitionStatus(SubPartitionStatus.END_OF_PUSH_RECEIVED.toString())
          || !partitionConsumptionStateMap.get(subPartitionId).isComplete()) {
        return;
      }
    }
    /*
     * We need to make sure END_OF_PUSH is reported first before COMPLETED can be reported, otherwise replica status
     * could go back from COMPLETED to END_OF_PUSH_RECEIVED and version push would fail.
     */
    CompletableFuture<Void> latchFuture = completionReportLatchMap.get(userPartition);
    if (latchFuture != null) {
      try {
        latchFuture.get();
      } catch (InterruptedException | ExecutionException e) {
        logger.info("Caught exception when waiting completion report condition", e);
      }
    }
    // Make sure we only report once for each user partition.
    if (statusReportMap.get(userPartition).get(SubPartitionStatus.COMPLETED.toString()).compareAndSet(false, true)) {
      notificationDispatcher.reportCompleted(partitionConsumptionState, forceCompletion);
      for (int subPartitionId : PartitionUtils.getSubPartitions(userPartition, amplificationFactor)) {
        PartitionConsumptionState subPartitionConsumptionState = partitionConsumptionStateMap.get(subPartitionId);
        if (subPartitionConsumptionState != null) {
          subPartitionConsumptionState.lagHasCaughtUp();
          subPartitionConsumptionState.completionReported();
        }
      }
    }
  }

  // This method is called when PartitionConsumptionState are not initialized
  public void reportError(int errorPartitionId, String message, Exception consumerEx) {
    notificationDispatcher.reportError(errorPartitionId, message, consumerEx);
  }

  public void reportError(Collection<PartitionConsumptionState> pcsList, String message, Exception consumerEx) {
    notificationDispatcher.reportError(getLeaderPcsList(pcsList), message, consumerEx);
  }

  private List<PartitionConsumptionState> getLeaderPcsList(Collection<PartitionConsumptionState> pcsList) {
    List<PartitionConsumptionState> leaderPcsList = new ArrayList<>();
    for (PartitionConsumptionState pcs : pcsList) {
      if (pcs.getPartition() == PartitionUtils.getLeaderSubPartition(pcs.getUserPartition(), pcs.getAmplificationFactor())) {
        leaderPcsList.add(pcs);
      }
    }
    return leaderPcsList;
  }

  public void reportQuotaViolated(PartitionConsumptionState partitionConsumptionState) {
    notificationDispatcher.reportQuotaViolated(partitionConsumptionState);
  }

  public void reportQuotaNotViolated(PartitionConsumptionState partitionConsumptionState) {
    notificationDispatcher.reportQuotaNotViolated(partitionConsumptionState);
  }

  public void reportStopped(PartitionConsumptionState partitionConsumptionState) {
    notificationDispatcher.reportStopped(partitionConsumptionState);
  }

  public void reportKilled(Collection<PartitionConsumptionState> pcsList, VeniceIngestionTaskKilledException ke) {
    notificationDispatcher.reportKilled(getLeaderPcsList(pcsList), ke);
  }

  public void reportDataRecoveryCompleted(PartitionConsumptionState partitionConsumptionState) {
    notificationDispatcher.reportDataRecoveryCompleted(partitionConsumptionState);
  }

  private void report(PartitionConsumptionState partitionConsumptionState, SubPartitionStatus subPartitionStatus, Runnable report) {
    report(partitionConsumptionState, subPartitionStatus, Optional.empty(), report);
  }

  /**
   * report status to all subPartitions of the given partitionConsumptionState's userPartition.
   */
  private void report(
      PartitionConsumptionState partitionConsumptionState,
      SubPartitionStatus subPartitionStatus,
      Optional<String> version,
      Runnable report) {
    if (amplificationFactor == 1) {
      report.run();
      return;
    }
    /*
     * Below we compose the version aware subPartition status name, so amplification factor can work properly with
     * multiple incremental pushes.
     */
    String versionAwareSubPartitionStatus = subPartitionStatus.name() + (version.map(s -> "-" + s).orElse(""));
    int userPartition = partitionConsumptionState.getUserPartition();
    statusReportMap.get(userPartition).putIfAbsent(versionAwareSubPartitionStatus, new AtomicBoolean(false));
    /*
     * If we are reporting EOP, it means it is a fresh ingestion and COMPLETED reporting will have to wait until
     * EOP has been reported.
     */
    if (subPartitionStatus.equals(SubPartitionStatus.END_OF_PUSH_RECEIVED)) {
      completionReportLatchMap.putIfAbsent(userPartition, new CompletableFuture<>());
    }

    if ((subPartitionStatus.equals(SubPartitionStatus.START_OF_INCREMENTAL_PUSH_RECEIVED)
        || (subPartitionStatus.equals(SubPartitionStatus.END_OF_INCREMENTAL_PUSH_RECEIVED))) &&
        incrementalPushPolicy.equals(IncrementalPushPolicy.INCREMENTAL_PUSH_SAME_AS_REAL_TIME)) {
      /*
       * For inc push to RT policy, the control msg is only consumed by leader subPartition. We need to record the
       * detailed subPartition status for every subPartition of the same user partition so that the below report logic
       * can be triggered.
       */
      for (int subPartition : PartitionUtils.getSubPartitions(userPartition, amplificationFactor)) {
        PartitionConsumptionState subPartitionConsumptionState = partitionConsumptionStateMap.get(subPartition);
        subPartitionConsumptionState.recordSubPartitionStatus(versionAwareSubPartitionStatus);
      }
    } else {
      partitionConsumptionState.recordSubPartitionStatus(versionAwareSubPartitionStatus);
    }

    // Check if all sub-partitions have received the same
    for (int subPartition : PartitionUtils.getSubPartitions(userPartition, amplificationFactor)) {
      PartitionConsumptionState consumptionState = partitionConsumptionStateMap.get(subPartition);
      if (consumptionState == null || !consumptionState.hasSubPartitionStatus(versionAwareSubPartitionStatus)) {
        return;
      }
    }
    // Make sure we only report once for each partition status.
    if (statusReportMap.get(userPartition).get(versionAwareSubPartitionStatus).compareAndSet(false, true)) {
      report.run();
      if (subPartitionStatus.equals(SubPartitionStatus.END_OF_PUSH_RECEIVED)) {
        completionReportLatchMap.get(userPartition).complete(null);
      }
    }
  }

  /**
   * This method contains the actual cleanup logic. It relies on AtomicBoolean to make sure only the first subPartition
   * gets the chance to re-initialize user-level partition status.
   */
  public void initializePartitionStatus(int subPartition) {
    int userPartition = PartitionUtils.getUserPartition(subPartition, amplificationFactor);
    // Make sure we only initialize once for each user partition.
    if (statusCleanupFlagMap.get(userPartition).compareAndSet(true, false)) {
      statusReportMap.put(userPartition, new VeniceConcurrentHashMap<>());
      /*
       * By default, we are allowed to report COMPLETED, but if we found that EOP is being reported, we need to block
       * completion reporting until EOP is fully reported.
       */
      completionReportLatchMap.remove(userPartition);
    }
  }

  /**
   * Set up clean up flag for the first sub-partition to re-initialize report status. This prevents re-subscription of
   * the same topic partition reuses old reporting status.
   */
  public void preparePartitionStatusCleanup(int userPartition) {
    statusCleanupFlagMap.put(userPartition, new AtomicBoolean(true));
  }
}

