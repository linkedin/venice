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
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * ReportStatusAdapter forwards status report requests to notificationDispatcher at USER-partition level. It will record
 * all sub-partitions status reporting and report only once for user-partition when all the preconditions are met.
 */
public class ReportStatusAdapter {
  private static final Logger logger = LogManager.getLogger(ReportStatusAdapter.class);
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
   * A user partition to a map from status to integer. The internal map indicates how many subPartitions has recorded
   * this status. This map is used to make sure each subPartition has recorded the status before reporting.
   */
  private final Map<Integer, Map<String, AtomicInteger>> statusRecordCounterMap = new VeniceConcurrentHashMap<>();
  /*
   * A user partition to a map from status to boolean. The internal map indicates whether all subPartitions have recorded
   * this status. This map is used to make sure each subPartition has recorded the status before reporting.
   */
  private final Map<Integer, Map<String, List<AtomicBoolean>>> subPartitionStatusRecordMap = new VeniceConcurrentHashMap<>();
  /*
   * A user partition to a map from status to boolean. The internal map indicates whether a certain ingestion status
   * has been reported or not for the given user partition. This map is used to make sure each user partition will
   * only report once for each status.
   */
  private final Map<Integer, Map<String, AtomicBoolean>> statusReportMap = new VeniceConcurrentHashMap<>();

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

  // This method is called when PartitionConsumptionState are not initialized
  public void reportError(int errorPartitionId, String message, Exception consumerEx) {
    notificationDispatcher.reportError(errorPartitionId, message, consumerEx);
  }

  public void reportError(Collection<PartitionConsumptionState> pcsList, String message, Exception consumerEx) {
    notificationDispatcher.reportError(getLeaderPcsList(pcsList), message, consumerEx);
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
    report(partitionConsumptionState, SubPartitionStatus.DATA_RECOVERY_COMPLETED,
        () -> notificationDispatcher.reportDataRecoveryCompleted(partitionConsumptionState));
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


  public void reportProgress(PartitionConsumptionState partitionConsumptionState) {
    report(partitionConsumptionState, SubPartitionStatus.PROGRESS,
        () -> notificationDispatcher.reportProgress(partitionConsumptionState));
  }

  public void reportTopicSwitchReceived(PartitionConsumptionState partitionConsumptionState) {
    report(partitionConsumptionState, SubPartitionStatus.TOPIC_SWITCH_RECEIVED,
        () -> notificationDispatcher.reportTopicSwitchReceived(partitionConsumptionState));
  }

  public void reportCatchUpVersionTopicOffsetLag(PartitionConsumptionState partitionConsumptionState) {
    report(partitionConsumptionState, SubPartitionStatus.CATCH_UP_BASE_TOPIC_OFFSET_LAG,
        () -> notificationDispatcher.reportCatchUpVersionTopicOffsetLag(partitionConsumptionState));
  }

  public void reportCompleted(PartitionConsumptionState partitionConsumptionState) {
    reportCompleted(partitionConsumptionState, false);
  }

  public void reportCompleted(PartitionConsumptionState partitionConsumptionState, boolean forceCompletion) {
    report(partitionConsumptionState, SubPartitionStatus.COMPLETED,
        () -> notificationDispatcher.reportCompleted(partitionConsumptionState, forceCompletion));
  }

  public void reportStartOfIncrementalPushReceived(PartitionConsumptionState partitionConsumptionState, String version) {
    report(partitionConsumptionState, SubPartitionStatus.START_OF_INCREMENTAL_PUSH_RECEIVED, Optional.of(version),
        () -> notificationDispatcher.reportStartOfIncrementalPushReceived(partitionConsumptionState, version));
  }

  public void reportEndOfIncrementalPushReceived(PartitionConsumptionState partitionConsumptionState, String version) {
    report(partitionConsumptionState, SubPartitionStatus.END_OF_INCREMENTAL_PUSH_RECEIVED, Optional.of(version),
        () -> notificationDispatcher.reportEndOfIncrementalPushReceived(partitionConsumptionState, version));
  }

  private void report(PartitionConsumptionState partitionConsumptionState, SubPartitionStatus subPartitionStatus, Runnable report) {
    report(partitionConsumptionState, subPartitionStatus, Optional.empty(), report);
  }

  private void report(
      PartitionConsumptionState partitionConsumptionState,
      SubPartitionStatus subPartitionStatus,
      Optional<String> version,
      Runnable report) {
    /*
     * Below we compose the version aware subPartition status name, so amplification factor can work properly with
     * multiple incremental pushes.
     */
    String versionAwareSubPartitionStatus = subPartitionStatus.name() + (version.map(s -> "-" + s).orElse(""));
    int userPartition = partitionConsumptionState.getUserPartition();
    // Initialize recording data structures for specific status.
    statusRecordCounterMap.get(userPartition).putIfAbsent(versionAwareSubPartitionStatus, new AtomicInteger(0));
    subPartitionStatusRecordMap.get(userPartition).putIfAbsent(versionAwareSubPartitionStatus, getAtomicBooleanList());
    statusReportMap.get(userPartition).putIfAbsent(versionAwareSubPartitionStatus, new AtomicBoolean(false));

    AtomicInteger statusRecordCounter = statusRecordCounterMap.get(userPartition).get(versionAwareSubPartitionStatus);
    int statusRecordCount;
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
      statusRecordCount = statusRecordCounter.addAndGet(amplificationFactor);
    } else {
      int subPartitionIndexInUserPartition = partitionConsumptionState.getPartition() - partitionConsumptionState.getUserPartition() * amplificationFactor;
      AtomicBoolean subPartitionStatusRecordFlag = subPartitionStatusRecordMap.get(userPartition).get(versionAwareSubPartitionStatus).get(subPartitionIndexInUserPartition);
      if (!subPartitionStatusRecordFlag.get()) {
        partitionConsumptionState.recordSubPartitionStatus(versionAwareSubPartitionStatus);
        subPartitionStatusRecordFlag.set(true);
        statusRecordCount = statusRecordCounter.incrementAndGet();
      } else {
        logger.info("Reported status " + versionAwareSubPartitionStatus + " already recorded for subPartition: " + partitionConsumptionState.getPartition());
        statusRecordCount = statusRecordCounter.get();
      }
    }
    logger.info("Received status report: " + versionAwareSubPartitionStatus + " from subPartition: "
        + partitionConsumptionState.getPartition() + ", current report count is " + statusRecordCount + "/" + amplificationFactor);
    if (statusRecordCount == amplificationFactor) {
      // This is safeguard to make sure we only report once for each partition status.
      if (statusReportMap.get(userPartition).get(versionAwareSubPartitionStatus).compareAndSet(false, true)) {
        logger.info("Reporting status " + versionAwareSubPartitionStatus + " for user partition: " + userPartition);
        report.run();
        if (subPartitionStatus.equals(SubPartitionStatus.COMPLETED)) {
          for (int subPartition : PartitionUtils.getSubPartitions(userPartition, amplificationFactor)) {
            PartitionConsumptionState subPartitionConsumptionState = partitionConsumptionStateMap.get(subPartition);
            subPartitionConsumptionState.completionReported();
          }
        }
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
      statusRecordCounterMap.put(userPartition, new VeniceConcurrentHashMap<>());
      subPartitionStatusRecordMap.put(userPartition, new VeniceConcurrentHashMap<>());
    }
  }

  /**
   * Set up cleanup flag for the first sub-partition to re-initialize report status. This prevents re-subscription of
   * the same topic partition reuses old reporting status.
   */
  public void preparePartitionStatusCleanup(int userPartition) {
    statusCleanupFlagMap.put(userPartition, new AtomicBoolean(true));
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

  private List<AtomicBoolean> getAtomicBooleanList() {
    List<AtomicBoolean> list = new ArrayList<>();
    for (int i = 0; i < amplificationFactor; i++) {
      list.add(new AtomicBoolean(false));
    }
    return list;
  }
}

