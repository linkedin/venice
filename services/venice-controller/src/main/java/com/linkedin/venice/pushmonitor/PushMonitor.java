package com.linkedin.venice.pushmonitor;

import com.linkedin.venice.helix.HelixCustomizedViewOfflinePushRepository;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.OfflinePushStrategy;
import com.linkedin.venice.meta.PartitionAssignment;
import com.linkedin.venice.meta.UncompletedPartition;
import com.linkedin.venice.pushstatushelper.PushStatusStoreReader;
import com.linkedin.venice.utils.Pair;
import java.util.List;
import java.util.Optional;
import java.util.Set;


/**
 * In Venice, Push is a task that asynchronously writes data to Venice. Currently, Venice supports 2 kinds of push schemes --
 * pushing from Hadoop(offline) and pushing from Samza(nearline).
 *
 * Push Monitor is a critical component in Venice controller that keeps track of ongoing and finished pushes
 * in the cluster.
 *
 * Inheritors are expected to be able to:
 * 1. persist recent push history even after restart. This is the first-hand information for debugging push failures
 * 2. Tell what a push's current status is.
 * 3. Switch store's versions when pushes are finished and new data is ready to serve traffic.
 */
public interface PushMonitor {
  /**
   * load previous push statuses when push monitor restarts
   */
  void loadAllPushes();

  /**
   * Stop monitoring all offline pushes.
   */
  void stopAllMonitoring();

  /**
   *
   * @param strategy on which criteria a push is considered to be completed. check {@link PushStatusDecider}
   *                 for more details
   *
   * Start monitoring a new push
   */
  void startMonitorOfflinePush(
      String kafkaTopic,
      int numberOfPartition,
      int replicaFactor,
      OfflinePushStrategy strategy);

  /**
   * Stop monitoring a push.
   *
   * This function should be called when:
   * 1. Retire a version;
   * 2. Leader controller transits to standby; in this case, controller shouldn't delete any push status.
   */
  void stopMonitorOfflinePush(String kafkaTopic, boolean deletePushStatus, boolean isForcedDelete);

  /**
   * Clean up all push statuses related to a store including all error pushes. This is called when
   * a store gets deleted.
   */
  void cleanupStoreStatus(String storeName);

  /**
   * Return a push's status (push status contains the history statuses and current status) and throw exception if it
   * doesn't exist
   */
  OfflinePushStatus getOfflinePushOrThrow(String topic);

  /**
   * @return return the current status. If it's in error, some debugging info is also presented.
   */
  Pair<ExecutionStatus, String> getPushStatusAndDetails(String topic);

  List<UncompletedPartition> getUncompletedPartitions(String topic);

  /**
   * Returns incremental push's status read from (ZooKeeper backed) OfflinePushStatus
   */
  Pair<ExecutionStatus, String> getIncrementalPushStatusAndDetails(
      String kafkaTopic,
      String incrementalPushVersion,
      HelixCustomizedViewOfflinePushRepository customizedViewOfflinePushRepository);

  /**
   * Returns incremental push's status read from push status store
   */
  Pair<ExecutionStatus, String> getIncrementalPushStatusFromPushStatusStore(
      String kafkaTopic,
      String incrementalPushVersion,
      HelixCustomizedViewOfflinePushRepository customizedViewRepo,
      PushStatusStoreReader pushStatusStoreReader);

  /**
   * Check if there are any ongoing incremental push for the given version topic.
   * @param kafkaTopic to check for ongoing incremental push
   * @return ongoing incremental push versions if there are any, otherwise an empty set is returned.
   */
  Set<String> getOngoingIncrementalPushVersions(String kafkaTopic);

  Set<String> getOngoingIncrementalPushVersions(String kafkaTopic, PushStatusStoreReader pushStatusStoreReader);

  /**
   * Find all ongoing pushes then return the topics associated to those pushes.
   */
  List<String> getTopicsOfOngoingOfflinePushes();

  /**
   * Mark a push to be as error. This is usually called when push is killed.
   */
  void markOfflinePushAsError(String topic, String statusDetails);

  /**
   * Here, we refresh the push status, in order to avoid a race condition where a small job could
   * already be completed. Previously, we would clobber the COMPLETED status with STARTED, which
   * would stall the job forever.
   *
   * Now, since we get the refreshed status, we can validate whether a transition to {@param newStatus}
   * is valid, before making the change. If if wouldn't be valid (because the job already completed
   * or already failed, for example), then we leave the status as is, rather than adding in the
   * new details.
   */
  void refreshAndUpdatePushStatus(String kafkaTopic, ExecutionStatus newStatus, Optional<String> newStatusDetails);

  /**
   * stats related operation.
   * TODO: we may want to move it out of the interface
   */
  void recordPushPreparationDuration(String topic, long offlinePushWaitTimeInSecond);

  List<Instance> getReadyToServeInstances(PartitionAssignment partitionAssignment, int partitionId);

  boolean isOfflinePushMonitorDaVinciPushStatusEnabled();
}
