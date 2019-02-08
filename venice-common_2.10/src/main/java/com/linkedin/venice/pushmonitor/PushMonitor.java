package com.linkedin.venice.pushmonitor;

import com.linkedin.venice.meta.OfflinePushStrategy;
import com.linkedin.venice.meta.PartitionAssignment;
import com.linkedin.venice.replication.TopicReplicator;
import com.linkedin.venice.utils.Pair;
import java.util.List;
import java.util.Map;
import java.util.Optional;


/**
 * Push is a task that asynchronously writes data to Venice. Currently, Venice supports 2 kinds of push schemes --
 * pushing from Hadoop(offline) and pushing from Samza(nearline). This interface defines operations that monitors
 * pushes.
 *
 * Inheritors are expected to be able to:
 * 1. persist recent push history even after restart. This is the first-hand information for debugging push failures
 * 2. Tell what a push's current status is.
 * 3. Switch store's versions when pushes are finished and new data is ready to serve traffic.
 *
 */
public interface PushMonitor {
  /**
   * load previous push statuses when push monitor restarts
   */
  void loadAllPushes();

  /**
   *
   * @param strategy on which criteria a push is considered to be completed. check {@link PushStatusDecider}
   *                 for more details
   *
   * Start monitoring a new push
   */
  void startMonitorOfflinePush(String kafkaTopic, int numberOfPartition, int replicaFactor, OfflinePushStrategy strategy);

  /**
   * Stop monitoring a push. This is usually called when a push either completes or fails.
   */
  void stopMonitorOfflinePush(String kafkaTopic);

  /**
   * return a push's status (push status contains the history statuses and current status)
   */
  OfflinePushStatus getOfflinePush(String topic);

  /**
   *
   * @param incrementalPushVersion if this is presented, monitor will return specific status regarding this
   *                               incremental push. (The status can be only either
   *                               {@link ExecutionStatus#START_OF_INCREMENTAL_PUSH_RECEIVED} or
   *                               {@link ExecutionStatus#END_OF_INCREMENTAL_PUSH_RECEIVED})
   * @return return the current status. If it's in error, some debugging info is also presented.
   */
  Pair<ExecutionStatus, Optional<String>> getPushStatusAndDetails(String topic, Optional<String> incrementalPushVersion);

  /**
   * Find all ongoing pushes then return the topics associated to those pushes.
   */
  List<String> getTopicsOfOngoingOfflinePushes();

  /**
   * Get the progress of the given offline push.
   * @return a map which's key is replica id and value is the kafka offset that replica already consumed.
   */
  Map<String, Long> getOfflinePushProgress(String topic);

  /**
   * Mark a push to be as error. This is usually called when push is killed.
   */
  void markOfflinePushAsError(String topic, String statusDetails);

  /**
   * Given a certain partition assignment, identify if a push would fail after that. This is usually called
   * when we'd like to change the number of storage nodes in the cluster
   */
  boolean wouldJobFail(String topic, PartitionAssignment partitionAssignmentAfterRemoving);

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

  /**
   * set up topicReplicator.
   * TODO: we may want to move it out of the interface
   */
  void setTopicReplicator(Optional<TopicReplicator> topicReplicator);
}
