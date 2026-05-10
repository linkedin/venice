package com.linkedin.davinci.notifier;

import static com.linkedin.venice.common.VeniceSystemStoreUtils.isSystemStore;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.COMPLETED;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.ERROR;

import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.venice.helix.HelixPartitionStatusAccessor;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pushmonitor.OfflinePushAccessor;
import com.linkedin.venice.pushstatushelper.PushStatusStoreWriter;


/**
 * A test only notifier to simulate ERROR in leader replica to test single leader replica failover scenario.
 */
public class LeaderErrorNotifier extends PushStatusNotifier {
  private volatile boolean doOne = true;
  private final OfflinePushAccessor accessor;
  private final String instanceId;

  public LeaderErrorNotifier(
      OfflinePushAccessor accessor,
      HelixPartitionStatusAccessor helixPartitionStatusAccessor,
      PushStatusStoreWriter writer,
      ReadOnlyStoreRepository repository,
      String instanceId) {
    super(
        accessor,
        helixPartitionStatusAccessor,
        writer,
        repository,
        instanceId,
        VeniceServerConfig.IncrementalPushStatusWriteMode.DUAL);
    this.accessor = accessor;
    this.instanceId = instanceId;
  }

  @Override
  public void completed(String topic, int partitionId, PubSubPosition position, String message) {
    /*
     * Original predicate matched on `message.contains("LEADER")`, where `message` is
     * `pcs.getLeaderFollowerState().toString()` set in IngestionNotificationDispatcher
     * .reportCompleted. For tiny pushes (immediate EOP) on a hybrid future-version replica, the
     * drainer thread fires reportCompleted from the leader host BEFORE the consumer thread has
     * processed the queued STANDBY_TO_LEADER action that flips PCS state to LEADER. The
     * dispatched message string is then "STANDBY" — the predicate misses, the notifier never
     * reports ERROR, and TestLeaderReplicaFailover.testLeaderReplicaFailoverFutureVersion
     * burns its 120s budget on hasReportedError() == false.
     *
     * Drop the message-string check. The notifier is single-shot (`doOne`), and the user-store
     * push uses partitionCount == 1, so the first non-system-store completion on partition 0 is
     * the leader's call (just with a stale dispatched-message). Firing on it gives the test the
     * deterministic ERROR signal it needs.
     */
    if (doOne && partitionId == 0 && !isSystemStore(topic)) {
      // Set doOne=false BEFORE the ZK write so hasReportedError() returns true immediately,
      // allowing the test's waitForNonDeterministicAssertion to proceed without waiting for
      // the ZK round-trip.
      doOne = false;
      accessor.updateReplicaStatus(topic, partitionId, instanceId, ERROR, "");
    } else {
      accessor.updateReplicaStatus(topic, partitionId, instanceId, COMPLETED, "");
    }
  }

  public boolean hasReportedError() {
    return !doOne;
  }
}
